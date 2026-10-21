package otelpgx

import (
	"context"
	"errors"
	"fmt"
	"net"
	"slices"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/attribute"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace/noop"
)

func TestTracer_sqlOperationName(t *testing.T) {
	tests := []struct {
		name    string
		tracer  *Tracer
		query   string
		expName string
	}{
		{
			name:    "Spaces only",
			query:   "SELECT * FROM users",
			tracer:  NewTracer(),
			expName: "SELECT",
		},
		{
			name:    "Newline and tab",
			query:   "UPDATE\n\tfoo",
			tracer:  NewTracer(),
			expName: "UPDATE",
		},
		{
			name:    "Additional whitespace",
			query:   " \n SELECT\n\t   *   FROM users  ",
			tracer:  NewTracer(),
			expName: "SELECT",
		},
		{
			name:    "Single word statement",
			query:   "BEGIN",
			tracer:  NewTracer(),
			expName: "BEGIN",
		},
		{
			name:    "Whitespace-only query",
			query:   " \n\t",
			tracer:  NewTracer(),
			expName: sqlOperationUnknown,
		},
		{
			name:    "Empty query",
			query:   "",
			tracer:  NewTracer(),
			expName: sqlOperationUnknown,
		},
		{
			name:    "Leading line comment (sqlc style)",
			query:   "-- name: GetUsers :many\nSELECT * FROM users",
			tracer:  NewTracer(),
			expName: "SELECT",
		},
		{
			name:    "Leading block comment",
			query:   "/* name: GetBooks :many */\nSELECT * FROM books",
			tracer:  NewTracer(),
			expName: "SELECT",
		},
		{
			name:    "Multiple leading comments",
			query:   "-- first\n  /* second\n spans lines */\n-- third\n\tDELETE FROM users",
			tracer:  NewTracer(),
			expName: "DELETE",
		},
		{
			name:    "Comment-only query",
			query:   "-- nothing here",
			tracer:  NewTracer(),
			expName: sqlOperationUnknown,
		},
		{
			name:    "Unterminated block comment",
			query:   "/* never closed SELECT 1",
			tracer:  NewTracer(),
			expName: sqlOperationUnknown,
		},
		{
			name:    "Trailing comment is kept",
			query:   "SELECT 1 -- trailing",
			tracer:  NewTracer(),
			expName: "SELECT",
		},
		{
			name:    "Functional span name (-- comment style)",
			query:   "-- name: GetUsers :many\nSELECT * FROM users",
			tracer:  NewTracer(WithSpanNameFunc(testSpanNameFunc)),
			expName: "GetUsers :many",
		},
		{
			name:    "Functional span name (/**/ comment style)",
			query:   "/* name: GetBooks :many */\nSELECT * FROM books",
			tracer:  NewTracer(WithSpanNameFunc(testSpanNameFunc)),
			expName: "GetBooks :many",
		},
		{
			name:    "Functional span name (# comment style)",
			query:   "# name: GetRecords :many\nSELECT * FROM records",
			tracer:  NewTracer(WithSpanNameFunc(testSpanNameFunc)),
			expName: "GetRecords :many",
		},
		{
			name:    "Functional span name (no annotation)",
			query:   "--\nSELECT * FROM user",
			tracer:  NewTracer(WithSpanNameFunc(testSpanNameFunc)),
			expName: sqlOperationUnknown,
		},
		{
			name:    "Custom SQL name query (normal comment)",
			query:   "-- foo \nSELECT * FROM users",
			tracer:  NewTracer(WithSpanNameFunc(testSpanNameFunc)),
			expName: sqlOperationUnknown,
		},
		{
			name:    "Custom SQL name query (invalid formatting)",
			query:   "foo \nSELECT * FROM users",
			tracer:  NewTracer(WithSpanNameFunc(testSpanNameFunc)),
			expName: sqlOperationUnknown,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.expName, tt.tracer.spanNameCtxFunc(context.TODO(), tt.query))
		})
	}
}

// testSpanNameFunc is an utility function for testing that attempts to get
// the first name of the query from a given SQL statement.
var testSpanNameFunc SpanNameFunc = func(query string) string {
	for _, line := range strings.Split(query, "\n") {
		var prefix string
		switch {
		case strings.HasPrefix(line, "--"):
			prefix = "--"
		case strings.HasPrefix(line, "/*"):
			prefix = "/*"
		case strings.HasPrefix(line, "#"):
			prefix = "#"
		default:
			continue
		}

		rest := line[len(prefix):]
		if !strings.HasPrefix(strings.TrimSpace(rest), "name") {
			continue
		}
		if !strings.Contains(rest, ":") {
			continue
		}
		if !strings.HasPrefix(rest, " name: ") {
			return sqlOperationUnknown
		}

		part := strings.Split(strings.TrimSpace(line), " ")
		if prefix == "/*" {
			part = part[:len(part)-1] // removes the trailing "*/" element
		}
		if len(part) == 2 {
			return sqlOperationUnknown
		}

		queryName := part[2]
		queryType := strings.TrimSpace(part[3])

		return queryName + " " + queryType
	}
	return sqlOperationUnknown
}

type spanNameCtxKey struct{}

func TestTracer_sqlOperationNameFromCtx(t *testing.T) {
	spanNameCtxFunc := func(ctx context.Context, query string) string {
		if v := ctx.Value(spanNameCtxKey{}); v != nil {
			if name, ok := v.(string); ok && name != "" {
				return name
			}
		}
		return "UNKNOWN"
	}
	tracer := NewTracer(WithSpanNameCtxFunc(spanNameCtxFunc))

	tests := []struct {
		desc string
		ctx  context.Context
		exp  string
	}{
		{
			desc: "With span name in context",
			ctx:  context.WithValue(context.TODO(), spanNameCtxKey{}, "MyCustomSpanName"),
			exp:  "MyCustomSpanName",
		},
		{
			desc: "Without span name in context",
			ctx:  context.TODO(),
			exp:  "UNKNOWN",
		},
	}
	for _, tt := range tests {
		t.Run(tt.desc, func(t *testing.T) {
			assert.Equal(t, tt.exp, tracer.spanNameCtxFunc(tt.ctx, "SELECT * FROM users"))
		})
	}
}

// newMockConn creates a *pgx.Conn with the given connection details, backed
// by a fake PostgreSQL server over net.Pipe — no real database needed.
func newMockConn(t *testing.T, host string, port uint16, user, database string) *pgx.Conn {
	t.Helper()

	client, server := net.Pipe()
	errCh := make(chan error, 1)

	go func() {
		var gErr error
		defer func() {
			if err := server.Close(); err != nil && gErr == nil {
				gErr = fmt.Errorf("server close: %w", err)
			}
			errCh <- gErr
		}()

		b := pgproto3.NewBackend(server, server)
		if _, err := b.ReceiveStartupMessage(); err != nil {
			gErr = fmt.Errorf("receive startup: %w", err)
			return
		}

		for _, msg := range []pgproto3.BackendMessage{
			&pgproto3.AuthenticationOk{},
			&pgproto3.BackendKeyData{SecretKey: make([]byte, 4)},
			&pgproto3.ReadyForQuery{TxStatus: 'I'},
		} {
			b.Send(msg)
		}
		if err := b.Flush(); err != nil {
			gErr = fmt.Errorf("flush: %w", err)
			return
		}

		// Drain until the client disconnects or sends Terminate.
		for {
			msg, err := b.Receive()
			if err != nil {
				break
			}
			if _, ok := msg.(*pgproto3.Terminate); ok {
				break
			}
		}
	}()

	dsn := fmt.Sprintf("postgres://%s@%s:%d/%s?sslmode=disable", user, host, int(port), database)
	config, err := pgx.ParseConfig(dsn)
	require.NoError(t, err)

	config.LookupFunc = func(ctx context.Context, host string) ([]string, error) {
		return []string{host}, nil
	}
	config.DialFunc = func(ctx context.Context, _, _ string) (net.Conn, error) {
		return client, nil
	}

	conn, err := pgx.ConnectConfig(context.Background(), config)
	require.NoError(t, err)

	t.Cleanup(func() {
		if err := conn.Close(context.Background()); err != nil {
			t.Errorf("close conn: %v", err)
		}
		if serverErr := <-errCh; serverErr != nil {
			t.Errorf("mock server: %v", serverErr)
		}
	})

	return conn
}

// findAttr returns the value for the given key in the attribute slice, and
// whether it was found.
func findAttr(attrs []attribute.KeyValue, key string) (attribute.Value, bool) {
	for _, a := range attrs {
		if string(a.Key) == key {
			return a.Value, true
		}
	}
	return attribute.Value{}, false
}

func TestTracer_spanAttributes(t *testing.T) {
	conn := newMockConn(t, "fakehost", 5432, "fakeuser", "fakedb")

	tests := []struct {
		name         string
		opts         []Option
		drive        func(ctx context.Context, tracer *Tracer, conn *pgx.Conn)
		wantStrAttrs map[string]string
		wantIntAttrs map[string]int64
		absentAttrs  []string
	}{
		{
			name: "query default",
			drive: func(ctx context.Context, tracer *Tracer, conn *pgx.Conn) {
				ctx = tracer.TraceQueryStart(ctx, conn, pgx.TraceQueryStartData{SQL: "SELECT * FROM users"})
				tracer.TraceQueryEnd(ctx, conn, pgx.TraceQueryEndData{})
			},
			wantStrAttrs: map[string]string{
				"db.system.name":    "postgresql",
				"server.address":    "fakehost",
				"user.name":         "fakeuser",
				"db.namespace":      "fakedb",
				"db.query.text":     "SELECT * FROM users",
				"db.operation.name": "SELECT",
			},
			wantIntAttrs: map[string]int64{
				"server.port": 5432,
			},
		},
		{
			name: "query without connection details",
			opts: []Option{WithDisableConnectionDetailsInAttributes()},
			drive: func(ctx context.Context, tracer *Tracer, conn *pgx.Conn) {
				ctx = tracer.TraceQueryStart(ctx, conn, pgx.TraceQueryStartData{SQL: "SELECT * FROM users"})
				tracer.TraceQueryEnd(ctx, conn, pgx.TraceQueryEndData{})
			},
			wantStrAttrs: map[string]string{
				"db.system.name":    "postgresql",
				"db.query.text":     "SELECT * FROM users",
				"db.operation.name": "SELECT",
			},
			absentAttrs: []string{"server.address", "server.port", "user.name", "db.namespace"},
		},
		{
			name: "query without SQL statement",
			opts: []Option{WithDisableSQLStatementInAttributes()},
			drive: func(ctx context.Context, tracer *Tracer, conn *pgx.Conn) {
				ctx = tracer.TraceQueryStart(ctx, conn, pgx.TraceQueryStartData{SQL: "SELECT * FROM users"})
				tracer.TraceQueryEnd(ctx, conn, pgx.TraceQueryEndData{})
			},
			wantStrAttrs: map[string]string{
				"db.system.name": "postgresql",
				"server.address": "fakehost",
				"user.name":      "fakeuser",
				"db.namespace":   "fakedb",
			},
			wantIntAttrs: map[string]int64{
				"server.port": 5432,
			},
			absentAttrs: []string{"db.query.text", "db.operation.name"},
		},
		{
			name: "query with parameters included",
			opts: []Option{WithIncludeQueryParameters()},
			drive: func(ctx context.Context, tracer *Tracer, conn *pgx.Conn) {
				ctx = tracer.TraceQueryStart(ctx, conn, pgx.TraceQueryStartData{
					SQL:  "SELECT * FROM users WHERE id = $1",
					Args: []any{42},
				})
				tracer.TraceQueryEnd(ctx, conn, pgx.TraceQueryEndData{})
			},
			wantStrAttrs: map[string]string{
				"db.system.name":    "postgresql",
				"server.address":    "fakehost",
				"user.name":         "fakeuser",
				"db.namespace":      "fakedb",
				"db.query.text":     "SELECT * FROM users WHERE id = $1",
				"db.operation.name": "SELECT",
			},
			wantIntAttrs: map[string]int64{
				"server.port": 5432,
			},
		},
		{
			name: "query with parameters included, filter allows",
			opts: []Option{WithIncludeQueryParameters(func(sql string) bool {
				return !strings.Contains(sql, "payments")
			})},
			drive: func(ctx context.Context, tracer *Tracer, conn *pgx.Conn) {
				ctx = tracer.TraceQueryStart(ctx, conn, pgx.TraceQueryStartData{
					SQL:  "SELECT * FROM users WHERE id = $1",
					Args: []any{42},
				})
				tracer.TraceQueryEnd(ctx, conn, pgx.TraceQueryEndData{})
			},
			wantStrAttrs: map[string]string{
				"db.query.text": "SELECT * FROM users WHERE id = $1",
			},
		},
		{
			name: "query with parameters included, filter blocks",
			opts: []Option{WithIncludeQueryParameters(func(sql string) bool {
				return !strings.Contains(sql, "payments")
			})},
			drive: func(ctx context.Context, tracer *Tracer, conn *pgx.Conn) {
				ctx = tracer.TraceQueryStart(ctx, conn, pgx.TraceQueryStartData{
					SQL:  "SELECT * FROM payments WHERE id = $1",
					Args: []any{42},
				})
				tracer.TraceQueryEnd(ctx, conn, pgx.TraceQueryEndData{})
			},
			wantStrAttrs: map[string]string{
				"db.query.text": "SELECT * FROM payments WHERE id = $1",
			},
			absentAttrs: []string{"pgx.query.parameters"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			exporter := tracetest.NewInMemoryExporter()
			tp := sdktrace.NewTracerProvider(sdktrace.WithSyncer(exporter))
			t.Cleanup(func() { require.NoError(t, tp.Shutdown(context.Background())) })

			opts := append([]Option{WithTracerProvider(tp)}, tt.opts...)
			tracer := NewTracer(opts...)

			ctx, parentSpan := tp.Tracer("test").Start(context.Background(), "parent")
			tt.drive(ctx, tracer, conn)
			parentSpan.End()

			spans := exporter.GetSpans()
			require.Greater(t, len(spans), 0, "no spans recorded")

			span := spans[0]

			for key, want := range tt.wantStrAttrs {
				v, ok := findAttr(span.Attributes, key)
				require.Truef(t, ok, "missing attribute %q", key)
				require.Equalf(t, want, v.AsString(), "attr %q = %q, want %q", key, v.AsString(), want)
			}

			for key, want := range tt.wantIntAttrs {
				v, ok := findAttr(span.Attributes, key)
				require.Truef(t, ok, "missing attribute %q", key)
				require.Equalf(t, want, v.AsInt64(), "attr %q = %q, want %d", key, v.AsInt64(), want)
			}

			for _, key := range tt.absentAttrs {
				_, ok := findAttr(span.Attributes, key)
				require.Falsef(t, ok, "unexpected attribute %q present")
			}
		})
	}
}

func TestTracer_spanName(t *testing.T) {
	conn := newMockConn(t, "fakehost", 5432, "fakeuser", "fakedb")

	const sql = "SELECT * FROM users"

	query := func(ctx context.Context, tracer *Tracer, conn *pgx.Conn) {
		ctx = tracer.TraceQueryStart(ctx, conn, pgx.TraceQueryStartData{SQL: sql})
		tracer.TraceQueryEnd(ctx, conn, pgx.TraceQueryEndData{})
	}
	prepare := func(ctx context.Context, tracer *Tracer, conn *pgx.Conn) {
		ctx = tracer.TracePrepareStart(ctx, conn, pgx.TracePrepareStartData{Name: "stmt1", SQL: sql})
		tracer.TracePrepareEnd(ctx, conn, pgx.TracePrepareEndData{})
	}
	batch := func(ctx context.Context, tracer *Tracer, conn *pgx.Conn) {
		tracer.TraceBatchQuery(ctx, conn, pgx.TraceBatchQueryData{SQL: sql})
	}

	tests := []struct {
		name     string
		opts     []Option
		drive    func(ctx context.Context, tracer *Tracer, conn *pgx.Conn)
		wantName string
	}{
		{
			name:     "query defaults to operation name",
			drive:    query,
			wantName: "SELECT",
		},
		{
			name:     "prepare defaults to operation name",
			drive:    prepare,
			wantName: "SELECT",
		},
		{
			name:     "batch query defaults to operation name",
			drive:    batch,
			wantName: "SELECT",
		},
		{
			name:     "query with full SQL",
			opts:     []Option{WithFullSQLInSpanName()},
			drive:    query,
			wantName: sql,
		},
		{
			name:     "query with prefix",
			opts:     []Option{WithQuerySpanNamePrefix()},
			drive:    query,
			wantName: "query SELECT",
		},
		{
			name:     "prepare with prefix",
			opts:     []Option{WithQuerySpanNamePrefix()},
			drive:    prepare,
			wantName: "prepare SELECT",
		},
		{
			name:     "batch query with prefix",
			opts:     []Option{WithQuerySpanNamePrefix()},
			drive:    batch,
			wantName: "batch query SELECT",
		},
		{
			// Full SQL plus the prefix reproduces the pre-compliance default
			// behavior exactly; this is the documented migration path.
			name:     "full SQL with prefix matches legacy default",
			opts:     []Option{WithFullSQLInSpanName(), WithQuerySpanNamePrefix()},
			drive:    query,
			wantName: "query " + sql,
		},
		{
			// A custom span name function now drives the span name by default,
			// not just the db.operation.name attribute.
			name: "custom span name func drives the name",
			opts: []Option{WithSpanNameCtxFunc(func(context.Context, string) string {
				return "custom"
			})},
			drive:    query,
			wantName: "custom",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			exporter := tracetest.NewInMemoryExporter()
			tp := sdktrace.NewTracerProvider(sdktrace.WithSyncer(exporter))
			t.Cleanup(func() { require.NoError(t, tp.Shutdown(context.Background())) })

			opts := append([]Option{WithTracerProvider(tp)}, tt.opts...)
			tracer := NewTracer(opts...)

			ctx, parentSpan := tp.Tracer("test").Start(context.Background(), "parent")
			tt.drive(ctx, tracer, conn)
			parentSpan.End()

			spans := exporter.GetSpans()
			require.Greater(t, len(spans), 0, "no spans recorded")

			assert.Equal(t, tt.wantName, spans[0].Name)
		})
	}
}

func TestTracer_metricOperationName(t *testing.T) {
	conn := newMockConn(t, "fakehost", 5432, "fakeuser", "fakedb")
	boom := errors.New("boom")

	tests := []struct {
		name       string
		tracerOpts []Option
		drive      func(ctx context.Context, tracer *Tracer, conn *pgx.Conn)
		metric     string
		// wantOperationName is "" to assert db.operation.name is absent.
		wantOperationName string
	}{
		{
			name:       "not attached by default",
			tracerOpts: nil,
			drive: func(ctx context.Context, tracer *Tracer, conn *pgx.Conn) {
				ctx = tracer.TraceQueryStart(ctx, conn, pgx.TraceQueryStartData{SQL: "SELECT * FROM users"})
				tracer.TraceQueryEnd(ctx, conn, pgx.TraceQueryEndData{})
			},
			metric:            "db.client.operation.duration",
			wantOperationName: "",
		},
		{
			name:       "select query duration carries the operation name",
			tracerOpts: []Option{WithMetricOperationName(SQLOperationName)},
			drive: func(ctx context.Context, tracer *Tracer, conn *pgx.Conn) {
				ctx = tracer.TraceQueryStart(ctx, conn, pgx.TraceQueryStartData{SQL: "SELECT * FROM users"})
				tracer.TraceQueryEnd(ctx, conn, pgx.TraceQueryEndData{})
			},
			metric:            "db.client.operation.duration",
			wantOperationName: "SELECT",
		},
		{
			name:       "insert query duration carries the operation name",
			tracerOpts: []Option{WithMetricOperationName(SQLOperationName)},
			drive: func(ctx context.Context, tracer *Tracer, conn *pgx.Conn) {
				ctx = tracer.TraceQueryStart(ctx, conn, pgx.TraceQueryStartData{SQL: "INSERT INTO users (id) VALUES (1)"})
				tracer.TraceQueryEnd(ctx, conn, pgx.TraceQueryEndData{})
			},
			metric:            "db.client.operation.duration",
			wantOperationName: "INSERT",
		},
		{
			name:       "delete query duration carries the operation name",
			tracerOpts: []Option{WithMetricOperationName(SQLOperationName)},
			drive: func(ctx context.Context, tracer *Tracer, conn *pgx.Conn) {
				ctx = tracer.TraceQueryStart(ctx, conn, pgx.TraceQueryStartData{SQL: "DELETE FROM users"})
				tracer.TraceQueryEnd(ctx, conn, pgx.TraceQueryEndData{})
			},
			metric:            "db.client.operation.duration",
			wantOperationName: "DELETE",
		},
		{
			name:       "update query errors carries the operation name",
			tracerOpts: []Option{WithMetricOperationName(SQLOperationName)},
			drive: func(ctx context.Context, tracer *Tracer, conn *pgx.Conn) {
				ctx = tracer.TraceQueryStart(ctx, conn, pgx.TraceQueryStartData{SQL: "UPDATE users SET name = $1"})
				tracer.TraceQueryEnd(ctx, conn, pgx.TraceQueryEndData{Err: boom})
			},
			metric:            "db.client.operation.errors",
			wantOperationName: "UPDATE",
		},
		{
			name:       "prepare duration carries the operation name",
			tracerOpts: []Option{WithMetricOperationName(SQLOperationName)},
			drive: func(ctx context.Context, tracer *Tracer, conn *pgx.Conn) {
				ctx = tracer.TracePrepareStart(ctx, conn, pgx.TracePrepareStartData{Name: "stmt1", SQL: "SELECT 1"})
				tracer.TracePrepareEnd(ctx, conn, pgx.TracePrepareEndData{})
			},
			metric:            "db.client.operation.duration",
			wantOperationName: "SELECT",
		},
		{
			name:       "batch query error carries its own operation name",
			tracerOpts: []Option{WithMetricOperationName(SQLOperationName)},
			drive: func(ctx context.Context, tracer *Tracer, conn *pgx.Conn) {
				tracer.TraceBatchQuery(ctx, conn, pgx.TraceBatchQueryData{
					SQL: "INSERT INTO users (id) VALUES (1)",
					Err: boom,
				})
			},
			metric:            "db.client.operation.errors",
			wantOperationName: "INSERT",
		},
		{
			name:       "batch aggregate duration has no operation name",
			tracerOpts: []Option{WithMetricOperationName(SQLOperationName)},
			drive: func(ctx context.Context, tracer *Tracer, conn *pgx.Conn) {
				ctx = tracer.TraceBatchStart(ctx, conn, pgx.TraceBatchStartData{})
				tracer.TraceBatchQuery(ctx, conn, pgx.TraceBatchQueryData{SQL: "INSERT INTO users (id) VALUES (1)"})
				tracer.TraceBatchQuery(ctx, conn, pgx.TraceBatchQueryData{SQL: "UPDATE users SET name = $1"})
				tracer.TraceBatchEnd(ctx, conn, pgx.TraceBatchEndData{})
			},
			metric:            "db.client.operation.duration",
			wantOperationName: "",
		},
		{
			name:       "custom OperationNameFunc drives the value",
			tracerOpts: []Option{WithMetricOperationName(func(context.Context, string) string { return "custom" })},
			drive: func(ctx context.Context, tracer *Tracer, conn *pgx.Conn) {
				ctx = tracer.TraceQueryStart(ctx, conn, pgx.TraceQueryStartData{SQL: "SELECT * FROM users"})
				tracer.TraceQueryEnd(ctx, conn, pgx.TraceQueryEndData{})
			},
			metric:            "db.client.operation.duration",
			wantOperationName: "custom",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			reader := sdkmetric.NewManualReader()
			provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))

			opts := append([]Option{WithMeterProvider(provider), WithTracerProvider(noop.NewTracerProvider())}, tt.tracerOpts...)
			tracer := NewTracer(opts...)

			ctx := context.Background()
			tt.drive(ctx, tracer, conn)

			var rm metricdata.ResourceMetrics
			require.NoError(t, reader.Collect(ctx, &rm))

			var dataPoints int
			var gotOperationName string
			var gotOK bool
			for _, sm := range rm.ScopeMetrics {
				for _, m := range sm.Metrics {
					if m.Name != tt.metric {
						continue
					}
					for _, attrs := range dataPointAttributes(m.Data) {
						dataPoints++
						if v, ok := attrs.Value(attribute.Key("db.operation.name")); ok {
							gotOperationName, gotOK = v.AsString(), true
						}
					}
				}
			}
			require.Greaterf(t, dataPoints, 0, "metric %q produced no data points", tt.metric)

			if tt.wantOperationName == "" {
				require.Falsef(t, gotOK, "unexpected db.operation.name=%q on %s", gotOperationName, tt.metric)
				return
			}
			require.Truef(t, gotOK, "missing db.operation.name on %s", tt.metric)
			assert.Equal(t, tt.wantOperationName, gotOperationName)
		})
	}
}

func TestTracer_metricAttributeOwnership(t *testing.T) {
	for _, count := range []int{0, 1, 2, 3, 7, 8, 15} {
		for _, separate := range []bool{false, true} {
			for _, named := range []bool{false, true} {
				t.Run(fmt.Sprintf("attrs=%d/separate=%t/named=%t", count, separate, named), func(t *testing.T) {
					ctx := context.Background()
					reader := sdkmetric.NewManualReader()
					provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
					t.Cleanup(func() { require.NoError(t, provider.Shutdown(ctx)) })

					var attrs []attribute.KeyValue
					for i := range count {
						attrs = append(attrs, attribute.String(fmt.Sprintf("zz.%02d", count-i), fmt.Sprint(i)))
					}
					if count > 0 {
						attrs = append(attrs, attribute.String("duplicate", "first"), attribute.String("duplicate", "last"))
					}
					before := slices.Clone(attrs)
					wantAttrs := map[attribute.Key]attribute.Value{
						"db.system.name": attribute.StringValue("postgresql"),
					}
					for _, attr := range before {
						wantAttrs[attr.Key] = attr.Value
					}

					opts := []Option{WithMeterProvider(provider), WithTracerProvider(noop.NewTracerProvider())}
					if separate {
						for _, attr := range attrs {
							opts = append(opts, WithMeterAttributes(attr))
						}
					} else {
						opts = append(opts, WithMeterAttributes(attrs...))
					}
					if named {
						opts = append(opts, WithMetricOperationName(SQLOperationName))
					}
					tracer := NewTracer(opts...)
					assert.Equal(t, before, attrs, "constructor modified caller's attributes")

					for _, err := range []error{nil, errors.New("operation failed")} {
						tracer.TraceQueryEnd(tracer.TraceQueryStart(ctx, nil, pgx.TraceQueryStartData{SQL: "SELECT 1"}), nil, pgx.TraceQueryEndData{Err: err})
						tracer.TracePrepareEnd(tracer.TracePrepareStart(ctx, nil, pgx.TracePrepareStartData{SQL: "SELECT 1"}), nil, pgx.TracePrepareEndData{Err: err})
						tracer.TraceCopyFromEnd(tracer.TraceCopyFromStart(ctx, nil, pgx.TraceCopyFromStartData{}), nil, pgx.TraceCopyFromEndData{Err: err})
						tracer.TraceBatchEnd(tracer.TraceBatchStart(ctx, nil, pgx.TraceBatchStartData{}), nil, pgx.TraceBatchEndData{Err: err})
						tracer.TraceConnectEnd(tracer.TraceConnectStart(ctx, pgx.TraceConnectStartData{}), pgx.TraceConnectEndData{Err: err})
						tracer.TraceAcquireEnd(tracer.TraceAcquireStart(ctx, nil, pgxpool.TraceAcquireStartData{}), nil, pgxpool.TraceAcquireEndData{Err: err})
					}

					var rm metricdata.ResourceMetrics
					require.NoError(t, reader.Collect(ctx, &rm))
					var seenMetrics []string
					for _, sm := range rm.ScopeMetrics {
						for _, m := range sm.Metrics {
							switch m.Name {
							case "db.client.operation.duration":
								data, ok := m.Data.(metricdata.Histogram[float64])
								require.True(t, ok, "duration must be a histogram")
								for _, point := range data.DataPoints {
									assert.Equal(t, uint64(2), point.Count)
								}
							case "db.client.operation.errors":
								data, ok := m.Data.(metricdata.Sum[int64])
								require.True(t, ok, "errors must be a counter")
								for _, point := range data.DataPoints {
									assert.Equal(t, int64(1), point.Value)
								}
							default:
								continue
							}
							seenMetrics = append(seenMetrics, m.Name)
							var seenOperations []string
							for _, pointAttrs := range dataPointAttributes(m.Data) {
								op, ok := pointAttrs.Value(PGXOperationTypeKey)
								require.True(t, ok, "%s missing operation type", m.Name)
								seenOperations = append(seenOperations, op.AsString())
								for key, want := range wantAttrs {
									got, ok := pointAttrs.Value(key)
									assert.True(t, ok, "%s/%s missing %s", m.Name, op.AsString(), key)
									assert.Equal(t, want, got, "%s/%s attribute %s", m.Name, op.AsString(), key)
								}
								name, hasName := pointAttrs.Value("db.operation.name")
								wantName := named && (op.AsString() == "query" || op.AsString() == "prepare")
								assert.Equal(t, wantName, hasName, "%s/%s operation name", m.Name, op.AsString())
								wantLen := len(wantAttrs) + 1
								if wantName {
									assert.Equal(t, attribute.StringValue("SELECT"), name)
									wantLen++
								}
								assert.Equal(t, wantLen, pointAttrs.Len())
							}
							assert.ElementsMatch(t, []string{"query", "prepare", "copy", "batch", "connect", "acquire"}, seenOperations, m.Name)
						}
					}
					assert.ElementsMatch(t, []string{"db.client.operation.duration", "db.client.operation.errors"}, seenMetrics)
					assert.Equal(t, before, attrs, "recording modified caller's attributes")
				})
			}
		}
	}
}

func TestTracer_metricAttributePrecedence(t *testing.T) {
	tests := []struct {
		name          string
		operationName OperationNameFunc
		wantQueryName string
	}{
		{name: "disabled", wantQueryName: "configured-last"},
		{
			name:          "non-empty name",
			operationName: func(context.Context, string) string { return "SELECT" },
			wantQueryName: "SELECT",
		},
		{
			name:          "empty name",
			operationName: func(context.Context, string) string { return "" },
			wantQueryName: "configured-last",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			reader := sdkmetric.NewManualReader()
			provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
			t.Cleanup(func() { require.NoError(t, provider.Shutdown(ctx)) })
			opts := []Option{
				WithMeterProvider(provider),
				WithTracerProvider(noop.NewTracerProvider()),
				WithMeterAttributes(
					PGXOperationTypeKey.String("configured-first"),
					attribute.String("db.operation.name", "configured-first"),
					attribute.String("db.system.name", "configured-first"),
				),
				WithMeterAttributes(
					PGXOperationTypeKey.String("configured-last"),
					attribute.String("db.operation.name", "configured-last"),
					attribute.String("db.system.name", "configured-last"),
				),
			}
			if tt.operationName != nil {
				opts = append(opts, WithMetricOperationName(tt.operationName))
			}
			tracer := NewTracer(opts...)
			boom := errors.New("operation failed")
			tracer.TraceQueryEnd(tracer.TraceQueryStart(ctx, nil, pgx.TraceQueryStartData{SQL: "SELECT 1"}), nil, pgx.TraceQueryEndData{Err: boom})
			tracer.TraceConnectEnd(tracer.TraceConnectStart(ctx, pgx.TraceConnectStartData{}), pgx.TraceConnectEndData{Err: boom})

			var rm metricdata.ResourceMetrics
			require.NoError(t, reader.Collect(ctx, &rm))
			var seenMetrics []string
			for _, sm := range rm.ScopeMetrics {
				for _, m := range sm.Metrics {
					if m.Name != "db.client.operation.duration" && m.Name != "db.client.operation.errors" {
						continue
					}
					seenMetrics = append(seenMetrics, m.Name)
					var seenOperations []string
					for _, attrs := range dataPointAttributes(m.Data) {
						op, ok := attrs.Value(PGXOperationTypeKey)
						require.True(t, ok)
						seenOperations = append(seenOperations, op.AsString())
						wantName := "configured-last"
						if op.AsString() == "query" {
							wantName = tt.wantQueryName
						}
						name, ok := attrs.Value("db.operation.name")
						assert.True(t, ok)
						assert.Equal(t, attribute.StringValue(wantName), name, "%s/%s operation name", m.Name, op.AsString())
						system, ok := attrs.Value("db.system.name")
						assert.True(t, ok)
						assert.Equal(t, attribute.StringValue("configured-last"), system, "%s/%s database system", m.Name, op.AsString())
						assert.Equal(t, 3, attrs.Len())
					}
					assert.ElementsMatch(t, []string{"query", "connect"}, seenOperations, m.Name)
				}
			}
			assert.ElementsMatch(t, []string{"db.client.operation.duration", "db.client.operation.errors"}, seenMetrics)
		})
	}
}
