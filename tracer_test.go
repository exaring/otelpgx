package otelpgx

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgproto3"
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

// TestTracer_metricOperationName asserts that, once WithMetricOperationName
// is set, db.client.operation.duration and db.client.operation.errors carry
// db.operation.name for query, prepare, and per-statement batch-query calls,
// that the whole-batch aggregate recorded in TraceBatchEnd does not (a batch
// can mix operation types, so there's no single name that describes it), and
// that the attribute is absent entirely when the option isn't set (the
// default).
//
// Every case uses a noop TracerProvider so no span is ever recording,
// demonstrating that metric recording — and this attribute along with it —
// is decoupled from trace sampling.
func TestTracer_metricOperationName(t *testing.T) {
	conn := newMockConn(t, "fakehost", 5432, "fakeuser", "fakedb")
	boom := errors.New("boom")

	tests := []struct {
		name       string
		tracerOpts []Option
		drive      func(ctx context.Context, tracer *Tracer, conn *pgx.Conn)
		// metric is the instrument to inspect; wantOperationName is the
		// expected db.operation.name value on one of its data points, or ""
		// to assert the attribute is absent from all of its data points.
		metric            string
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
