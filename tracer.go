package otelpgx

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"runtime/debug"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/metric"
	semconv "go.opentelemetry.io/otel/semconv/v1.27.0"
	"go.opentelemetry.io/otel/trace"
)

const (
	tracerName = "github.com/exaring/otelpgx"
	meterName  = "github.com/exaring/otelpgx"
)

const (
	sqlOperationUnknown = "UNKNOWN"
)

const (
	pgxOperationQuery   = "query"
	pgxOperationCopy    = "copy"
	pgxOperationBatch   = "batch"
	pgxOperationConnect = "connect"
	pgxOperationPrepare = "prepare"
	pgxOperationAcquire = "acquire"
)

const (
	// RowsAffectedKey represents the number of rows affected.
	RowsAffectedKey = attribute.Key("pgx.rows_affected")
	// QueryParametersKey represents the query parameters.
	QueryParametersKey = attribute.Key("pgx.query.parameters")
	// PrepareStmtNameKey represents the prepared statement name.
	PrepareStmtNameKey = attribute.Key("pgx.prepare_stmt.name")
	// SQLStateKey represents PostgreSQL error code,
	// see https://www.postgresql.org/docs/current/errcodes-appendix.html.
	SQLStateKey = attribute.Key("pgx.sql_state")
	// PGXOperationTypeKey represents the pgx tracer operation type
	PGXOperationTypeKey = attribute.Key("pgx.operation.type")
	// DBClientOperationErrorsKey represents the count of operation errors
	DBClientOperationErrorsKey = attribute.Key("db.client.operation.errors")
)

type startTimeCtxKey struct{}

var _ pgxpool.AcquireTracer = (*Tracer)(nil)

// Tracer is a wrapper around the pgx tracer interfaces which instrument
// queries with both tracing and metrics.
// Use [NewTracer] to create a new instance.
type Tracer struct {
	tracer      trace.Tracer
	meter       metric.Meter
	tracerAttrs []attribute.KeyValue
	meterAttrs  []attribute.KeyValue

	spanStartOptionsPool sync.Pool
	attributeSlicePool   sync.Pool
	metricAttrs          map[string]attribute.Set

	operationDuration metric.Int64Histogram
	operationErrors   metric.Int64Counter

	trimQuerySpanName    bool
	spanNameFunc         SpanNameFunc
	prefixQuerySpanName  bool
	logSQLStatement      bool
	logConnectionDetails bool
	includeParams        bool
}

type tracerConfig struct {
	tracerProvider trace.TracerProvider
	meterProvider  metric.MeterProvider

	tracerAttrs []attribute.KeyValue
	meterAttrs  []attribute.KeyValue

	trimQuerySpanName    bool
	spanNameFunc         SpanNameFunc
	prefixQuerySpanName  bool
	logSQLStatement      bool
	logConnectionDetails bool
	includeParams        bool
}

// NewTracer returns a new Tracer.
func NewTracer(opts ...Option) *Tracer {
	cfg := &tracerConfig{
		tracerProvider: otel.GetTracerProvider(),
		meterProvider:  otel.GetMeterProvider(),
		tracerAttrs: []attribute.KeyValue{
			semconv.DBSystemPostgreSQL,
		},
		meterAttrs: []attribute.KeyValue{
			semconv.DBSystemPostgreSQL,
		},
		trimQuerySpanName:    false,
		spanNameFunc:         defaultSpanNameFunc,
		prefixQuerySpanName:  true,
		logSQLStatement:      true,
		logConnectionDetails: true,
		includeParams:        false,
	}

	for _, opt := range opts {
		opt.apply(cfg)
	}

	tracer := &Tracer{
		tracer: cfg.tracerProvider.Tracer(tracerName, trace.WithInstrumentationVersion(findOwnImportedVersion())),
		meter:  cfg.meterProvider.Meter(meterName, metric.WithInstrumentationVersion(findOwnImportedVersion())),
		spanStartOptionsPool: sync.Pool{
			New: func() any {
				s := make([]trace.SpanStartOption, 0, 10)
				return &s
			},
		},
		attributeSlicePool: sync.Pool{
			New: func() any {
				s := make([]attribute.KeyValue, 0, 10)
				return &s
			},
		},
		tracerAttrs:          cfg.tracerAttrs,
		meterAttrs:           cfg.meterAttrs,
		trimQuerySpanName:    cfg.trimQuerySpanName,
		spanNameFunc:         cfg.spanNameFunc,
		prefixQuerySpanName:  cfg.prefixQuerySpanName,
		logSQLStatement:      cfg.logSQLStatement,
		logConnectionDetails: cfg.logConnectionDetails,
		includeParams:        cfg.includeParams,
	}

	tracer.createMetrics()
	tracer.createAttributeSets()

	return tracer
}

// createMetrics initializes all synchronous metrics tracked by Tracer.
// Any errors encountered upon metric creation will be sent to the globally assigned OpenTelemetry ErrorHandler.
func (t *Tracer) createMetrics() {
	var err error

	t.operationDuration, err = t.meter.Int64Histogram(
		semconv.DBClientOperationDurationName,
		metric.WithDescription(semconv.DBClientOperationDurationDescription),
		metric.WithUnit(semconv.DBClientOperationDurationUnit),
	)
	if err != nil {
		otel.Handle(err)
	}

	t.operationErrors, err = t.meter.Int64Counter(
		string(DBClientOperationErrorsKey),
		metric.WithDescription("The count of database client operation errors"),
	)
	if err != nil {
		otel.Handle(err)
	}
}

func (t *Tracer) createAttributeSets() {
	t.metricAttrs = make(map[string]attribute.Set)
	operations := []string{
		pgxOperationQuery,
		pgxOperationCopy,
		pgxOperationBatch,
		pgxOperationConnect,
		pgxOperationPrepare,
		pgxOperationAcquire,
	}
	for _, op := range operations {
		attrs := append(t.meterAttrs, PGXOperationTypeKey.String(op))
		t.metricAttrs[op] = attribute.NewSet(attrs...)
	}
}

// recordSpanError handles all error handling to be applied on the provided span.
// The provided error must be non-nil and not a sql.ErrNoRows error.
// Otherwise, recordSpanError will be a no-op.
func recordSpanError(span trace.Span, err error) {
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())

		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) {
			span.SetAttributes(SQLStateKey.String(pgErr.Code))
		}
	}
}

// incrementOperationErrorCount will increment the operation error count metric for any provided error
// that is non-nil and not sql.ErrNoRows. Otherwise, incrementOperationErrorCount becomes a no-op.
func (t *Tracer) incrementOperationErrorCount(ctx context.Context, err error, pgxOperation string) {
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		t.operationErrors.Add(ctx, 1, metric.WithAttributeSet(
			t.metricAttrs[pgxOperation],
		))
	}
}

// recordOperationDuration will compute and record the time since the start of an operation.
func (t *Tracer) recordOperationDuration(ctx context.Context, pgxOperation string) {
	if startTime, ok := ctx.Value(startTimeCtxKey{}).(time.Time); ok {
		t.operationDuration.Record(ctx, time.Since(startTime).Milliseconds(), metric.WithAttributeSet(
			t.metricAttrs[pgxOperation],
		))
	}
}

// connectionAttributesFromConfig returns a SpanStartOption that contains
// attributes from the given connection config.
func connectionAttributesFromConfig(config *pgx.ConnConfig) []attribute.KeyValue {
	if config != nil {
		return []attribute.KeyValue{
			semconv.DBSystemPostgreSQL,
			semconv.ServerAddress(config.Host),
			semconv.ServerPort(int(config.Port)),
			semconv.UserName(config.User),
			semconv.DBNamespace(config.Database),
		}
	}
	return nil
}

// TraceQueryStart is called at the beginning of Query, QueryRow, and Exec calls.
// The returned context is used for the rest of the call and will be passed to TraceQueryEnd.
func (t *Tracer) TraceQueryStart(ctx context.Context, conn *pgx.Conn, data pgx.TraceQueryStartData) context.Context {
	ctx = context.WithValue(ctx, startTimeCtxKey{}, time.Now())

	if !trace.SpanFromContext(ctx).IsRecording() {
		return ctx
	}

	optsP := t.spanStartOptionsPool.Get().(*[]trace.SpanStartOption)
	defer t.spanStartOptionsPool.Put(optsP)
	attrsP := t.attributeSlicePool.Get().(*[]attribute.KeyValue)
	defer t.attributeSlicePool.Put(attrsP)

	// reslice to empty
	opts := (*optsP)[:0]
	attrs := (*attrsP)[:0]

	attrs = append(attrs, t.tracerAttrs...)

	if t.logConnectionDetails && conn != nil {
		attrs = append(attrs, connectionAttributesFromConfig(conn.Config())...)
	}

	if t.logSQLStatement {
		attrs = append(attrs,
			semconv.DBQueryText(data.SQL),
			semconv.DBOperationName(t.spanNameFunc(data.SQL)),
		)

		if t.includeParams {
			attrs = append(attrs, makeParamsAttribute(data.Args))
		}
	}

	opts = append(opts,
		trace.WithSpanKind(trace.SpanKindClient),
		trace.WithAttributes(attrs...),
	)

	spanName := data.SQL
	if t.trimQuerySpanName {
		spanName = t.spanNameFunc(data.SQL)
	}

	if t.prefixQuerySpanName {
		spanName = "query " + spanName
	}

	ctx, _ = t.tracer.Start(ctx, spanName, opts...)

	return ctx
}

// TraceQueryEnd is called at the end of Query, QueryRow, and Exec calls.
func (t *Tracer) TraceQueryEnd(ctx context.Context, _ *pgx.Conn, data pgx.TraceQueryEndData) {
	span := trace.SpanFromContext(ctx)
	recordSpanError(span, data.Err)
	t.incrementOperationErrorCount(ctx, data.Err, pgxOperationQuery)

	if data.Err == nil {
		span.SetAttributes(RowsAffectedKey.Int64(data.CommandTag.RowsAffected()))
	}

	span.End()

	t.recordOperationDuration(ctx, pgxOperationQuery)
}

// TraceCopyFromStart is called at the beginning of CopyFrom calls. The
// returned context is used for the rest of the call and will be passed to
// TraceCopyFromEnd.
func (t *Tracer) TraceCopyFromStart(ctx context.Context, conn *pgx.Conn, data pgx.TraceCopyFromStartData) context.Context {
	ctx = context.WithValue(ctx, startTimeCtxKey{}, time.Now())

	if !trace.SpanFromContext(ctx).IsRecording() {
		return ctx
	}

	optsP := t.spanStartOptionsPool.Get().(*[]trace.SpanStartOption)
	defer t.spanStartOptionsPool.Put(optsP)
	attrsP := t.attributeSlicePool.Get().(*[]attribute.KeyValue)
	defer t.attributeSlicePool.Put(attrsP)

	// reslice to empty
	opts := (*optsP)[:0]
	attrs := (*attrsP)[:0]

	attrs = append(attrs, t.tracerAttrs...)
	attrs = append(attrs, semconv.DBCollectionName(data.TableName.Sanitize()))

	if t.logConnectionDetails && conn != nil {
		attrs = append(attrs, connectionAttributesFromConfig(conn.Config())...)
	}

	opts = append(opts,
		trace.WithSpanKind(trace.SpanKindClient),
		trace.WithAttributes(attrs...),
	)

	ctx, _ = t.tracer.Start(ctx, "copy_from "+data.TableName.Sanitize(), opts...)

	return ctx
}

// TraceCopyFromEnd is called at the end of CopyFrom calls.
func (t *Tracer) TraceCopyFromEnd(ctx context.Context, _ *pgx.Conn, data pgx.TraceCopyFromEndData) {
	span := trace.SpanFromContext(ctx)
	recordSpanError(span, data.Err)
	t.incrementOperationErrorCount(ctx, data.Err, pgxOperationCopy)

	if data.Err == nil {
		span.SetAttributes(RowsAffectedKey.Int64(data.CommandTag.RowsAffected()))
	}

	span.End()

	t.recordOperationDuration(ctx, pgxOperationCopy)
}

// TraceBatchStart is called at the beginning of SendBatch calls. The returned
// context is used for the rest of the call and will be passed to
// TraceBatchQuery and TraceBatchEnd.
func (t *Tracer) TraceBatchStart(ctx context.Context, conn *pgx.Conn, data pgx.TraceBatchStartData) context.Context {
	ctx = context.WithValue(ctx, startTimeCtxKey{}, time.Now())

	if !trace.SpanFromContext(ctx).IsRecording() {
		return ctx
	}

	var size int
	if b := data.Batch; b != nil {
		size = b.Len()
	}

	optsP := t.spanStartOptionsPool.Get().(*[]trace.SpanStartOption)
	defer t.spanStartOptionsPool.Put(optsP)
	attrsP := t.attributeSlicePool.Get().(*[]attribute.KeyValue)
	defer t.attributeSlicePool.Put(attrsP)

	// reslice to empty
	opts := (*optsP)[:0]
	attrs := (*attrsP)[:0]

	attrs = append(attrs, t.tracerAttrs...)
	attrs = append(attrs, semconv.DBOperationBatchSize(size))

	if t.logConnectionDetails && conn != nil {
		attrs = append(attrs, connectionAttributesFromConfig(conn.Config())...)
	}

	opts = append(opts,
		trace.WithSpanKind(trace.SpanKindClient),
		trace.WithAttributes(attrs...),
	)

	ctx, _ = t.tracer.Start(ctx, "batch start", opts...)

	return ctx
}

// TraceBatchQuery is called at the after each query in a batch.
func (t *Tracer) TraceBatchQuery(ctx context.Context, conn *pgx.Conn, data pgx.TraceBatchQueryData) {
	t.incrementOperationErrorCount(ctx, data.Err, pgxOperationBatch)

	if !trace.SpanFromContext(ctx).IsRecording() {
		return
	}

	optsP := t.spanStartOptionsPool.Get().(*[]trace.SpanStartOption)
	defer t.spanStartOptionsPool.Put(optsP)
	attrsP := t.attributeSlicePool.Get().(*[]attribute.KeyValue)
	defer t.attributeSlicePool.Put(attrsP)

	// reslice to empty
	opts := (*optsP)[:0]
	attrs := (*attrsP)[:0]

	attrs = append(attrs, t.tracerAttrs...)

	if t.logConnectionDetails && conn != nil {
		attrs = append(attrs, connectionAttributesFromConfig(conn.Config())...)
	}

	if t.logSQLStatement {
		attrs = append(attrs,
			semconv.DBQueryText(data.SQL),
			semconv.DBOperationName(t.spanNameFunc(data.SQL)),
		)

		if t.includeParams {
			attrs = append(attrs, makeParamsAttribute(data.Args))
		}
	}

	opts = append(opts,
		trace.WithSpanKind(trace.SpanKindClient),
		trace.WithAttributes(attrs...),
	)

	var spanName string
	if t.trimQuerySpanName {
		spanName = t.spanNameFunc(data.SQL)
		if t.prefixQuerySpanName {
			spanName = "query " + spanName
		}
	} else {
		spanName = data.SQL
		if t.prefixQuerySpanName {
			spanName = "batch query " + spanName
		}
	}

	_, span := t.tracer.Start(ctx, spanName, opts...)
	recordSpanError(span, data.Err)

	span.End()
}

// TraceBatchEnd is called at the end of SendBatch calls.
func (t *Tracer) TraceBatchEnd(ctx context.Context, _ *pgx.Conn, data pgx.TraceBatchEndData) {
	span := trace.SpanFromContext(ctx)
	recordSpanError(span, data.Err)
	t.incrementOperationErrorCount(ctx, data.Err, pgxOperationBatch)

	span.End()

	t.recordOperationDuration(ctx, pgxOperationBatch)
}

// TraceConnectStart is called at the beginning of Connect and ConnectConfig
// calls. The returned context is used for the rest of the call and will be
// passed to TraceConnectEnd.
func (t *Tracer) TraceConnectStart(ctx context.Context, data pgx.TraceConnectStartData) context.Context {
	ctx = context.WithValue(ctx, startTimeCtxKey{}, time.Now())

	if !trace.SpanFromContext(ctx).IsRecording() {
		return ctx
	}

	optsP := t.spanStartOptionsPool.Get().(*[]trace.SpanStartOption)
	defer t.spanStartOptionsPool.Put(optsP)
	attrsP := t.attributeSlicePool.Get().(*[]attribute.KeyValue)
	defer t.attributeSlicePool.Put(attrsP)

	// reslice to empty
	opts := (*optsP)[:0]
	attrs := (*attrsP)[:0]

	attrs = append(attrs, t.tracerAttrs...)

	if t.logConnectionDetails && data.ConnConfig != nil {
		attrs = append(attrs, connectionAttributesFromConfig(data.ConnConfig)...)
	}

	opts = append(opts,
		trace.WithSpanKind(trace.SpanKindClient),
		trace.WithAttributes(attrs...),
	)

	ctx, _ = t.tracer.Start(ctx, "connect", opts...)

	return ctx
}

// TraceConnectEnd is called at the end of Connect and ConnectConfig calls.
func (t *Tracer) TraceConnectEnd(ctx context.Context, data pgx.TraceConnectEndData) {
	span := trace.SpanFromContext(ctx)
	recordSpanError(span, data.Err)
	t.incrementOperationErrorCount(ctx, data.Err, pgxOperationConnect)

	span.End()

	t.recordOperationDuration(ctx, pgxOperationConnect)
}

// TracePrepareStart is called at the beginning of Prepare calls. The returned
// context is used for the rest of the call and will be passed to
// TracePrepareEnd.
func (t *Tracer) TracePrepareStart(ctx context.Context, conn *pgx.Conn, data pgx.TracePrepareStartData) context.Context {
	ctx = context.WithValue(ctx, startTimeCtxKey{}, time.Now())

	if !trace.SpanFromContext(ctx).IsRecording() {
		return ctx
	}

	optsP := t.spanStartOptionsPool.Get().(*[]trace.SpanStartOption)
	defer t.spanStartOptionsPool.Put(optsP)
	attrsP := t.attributeSlicePool.Get().(*[]attribute.KeyValue)
	defer t.attributeSlicePool.Put(attrsP)

	// reslice to empty
	opts := (*optsP)[:0]
	attrs := (*attrsP)[:0]

	attrs = append(attrs, t.tracerAttrs...)

	if data.Name != "" {
		attrs = append(attrs, PrepareStmtNameKey.String(data.Name))
	}

	if t.logConnectionDetails && conn != nil {
		attrs = append(attrs, connectionAttributesFromConfig(conn.Config())...)
	}

	attrs = append(attrs, semconv.DBOperationName(t.spanNameFunc(data.SQL)))

	if t.logSQLStatement {
		attrs = append(attrs, semconv.DBQueryText(data.SQL))
	}

	opts = append(opts,
		trace.WithSpanKind(trace.SpanKindClient),
		trace.WithAttributes(attrs...),
	)

	spanName := data.SQL
	if t.trimQuerySpanName {
		spanName = t.spanNameFunc(data.SQL)
	}
	if t.prefixQuerySpanName {
		spanName = "prepare " + spanName
	}

	ctx, _ = t.tracer.Start(ctx, spanName, opts...)

	return ctx
}

// TracePrepareEnd is called at the end of Prepare calls.
func (t *Tracer) TracePrepareEnd(ctx context.Context, _ *pgx.Conn, data pgx.TracePrepareEndData) {
	span := trace.SpanFromContext(ctx)
	recordSpanError(span, data.Err)
	t.incrementOperationErrorCount(ctx, data.Err, pgxOperationPrepare)

	span.End()

	t.recordOperationDuration(ctx, pgxOperationPrepare)
}

// TraceAcquireStart is called at the beginning of Acquire.
// The returned context is used for the rest of the call and will be passed to the TraceAcquireEnd.
func (t *Tracer) TraceAcquireStart(ctx context.Context, pool *pgxpool.Pool, data pgxpool.TraceAcquireStartData) context.Context {
	ctx = context.WithValue(ctx, startTimeCtxKey{}, time.Now())

	if !trace.SpanFromContext(ctx).IsRecording() {
		return ctx
	}

	optsP := t.spanStartOptionsPool.Get().(*[]trace.SpanStartOption)
	defer t.spanStartOptionsPool.Put(optsP)
	attrsP := t.attributeSlicePool.Get().(*[]attribute.KeyValue)
	defer t.attributeSlicePool.Put(attrsP)

	// reslice to empty
	opts := (*optsP)[:0]
	attrs := (*attrsP)[:0]

	attrs = append(attrs, t.tracerAttrs...)

	if t.logConnectionDetails && pool != nil && pool.Config() != nil && pool.Config().ConnConfig != nil {
		attrs = append(attrs, connectionAttributesFromConfig(pool.Config().ConnConfig)...)
	}

	opts = append(opts,
		trace.WithSpanKind(trace.SpanKindClient),
		trace.WithAttributes(attrs...),
	)

	ctx, _ = t.tracer.Start(ctx, "pool.acquire", opts...)

	return ctx
}

// TraceAcquireEnd is called when a connection has been acquired.
func (t *Tracer) TraceAcquireEnd(ctx context.Context, _ *pgxpool.Pool, data pgxpool.TraceAcquireEndData) {
	span := trace.SpanFromContext(ctx)
	recordSpanError(span, data.Err)
	t.incrementOperationErrorCount(ctx, data.Err, pgxOperationAcquire)

	span.End()

	t.recordOperationDuration(ctx, pgxOperationAcquire)
}

func makeParamsAttribute(args []any) attribute.KeyValue {
	ss := make([]string, len(args))
	for i := range args {
		ss[i] = fmt.Sprintf("%+v", args[i])
	}

	return QueryParametersKey.StringSlice(ss)
}

func findOwnImportedVersion() string {
	buildInfo, ok := debug.ReadBuildInfo()
	if ok {
		for _, dep := range buildInfo.Deps {
			if dep.Path == tracerName {
				return dep.Version
			}
		}
	}

	return "unknown"
}
