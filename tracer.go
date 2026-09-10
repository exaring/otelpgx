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
	semconv "go.opentelemetry.io/otel/semconv/v1.40.0"
	"go.opentelemetry.io/otel/semconv/v1.40.0/dbconv"
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

// metricOperationNameCtxKey threads WithMetricOperationName's computed value
// from Trace*Start to Trace*End (query, prepare).
type metricOperationNameCtxKey struct{}

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

	operationDuration dbconv.ClientOperationDuration
	operationErrors   metric.Int64Counter

	fullQuerySpanName       bool
	spanNameCtxFunc         SpanNameCtxFunc
	prefixQuerySpanName     bool
	logSQLStatement         bool
	logConnectionDetails    bool
	includeParams           bool
	queryParamsFilter       QueryParametersFilterFunc
	disableAcquireTracer    bool
	metricOperationNameFunc OperationNameFunc
}

type tracerConfig struct {
	tracerProvider trace.TracerProvider
	meterProvider  metric.MeterProvider

	tracerAttrs []attribute.KeyValue
	meterAttrs  []attribute.KeyValue

	fullQuerySpanName       bool
	spanNameCtxFunc         SpanNameCtxFunc
	prefixQuerySpanName     bool
	logSQLStatement         bool
	logConnectionDetails    bool
	includeParams           bool
	queryParamsFilter       QueryParametersFilterFunc
	disableAcquireTracer    bool
	metricOperationNameFunc OperationNameFunc
}

// NewTracer returns a new Tracer.
func NewTracer(opts ...Option) *Tracer {
	cfg := &tracerConfig{
		tracerProvider: otel.GetTracerProvider(),
		meterProvider:  otel.GetMeterProvider(),
		tracerAttrs: []attribute.KeyValue{
			semconv.DBSystemNamePostgreSQL,
		},
		meterAttrs: []attribute.KeyValue{
			semconv.DBSystemNamePostgreSQL,
		},
		fullQuerySpanName:    false,
		spanNameCtxFunc:      SQLOperationName,
		prefixQuerySpanName:  false,
		logSQLStatement:      true,
		logConnectionDetails: true,
		includeParams:        false,
		disableAcquireTracer: false,
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
		tracerAttrs:             cfg.tracerAttrs,
		meterAttrs:              cfg.meterAttrs,
		fullQuerySpanName:       cfg.fullQuerySpanName,
		spanNameCtxFunc:         cfg.spanNameCtxFunc,
		prefixQuerySpanName:     cfg.prefixQuerySpanName,
		logSQLStatement:         cfg.logSQLStatement,
		logConnectionDetails:    cfg.logConnectionDetails,
		includeParams:           cfg.includeParams,
		queryParamsFilter:       cfg.queryParamsFilter,
		disableAcquireTracer:    cfg.disableAcquireTracer,
		metricOperationNameFunc: cfg.metricOperationNameFunc,
	}

	tracer.createMetrics()
	tracer.createAttributeSets()

	return tracer
}

// createMetrics initializes all synchronous metrics tracked by Tracer.
// Any errors encountered upon metric creation will be sent to the globally assigned OpenTelemetry ErrorHandler.
func (t *Tracer) createMetrics() {
	var err error

	t.operationDuration, err = dbconv.NewClientOperationDuration(
		t.meter,
		metric.WithExplicitBucketBoundaries(
			0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1, 5, 10,
		),
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
//
// operationName is the SQL operation name (e.g. "SELECT") to additionally
// attach as db.operation.name, or "" when none applies to this call (e.g.
// connect/acquire/copy, or the aggregate end of a batch).
func (t *Tracer) incrementOperationErrorCount(ctx context.Context, err error, pgxOperation, operationName string) {
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		t.operationErrors.Add(ctx, 1, metric.WithAttributeSet(
			t.attributeSetFor(pgxOperation, operationName),
		))
	}
}

// recordOperationDuration will compute and record the time since the start of
// an operation. See incrementOperationErrorCount for the operationName parameter.
func (t *Tracer) recordOperationDuration(ctx context.Context, pgxOperation, operationName string) {
	if startTime, ok := ctx.Value(startTimeCtxKey{}).(time.Time); ok {
		t.operationDuration.RecordSet(ctx, time.Since(startTime).Seconds(), t.attributeSetFor(pgxOperation, operationName))
	}
}

// attributeSetFor returns the attribute.Set to record metrics against for a
// given pgx operation kind, additionally carrying db.operation.name when
// operationName is non-empty. This isn't cached: benchmarking showed building
// a fresh attribute.Set costs ~150ns/3 allocs versus a cached lookup, which is
// immaterial next to a database round trip, and a cache keyed on
// operationName could grow unboundedly large for a caller-supplied
// OperationNameFunc with high-cardinality output.
func (t *Tracer) attributeSetFor(pgxOperation, operationName string) attribute.Set {
	if operationName == "" {
		return t.metricAttrs[pgxOperation]
	}

	attrs := append(append([]attribute.KeyValue{}, t.meterAttrs...),
		PGXOperationTypeKey.String(pgxOperation),
		t.operationDuration.AttrOperationName(operationName),
	)
	return attribute.NewSet(attrs...)
}

// connectionAttributesFromConfig returns a SpanStartOption that contains
// attributes from the given connection config.
func connectionAttributesFromConfig(config *pgx.ConnConfig) []attribute.KeyValue {
	if config != nil {
		return []attribute.KeyValue{
			semconv.DBSystemNamePostgreSQL,
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

	// Runs regardless of trace sampling, unlike the span-naming hooks below.
	if t.metricOperationNameFunc != nil {
		ctx = context.WithValue(ctx, metricOperationNameCtxKey{}, t.metricOperationNameFunc(ctx, data.SQL))
	}

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

	operationName := t.spanNameCtxFunc(ctx, data.SQL)

	if t.logSQLStatement {
		attrs = append(attrs,
			semconv.DBQueryText(data.SQL),
			semconv.DBOperationName(operationName),
		)

		if t.includeParams && t.shouldRecordParams(data.SQL) {
			attrs = append(attrs, makeParamsAttribute(data.Args))
		}
	}

	opts = append(opts,
		trace.WithSpanKind(trace.SpanKindClient),
		trace.WithAttributes(attrs...),
	)

	spanName := t.spanName(data.SQL, operationName, "query ")

	ctx, _ = t.tracer.Start(ctx, spanName, opts...)

	return ctx
}

// spanName builds the span name following the OpenTelemetry database span
// conventions: the low-cardinality operation name (operationName, e.g.
// "SELECT") by default, or the full SQL statement when WithFullSQLInSpanName is
// set, optionally prefixed when WithQuerySpanNamePrefix is set.
//
// See https://opentelemetry.io/docs/specs/semconv/db/database-spans/ for the
// span name guidance this follows.
func (t *Tracer) spanName(sql, operationName, prefix string) string {
	name := operationName
	if t.fullQuerySpanName {
		name = sql
	}

	if t.prefixQuerySpanName {
		name = prefix + name
	}

	return name
}

// TraceQueryEnd is called at the end of Query, QueryRow, and Exec calls.
func (t *Tracer) TraceQueryEnd(ctx context.Context, _ *pgx.Conn, data pgx.TraceQueryEndData) {
	span := trace.SpanFromContext(ctx)
	operationName, _ := ctx.Value(metricOperationNameCtxKey{}).(string)
	t.incrementOperationErrorCount(ctx, data.Err, pgxOperationQuery, operationName)
	t.recordOperationDuration(ctx, pgxOperationQuery, operationName)

	if !span.IsRecording() {
		return
	}

	recordSpanError(span, data.Err)

	if data.Err == nil {
		span.SetAttributes(RowsAffectedKey.Int64(data.CommandTag.RowsAffected()))
	}

	span.End()
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
	t.incrementOperationErrorCount(ctx, data.Err, pgxOperationCopy, "")
	t.recordOperationDuration(ctx, pgxOperationCopy, "")

	if !span.IsRecording() {
		return
	}

	if data.Err == nil {
		span.SetAttributes(RowsAffectedKey.Int64(data.CommandTag.RowsAffected()))
	}

	recordSpanError(span, data.Err)
	span.End()
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
	// Unlike the aggregate TraceBatchEnd, each call here describes exactly one
	// statement, so a single operation name always applies.
	var metricOperationName string
	if t.metricOperationNameFunc != nil {
		metricOperationName = t.metricOperationNameFunc(ctx, data.SQL)
	}
	t.incrementOperationErrorCount(ctx, data.Err, pgxOperationBatch, metricOperationName)

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

	operationName := t.spanNameCtxFunc(ctx, data.SQL)

	if t.logSQLStatement {
		attrs = append(attrs,
			semconv.DBQueryText(data.SQL),
			semconv.DBOperationName(operationName),
		)

		if t.includeParams && t.shouldRecordParams(data.SQL) {
			attrs = append(attrs, makeParamsAttribute(data.Args))
		}
	}

	opts = append(opts,
		trace.WithSpanKind(trace.SpanKindClient),
		trace.WithAttributes(attrs...),
	)

	spanName := t.spanName(data.SQL, operationName, "batch query ")

	_, span := t.tracer.Start(ctx, spanName, opts...)
	recordSpanError(span, data.Err)

	span.End()
}

// TraceBatchEnd is called at the end of SendBatch calls. The aggregate
// duration/error count for the whole batch is recorded with no
// db.operation.name: a batch may mix operation types, so there is no single
// name that describes it (per-statement names are attached in
// TraceBatchQuery instead).
func (t *Tracer) TraceBatchEnd(ctx context.Context, _ *pgx.Conn, data pgx.TraceBatchEndData) {
	span := trace.SpanFromContext(ctx)
	t.incrementOperationErrorCount(ctx, data.Err, pgxOperationBatch, "")
	t.recordOperationDuration(ctx, pgxOperationBatch, "")

	if !span.IsRecording() {
		return
	}

	recordSpanError(span, data.Err)
	span.End()
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
	t.incrementOperationErrorCount(ctx, data.Err, pgxOperationConnect, "")
	t.recordOperationDuration(ctx, pgxOperationConnect, "")

	if !span.IsRecording() {
		return
	}

	recordSpanError(span, data.Err)
	span.End()
}

// TracePrepareStart is called at the beginning of Prepare calls. The returned
// context is used for the rest of the call and will be passed to
// TracePrepareEnd.
func (t *Tracer) TracePrepareStart(ctx context.Context, conn *pgx.Conn, data pgx.TracePrepareStartData) context.Context {
	ctx = context.WithValue(ctx, startTimeCtxKey{}, time.Now())

	// Runs regardless of trace sampling, unlike the span-naming hooks below.
	if t.metricOperationNameFunc != nil {
		ctx = context.WithValue(ctx, metricOperationNameCtxKey{}, t.metricOperationNameFunc(ctx, data.SQL))
	}

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

	operationName := t.spanNameCtxFunc(ctx, data.SQL)
	attrs = append(attrs, semconv.DBOperationName(operationName))

	if t.logSQLStatement {
		attrs = append(attrs, semconv.DBQueryText(data.SQL))
	}

	opts = append(opts,
		trace.WithSpanKind(trace.SpanKindClient),
		trace.WithAttributes(attrs...),
	)

	spanName := t.spanName(data.SQL, operationName, "prepare ")

	ctx, _ = t.tracer.Start(ctx, spanName, opts...)

	return ctx
}

// TracePrepareEnd is called at the end of Prepare calls.
func (t *Tracer) TracePrepareEnd(ctx context.Context, _ *pgx.Conn, data pgx.TracePrepareEndData) {
	span := trace.SpanFromContext(ctx)
	operationName, _ := ctx.Value(metricOperationNameCtxKey{}).(string)
	t.incrementOperationErrorCount(ctx, data.Err, pgxOperationPrepare, operationName)
	t.recordOperationDuration(ctx, pgxOperationPrepare, operationName)

	if !span.IsRecording() {
		return
	}

	recordSpanError(span, data.Err)
	span.End()
}

// TraceAcquireStart is called at the beginning of Acquire.
// The returned context is used for the rest of the call and will be passed to the TraceAcquireEnd.
// If WithDisableAcquireTracer was set, then the function is no-op.
func (t *Tracer) TraceAcquireStart(ctx context.Context, pool *pgxpool.Pool, data pgxpool.TraceAcquireStartData) context.Context {
	if t.disableAcquireTracer {
		return ctx
	}

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
// If WithDisableAcquireTracer was set, then the function is no-op.
func (t *Tracer) TraceAcquireEnd(ctx context.Context, _ *pgxpool.Pool, data pgxpool.TraceAcquireEndData) {
	if t.disableAcquireTracer {
		return
	}

	span := trace.SpanFromContext(ctx)
	t.incrementOperationErrorCount(ctx, data.Err, pgxOperationAcquire, "")
	t.recordOperationDuration(ctx, pgxOperationAcquire, "")

	if !span.IsRecording() {
		return
	}

	recordSpanError(span, data.Err)
	span.End()
}

func (t *Tracer) shouldRecordParams(sql string) bool {
	if t.queryParamsFilter == nil {
		return true
	}
	return t.queryParamsFilter(sql)
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
