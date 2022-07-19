package otelpgx

import (
	"context"
	"strings"

	"github.com/jackc/pgx/v5"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	semconv "go.opentelemetry.io/otel/semconv/v1.10.0"
	"go.opentelemetry.io/otel/trace"

	"github.com/exaring/otelpgx/internal"
)

// Tracer is a wrapper around the pgx tracer interfaces which instrument
// queries.
type Tracer struct {
	tracer            trace.Tracer
	attrs             []attribute.KeyValue
	trimQuerySpanName bool
	logSQLStatement   bool
}

type tracerConfig struct {
	tp                trace.TracerProvider
	attrs             []attribute.KeyValue
	trimQuerySpanName bool
	logSQLStatement   bool
}

// NewTracer returns a new Tracer.
func NewTracer(opts ...Option) *Tracer {
	cfg := &tracerConfig{
		tp: otel.GetTracerProvider(),
		attrs: []attribute.KeyValue{
			semconv.DBSystemPostgreSQL,
		},
		trimQuerySpanName: false,
		logSQLStatement:   true,
	}

	for _, opt := range opts {
		opt.apply(cfg)
	}

	return &Tracer{
		tracer:            cfg.tp.Tracer(internal.TracerName, trace.WithInstrumentationVersion(internal.InstrumentationVersion)),
		attrs:             cfg.attrs,
		trimQuerySpanName: cfg.trimQuerySpanName,
		logSQLStatement:   cfg.logSQLStatement,
	}
}

func recordError(span trace.Span, err error) {
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
	}
}

// TraceQueryStart is called at the beginning of Query, QueryRow, and Exec calls.
// The returned context is used for the rest of the call and will be passed to TraceQueryEnd.
func (t *Tracer) TraceQueryStart(ctx context.Context, conn *pgx.Conn, data pgx.TraceQueryStartData) context.Context {
	if !trace.SpanFromContext(ctx).IsRecording() {
		return ctx
	}

	opts := []trace.SpanStartOption{
		trace.WithSpanKind(trace.SpanKindClient),
		trace.WithAttributes(t.attrs...),
	}

	if conn != nil && conn.Config() != nil {
		opts = append(opts,
			trace.WithAttributes(attribute.String("database.host", conn.Config().Host)),
			trace.WithAttributes(attribute.Int("database.port", int(conn.Config().Port))),
			trace.WithAttributes(attribute.String("database.user", conn.Config().User)))
	}

	if t.logSQLStatement {
		opts = append(opts, trace.WithAttributes(semconv.DBStatementKey.String(data.SQL)))
	}

	spanName := "query " + data.SQL
	if t.trimQuerySpanName {
		spanName = "query " + strings.Split(data.SQL, " ")[0]
	}

	ctx, _ = t.tracer.Start(ctx, spanName, opts...)

	return ctx
}

// TraceQueryEnd is called at the end of Query, QueryRow, and Exec calls.
func (t *Tracer) TraceQueryEnd(ctx context.Context, _ *pgx.Conn, data pgx.TraceQueryEndData) {
	span := trace.SpanFromContext(ctx)
	recordError(span, data.Err)

	span.End()
}

// TraceCopyFromStart is called at the beginning of CopyFrom calls. The
// returned context is used for the rest of the call and will be passed to
// TraceCopyFromEnd.
func (t *Tracer) TraceCopyFromStart(ctx context.Context, conn *pgx.Conn, data pgx.TraceCopyFromStartData) context.Context {
	if !trace.SpanFromContext(ctx).IsRecording() {
		return ctx
	}

	opts := []trace.SpanStartOption{
		trace.WithSpanKind(trace.SpanKindClient),
		trace.WithAttributes(t.attrs...),
		trace.WithAttributes(attribute.String("db.table", data.TableName.Sanitize())),
	}

	if conn != nil && conn.Config() != nil {
		opts = append(opts,
			trace.WithAttributes(attribute.String("database.host", conn.Config().Host)),
			trace.WithAttributes(attribute.Int("database.port", int(conn.Config().Port))),
			trace.WithAttributes(attribute.String("database.user", conn.Config().User)))
	}

	ctx, _ = t.tracer.Start(ctx, "copy_from "+data.TableName.Sanitize(), opts...)

	return ctx
}

// TraceCopyFromEnd is called at the end of CopyFrom calls.
func (t *Tracer) TraceCopyFromEnd(ctx context.Context, _ *pgx.Conn, data pgx.TraceCopyFromEndData) {
	span := trace.SpanFromContext(ctx)
	recordError(span, data.Err)

	span.End()
}

// TraceBatchStart is called at the beginning of SendBatch calls. The returned
// context is used for the rest of the call and will be passed to
// TraceBatchQuery and TraceBatchEnd.
func (t *Tracer) TraceBatchStart(ctx context.Context, conn *pgx.Conn, data pgx.TraceBatchStartData) context.Context {
	if !trace.SpanFromContext(ctx).IsRecording() {
		return ctx
	}

	var size int
	if b := data.Batch; b != nil {
		size = b.Len()
	}

	opts := []trace.SpanStartOption{
		trace.WithSpanKind(trace.SpanKindClient),
		trace.WithAttributes(t.attrs...),
		trace.WithAttributes(attribute.Int("pgx.batch.size", size)),
	}

	if conn != nil && conn.Config() != nil {
		opts = append(opts,
			trace.WithAttributes(attribute.String("database.host", conn.Config().Host)),
			trace.WithAttributes(attribute.Int("database.port", int(conn.Config().Port))),
			trace.WithAttributes(attribute.String("database.user", conn.Config().User)))
	}

	ctx, _ = t.tracer.Start(ctx, "batch start", opts...)

	return ctx
}

// TraceBatchQuery is called at the after each query in a batch.
func (t *Tracer) TraceBatchQuery(ctx context.Context, conn *pgx.Conn, data pgx.TraceBatchQueryData) {
	opts := []trace.SpanStartOption{
		trace.WithSpanKind(trace.SpanKindClient),
		trace.WithAttributes(t.attrs...),
	}

	if conn != nil && conn.Config() != nil {
		opts = append(opts,
			trace.WithAttributes(attribute.String("database.host", conn.Config().Host)),
			trace.WithAttributes(attribute.Int("database.port", int(conn.Config().Port))),
			trace.WithAttributes(attribute.String("database.user", conn.Config().User)))
	}

	if t.logSQLStatement {
		opts = append(opts, trace.WithAttributes(semconv.DBStatementKey.String(data.SQL)))
	}

	spanName := "batch query " + data.SQL
	if t.trimQuerySpanName {
		spanName = "query " + strings.Split(data.SQL, " ")[0]
	}

	_, span := t.tracer.Start(ctx, spanName, opts...)
	recordError(span, data.Err)

	span.End()
}

// TraceBatchEnd is called at the end of SendBatch calls.
func (t *Tracer) TraceBatchEnd(ctx context.Context, _ *pgx.Conn, data pgx.TraceBatchEndData) {
	span := trace.SpanFromContext(ctx)
	recordError(span, data.Err)

	span.End()
}

// TraceConnectStart is called at the beginning of Connect and ConnectConfig
// calls. The returned context is used for the rest of the call and will be
// passed to TraceConnectEnd.
func (t *Tracer) TraceConnectStart(ctx context.Context, data pgx.TraceConnectStartData) context.Context {
	if !trace.SpanFromContext(ctx).IsRecording() {
		return ctx
	}

	opts := []trace.SpanStartOption{
		trace.WithSpanKind(trace.SpanKindClient),
		trace.WithAttributes(t.attrs...),
	}

	if data.ConnConfig != nil {
		opts = append(opts,
			trace.WithAttributes(attribute.String("database.host", data.ConnConfig.Host)),
			trace.WithAttributes(attribute.Int("database.port", int(data.ConnConfig.Port))),
			trace.WithAttributes(attribute.String("database.user", data.ConnConfig.User)))
	}

	ctx, _ = t.tracer.Start(ctx, "connect", opts...)

	return ctx
}

// TraceConnectEnd is called at the end of Connect and ConnectConfig calls.
func (t *Tracer) TraceConnectEnd(ctx context.Context, data pgx.TraceConnectEndData) {
	span := trace.SpanFromContext(ctx)
	recordError(span, data.Err)

	span.End()
}

// TracePrepareStart is called at the beginning of Prepare calls. The returned
// context is used for the rest of the call and will be passed to
// TracePrepareEnd.
func (t *Tracer) TracePrepareStart(ctx context.Context, conn *pgx.Conn, data pgx.TracePrepareStartData) context.Context {
	if !trace.SpanFromContext(ctx).IsRecording() {
		return ctx
	}

	opts := []trace.SpanStartOption{
		trace.WithSpanKind(trace.SpanKindClient),
		trace.WithAttributes(t.attrs...),
	}

	if conn != nil && conn.Config() != nil {
		opts = append(opts,
			trace.WithAttributes(attribute.String("database.host", conn.Config().Host)),
			trace.WithAttributes(attribute.Int("database.port", int(conn.Config().Port))),
			trace.WithAttributes(attribute.String("database.user", conn.Config().User)))
	}

	if t.logSQLStatement {
		opts = append(opts, trace.WithAttributes(semconv.DBStatementKey.String(data.SQL)))
	}

	spanName := "prepare " + data.SQL
	if t.trimQuerySpanName {
		spanName = "prepare " + strings.Split(data.SQL, " ")[0]
	}

	ctx, _ = t.tracer.Start(ctx, spanName, opts...)

	return ctx
}

// TracePrepareEnd is called at the end of Prepare calls.
func (t *Tracer) TracePrepareEnd(ctx context.Context, _ *pgx.Conn, data pgx.TracePrepareEndData) {
	span := trace.SpanFromContext(ctx)
	recordError(span, data.Err)

	span.End()
}
