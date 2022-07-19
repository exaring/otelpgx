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

func (t *Tracer) TraceQueryEnd(ctx context.Context, _ *pgx.Conn, data pgx.TraceQueryEndData) {
	span := trace.SpanFromContext(ctx)
	recordError(span, data.Err)

	span.End()
}

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

func (t *Tracer) TraceCopyFromEnd(ctx context.Context, _ *pgx.Conn, data pgx.TraceCopyFromEndData) {
	span := trace.SpanFromContext(ctx)
	recordError(span, data.Err)

	span.End()
}

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

func (t *Tracer) TraceBatchEnd(ctx context.Context, _ *pgx.Conn, data pgx.TraceBatchEndData) {
	span := trace.SpanFromContext(ctx)
	recordError(span, data.Err)

	span.End()
}

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

func (t *Tracer) TraceConnectEnd(ctx context.Context, data pgx.TraceConnectEndData) {
	span := trace.SpanFromContext(ctx)
	recordError(span, data.Err)

	span.End()
}

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

func (t *Tracer) TracePrepareEnd(ctx context.Context, _ *pgx.Conn, data pgx.TracePrepareEndData) {
	span := trace.SpanFromContext(ctx)
	recordError(span, data.Err)

	span.End()
}
