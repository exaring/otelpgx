package otelpgx

import (
	"context"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"
)

// Option specifies instrumentation configuration options.
type Option interface {
	apply(*tracerConfig)
}

type optionFunc func(*tracerConfig)

func (o optionFunc) apply(c *tracerConfig) {
	o(c)
}

// WithTracerProvider specifies a tracer provider to use for creating a tracer.
// If none is specified, the global provider is used.
func WithTracerProvider(provider trace.TracerProvider) Option {
	return optionFunc(func(cfg *tracerConfig) {
		if provider != nil {
			cfg.tracerProvider = provider
		}
	})
}

// WithMeterProvider specifies a meter provider to use for creating a meter.
// If none is specified, the global provider is used.
func WithMeterProvider(provider metric.MeterProvider) Option {
	return optionFunc(func(cfg *tracerConfig) {
		if provider != nil {
			cfg.meterProvider = provider
		}
	})
}

// Deprecated: Use WithTracerAttributes.
//
// WithAttributes specifies additional attributes to be added to spans.
// This is exactly equivalent to using WithTracerAttributes.
func WithAttributes(attrs ...attribute.KeyValue) Option {
	return WithTracerAttributes(attrs...)
}

// WithTracerAttributes specifies additional attributes to be added to spans.
func WithTracerAttributes(attrs ...attribute.KeyValue) Option {
	return optionFunc(func(cfg *tracerConfig) {
		cfg.tracerAttrs = append(cfg.tracerAttrs, attrs...)
	})
}

// WithMeterAttributes specifies additional attributes to be added to metrics.
func WithMeterAttributes(attrs ...attribute.KeyValue) Option {
	return optionFunc(func(cfg *tracerConfig) {
		cfg.meterAttrs = append(cfg.meterAttrs, attrs...)
	})
}

// Deprecated: This is now the default behavior; use [WithFullSQLInSpanName] to
// opt back into the previous behavior of using the whole SQL statement.
//
// WithTrimSQLInSpanName uses the SQL statement's first word (the operation
// name, e.g. "SELECT") as the span name.
func WithTrimSQLInSpanName() Option {
	return optionFunc(func(cfg *tracerConfig) {
		cfg.fullQuerySpanName = false
	})
}

// WithFullSQLInSpanName uses the whole SQL statement as the span name.
//
// This is generally discouraged: the OpenTelemetry database span conventions
// recommend a low-cardinality span name, and redaction/masking rules are
// typically applied to the db.query.text attribute rather than the span name,
// so any sensitive data embedded in the statement can leak through the name.
// By default, the low-cardinality operation name (e.g. "SELECT") is used
// instead.
//
// See https://opentelemetry.io/docs/specs/semconv/db/database-spans/.
func WithFullSQLInSpanName() Option {
	return optionFunc(func(cfg *tracerConfig) {
		cfg.fullQuerySpanName = true
	})
}

// WithDisableAcquireTracer disables tracing for connection acquire events from
// the connection pool. By default, acquire tracing is enabled.
func WithDisableAcquireTracer() Option {
	return optionFunc(func(cfg *tracerConfig) {
		cfg.disableAcquireTracer = true
	})
}

// SpanNameFunc is a function that can be used to generate a span name for a
// SQL. The function will be called with the SQL statement as a parameter.
type SpanNameFunc func(stmt string) string

// SpanNameCtxFunc is a function that can be used to generate a span name for a
// SQL. The function will be called with the context.Context and SQL statement
// as a parameter.
type SpanNameCtxFunc func(ctx context.Context, stmt string) string

// WithSpanNameFunc will use the provided function to generate the span name for
// a SQL statement. The function will be called with the SQL statement as a
// parameter.
//
// By default, the low-cardinality operation name (e.g. "SELECT") is extracted
// from the SQL statement and used as the span name. This function also
// determines the value of the db.operation.name attribute.
func WithSpanNameFunc(fn SpanNameFunc) Option {
	return WithSpanNameCtxFunc(func(_ context.Context, stmt string) string {
		return fn(stmt)
	})
}

// WithSpanNameCtxFunc will use the provided function to generate the span name
// for a SQL statement. The function will be called with the context.Context and
// SQL statement as a parameter.
//
// By default, the low-cardinality operation name (e.g. "SELECT") is extracted
// from the SQL statement and used as the span name. This function also
// determines the value of the db.operation.name attribute.
func WithSpanNameCtxFunc(fn SpanNameCtxFunc) Option {
	return optionFunc(func(cfg *tracerConfig) {
		cfg.spanNameCtxFunc = fn
	})
}

// Deprecated: Span names are no longer prefixed by default, so this is a no-op.
// Use [WithQuerySpanNamePrefix] to opt back into the previous prefixing
// behavior.
//
// WithDisableQuerySpanNamePrefix disables the prefix for the span name.
func WithDisableQuerySpanNamePrefix() Option {
	return optionFunc(func(cfg *tracerConfig) {
		cfg.prefixQuerySpanName = false
	})
}

// WithQuerySpanNamePrefix prefixes the span name with the operation kind, i.e.
// "query ", "prepare " or "batch query ". By default no prefix is added so that
// span names follow the OpenTelemetry database span conventions.
func WithQuerySpanNamePrefix() Option {
	return optionFunc(func(cfg *tracerConfig) {
		cfg.prefixQuerySpanName = true
	})
}

// WithDisableConnectionDetailsInAttributes will disable logging the connection details.
// in the span's attributes.
func WithDisableConnectionDetailsInAttributes() Option {
	return optionFunc(func(cfg *tracerConfig) {
		cfg.logConnectionDetails = false
	})
}

// WithDisableSQLStatementInAttributes will disable logging the SQL statement in the span's
// attributes.
func WithDisableSQLStatementInAttributes() Option {
	return optionFunc(func(cfg *tracerConfig) {
		cfg.logSQLStatement = false
	})
}

// QueryParametersFilterFunc is a predicate that controls whether query parameters are recorded
// for a given SQL statement. Return true to record parameters, false to omit them.
type QueryParametersFilterFunc func(sql string) bool

// WithIncludeQueryParameters includes the SQL query parameters in the span attribute with key pgx.query.parameters.
// This is implicitly disabled if WithDisableSQLStatementInAttributes is used.
//
// An optional filter can be provided to omit parameters for specific queries.
func WithIncludeQueryParameters(filter ...QueryParametersFilterFunc) Option {
	return optionFunc(func(cfg *tracerConfig) {
		cfg.includeParams = true
		if len(filter) > 0 {
			cfg.queryParamsFilter = filter[0]
		}
	})
}

// StatsOption allows for managing RecordStats configuration using functional options.
type StatsOption interface {
	applyStatsOptions(o *statsOptions)
}

type statsOptions struct {
	// meterProvider sets the metric.MeterProvider. If nil, the global Provider will be used.
	meterProvider metric.MeterProvider

	// minimumReadDBStatsInterval sets the minimum interval between calls to db.Stats(). Negative values are ignored.
	minimumReadDBStatsInterval time.Duration

	// defaultAttributes will be set to each metrics as default.
	defaultAttributes []attribute.KeyValue
}

type statsOptionFunc func(o *statsOptions)

func (f statsOptionFunc) applyStatsOptions(o *statsOptions) {
	f(o)
}

// WithStatsMeterProvider sets meter provider to use for pgx stat metric collection.
func WithStatsMeterProvider(provider metric.MeterProvider) StatsOption {
	return statsOptionFunc(func(o *statsOptions) {
		o.meterProvider = provider
	})
}

// WithStatsAttributes specifies additional attributes to be added to pgx stat metrics.
func WithStatsAttributes(attrs ...attribute.KeyValue) StatsOption {
	return statsOptionFunc(func(o *statsOptions) {
		o.defaultAttributes = append(o.defaultAttributes, attrs...)
	})
}

// WithMinimumReadDBStatsInterval sets the minimum interval between calls to db.Stats(). Negative values are ignored.
func WithMinimumReadDBStatsInterval(interval time.Duration) StatsOption {
	return statsOptionFunc(func(o *statsOptions) {
		o.minimumReadDBStatsInterval = interval
	})
}
