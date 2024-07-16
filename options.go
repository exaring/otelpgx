package otelpgx

import (
	"context"

	"go.opentelemetry.io/otel/attribute"
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
			cfg.tp = provider
		}
	})
}

// WithAttributes specifies additional attributes to be added to the span.
func WithAttributes(attrs ...attribute.KeyValue) Option {
	return optionFunc(func(cfg *tracerConfig) {
		cfg.attrs = append(cfg.attrs, attrs...)
	})
}

// WithTrimSQLInSpanName will use the SQL statement's first word as the span
// name. By default, the whole SQL statement is used as a span name, where
// applicable.
func WithTrimSQLInSpanName() Option {
	return optionFunc(func(cfg *tracerConfig) {
		cfg.trimQuerySpanName = true
	})
}

// SpanNameFunc is a function that can be used to generate a span name for a
// SQL. The function will be called with the current context and the SQL statement as a parameter.
type SpanNameFunc func(ctx context.Context, stmt string) string

// WithSpanNameFunc will use the provided function to generate the span name for
// a SQL statement. The function will be called with the SQL statement as a
// parameter.
//
// By default, the whole SQL statement is used as a span name, where applicable.
func WithSpanNameFunc(fn SpanNameFunc) Option {
	return optionFunc(func(cfg *tracerConfig) {
		cfg.spanNameFunc = fn
	})
}

// WithDisableQuerySpanNamePrefix will disable the default prefix for the span
// name. By default, the span name is prefixed with "batch query" or "query".
func WithDisableQuerySpanNamePrefix() Option {
	return optionFunc(func(cfg *tracerConfig) {
		cfg.prefixQuerySpanName = false
	})
}

// WithDisableSQLStatementInAttributes will disable logging the SQL statement in the span's
// attributes.
func WithDisableSQLStatementInAttributes() Option {
	return optionFunc(func(cfg *tracerConfig) {
		cfg.logSQLStatement = false
	})
}

// WithIncludeQueryParameters includes the SQL query parameters in the span attribute with key pgx.query.parameters.
// This is implicitly disabled if WithDisableSQLStatementInAttributes is used.
func WithIncludeQueryParameters() Option {
	return optionFunc(func(cfg *tracerConfig) {
		cfg.includeParams = true
	})
}
