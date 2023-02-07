package otelpgx

import (
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

// StatsOption allows for managing otelsql configuration using functional options.
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

// WithMeterProvider sets meter provider.
func WithMeterProvider(p metric.MeterProvider) StatsOption {
	return struct {
		statsOptionFunc
	}{
		statsOptionFunc: func(o *statsOptions) {
			o.meterProvider = p
		},
	}
}

// WithMinimumReadDBStatsInterval sets the minimum interval between calls to db.Stats(). Negative values are ignored.
func WithMinimumReadDBStatsInterval(interval time.Duration) StatsOption {
	return statsOptionFunc(func(o *statsOptions) {
		o.minimumReadDBStatsInterval = interval
	})
}
