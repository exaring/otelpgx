//go:build go1.24
// +build go1.24

package otelpgx_test

import (
	"context"
	"os"
	"testing"

	"github.com/exaring/otelpgx"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

const query = `SELECT setting::int FROM pg_settings WHERE name = 'max_connections'`

func BenchmarkTracer(b *testing.B) {
	dsn := os.Getenv("DSN")
	if dsn == "" {
		b.Skip("DSN not set, skipping")
	}
	ctx := context.Background()
	config, err := pgxpool.ParseConfig(dsn)
	require.NoError(b, err, "parse pgx pool config")
	tracerProvider := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(tracetest.NewNoopExporter()),
		sdktrace.WithSampler(sdktrace.AlwaysSample()),
	)
	config.ConnConfig.Tracer = otelpgx.NewTracer(otelpgx.WithTracerProvider(tracerProvider))
	pool, err := pgxpool.NewWithConfig(ctx, config)
	require.NoError(b, err, "create pgx pool")
	b.Cleanup(pool.Close)

	tracer := tracerProvider.Tracer("otelpgx")
	ctx, span := tracer.Start(ctx, "query")
	defer span.End()

	var maxConns int32

	b.ReportAllocs()
	for b.Loop() {
		tx, err := pool.Begin(ctx)
		require.NoError(b, err, "begin transaction")
		if err := tx.QueryRow(ctx, query).Scan(&maxConns); err != nil {
			_ = tx.Rollback(ctx)
			b.Fatal(err)
		}
		require.NoError(b, tx.Rollback(ctx), "rollback transaction")
	}
}
