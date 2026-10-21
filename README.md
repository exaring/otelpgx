[![Go Reference](https://pkg.go.dev/badge/github.com/exaring/otelpgx.svg)](https://pkg.go.dev/github.com/exaring/otelpgx)

# otelpgx

Provides [OpenTelemetry](https://github.com/open-telemetry/opentelemetry-go)
instrumentation for the [jackc/pgx](https://github.com/jackc/pgx) library.

## Requirements

- go 1.25 (or higher)
- pgx v5 (or higher)

## Usage

Make sure you have a suitable pgx version:

```bash
go get github.com/jackc/pgx/v5
```

Install the library:

```go
go get github.com/exaring/otelpgx
```

Create the tracer as part of your connection:

```go
func run(ctx context.Context, connString string) (err error) {
    cfg, err := pgxpool.ParseConfig(connString)
    if err != nil {
        return fmt.Errorf("create connection pool: %w", err)
    }
    cfg.ConnConfig.Tracer = otelpgx.NewTracer()

    pool, err := pgxpool.NewWithConfig(ctx, cfg)
    if err != nil {
        return fmt.Errorf("connect to database: %w", err)
    }
    defer pool.Close()

    stopStats, err := otelpgx.RegisterStats(pool)
    if err != nil {
        return fmt.Errorf("unable to record database stats: %w", err)
    }
    // Deferred calls run in reverse order: unregister before closing the pool.
    defer func() { err = errors.Join(err, stopStats()) }()

    // Run application work using pool in this scope.
    return nil
}
```

`RegisterStats` returns an idempotent cleanup function. A collection already in
progress may finish before cleanup returns. `pool.Close()` does not unregister
statistics automatically. The source-compatible `RecordStats` API keeps its
registration for the lifetime of the meter provider.

See [options.go](options.go) for the full list of options.
