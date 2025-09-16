[![Go Reference](https://pkg.go.dev/badge/github.com/exaring/otelpgx.svg)](https://pkg.go.dev/github.com/exaring/otelpgx)

# otelpgx

Provides [OpenTelemetry](https://github.com/open-telemetry/opentelemetry-go)
instrumentation for the [jackc/pgx](https://github.com/jackc/pgx) library.

## Requirements

- go 1.22 (or higher)
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
cfg, err := pgxpool.ParseConfig(connString)
if err != nil {
    return nil, fmt.Errorf("create connection pool: %w", err)
}

cfg.ConnConfig.Tracer = otelpgx.NewTracer()

conn, err := pgxpool.NewWithConfig(ctx, cfg)
if err != nil {
    return nil, fmt.Errorf("connect to database: %w", err)
}

if err := otelpgx.RecordStats(conn); err != nil {
    return nil, fmt.Errorf("unable to record database stats: %w", err)
}
```

See [options.go](options.go) for the full list of options.


## Provided Metrics

The following metrics are provided by this library:

| *Metric Name*                      | *Description*                                                                                                                               |
|------------------------------------|---------------------------------------------------------------------------------------------------------------------------------------------|
| `pgxpool.acquire_duration`         | Total duration of all successful acquires from the pool in nanoseconds.                                                                     |
| `pgxpool.acquired_connections`     | Number of currently acquired connections in the pool.                                                                                       |
| `pgxpool.acquires`                 | Cumulative count of successful acquires from the pool.                                                                                      |
| `pgxpool.canceled_acquires`        | Cumulative count of acquires from the pool that were canceled by a context.                                                                 |
| `pgxpool.constructing_connections` | Number of connections with construction in progress in the pool.                                                                            |
| `pgxpool.empty_acquire`            | Cumulative count of successful acquires from the pool that waited for a resource to be released or constructed because the pool was empty.  |
| `pgxpool.empty_acquire_wait_time`  | Total time waited for successful acquires from the pool for a resource to be released or constructed because the pool was empty.            |
| `pgxpool.idle_connections`         | Number of currently idle connections in the pool.                                                                                           |
| `pgxpool.max_connections`          | Maximum size of the pool.                                                                                                                   |
| `pgxpool.max_idle_destroys`        | Cumulative count of connections destroyed because they exceeded MaxConnectionsIdleTime.                                                     |
| `pgxpool.max_lifetime_destroys`    | Cumulative count of connections destroyed because they exceeded MaxConnectionsLifetime.                                                     |
| `pgxpool.new_connections`          | Cumulative count of new connections opened.                                                                                                 |
| `pgxpool.total_connections`        | Total number of resources currently in the pool. The value is the sum of ConstructingConnections, AcquiredConnections, and IdleConnections. |

See also [`meter.go`](meter.go) for a complete and up-to-date list of metrics.
