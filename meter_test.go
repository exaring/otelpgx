package otelpgx

import (
	"context"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
	"go.opentelemetry.io/otel/attribute"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	semconv "go.opentelemetry.io/otel/semconv/v1.40.0"
)

func TestRecordStats_UserAttrsOverrideLibraryDefaults(t *testing.T) {
	ctx := context.Background()

	cfg, err := pgxpool.ParseConfig("postgres://user@127.0.0.1:5432/somedb")
	if err != nil {
		t.Fatalf("pgxpool.ParseConfig: %v", err)
	}

	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatalf("pgxpool.NewWithConfig: %v", err)
	}
	t.Cleanup(pool.Close)

	reader := sdkmetric.NewManualReader()
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))

	const overridePoolName = "my-logical-pool"

	err = RecordStats(pool,
		WithStatsMeterProvider(provider),
		WithStatsAttributes(semconv.DBClientConnectionPoolName(overridePoolName)),
	)
	if err != nil {
		t.Fatalf("RecordStats: %v", err)
	}

	var rm metricdata.ResourceMetrics
	if err := reader.Collect(ctx, &rm); err != nil {
		t.Fatalf("reader.Collect: %v", err)
	}

	if len(rm.ScopeMetrics) == 0 {
		t.Fatal("expected at least one scope metric to be collected")
	}

	var checked int
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			for _, attrs := range dataPointAttributes(m.Data) {
				poolNameVal, ok := attrs.Value(semconv.DBClientConnectionPoolNameKey)
				if !ok {
					t.Errorf("metric %q: missing %s attribute", m.Name, semconv.DBClientConnectionPoolNameKey)
					continue
				}
				if poolNameVal.AsString() != overridePoolName {
					t.Errorf("metric %q: %s = %q, want user-supplied override %q",
						m.Name, semconv.DBClientConnectionPoolNameKey, poolNameVal.AsString(), overridePoolName)
				}

				// db.system.name should still carry the library default since the user did not override it.
				dbSystemVal, ok := attrs.Value(semconv.DBSystemNameKey)
				if !ok {
					t.Errorf("metric %q: missing %s attribute", m.Name, semconv.DBSystemNameKey)
				} else if dbSystemVal.AsString() != semconv.DBSystemNamePostgreSQL.Value.AsString() {
					t.Errorf("metric %q: %s = %q, want library default %q",
						m.Name, semconv.DBSystemNameKey, dbSystemVal.AsString(), semconv.DBSystemNamePostgreSQL.Value.AsString())
				}

				checked++
			}
		}
	}

	if checked == 0 {
		t.Fatal("collection produced no data points to verify attributes on")
	}
}

// dataPointAttributes returns the attribute set attached to every data point
// in the given aggregation, across the metric shapes that recordStats emits
// (Sum[int64] for counters/up-down counters, Gauge[int64] for the max gauge).
func dataPointAttributes(data metricdata.Aggregation) []attribute.Set {
	var sets []attribute.Set
	switch d := data.(type) {
	case metricdata.Sum[int64]:
		for _, dp := range d.DataPoints {
			sets = append(sets, dp.Attributes)
		}
	case metricdata.Gauge[int64]:
		for _, dp := range d.DataPoints {
			sets = append(sets, dp.Attributes)
		}
	}
	return sets
}
