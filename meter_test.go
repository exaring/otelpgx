package otelpgx

import (
	"context"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
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
// (Sum[int64] for counters/up-down counters, Gauge[int64] for the max gauge)
// and the shape Tracer's db.client.operation.duration histogram emits
// (Histogram[float64]).
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
	case metricdata.Histogram[float64]:
		for _, dp := range d.DataPoints {
			sets = append(sets, dp.Attributes)
		}
	}
	return sets
}

type countingStatsPool struct {
	*pgxpool.Pool
	reads            atomic.Int64
	entered, release chan struct{}
	once             sync.Once
}

func (p *countingStatsPool) Stat() *pgxpool.Stat {
	p.reads.Add(1)
	if p.entered != nil {
		p.once.Do(func() { close(p.entered); <-p.release })
	}
	return p.Pool.Stat()
}

func newStatsPool(t *testing.T) *countingStatsPool {
	t.Helper()
	p, err := pgxpool.New(context.Background(), "postgres://user@127.0.0.1/db?sslmode=disable")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(p.Close)
	return &countingStatsPool{Pool: p}
}

func TestRegisterStats_lifecycle(t *testing.T) {
	ctx := context.Background()
	r := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(r))
	shutdown := sync.OnceValue(func() error { return mp.Shutdown(ctx) })
	t.Cleanup(func() {
		if err := shutdown(); err != nil {
			t.Error(err)
		}
	})
	pools := []*countingStatsPool{newStatsPool(t), newStatsPool(t)}
	stops := make([]func() error, 2)
	for i, p := range pools {
		var err error
		stops[i], err = RegisterStats(p, WithStatsMeterProvider(mp), WithMinimumReadDBStatsInterval(0), WithStatsAttributes(attribute.Int("pool", i)))
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() {
			if err := stops[i](); err != nil {
				t.Error(err)
			}
		})
	}
	var rm metricdata.ResourceMetrics
	if err := r.Collect(ctx, &rm); err != nil {
		t.Fatal(err)
	}
	if err := stops[0](); err != nil {
		t.Fatal(err)
	}
	before := pools[0].reads.Load()
	var wg sync.WaitGroup
	for range 4 {
		wg.Go(func() {
			if err := stops[0](); err != nil {
				t.Error(err)
			}
		})
	}
	wg.Wait()
	if err := r.Collect(ctx, &rm); err != nil {
		t.Fatal(err)
	}
	if pools[0].reads.Load() != before || pools[1].reads.Load() != 2 {
		t.Fatal("registrations are not independent")
	}
	points := 0
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			attrs := dataPointAttributes(m.Data)
			if len(attrs) != 1 {
				t.Errorf("metric %q: got %d points, want only the active pool", m.Name, len(attrs))
			}
			for _, attrs := range attrs {
				v, ok := attrs.Value("pool")
				if !ok || v.AsInt64() != 1 {
					t.Errorf("metric %q: got pool attribute %v, want 1", m.Name, v)
				}
				points++
			}
		}
	}
	if points != 13 {
		t.Fatalf("got %d stats series, want 13", points)
	}
	if err := shutdown(); err != nil {
		t.Fatal(err)
	}
	if err := stops[1](); err != nil {
		t.Fatal(err)
	}
}

func TestRegisterStats_unregisterDuringCollection(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	r := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(r))
	t.Cleanup(func() {
		if err := mp.Shutdown(context.Background()); err != nil {
			t.Error(err)
		}
	})
	p := newStatsPool(t)
	p.entered, p.release = make(chan struct{}), make(chan struct{})
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(p.release) }) }
	defer release()
	unregistering := make(chan struct{})
	meter := mp.Meter("test")
	provider := statsTestProvider{meter: statsTestMeter{
		Meter: meter,
		register: func(cb metric.Callback, instruments ...metric.Observable) (metric.Registration, error) {
			registration, err := meter.RegisterCallback(cb, instruments...)
			if err != nil {
				return nil, err
			}
			return statsTestRegistration{unregister: func() error {
				close(unregistering)
				return registration.Unregister()
			}}, nil
		},
	}}
	stop, err := RegisterStats(p, WithStatsMeterProvider(provider))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := stop(); err != nil {
			t.Error(err)
		}
	})
	collected := make(chan error, 1)
	go func() {
		var rm metricdata.ResourceMetrics
		collected <- r.Collect(ctx, &rm)
	}()
	select {
	case <-p.entered:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	stopped := make(chan error, 1)
	go func() { stopped <- stop() }()
	select {
	case <-unregistering:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	release()
	for _, ch := range []chan error{collected, stopped} {
		select {
		case err := <-ch:
			if err != nil {
				t.Fatal(err)
			}
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}
	}
	before := p.reads.Load()
	var rm metricdata.ResourceMetrics
	if err := r.Collect(ctx, &rm); err != nil {
		t.Fatal(err)
	}
	if p.reads.Load() != before {
		t.Fatal("callback called after cleanup")
	}
}

// statsTestProvider supplies a meter whose registration behavior a test controls.
type statsTestProvider struct {
	metric.MeterProvider
	meter metric.Meter
}

func (p statsTestProvider) Meter(string, ...metric.MeterOption) metric.Meter { return p.meter }

type statsTestMeter struct {
	metric.Meter
	register      func(metric.Callback, ...metric.Observable) (metric.Registration, error)
	failName      string
	instrumentErr error
}

func (m statsTestMeter) RegisterCallback(cb metric.Callback, instruments ...metric.Observable) (metric.Registration, error) {
	return m.register(cb, instruments...)
}

func (m statsTestMeter) Int64ObservableCounter(name string, opts ...metric.Int64ObservableCounterOption) (metric.Int64ObservableCounter, error) {
	if name == m.failName {
		return nil, m.instrumentErr
	}
	return m.Meter.Int64ObservableCounter(name, opts...)
}

func (m statsTestMeter) Int64ObservableUpDownCounter(name string, opts ...metric.Int64ObservableUpDownCounterOption) (metric.Int64ObservableUpDownCounter, error) {
	if name == m.failName {
		return nil, m.instrumentErr
	}
	return m.Meter.Int64ObservableUpDownCounter(name, opts...)
}

func (m statsTestMeter) Int64ObservableGauge(name string, opts ...metric.Int64ObservableGaugeOption) (metric.Int64ObservableGauge, error) {
	if name == m.failName {
		return nil, m.instrumentErr
	}
	return m.Meter.Int64ObservableGauge(name, opts...)
}

type statsTestRegistration struct {
	metric.Registration
	unregister func() error
}

func (r statsTestRegistration) Unregister() error { return r.unregister() }

type discardStatsObserver struct{ metric.Observer }

func (discardStatsObserver) ObserveInt64(metric.Int64Observable, int64, ...metric.ObserveOption) {}
func (discardStatsObserver) ObserveFloat64(metric.Float64Observable, float64, ...metric.ObserveOption) {
}

func TestRecordStats_synchronousRegistration(t *testing.T) {
	mp := sdkmetric.NewMeterProvider()
	t.Cleanup(func() {
		if err := mp.Shutdown(context.Background()); err != nil {
			t.Error(err)
		}
	})
	p := newStatsPool(t)
	provider := statsTestProvider{meter: statsTestMeter{
		Meter: mp.Meter("test"),
		register: func(cb metric.Callback, _ ...metric.Observable) (metric.Registration, error) {
			if err := cb(context.Background(), discardStatsObserver{}); err != nil {
				return nil, err
			}
			return statsTestRegistration{unregister: func() error { return nil }}, nil
		},
	}}
	done := make(chan error, 1)
	go func() { done <- RecordStats(p, WithStatsMeterProvider(provider)) }()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
		if got := p.reads.Load(); got != 1 {
			t.Errorf("Stat called %d times, want 1", got)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("synchronous registration callback deadlocked")
	}
}

func TestRegisterStats_registrationErrors(t *testing.T) {
	registrationErr := errors.New("registration failed")
	unregisterErr := errors.New("unregister failed")
	tests := []struct {
		name          string
		partial       bool
		unregisterErr error
	}{
		{name: "no registration"},
		{name: "partial registration", partial: true},
		{name: "partial registration with failed unregister", partial: true, unregisterErr: unregisterErr},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mp := sdkmetric.NewMeterProvider()
			t.Cleanup(func() {
				if err := mp.Shutdown(context.Background()); err != nil {
					t.Error(err)
				}
			})
			p := newStatsPool(t)
			var callback metric.Callback
			var unregistered int
			provider := statsTestProvider{meter: statsTestMeter{
				Meter: mp.Meter("test"),
				register: func(cb metric.Callback, _ ...metric.Observable) (metric.Registration, error) {
					callback = cb
					if err := cb(context.Background(), discardStatsObserver{}); err != nil {
						return nil, err
					}
					if !tt.partial {
						return nil, registrationErr
					}
					return statsTestRegistration{unregister: func() error {
						unregistered++
						return tt.unregisterErr
					}}, registrationErr
				},
			}}
			stop, err := RegisterStats(p, WithStatsMeterProvider(provider), WithMinimumReadDBStatsInterval(0))
			if stop != nil {
				t.Fatal("unexpected cleanup function after failed registration")
			}
			if !errors.Is(err, registrationErr) {
				t.Errorf("got error %v, want registration error", err)
			}
			if tt.unregisterErr != nil && !errors.Is(err, tt.unregisterErr) {
				t.Errorf("got error %v, want unregister error too", err)
			}
			wantUnregistered := 0
			if tt.partial {
				wantUnregistered = 1
			}
			if unregistered != wantUnregistered {
				t.Errorf("Unregister called %d times, want %d", unregistered, wantUnregistered)
			}
			if p.reads.Load() != 1 {
				t.Fatal("synchronous callback did not read pool statistics")
			}
			if tt.unregisterErr != nil {
				// The backend could not remove this callback. It must no longer
				// observe the pool, since the caller has no cleanup handle.
				if err := callback(context.Background(), discardStatsObserver{}); err != nil {
					t.Fatal(err)
				}
				if p.reads.Load() != 1 {
					t.Fatal("callback still reads pool after failed registration")
				}
			}
		})
	}
}

func TestRegisterStats_cleanupRetry(t *testing.T) {
	mp := sdkmetric.NewMeterProvider()
	t.Cleanup(func() {
		if err := mp.Shutdown(context.Background()); err != nil {
			t.Error(err)
		}
	})
	p := newStatsPool(t)
	unregisterErr := errors.New("unregister failed")
	var callback metric.Callback
	var attempts atomic.Int64
	provider := statsTestProvider{meter: statsTestMeter{
		Meter: mp.Meter("test"),
		register: func(cb metric.Callback, _ ...metric.Observable) (metric.Registration, error) {
			callback = cb
			return statsTestRegistration{unregister: func() error {
				if attempts.Add(1) == 1 {
					return unregisterErr
				}
				return nil
			}}, nil
		},
	}}
	stop, err := RegisterStats(p, WithStatsMeterProvider(provider), WithMinimumReadDBStatsInterval(0))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := stop(); err != nil {
			t.Error(err)
		}
	})
	if err := stop(); !errors.Is(err, unregisterErr) {
		t.Fatalf("got error %v, want unregister error", err)
	}
	if err := callback(context.Background(), discardStatsObserver{}); err != nil {
		t.Fatal(err)
	}
	if p.reads.Load() != 1 {
		t.Fatal("failed cleanup stopped collection before unregister succeeded")
	}
	var wg sync.WaitGroup
	for range 8 {
		wg.Go(func() {
			if err := stop(); err != nil {
				t.Error(err)
			}
		})
	}
	wg.Wait()
	if attempts.Load() != 2 {
		t.Errorf("Unregister called %d times, want failed attempt and one successful retry", attempts.Load())
	}
	if err := callback(context.Background(), discardStatsObserver{}); err != nil {
		t.Fatal(err)
	}
	if p.reads.Load() != 1 {
		t.Fatal("callback still reads pool after successful cleanup")
	}
}

func TestRegisterStats_instrumentErrors(t *testing.T) {
	for _, name := range []string{
		"pgxpool.acquires", "pgxpool.acquire_duration", "pgxpool.acquired_connections",
		"pgxpool.canceled_acquires", "pgxpool.constructing_connections", "pgxpool.empty_acquire",
		"pgxpool.idle_connections", "pgxpool.max_connections", "pgxpool.max_idle_destroys",
		"pgxpool.max_lifetime_destroys", "pgxpool.new_connections", "pgxpool.total_connections",
		"pgxpool.empty_acquire_wait_time",
	} {
		t.Run(name, func(t *testing.T) {
			mp := sdkmetric.NewMeterProvider()
			t.Cleanup(func() {
				if err := mp.Shutdown(context.Background()); err != nil {
					t.Error(err)
				}
			})
			p := newStatsPool(t)
			boom := errors.New("instrument failed")
			provider := statsTestProvider{meter: statsTestMeter{
				Meter: mp.Meter("test"), failName: name, instrumentErr: boom,
				register: func(metric.Callback, ...metric.Observable) (metric.Registration, error) {
					t.Error("registered callback after instrument creation failed")
					return nil, nil
				},
			}}
			stop, err := RegisterStats(p, WithStatsMeterProvider(provider))
			if stop != nil {
				t.Error("unexpected cleanup function on instrument error")
			}
			if !errors.Is(err, boom) || !strings.Contains(err.Error(), name) {
				t.Errorf("got error %v, want wrapped instrument error for %s", err, name)
			}
			if p.reads.Load() != 0 {
				t.Error("read pool statistics despite instrument creation error")
			}
		})
	}
}
