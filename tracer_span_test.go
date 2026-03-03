package otelpgx

import (
	"context"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

func setupTestTracer(t *testing.T) (*Tracer, *tracetest.InMemoryExporter, *sdktrace.TracerProvider) {
	t.Helper()

	exporter := tracetest.NewInMemoryExporter()
	tp := sdktrace.NewTracerProvider(
		sdktrace.WithSyncer(exporter),
		sdktrace.WithSampler(sdktrace.AlwaysSample()),
	)

	tracer := NewTracer(WithTracerProvider(tp))
	return tracer, exporter, tp
}

func TestTraceAcquire_NoSpan(t *testing.T) {
	tracer, exporter, tp := setupTestTracer(t)

	parentTracer := tp.Tracer("test")
	ctx, parentSpan := parentTracer.Start(context.Background(), "parent")

	ctx = tracer.TraceAcquireStart(ctx, nil, pgxpool.TraceAcquireStartData{})
	tracer.TraceAcquireEnd(ctx, nil, pgxpool.TraceAcquireEndData{})

	parentSpan.End()

	spans := exporter.GetSpans()

	for _, s := range spans {
		if s.Name == "pool.acquire" {
			t.Error("pool.acquire span should not be created")
		}
	}

	if len(spans) != 1 {
		t.Errorf("expected exactly 1 span (the parent), got %d", len(spans))
	}
	if spans[0].Name != "parent" {
		t.Errorf("expected span name 'parent', got %q", spans[0].Name)
	}
}

func TestTracePrepare_NoSpan_SetsAttribute(t *testing.T) {
	tracer, exporter, tp := setupTestTracer(t)

	parentTracer := tp.Tracer("test")
	ctx, parentSpan := parentTracer.Start(context.Background(), "query SELECT 1")

	ctx = tracer.TracePrepareStart(ctx, nil, pgx.TracePrepareStartData{
		Name: "stmt1",
		SQL:  "SELECT 1",
	})
	tracer.TracePrepareEnd(ctx, nil, pgx.TracePrepareEndData{})

	parentSpan.End()

	spans := exporter.GetSpans()

	for _, s := range spans {
		if s.Name == "prepare SELECT 1" || s.Name == "SELECT 1" {
			t.Errorf("prepare span should not be created, found span %q", s.Name)
		}
	}

	if len(spans) != 1 {
		t.Errorf("expected exactly 1 span (the parent), got %d", len(spans))
	}

	parentAttrs := spans[0].Attributes
	var found bool
	for _, attr := range parentAttrs {
		if attr.Key == PrepareDurationKey {
			found = true
			if attr.Value.Type().String() != "INT64" {
				t.Errorf("expected pgx.prepare.duration to be INT64, got %s", attr.Value.Type())
			}
			if attr.Value.AsInt64() < 0 {
				t.Errorf("expected pgx.prepare.duration >= 0, got %d", attr.Value.AsInt64())
			}
		}
	}

	if !found {
		t.Error("expected pgx.prepare.duration attribute on parent span, but not found")
	}
}
