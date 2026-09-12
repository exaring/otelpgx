package otelpgx

import (
	"context"
	"testing"
)

func BenchmarkDefaultSpanNameCtxFunc(b *testing.B) {
	benchmarks := []struct {
		name  string
		query string
	}{
		{
			name:  "plain",
			query: "SELECT id FROM bar.foo WHERE id = $1",
		},
		{
			name:  "sqlc line comment",
			query: "-- name: GetFoo :one\nSELECT id FROM bar.foo WHERE id = $1",
		},
		{
			name:  "block comment",
			query: "/* name: GetFoo :one */\nSELECT id FROM bar.foo WHERE id = $1",
		},
	}
	ctx := context.Background()
	for _, bm := range benchmarks {
		b.Run(bm.name, func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				_ = defaultSpanNameCtxFunc(ctx, bm.query)
			}
		})
	}
}
