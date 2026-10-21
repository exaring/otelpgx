package otelpgx

import (
	"context"
	"runtime"
	"strconv"
	"strings"
	"testing"
)

func TestSQLOperationNameBoundaries(t *testing.T) {
	for _, tc := range []struct {
		name string
		sql  string
		want string
	}{
		{"empty", "", "UNKNOWN"},
		{"whitespace", " \t\r\n\f\v", "UNKNOWN"},
		{"line comment only", "-- only", "UNKNOWN"},
		{"block comment only", "/* only */", "UNKNOWN"},
		{"nested comment only", "/* outer /* inner */ tail */", "UNKNOWN"},
		{"unterminated block comment", "/* unclosed", "UNKNOWN"},
		{"unterminated outer comment", "/* outer /* inner */", "UNKNOWN"},
		{"overlapping delimiters", "/*/*/ SELECT 1", "UNKNOWN"},
		{"adjacent nested delimiters", "/*/**/*/SELECT 1", "SELECT"},
		{"closed inner comment only", "/*/**/SELECT 1", "UNKNOWN"},
		{"semicolon boundary", "BEGIN;", "BEGIN"},
		{"parenthesis boundary", "SELECT(1)", "SELECT"},
		{"literal in parentheses", "SELECT('sensitive-example')", "SELECT"},
		{"quote boundary", "SELECT'sensitive-example'", "SELECT"},
		{"adjacent block comment", "SELECT/* comment */ 1", "SELECT"},
		{"adjacent line comment", "SELECT-- comment\n1", "SELECT"},
		{"lowercase", "select 1", "SELECT"},
		{"British spelling", "ANALYSE accounts", "ANALYSE"},
		{"mixed case British spelling", "aNaLySe accounts", "ANALYSE"},
		{"longest keyword", "DEALLOCATE example", "DEALLOCATE"},
		{"longest lowercase keyword", "checkpoint", "CHECKPOINT"},
		{"longest keyword with digit suffix", "CHECKPOINT1", "UNKNOWN"},
		{"overlong identifier", "DEALLOCATED", "UNKNOWN"},
		{"nested block comment", "/* outer /* inner */ tail */ SELECT 1", "SELECT"},
		{"CR line ending", "-- comment\rSELECT 1", "SELECT"},
		{"CRLF line ending", "-- comment\r\nSELECT 1", "SELECT"},
		{"LF line ending", "-- comment\nSELECT 1", "SELECT"},
		{"mixed comments", "/* a */-- b\r/* c */update t SET x=1", "UPDATE"},
		{"letter suffix", "SELECTidentifier", "UNKNOWN"},
		{"underscore suffix", "SELECT_1", "UNKNOWN"},
		{"digit suffix", "SELECT1", "UNKNOWN"},
		{"dollar suffix", "SELECT$identifier", "UNKNOWN"},
		{"non-ASCII suffix", "SELECTé", "UNKNOWN"},
		{"quoted word", "'SELECT'", "UNKNOWN"},
		{"quoted identifier", `"SELECT"`, "UNKNOWN"},
		{"number", "42", "UNKNOWN"},
		{"invalid UTF-8 prefix", "\xffSELECT 1", "UNKNOWN"},
		{"invalid UTF-8 suffix", "SELECT\xff", "UNKNOWN"},
		{"unknown token", "nonsense data", "UNKNOWN"},
		{"unknown token before keyword", "nonsense SELECT 1", "UNKNOWN"},
		{"partial keyword", "SEL", "UNKNOWN"},
		{"syntax is not validated", "SELECT (", "SELECT"},
		{"CTE", "WITH t AS (SELECT 1) SELECT * FROM t", "WITH"},
		{"explain", "EXPLAIN SELECT 1", "EXPLAIN"},
		{"multiple statements", "SELECT 1; DELETE FROM t", "SELECT"},
		{"long token", strings.Repeat("a", 1<<20), "UNKNOWN"},
		{"long literal", "SELECT('" + strings.Repeat("x", 1<<20) + "')", "SELECT"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := SQLOperationName(context.Background(), tc.sql); got != tc.want {
				t.Fatalf("got %.80q (len=%d), want %q", got, len(got), tc.want)
			}
		})
	}
}

func TestSQLOperationNameLongComments(t *testing.T) {
	ctx := context.Background()
	const query = "SELECT 1;\r\nDELETE FROM users"
	for _, size := range []int{0, 1, 59, 60, 61, 63, 64, 65, 251, 252, 253, 255, 256, 257, 315, 316, 317, 319, 320, 321, 1024, 64 << 10} {
		t.Run(strconv.Itoa(size), func(t *testing.T) {
			body := strings.Repeat("x", size)
			for _, ending := range []string{"\n", "\r", "\r\n"} {
				if got := SQLOperationName(ctx, "--"+body+ending+query); got != "SELECT" {
					t.Fatalf("line ending %q: got %q, want SELECT", ending, got)
				}
			}
			if got := SQLOperationName(ctx, "--"+body); got != "UNKNOWN" {
				t.Fatalf("unterminated line comment: got %q, want UNKNOWN", got)
			}
		})
	}
	for _, body := range []string{
		strings.Repeat("x", 64<<10),
		strings.Repeat("/", 64<<10),
		strings.Repeat("*", 64<<10),
		"outer /* " + strings.Repeat("x", 64<<10) + " */ tail",
		strings.Repeat("/*", 1024) + strings.Repeat("*/", 1024),
	} {
		if got := SQLOperationName(ctx, "/* "+body+" */SELECT 1"); got != "SELECT" {
			t.Fatalf("block body %.40q: got %q, want SELECT", body, got)
		}
	}
}

func FuzzSQLOperationName(f *testing.F) {
	for _, s := range []string{
		"SELECT(1)",
		"/* a /* b */ c */ select 1",
		"-- x\rSELECT 1",
		"\xff",
		"SELECT('secret')",
		"aNaLySe accounts",
		"SELECT$identifier",
		"SELECTé",
		"/*/*/ SELECT 1",
		"/*/**/*/SELECT 1",
		"--" + strings.Repeat("x", 320) + "\rSELECT 1",
	} {
		f.Add(s)
	}
	f.Fuzz(func(t *testing.T, sql string) {
		if len(sql) > 65536 {
			t.Skip()
		}
		got := SQLOperationName(context.Background(), sql)
		if len(got) == 0 || len(got) > len("CHECKPOINT") {
			t.Fatalf("unbounded result %q", got)
		}
		for _, c := range got {
			if c < 'A' || c > 'Z' {
				t.Fatalf("non-keyword result %q", got)
			}
		}
		if got := SQLOperationName(context.Background(), "SELECT("+sql+")"); got != "SELECT" {
			t.Fatalf("query body changed operation name to %q", got)
		}
		if got := SQLOperationName(context.Background(), "SELECT_"+sql); got != "UNKNOWN" {
			t.Fatalf("identifier prefix mistaken for keyword: %q", got)
		}
	})
}

type operationNameBenchmark struct {
	name, query string
}

func operationNameBenchmarks() []operationNameBenchmark {
	const query = "SELECT id FROM bar.foo WHERE id = $1"
	text4K := strings.Repeat("annotation ", 372)
	text64K := strings.Repeat("annotation ", 5958)
	return []operationNameBenchmark{
		{"plain", query},
		{"mixed case", "sElEcT id FROM bar.foo WHERE id = $1"},
		{"sqlc line comment", "-- name: GetFoo :one\n" + query},
		{"CR line comment", "-- name: GetFoo :one\r" + query},
		{"short CR with long query", "-- note\rSELECT('" + text64K + "')"},
		{"block comment", "/* name: GetFoo :one */\n" + query},
		{"nested block comment", "/* outer /* inner */ tail */ " + query},
		{"adjacent literal", "SELECT('sensitive-example')"},
		{"identifier", "SELECT_identifier"},
		{"long token", strings.Repeat("a", 64<<10)},
		{"long literal", "SELECT('" + strings.Repeat("x", 64<<10) + "')"},
		{"long line comment", "-- " + text64K + "\n" + query},
		{"long CR line comment", "-- " + text64K + "\r" + query},
		{"block 4KiB", "/* " + text4K + " */ " + query},
		{"block 64KiB", "/* " + text64K + " */ " + query},
		{"nested block 64KiB", "/* " + text64K + " /* inner */ tail */ " + query},
		{"inner block 64KiB", "/* outer /* " + text64K + " */ tail */ " + query},
		{"dense nesting", strings.Repeat("/*", 1024) + strings.Repeat("*/", 1024) + query},
		{"dense nesting after text", "/* outer x " + strings.Repeat("/*", 1024) + strings.Repeat("*/", 1024) + " */ " + query},
		{"dense stars", "/* " + strings.Repeat("*", 64<<10) + " */ " + query},
		{"dense slashes", "/* " + strings.Repeat("/", 64<<10) + " */ " + query},
		{"many comments", strings.Repeat("/* annotation */ ", 128) + query},
		{"unterminated block", "/* " + text64K},
	}
}

func BenchmarkSQLOperationName(b *testing.B) {
	ctx := context.Background()
	for _, bm := range operationNameBenchmarks() {
		b.Run(bm.name, func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				_ = SQLOperationName(ctx, bm.query)
			}
		})
	}
}

func BenchmarkSQLOperationNameParallel(b *testing.B) {
	ctx := context.Background()
	for _, bm := range operationNameBenchmarks() {
		if bm.name != "plain" && bm.name != "block 4KiB" && bm.name != "dense nesting" {
			continue
		}
		b.Run(bm.name, func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				var name string
				for pb.Next() {
					name = SQLOperationName(ctx, bm.query)
				}
				runtime.KeepAlive(name)
			})
		})
	}
}
