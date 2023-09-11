package otelpgx

import (
	"strings"
	"testing"
)

func TestTracer_sqlOperationName(t *testing.T) {
	tests := []struct {
		name    string
		tracer  *Tracer
		query   string
		expName string
	}{
		{
			name:    "Spaces only",
			query:   "SELECT * FROM users",
			tracer:  NewTracer(),
			expName: "SELECT",
		},
		{
			name:    "Newline and tab",
			query:   "UPDATE\n\tfoo",
			tracer:  NewTracer(),
			expName: "UPDATE",
		},
		{
			name:    "Additional whitespace",
			query:   " \n SELECT\n\t   *   FROM users  ",
			tracer:  NewTracer(),
			expName: "SELECT",
		},
		{
			name:    "Whitespace-only query",
			query:   " \n\t",
			tracer:  NewTracer(),
			expName: sqlOperationUnknown,
		},
		{
			name:    "Empty query",
			query:   "",
			tracer:  NewTracer(),
			expName: sqlOperationUnknown,
		},
		{
			name:    "Functional span name (-- comment style)",
			query:   "-- name: GetUsers :many\nSELECT * FROM users",
			tracer:  NewTracer(WithSpanNameFunc(defaultSpanNameFunc)),
			expName: "GetUsers :many",
		},
		{
			name:    "Functional span name (/**/ comment style)",
			query:   "/* name: GetBooks :many */\nSELECT * FROM books",
			tracer:  NewTracer(WithSpanNameFunc(defaultSpanNameFunc)),
			expName: "GetBooks :many",
		},
		{
			name:    "Functional span name (# comment style)",
			query:   "# name: GetRecords :many\nSELECT * FROM records",
			tracer:  NewTracer(WithSpanNameFunc(defaultSpanNameFunc)),
			expName: "GetRecords :many",
		},
		{
			name:    "Functional span name (no annotation)",
			query:   "--\nSELECT * FROM user",
			tracer:  NewTracer(WithSpanNameFunc(defaultSpanNameFunc)),
			expName: sqlOperationUnknown,
		},
		{
			name:    "Custom SQL name query (normal comment)",
			query:   "-- foo \nSELECT * FROM users",
			tracer:  NewTracer(WithSpanNameFunc(defaultSpanNameFunc)),
			expName: sqlOperationUnknown,
		},
		{
			name:    "Custom SQL name query (invalid formatting)",
			query:   "foo \nSELECT * FROM users",
			tracer:  NewTracer(WithSpanNameFunc(defaultSpanNameFunc)),
			expName: sqlOperationUnknown,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tr := tt.tracer
			if got := tr.sqlOperationName(tt.query); got != tt.expName {
				t.Errorf("Tracer.sqlOperationName() = %v, want %v", got, tt.expName)
			}
		})
	}
}

// defaultSpanNameFunc is an utility function for testing that attempts to get
// the first name of the query from a given SQL statement.
var defaultSpanNameFunc SpanNameFunc = func(query string) string {
	for _, line := range strings.Split(query, "\n") {
		var prefix string
		switch {
		case strings.HasPrefix(line, "--"):
			prefix = "--"

		case strings.HasPrefix(line, "/*"):
			prefix = "/*"

		case strings.HasPrefix(line, "#"):
			prefix = "#"
		default:
			continue
		}

		rest := line[len(prefix):]
		if !strings.HasPrefix(strings.TrimSpace(rest), "name") {
			continue
		}
		if !strings.Contains(rest, ":") {
			continue
		}
		if !strings.HasPrefix(rest, " name: ") {
			return sqlOperationUnknown
		}

		part := strings.Split(strings.TrimSpace(line), " ")
		if prefix == "/*" {
			part = part[:len(part)-1] // removes the trailing "*/" element
		}
		if len(part) == 2 {
			return sqlOperationUnknown
		}

		queryName := part[2]
		queryType := strings.TrimSpace(part[3])

		return queryName + " " + queryType
	}
	return sqlOperationUnknown
}
