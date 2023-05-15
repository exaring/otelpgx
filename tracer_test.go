package otelpgx

import (
	"strings"
	"testing"
)

func TestSqlOperationName(t *testing.T) {
	tests := []struct {
		name         string
		query        string
		spanNameFunc func(string) string
		expName      string
	}{
		{
			name:         "Spaces only",
			query:        "SELECT * FROM users",
			spanNameFunc: nil,
			expName:      "SELECT",
		},
		{
			name:         "Newline and tab",
			query:        "UPDATE\n\tfoo",
			spanNameFunc: nil,
			expName:      "UPDATE",
		},
		{
			name:         "Additional whitespace",
			query:        " \n SELECT\n\t   *   FROM users  ",
			spanNameFunc: nil,
			expName:      "SELECT",
		},
		{
			name:         "Whitespace-only query",
			query:        " \n\t",
			spanNameFunc: nil,
			expName:      "UNKNOWN",
		},
		{
			name:         "Empty query",
			query:        "",
			spanNameFunc: nil,
			expName:      "UNKNOWN",
		},
		{
			name:         "Custom SQL name query",
			query:        "-- name: GetUsers :many\nSELECT * FROM users",
			spanNameFunc: defaultSpanNameFunc(),
			expName:      "GetUsers :many",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			name := sqlOperationName(tt.query, tt.spanNameFunc)
			if name != tt.expName {
				t.Errorf("Got name %q, expected %q", name, tt.expName)
			}
		})
	}
}

func defaultSpanNameFunc() SpanNameFunc {
	return func(query string) string {
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
				return sqlOperationUnknkown
			}

			part := strings.Split(strings.TrimSpace(line), " ")
			if prefix == "/*" {
				part = part[:len(part)-1] // removes the trailing "*/" element
			}
			if len(part) == 2 {
				return sqlOperationUnknkown
			}

			queryName := part[2]
			queryType := strings.TrimSpace(part[3])

			return queryName + " " + queryType
		}
		return sqlOperationUnknkown
	}
}
