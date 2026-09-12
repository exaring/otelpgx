package otelpgx

import (
	"context"
	"strings"
	"unicode"
)

// defaultSpanNameCtxFunc attempts to get the first 'word' from a given SQL query, which usually
// is the operation name (e.g. 'SELECT'). Leading SQL comments are skipped.
func defaultSpanNameCtxFunc(_ context.Context, stmt string) string {
	for word := range strings.FieldsSeq(skipLeadingComments(stmt)) {
		return strings.ToUpper(word)
	}

	return sqlOperationUnknown
}

// skipLeadingComments strips leading whitespace, "--" line comments and "/* */"
// block comments (not nested); an unterminated block comment yields "". Needed
// because tools like sqlc prefix every query with a "-- name: ..." comment.
func skipLeadingComments(stmt string) string {
	for {
		stmt = strings.TrimLeftFunc(stmt, unicode.IsSpace)
		switch {
		case strings.HasPrefix(stmt, "--"):
			_, rest, ok := strings.Cut(stmt, "\n")
			if !ok {
				return ""
			}
			stmt = rest
		case strings.HasPrefix(stmt, "/*"):
			_, rest, ok := strings.Cut(stmt[2:], "*/")
			if !ok {
				return ""
			}
			stmt = rest
		default:
			return stmt
		}
	}
}
