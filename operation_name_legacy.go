//go:build !go1.24
// +build !go1.24

package otelpgx

import (
	"context"
	"strings"
	"unicode"
)

// defaultSpanNameCtxFunc attempts to get the first 'word' from a given SQL query, which usually
// is the operation name (e.g. 'SELECT').
func defaultSpanNameCtxFunc(_ context.Context, stmt string) string {
	stmt = strings.TrimSpace(stmt)
	end := strings.IndexFunc(stmt, unicode.IsSpace)
	if end < 0 && len(stmt) > 0 {
		// No space found, use the whole statement.
		end = len(stmt)
	} else if end < 0 {
		// Fall back to a fixed value to prevent creating lots of tracing operations
		// differing only by the amount of whitespace in them (in case we'd fall back
		// to the full query or a cut-off version).
		return sqlOperationUnknown
	}
	return strings.ToUpper(stmt[:end])
}
