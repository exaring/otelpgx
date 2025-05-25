//go:build !go1.24
// +build !go1.24

package otelpgx

import (
	"strings"
	"unicode"
)

// sqlOperationName attempts to get the first 'word' from a given SQL query, which usually
// is the operation name (e.g. 'SELECT').
func (t *Tracer) sqlOperationName(stmt string) string {
	// If a custom function is provided, use that. Otherwise, fall back to the
	// default implementation. This allows users to override the default
	// behavior without having to reimplement it.
	if t.spanNameFunc != nil {
		return t.spanNameFunc(stmt)
	}

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
