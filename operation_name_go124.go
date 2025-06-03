//go:build go1.24
// +build go1.24

package otelpgx

import (
	"strings"
)

// sqlOperationName attempts to get the first 'word' from a given SQL query, which usually
// is the operation name (e.g. 'SELECT').
func (t *Tracer) sqlOperationName(stmt string) string {
	if t.spanNameFunc != nil {
		return t.spanNameFunc(stmt)
	}

	for word := range strings.FieldsSeq(stmt) {
		return strings.ToUpper(word)
	}

	return sqlOperationUnknown
}
