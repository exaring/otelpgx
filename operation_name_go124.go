//go:build go1.24
// +build go1.24

package otelpgx

import (
	"strings"
)

// defaultSpanNameFunc attempts to get the first 'word' from a given SQL query, which usually
// is the operation name (e.g. 'SELECT').
func defaultSpanNameFunc(stmt string) string {
	for word := range strings.FieldsSeq(stmt) {
		return strings.ToUpper(word)
	}

	return sqlOperationUnknown
}
