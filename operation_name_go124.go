//go:build go1.24
// +build go1.24

package otelpgx

import (
	"context"
	"strings"
)

// defaultSpanNameCtxFunc attempts to get the first 'word' from a given SQL query, which usually
// is the operation name (e.g. 'SELECT'). Leading SQL comments are skipped.
func defaultSpanNameCtxFunc(_ context.Context, stmt string) string {
	for word := range strings.FieldsSeq(skipLeadingComments(stmt)) {
		return strings.ToUpper(word)
	}

	return sqlOperationUnknown
}
