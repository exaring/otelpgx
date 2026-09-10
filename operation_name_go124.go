//go:build go1.24
// +build go1.24

package otelpgx

import (
	"context"
	"strings"
)

// SQLOperationName attempts to get the first 'word' from a given SQL query,
// which usually is the operation name (e.g. 'SELECT'). It's the default
// [SpanNameCtxFunc], and can also be passed to [WithMetricOperationName] to
// use the same low-cardinality parser for db.operation.name on metrics.
func SQLOperationName(_ context.Context, stmt string) string {
	for word := range strings.FieldsSeq(stmt) {
		return strings.ToUpper(word)
	}

	return sqlOperationUnknown
}
