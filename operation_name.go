package otelpgx

import (
	"context"
	"strings"
	"unicode"
)

// SQLOperationName returns the leading PostgreSQL command keyword in uppercase,
// or "UNKNOWN" if the first token is not recognized. Leading whitespace and SQL
// comments, including nested block comments, are skipped.
//
// Only a fixed set of command keywords is recognized. SQL syntax is not validated:
// WITH and EXPLAIN are returned without inspecting the following query.
// It is the default [SpanNameCtxFunc] and can be used with [WithMetricOperationName].
func SQLOperationName(_ context.Context, stmt string) string {
	stmt = skipLeadingComments(stmt)
	// CHECKPOINT and DEALLOCATE are the longest recognized keywords.
	var keyword [len("CHECKPOINT")]byte
	i := 0
	for i < len(stmt) && ((stmt[i] >= 'A' && stmt[i] <= 'Z') || (stmt[i] >= 'a' && stmt[i] <= 'z')) {
		if i == len(keyword) {
			return sqlOperationUnknown
		}
		c := stmt[i]
		if c >= 'a' && c <= 'z' {
			c -= 'a' - 'A'
		}
		keyword[i] = c
		i++
	}
	// Reject identifier continuations instead of matching a keyword prefix.
	// Non-ASCII bytes are rejected here even when they are malformed UTF-8.
	if i < len(stmt) && (stmt[i] >= '0' && stmt[i] <= '9' || stmt[i] == '_' || stmt[i] == '$' || stmt[i] >= 0x80) {
		return sqlOperationUnknown
	}
	// Return keyword literals to avoid allocating the result.
	switch string(keyword[:i]) {
	case "ABORT":
		return "ABORT"
	case "ALTER":
		return "ALTER"
	case "ANALYSE":
		return "ANALYSE"
	case "ANALYZE":
		return "ANALYZE"
	case "BEGIN":
		return "BEGIN"
	case "CALL":
		return "CALL"
	case "CHECKPOINT":
		return "CHECKPOINT"
	case "CLOSE":
		return "CLOSE"
	case "CLUSTER":
		return "CLUSTER"
	case "COMMENT":
		return "COMMENT"
	case "COMMIT":
		return "COMMIT"
	case "COPY":
		return "COPY"
	case "CREATE":
		return "CREATE"
	case "DEALLOCATE":
		return "DEALLOCATE"
	case "DECLARE":
		return "DECLARE"
	case "DELETE":
		return "DELETE"
	case "DISCARD":
		return "DISCARD"
	case "DO":
		return "DO"
	case "DROP":
		return "DROP"
	case "END":
		return "END"
	case "EXECUTE":
		return "EXECUTE"
	case "EXPLAIN":
		return "EXPLAIN"
	case "FETCH":
		return "FETCH"
	case "GRANT":
		return "GRANT"
	case "IMPORT":
		return "IMPORT"
	case "INSERT":
		return "INSERT"
	case "LISTEN":
		return "LISTEN"
	case "LOAD":
		return "LOAD"
	case "LOCK":
		return "LOCK"
	case "MERGE":
		return "MERGE"
	case "MOVE":
		return "MOVE"
	case "NOTIFY":
		return "NOTIFY"
	case "PREPARE":
		return "PREPARE"
	case "REASSIGN":
		return "REASSIGN"
	case "REFRESH":
		return "REFRESH"
	case "REINDEX":
		return "REINDEX"
	case "RELEASE":
		return "RELEASE"
	case "RESET":
		return "RESET"
	case "REVOKE":
		return "REVOKE"
	case "ROLLBACK":
		return "ROLLBACK"
	case "SAVEPOINT":
		return "SAVEPOINT"
	case "SECURITY":
		return "SECURITY"
	case "SELECT":
		return "SELECT"
	case "SET":
		return "SET"
	case "SHOW":
		return "SHOW"
	case "START":
		return "START"
	case "TABLE":
		return "TABLE"
	case "TRUNCATE":
		return "TRUNCATE"
	case "UNLISTEN":
		return "UNLISTEN"
	case "UPDATE":
		return "UPDATE"
	case "VACUUM":
		return "VACUUM"
	case "VALUES":
		return "VALUES"
	case "WITH":
		return "WITH"
	}
	return sqlOperationUnknown
}

// skipLeadingComments removes leading whitespace and SQL comments. Block
// comments may nest; an unterminated one yields "". Line comments end at CR or LF.
func skipLeadingComments(stmt string) string {
	for {
		stmt = strings.TrimLeftFunc(stmt, unicode.IsSpace)
		switch {
		case strings.HasPrefix(stmt, "--"):
			i := indexLineEnd(stmt)
			if i < 0 {
				return ""
			}
			stmt = stmt[i+1:]
		case strings.HasPrefix(stmt, "/*"):
			stmt = skipBlockComment(stmt[2:])
		default:
			return stmt
		}
	}
}

// skipBlockComment receives the text after an opening "/*" and returns the text
// after its matching "*/", or "" if the comment is unterminated.
func skipBlockComment(stmt string) string {
	depth := 1
	for i := 0; i < len(stmt)-1; {
		switch stmt[i : i+2] {
		case "/*":
			depth++
			i += 2
		case "*/":
			depth--
			i += 2
			if depth == 0 {
				return stmt[i:]
			}
		default:
			// Skip text and runs of '/' or '*' by searching for the other
			// delimiter byte. Resume at the preceding byte if it forms a pair.
			next, previous := byte('/'), byte('*')
			if stmt[i] == '/' {
				next, previous = previous, next
			}
			n := strings.IndexByte(stmt[i+1:], next)
			if n < 0 {
				return ""
			}
			i += n + 1
			if stmt[i-1] == previous {
				i--
			}
		}
	}
	return ""
}

// indexLineEnd uses bounded chunks so an early CR does not require searching
// the rest of a large SQL statement for LF.
func indexLineEnd(stmt string) int {
	// Keep short annotations on the single-pass path.
	i := strings.IndexAny(stmt[:min(len(stmt), 64)], "\r\n")
	if i >= 0 || len(stmt) <= 64 {
		return i
	}
	for offset := 64; offset < len(stmt); {
		end := offset + min(256, len(stmt)-offset)
		chunk := stmt[offset:end]
		lf := strings.IndexByte(chunk, '\n')
		if lf >= 0 {
			chunk = chunk[:lf]
		}
		if cr := strings.IndexByte(chunk, '\r'); cr >= 0 {
			return offset + cr
		}
		if lf >= 0 {
			return offset + lf
		}
		offset = end
	}
	return -1
}
