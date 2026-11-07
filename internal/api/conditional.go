package api

import (
	"fmt"
	"net/http"
	"strings"

	"github.com/ParthMalhotra07/ReliefGrid/internal/store"
)

func entityTag(version uint64) string { return fmt.Sprintf(`"v%d"`, version) }

// checkIfMatch uses strong comparison. The eventual committed CAS is still
// authoritative: a matching initial read alone never authorizes a mutation.
func checkIfMatch(w http.ResponseWriter, r *http.Request, version uint64) error {
	values := r.Header.Values("If-Match")
	if len(values) == 0 {
		return nil
	}
	value := strings.TrimSpace(strings.Join(values, ","))
	if len(value) > 4096 || value == "" {
		return invalid("If-Match", "invalid entity-tag condition")
	}
	if value == "*" {
		return nil
	}
	matched := false
	for {
		weak := strings.HasPrefix(value, "W/")
		if weak {
			value = value[2:]
		}
		if len(value) < 2 || value[0] != '"' {
			return invalid("If-Match", "expected a quoted entity tag or *")
		}
		end := strings.IndexByte(value[1:], '"') + 1
		if end == 0 {
			return invalid("If-Match", "unterminated entity tag")
		}
		for i := 1; i < end; i++ {
			if value[i] < 0x21 || value[i] == 0x7f {
				return invalid("If-Match", "invalid entity tag")
			}
		}
		matched = matched || (!weak && value[:end+1] == entityTag(version))
		value = strings.TrimSpace(value[end+1:])
		if value == "" {
			break
		}
		if value[0] != ',' {
			return invalid("If-Match", "expected entity-tag separator")
		}
		value = strings.TrimSpace(value[1:])
		if value == "" {
			return invalid("If-Match", "empty entity-tag alternative")
		}
	}
	if !matched {
		w.Header().Set("ETag", entityTag(version))
		return issue(412, "PRECONDITION_FAILED", "entity changed; reread before a conditional update")
	}
	return nil
}

func committedUpdate(w http.ResponseWriter, r *http.Request, result store.Result) error {
	if len(r.Header.Values("If-Match")) > 0 && result.Outcome == store.OutcomeVersionConflict {
		w.Header().Set("ETag", entityTag(result.Version))
		return issue(412, "PRECONDITION_FAILED", "entity changed before the conditional update committed")
	}
	return committed(result)
}
