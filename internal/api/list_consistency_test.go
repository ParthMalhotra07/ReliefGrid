package api_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/url"
	"sync"
	"testing"

	"github.com/ParthMalhotra07/ReliefGrid/internal/store"
)

// scanModes records the read mode of every scan the API makes.
type scanModes struct {
	store.Store
	mu    sync.Mutex
	modes []store.Consistency
}

func (s *scanModes) Scan(ctx context.Context, q store.ScanQuery) (store.ScanResult, error) {
	s.mu.Lock()
	s.modes = append(s.modes, q.Consistency)
	s.mu.Unlock()
	return s.Store.Scan(ctx, q)
}

func (s *scanModes) take() []store.Consistency {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := s.modes
	s.modes = nil
	return out
}

// S4.1: a public list reports the read mode its scans were actually read
// in, not a fixed label. Every scan behind a list page, the first-page probe
// and the scans behind a cursor page included, names linearizable (the read
// fence), and the page says linearizable.
func TestAPIListsReportTheReadModeUsed(t *testing.T) {
	ctx, s := realStore(t)
	observed := &scanModes{Store: s}
	h := handler(t, observed, now, "dispatcher-a")
	for _, label := range []string{"list-mode-1", "list-mode-2", "list-mode-3"} {
		object(t, call(ctx, h, http.MethodPost, "/api/v1/requests", requestBody, fixtureKey(label)), http.StatusCreated)
		object(t, call(ctx, h, http.MethodPost, "/api/v1/resources", resourceBody, fixtureKey("res-"+label)), http.StatusCreated)
	}
	observed.take()
	for _, base := range []string{"/api/v1/requests?type=WATER&limit=1", "/api/v1/resources?type=WATER&limit=1"} {
		path := base
		for page := 0; ; page++ {
			list := object(t, call(ctx, h, http.MethodGet, path, "", ""), http.StatusOK)
			modes := observed.take()
			if len(modes) == 0 {
				t.Fatalf("%s page %d: no scan observed", base, page)
			}
			for _, m := range modes {
				if m != store.ConsistencyLinearizable {
					t.Errorf("%s page %d: scanned %q, want linearizable (all: %v)", base, page, m, modes)
				}
			}
			if list["consistency"] != string(modes[len(modes)-1]) {
				t.Errorf("%s page %d: reported consistency %v, but the scans were read %v", base, page, list["consistency"], modes)
			}
			next, _ := list["nextCursor"].(string)
			if next == "" {
				if page == 0 {
					t.Fatalf("%s: one page for 3 rows at limit=1, want a cursor page: %s", base, mustJSON(t, list))
				}
				break
			}
			if page > 5 {
				t.Fatalf("%s: more pages than rows", base)
			}
			path = base + "&cursor=" + url.QueryEscape(next)
		}
	}
}

func mustJSON(t *testing.T, v any) string {
	t.Helper()
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}
