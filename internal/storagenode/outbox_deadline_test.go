package storagenode

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/ParthMalhotra07/ReliefGrid/internal/internalapi"
)

// Issue #27: the outbox long-poll is read + wait + read. The whole handler
// must fit the storage budget (s.timeout) and the request's own deadline,
// whichever is sooner: the wait is shortened to leave room for the second
// read, and skipped when there is none, so the coordinator's 4 s and the
// client's 5 s budgets hold however slow the reads are.
func TestOutboxLongPollFitsTheDeadline(t *testing.T) {
	const budget = 2 * time.Second // newTestServer's storage timeout
	const slack = 100 * time.Millisecond
	path := fmt.Sprintf("/internal/outbox?shard=%d&limit=10&wait=1", testShard)

	for _, tc := range []struct {
		name      string
		readDelay time.Duration
		deadline  time.Duration // the request's own deadline; 0: none
		bound     time.Duration // the answer must arrive within this
		waits     bool          // whether a (shortened) wait fits
	}{
		// 0.9 + 1 + 0.9 = 2.8 s unbounded; the wait must shrink.
		{"slow reads, storage budget", 900 * time.Millisecond, 0, budget, true},
		// 1.2 + 1.2 > 2 s: no room for a second read, so no wait.
		{"reads too slow to wait", 1200 * time.Millisecond, 0, budget, false},
		// The coordinator's attempt deadline is sooner than the budget.
		{"request deadline", 500 * time.Millisecond, 1500 * time.Millisecond, 1500 * time.Millisecond, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			n := newFakeNode(t)
			n.readDelay = tc.readDelay
			h := newTestServer(t, n).dataRoutes()
			ctx := context.Background()
			if tc.deadline > 0 {
				var cancel context.CancelFunc
				ctx, cancel = context.WithTimeout(ctx, tc.deadline)
				defer cancel()
			}
			rec := httptest.NewRecorder()
			start := time.Now()
			h.ServeHTTP(rec, httptest.NewRequestWithContext(ctx, http.MethodGet, path, nil))
			took := time.Since(start)

			if took > tc.bound+slack {
				t.Fatalf("long-poll took %s, want at most %s", took, tc.bound)
			}
			if rec.Code != http.StatusOK {
				t.Fatalf("long-poll = %d %s, want 200 with an empty page", rec.Code, rec.Body.String())
			}
			if page := decodeJSON[internalapi.OutboxPage](t, rec); len(page.Events) != 0 {
				t.Fatalf("events = %v, want none", page.Events)
			}
			n.mu.Lock()
			waits, reads := len(n.waits), len(n.modes)
			n.mu.Unlock()
			if tc.waits && (waits != 1 || reads != 2) {
				t.Fatalf("%d waits and %d reads; want a shortened wait and a second read", waits, reads)
			}
			if !tc.waits && (waits != 0 || reads != 1) {
				t.Fatalf("%d waits and %d reads; with no room for a second read, want no wait", waits, reads)
			}
		})
	}
}
