package fsm_test

import (
	"fmt"
	"testing"

	"github.com/ParthMalhotra07/ReliefGrid/internal/store"
)

// requestStatuses is every request status the FSM accepts.
var requestStatuses = []store.RequestStatus{
	store.RequestPending, store.RequestWaiting, store.RequestMatching, store.RequestMatched,
	store.RequestInProgress, store.RequestRetrying, store.RequestCancelling, store.RequestFulfilling,
	store.RequestCancelled, store.RequestFulfilled,
}

// allowedTransitions is the request lifecycle written out from the plan, not
// from apply.go. Every pair absent here must be a DOMAIN_CONFLICT.
var allowedTransitions = map[store.RequestStatus][]store.RequestStatus{
	// §17: CAS PENDING/WAITING to MATCHING. §19: no intent, cancel directly.
	// §20: no candidate and no live intent persists WAITING plus a retry event.
	store.RequestPending: {store.RequestMatching, store.RequestWaiting, store.RequestCancelled},
	store.RequestWaiting: {store.RequestMatching, store.RequestCancelled},
	// §17: confirm, or fast retarget to MATCHING(b) / WAITING after closure.
	// §18: voluntary abandon enters RETRYING. §19: cancel through cleanup.
	store.RequestMatching: {store.RequestMatched, store.RequestMatching, store.RequestWaiting,
		store.RequestRetrying, store.RequestCancelling},
	// §17 state table: progress, cancel through cleanup, or enter FULFILLING.
	store.RequestMatched:    {store.RequestInProgress, store.RequestCancelling, store.RequestFulfilling},
	store.RequestInProgress: {store.RequestCancelling, store.RequestFulfilling},
	// §18: close, then WAITING; never confirm. §19: cancellation may take over.
	store.RequestRetrying: {store.RequestWaiting, store.RequestCancelling},
	// §19: terminal only after closure.
	store.RequestCancelling: {store.RequestCancelled},
	store.RequestFulfilling: {store.RequestFulfilled},
	// Terminal: matching never restarts.
	store.RequestCancelled: nil,
	store.RequestFulfilled: nil,
}

// pathTo reaches each status from PENDING through allowed moves only.
var pathTo = map[store.RequestStatus][]store.RequestStatus{
	store.RequestPending:    nil,
	store.RequestWaiting:    {store.RequestWaiting},
	store.RequestMatching:   {store.RequestMatching},
	store.RequestMatched:    {store.RequestMatching, store.RequestMatched},
	store.RequestInProgress: {store.RequestMatching, store.RequestMatched, store.RequestInProgress},
	store.RequestRetrying:   {store.RequestMatching, store.RequestRetrying},
	store.RequestCancelling: {store.RequestMatching, store.RequestCancelling},
	store.RequestFulfilling: {store.RequestMatching, store.RequestMatched, store.RequestFulfilling},
	store.RequestCancelled:  {store.RequestCancelled},
	store.RequestFulfilled:  {store.RequestMatching, store.RequestMatched, store.RequestFulfilling, store.RequestFulfilled},
}

const tableRequest = "q-table"

// currentTransition proposes from -> to against the request's CURRENT version,
// status and attempt, with the payload `to` needs, so only the transition
// table can reject it.
func currentTransition(h *harness, op string, to store.RequestStatus) store.TransitionRequest {
	h.t.Helper()
	rec := h.read(store.RequestKey(tableRequest))
	cmd := store.TransitionRequest{Meta: meta(op), Key: rec.Key, ExpectedVersion: rec.Version,
		ExpectedStatus: rec.Request.Status, To: to}
	if a := rec.Request.CurrentAttempt; a != nil {
		cmd.ExpectedAttemptID = a.AttemptID
	}
	switch to {
	case store.RequestMatching:
		a := attempt("a-"+op, "r1")
		cmd.Attempt = &a
	case store.RequestWaiting:
		cmd.NextAttemptAt = t0 + 5000
	case store.RequestCancelling, store.RequestFulfilling:
		cmd.Disposition = store.ResourceAvailable
	}
	return cmd
}

func requestAt(t *testing.T, from store.RequestStatus) *harness {
	t.Helper()
	h := newHarness(t)
	h.apply(store.CreateEntity{Meta: meta("c-" + tableRequest), Key: store.RequestKey(tableRequest), Request: request(tableRequest, store.TypeWater)})
	for i, step := range pathTo[from] {
		expectOutcome(t, fmt.Sprintf("setup step %d -> %s", i, step), h.apply(currentTransition(h, fmt.Sprintf("setup-%d", i), step)), store.OutcomeOK)
	}
	if rec := h.read(store.RequestKey(tableRequest)); rec.Request.Status != from {
		t.Fatalf("setup reached %s, want %s", rec.Request.Status, from)
	}
	return h
}

// Verify: every (from, to) request pair is accepted exactly when the plan's
// lifecycle allows it and is otherwise DOMAIN_CONFLICT with no state change.
func TestRequestTransitionTableIsExhaustive(t *testing.T) {
	if len(allowedTransitions) != len(requestStatuses) || len(pathTo) != len(requestStatuses) {
		t.Fatal("table does not cover every request status")
	}
	for _, from := range requestStatuses {
		allowed := map[store.RequestStatus]bool{}
		for _, to := range allowedTransitions[from] {
			allowed[to] = true
		}
		for _, to := range requestStatuses {
			t.Run(fmt.Sprintf("%s->%s", from, to), func(t *testing.T) {
				h := requestAt(t, from)
				before := h.read(store.RequestKey(tableRequest))
				got := h.apply(currentTransition(h, "probe", to))
				after := h.read(store.RequestKey(tableRequest))
				if allowed[to] {
					expectOutcome(t, "allowed transition", got, store.OutcomeOK)
					if after.Request.Status != to || after.Version != before.Version+1 {
						t.Fatalf("committed v%d %s, want v%d %s", after.Version, after.Request.Status, before.Version+1, to)
					}
					return
				}
				expectOutcome(t, "disallowed transition", got, store.OutcomeDomainConflict)
				if after.Version != before.Version || after.Request.Status != from {
					t.Fatalf("rejected transition changed the request: v%d %s", after.Version, after.Request.Status)
				}
			})
		}
	}
}

// Verify: RetryWorkflow keeps any non-terminal state and is DOMAIN_CONFLICT
// for a terminal request (§18, §19), against the current version.
func TestRetryWorkflowRespectsTerminalStates(t *testing.T) {
	for _, from := range requestStatuses {
		t.Run(string(from), func(t *testing.T) {
			h := requestAt(t, from)
			before := h.read(store.RequestKey(tableRequest))
			got := h.apply(store.RetryWorkflow{Meta: meta("retry"), Key: before.Key, ExpectedVersion: before.Version, NextAttemptAt: t0 + 5000})
			after := h.read(store.RequestKey(tableRequest))
			if from == store.RequestCancelled || from == store.RequestFulfilled {
				expectOutcome(t, "terminal retry", got, store.OutcomeDomainConflict)
				if after.Version != before.Version {
					t.Fatal("rejected retry changed the request")
				}
				return
			}
			expectOutcome(t, "retry", got, store.OutcomeOK)
			if after.Request.Status != from || after.Version != before.Version+1 || after.Request.NextAttemptAt != t0+5000 {
				t.Fatalf("retry changed state or missed schedule: v%d %s next=%d", after.Version, after.Request.Status, after.Request.NextAttemptAt)
			}
		})
	}
}
