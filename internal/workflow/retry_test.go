package workflow

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/ParthMalhotra07/ReliefGrid/internal/fsm/memstore"
	"github.com/ParthMalhotra07/ReliefGrid/internal/outbox"
	"github.com/ParthMalhotra07/ReliefGrid/internal/store"
)

func retryIntentFixture(t *testing.T, ctx context.Context, s store.Store, held bool) (store.Record, store.TryClaim) {
	t.Helper()
	createRequest(t, ctx, s, request(cleanupRequest))
	createResource(t, ctx, s, resource(cleanupResource, .01))
	a := store.Attempt{AttemptID: cleanupAttempt, ResourceID: cleanupResource, Fingerprint: "frozen-retry-input"}
	r, err := s.TransitionRequest(ctx, store.TransitionRequest{
		Meta: store.Meta{OperationID: "retry-intent", At: testNow.UnixMilli()}, Key: store.RequestKey(cleanupRequest),
		ExpectedVersion: 1, ExpectedStatus: store.RequestPending, To: store.RequestMatching, Attempt: &a,
	})
	mustOutcome(t, r, err, store.OutcomeOK)
	claim := store.TryClaim{Meta: store.Meta{OperationID: "retry-initial-claim", At: testNow.UnixMilli()},
		ResourceKey: store.ResourceKey(cleanupResource), RequestID: cleanupRequest, Attempt: a, Type: store.TypeWater}
	if held {
		r, err = s.TryClaim(ctx, claim)
		mustOutcome(t, r, err, store.ClaimSuccess)
	}
	return read(t, ctx, s, store.RequestKey(cleanupRequest)), claim
}

func TestRetryStateAwareSchedulePreservesAllWorkStates(t *testing.T) {
	for _, status := range []store.RequestStatus{store.RequestPending, store.RequestWaiting, store.RequestMatching, store.RequestRetrying, store.RequestCancelling, store.RequestFulfilling} {
		t.Run(string(status), func(t *testing.T) {
			ctx, raw := testStore(t)
			switch status {
			case store.RequestPending, store.RequestWaiting:
				createRequest(t, ctx, raw, request(cleanupRequest))
				if status == store.RequestWaiting {
					r, err := raw.TransitionRequest(ctx, store.TransitionRequest{Meta: store.Meta{OperationID: "initial-wait", At: testNow.UnixMilli()},
						Key: store.RequestKey(cleanupRequest), ExpectedVersion: 1, ExpectedStatus: store.RequestPending, To: store.RequestWaiting})
					mustOutcome(t, r, err, store.OutcomeOK)
				}
			case store.RequestMatching, store.RequestRetrying:
				rec, _ := retryIntentFixture(t, ctx, raw, true)
				if status == store.RequestRetrying {
					r, err := raw.TransitionRequest(ctx, store.TransitionRequest{Meta: store.Meta{OperationID: "initial-retrying", At: testNow.UnixMilli()},
						Key: rec.Key, ExpectedVersion: rec.Version, ExpectedStatus: store.RequestMatching, ExpectedAttemptID: cleanupAttempt, To: store.RequestRetrying})
					mustOutcome(t, r, err, store.OutcomeOK)
				}
			default:
				cleanupFixture(t, ctx, raw, true, status == store.RequestFulfilling, true, false)
			}
			before := read(t, ctx, raw, store.RequestKey(cleanupRequest))
			cfg := testConfig(raw, 1)
			clock := testNow.Add(5 * time.Minute)
			cfg.Now = func() time.Time { return clock }
			m, err := New(cfg)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := m.ScheduleRetry(ctx, cleanupRequest, store.ErrUnknownOutcome); err != nil {
				t.Fatal(err)
			}
			after := read(t, ctx, raw, before.Key)
			if after.Request.Status != before.Request.Status || !reflect.DeepEqual(after.Request.CurrentAttempt, before.Request.CurrentAttempt) ||
				after.Request.Disposition != before.Request.Disposition || after.Request.RetryCount != before.Request.RetryCount+1 || after.Request.NextAttemptAt <= clock.UnixMilli() || after.Version != before.Version+1 {
				t.Fatalf("retry discarded state/intent/disposition: before=%+v after=%+v", before, after)
			}
			rows := cleanupEvents(t, ctx, raw)
			if len(rows) != 1 || rows[0].Exchange != "relief.retry" || rows[0].EventType != "request.retry" || rows[0].EntityVersion != after.Version {
				t.Fatalf("retry lacks atomic handoff: %+v", rows)
			}
			wantRoute := "delay.5s"
			if status == store.RequestWaiting {
				wantRoute = "delay.30s"
			}
			if rows[0].RoutingKey != wantRoute {
				t.Fatalf("tier = %s; want %s", rows[0].RoutingKey, wantRoute)
			}
			if _, err := m.ScheduleRetry(ctx, cleanupRequest, errors.New("duplicate old event")); err != nil {
				t.Fatal(err)
			}
			if _, err := m.Match(ctx, cleanupRequest); err != nil {
				t.Fatalf("future work was not deferred: %v", err)
			}
			if got := read(t, ctx, raw, before.Key); !reflect.DeepEqual(got, after) || len(cleanupEvents(t, ctx, raw)) != 1 {
				t.Fatal("old delivery bypassed due time or advanced counters")
			}
		})
	}
}

func TestRetryUnknownClaimKeepsIntentAndExactCommand(t *testing.T) {
	for _, committed := range []bool{false, true} {
		t.Run(map[bool]string{false: "before commit", true: "lost response"}[committed], func(t *testing.T) {
			ctx, raw := testStore(t)
			createRequest(t, ctx, raw, request(cleanupRequest))
			createResource(t, ctx, raw, resource(cleanupResource, .01))
			drop := true
			s := &traceStore{Store: memstore.WithFaults(raw, func(c memstore.Call) memstore.Fault {
				if drop && c.Method == "TryClaim" {
					return memstore.Fault{DropBefore: !committed, DropResponse: committed}
				}
				return memstore.Fault{}
			})}
			clock := testNow
			cfg := testConfig(s, 1)
			cfg.Now = func() time.Time { return clock }
			m, err := New(cfg)
			if err != nil {
				t.Fatal(err)
			}
			if err := m.Handle(ctx, cleanupRequest); err != nil {
				t.Fatalf("durable retry handoff failed: %v", err)
			}
			r := read(t, ctx, raw, store.RequestKey(cleanupRequest))
			if r.Request.Status != store.RequestMatching || r.Request.CurrentAttempt == nil || r.Request.NextAttemptAt <= clock.UnixMilli() || r.Request.RetryCount != 1 {
				t.Fatalf("unknown became rejection/WAITING: %+v", r)
			}
			original := s.claims[0]
			clock = testNow.Add(time.Second)
			if err := m.Handle(ctx, cleanupRequest); err != nil {
				t.Fatal(err)
			}
			if len(s.claims) != 2 {
				t.Fatal("old event bypassed the durable backoff")
			}
			drop = false
			clock = testNow.Add(6 * time.Second)
			restarted, err := New(cfg)
			if err != nil {
				t.Fatal(err)
			}
			if err := restarted.Handle(ctx, cleanupRequest); err != nil {
				t.Fatal(err)
			}
			matched(t, ctx, raw, cleanupRequest)
			for _, c := range s.claims {
				if !reflect.DeepEqual(c, original) {
					t.Fatalf("retry changed immutable claim: original=%+v later=%+v", original, c)
				}
			}
		})
	}
}

func TestRetryRetargetFencesConfirmation(t *testing.T) {
	for _, confirmationWins := range []bool{true, false} {
		t.Run(map[bool]string{true: "confirmation first", false: "RETRYING first"}[confirmationWins], func(t *testing.T) {
			_ = schedule(t, "SCH08")
			ctx, raw := testStore(t)
			initial, claim := retryIntentFixture(t, ctx, raw, true)
			s := &cleanupTrace{Store: raw}
			confirmed := false
			s.beforeFinal = func(ctx context.Context, cmd store.TransitionRequest) {
				if (confirmationWins && cmd.To == store.RequestRetrying) || (!confirmationWins && cmd.To == store.RequestWaiting) {
					if confirmed {
						return
					}
					confirmed = true
					r, err := raw.TransitionRequest(ctx, store.TransitionRequest{Meta: store.Meta{OperationID: "racing-confirm", At: testNow.UnixMilli()},
						Key: initial.Key, ExpectedVersion: initial.Version, ExpectedStatus: store.RequestMatching, ExpectedAttemptID: cleanupAttempt, To: store.RequestMatched})
					want := store.OutcomeVersionConflict
					if confirmationWins {
						want = store.OutcomeOK
					}
					mustOutcome(t, r, err, want)
				}
				if cmd.To == store.RequestWaiting {
					cl, err := raw.ReadClosure(ctx, claim.ResourceKey, cleanupAttempt)
					if err != nil || cl.RequestID != cleanupRequest {
						t.Fatalf("intent cleared before exact closure: %+v %v", cl, err)
					}
				}
			}
			m, err := New(testConfig(s, 1))
			if err != nil {
				t.Fatal(err)
			}
			if _, err := m.Abandon(ctx, cleanupRequest); err != nil {
				t.Fatal(err)
			}
			if !confirmed {
				t.Fatal("confirmation barrier was not exercised")
			}
			r := read(t, ctx, raw, initial.Key)
			if confirmationWins {
				if r.Request.Status != store.RequestMatched || len(s.closes) != 0 {
					t.Fatal("losing RETRYING released the confirmed owner")
				}
				matched(t, ctx, raw, cleanupRequest)
			} else {
				if r.Request.Status != store.RequestWaiting || r.Request.CurrentAttempt != nil || r.Request.NextAttemptAt == 0 {
					t.Fatalf("retrying did not fence and durably hand off: %+v", r)
				}
				claim.OperationID = "late-after-retarget"
				result, err := raw.TryClaim(ctx, claim)
				mustOutcome(t, result, err, store.ClaimClosed)
			}
		})
	}
}

func TestRetryUnknownClosureNeverClearsAttempt(t *testing.T) {
	ctx, raw := testStore(t)
	initial, _ := retryIntentFixture(t, ctx, raw, true)
	drop := true
	s := memstore.WithFaults(raw, func(c memstore.Call) memstore.Fault {
		return memstore.Fault{DropBefore: drop && c.Method == "CloseAttempt"}
	})
	m, err := New(testConfig(s, 1))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := m.Abandon(ctx, cleanupRequest); !errors.Is(err, store.ErrUnknownOutcome) {
		t.Fatalf("closure failure = %v", err)
	}
	r := read(t, ctx, raw, initial.Key)
	if r.Request.Status != store.RequestRetrying || !reflect.DeepEqual(r.Request.CurrentAttempt, initial.Request.CurrentAttempt) {
		t.Fatalf("unknown close cleared the intent: %+v", r)
	}
	for _, e := range cleanupEvents(t, ctx, raw) {
		if e.EventType == "request.retry" {
			t.Fatal("WAITING retry was emitted before closure")
		}
	}
	drop = false
	if _, err := m.Match(ctx, cleanupRequest); err != nil {
		t.Fatal(err)
	}
	r = read(t, ctx, raw, initial.Key)
	if r.Request.Status != store.RequestWaiting || r.Request.CurrentAttempt != nil {
		t.Fatalf("retry cleanup did not resume: %+v", r)
	}
}

type retryRecorder struct {
	store.Store
	retries []store.RetryWorkflow
}

func (s *retryRecorder) RetryWorkflow(ctx context.Context, cmd store.RetryWorkflow) (store.Result, error) {
	s.retries = append(s.retries, cmd)
	return s.Store.RetryWorkflow(ctx, cmd)
}

func TestRetryDroppedScheduleResponseReplaysIdenticalCommand(t *testing.T) {
	ctx, raw := testStore(t)
	createRequest(t, ctx, raw, request(cleanupRequest))
	dropped := false
	s := &retryRecorder{Store: memstore.WithFaults(raw, func(c memstore.Call) memstore.Fault {
		if c.Method == "RetryWorkflow" && !dropped {
			dropped = true
			return memstore.Fault{DropResponse: true}
		}
		return memstore.Fault{}
	})}
	m, err := New(testConfig(s, 1))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := m.ScheduleRetry(ctx, cleanupRequest, store.ErrUnknownOutcome); err != nil {
		t.Fatal(err)
	}
	if len(s.retries) != 2 || !reflect.DeepEqual(s.retries[0], s.retries[1]) {
		t.Fatalf("uncertain schedule changed on retry: %+v", s.retries)
	}
	r := read(t, ctx, raw, store.RequestKey(cleanupRequest))
	if r.Request.RetryCount != 1 || len(cleanupEvents(t, ctx, raw)) != 1 {
		t.Fatal("lost response duplicated durable retries")
	}
}

type retryBrokerDown struct{}

func (retryBrokerDown) Publish(context.Context, store.OutboxEvent) error {
	return errors.New("broker unavailable")
}
func (retryBrokerDown) Close() error { return nil }

func TestRetryBrokerDownLeavesIntentAndOutboxPending(t *testing.T) {
	_ = schedule(t, "SCH15")
	ctx, raw := testStore(t)
	initial, _ := retryIntentFixture(t, ctx, raw, true)
	m, err := New(testConfig(raw, 1))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := m.ScheduleRetry(ctx, cleanupRequest, store.ErrUnknownOutcome); err != nil {
		t.Fatal(err)
	}
	r, err := outbox.New(outbox.Config{Store: raw})
	if err != nil {
		t.Fatal(err)
	}
	for shard := range raw.Shards() {
		report, err := r.Step(ctx, shard, retryBrokerDown{})
		if report.Read > 0 && (err == nil || report.Published != 0 || report.Marked != 0) {
			t.Fatalf("broker-down event was marked: %+v %v", report, err)
		}
	}
	current := read(t, ctx, raw, initial.Key)
	if current.Request.Status != store.RequestMatching || !reflect.DeepEqual(current.Request.CurrentAttempt, initial.Request.CurrentAttempt) {
		t.Fatal("broker failure discarded intent")
	}
	rows := cleanupEvents(t, ctx, raw)
	if len(rows) != 1 || rows[0].Published {
		t.Fatalf("retry event lost during broker outage: %+v", rows)
	}
}

func TestRetryRepeatedNoCandidateUsesAllBoundedTiers(t *testing.T) {
	ctx, raw := testStore(t)
	createRequest(t, ctx, raw, request(cleanupRequest))
	clock := testNow
	cfg := testConfig(raw, 1)
	cfg.Now = func() time.Time { return clock }
	m, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	for index, delay := range []int64{5000, 30000, 120000, 120000} {
		r, err := m.Match(ctx, cleanupRequest)
		if err != nil || r.Outcome != "NO_CANDIDATE" {
			t.Fatalf("due no-candidate pass %d: %+v %v", index+1, r, err)
		}
		rec := read(t, ctx, raw, store.RequestKey(cleanupRequest))
		if rec.Request.Status != store.RequestWaiting || rec.Request.RetryCount != index+1 || rec.Request.NextAttemptAt != clock.UnixMilli()+delay {
			t.Fatalf("wrong bounded retry: %+v", rec)
		}
		rows := cleanupEvents(t, ctx, raw)
		if len(rows) != index+1 {
			t.Fatal("retry tier lost/duplicated an outbox row")
		}
		wantRoute := "delay.5s"
		if delay == 30000 {
			wantRoute = "delay.30s"
		}
		if delay == 120000 {
			wantRoute = "delay.120s"
		}
		if rows[len(rows)-1].RoutingKey != wantRoute {
			t.Fatalf("wrong retry route: %+v", rows)
		}
		clock = time.UnixMilli(rec.Request.NextAttemptAt + 1)
	}
}

func TestRetryIncompleteScanSchedulesWithoutNoCandidateOrIntent(t *testing.T) {
	ctx, raw := testStore(t)
	createRequest(t, ctx, raw, request(cleanupRequest))
	s := memstore.WithFaults(raw, func(c memstore.Call) memstore.Fault {
		if c.Method == "Scan" {
			return memstore.Fault{FailShards: []int{1}}
		}
		return memstore.Fault{}
	})
	m, err := New(testConfig(s, 1))
	if err != nil {
		t.Fatal(err)
	}
	if err := m.Handle(ctx, cleanupRequest); err != nil {
		t.Fatal(err)
	}
	r := read(t, ctx, raw, store.RequestKey(cleanupRequest))
	if r.Request.Status != store.RequestPending || r.Request.CurrentAttempt != nil || r.Request.LastError != "INCOMPLETE_SCAN" || r.Request.NextAttemptAt == 0 {
		t.Fatalf("partial scan became a negative match result: %+v", r)
	}
}
