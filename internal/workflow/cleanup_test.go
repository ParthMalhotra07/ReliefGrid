package workflow

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/santhosh-tekuri/jsonschema/v6"

	"github.com/ParthMalhotra07/ReliefGrid/internal/fsm/memstore"
	"github.com/ParthMalhotra07/ReliefGrid/internal/store"
)

const (
	cleanupRequest  = "550e8400-e29b-41d4-a716-446655440000"
	cleanupResource = "660e8400-e29b-41d4-a716-446655440000"
	cleanupAttempt  = "770e8400-e29b-41d4-a716-446655440000"
)

// Only observes calls and introduces barriers. All CAS/closure/claim outcomes
// come from the actual SQLite FSM, including injected lost responses.
type cleanupTrace struct {
	store.Store
	mu          sync.Mutex
	closes      []store.CloseAttempt
	finals      []store.TransitionRequest
	beforeFinal func(context.Context, store.TransitionRequest)
	afterRead   func(context.Context, store.Record) error
}

func (s *cleanupTrace) ReadRecord(ctx context.Context, key string) (store.Record, error) {
	r, err := s.Store.ReadRecord(ctx, key)
	if err == nil && s.afterRead != nil {
		err = s.afterRead(ctx, r)
	}
	return r, err
}

func (s *cleanupTrace) CloseAttempt(ctx context.Context, c store.CloseAttempt) (store.Result, error) {
	s.mu.Lock()
	s.closes = append(s.closes, c)
	s.mu.Unlock()
	return s.Store.CloseAttempt(ctx, c)
}

func (s *cleanupTrace) TransitionRequest(ctx context.Context, c store.TransitionRequest) (store.Result, error) {
	s.mu.Lock()
	s.finals = append(s.finals, c)
	s.mu.Unlock()
	if s.beforeFinal != nil {
		s.beforeFinal(ctx, c)
	}
	return s.Store.TransitionRequest(ctx, c)
}

func cleanupFixture(t *testing.T, ctx context.Context, s store.Store, held, fulfilled, reusable, expired bool) (store.Record, store.TryClaim) {
	t.Helper()
	createRequest(t, ctx, s, request(cleanupRequest))
	r := resource(cleanupResource, .01)
	r.Reusable = reusable
	createResource(t, ctx, s, r)
	a := store.Attempt{AttemptID: cleanupAttempt, ResourceID: cleanupResource, Fingerprint: "frozen-cleanup-input"}
	out, err := s.TransitionRequest(ctx, store.TransitionRequest{
		Meta: store.Meta{OperationID: "cleanup-intent", At: testNow.UnixMilli()},
		Key:  store.RequestKey(cleanupRequest), ExpectedVersion: 1, ExpectedStatus: store.RequestPending,
		To: store.RequestMatching, Attempt: &a,
	})
	mustOutcome(t, out, err, store.OutcomeOK)
	claim := store.TryClaim{Meta: store.Meta{OperationID: "cleanup-claim", At: testNow.UnixMilli()},
		ResourceKey: store.ResourceKey(cleanupResource), RequestID: cleanupRequest, Attempt: a, Type: store.TypeWater}
	if held {
		out, err = s.TryClaim(ctx, claim)
		mustOutcome(t, out, err, store.ClaimSuccess)
	}
	if fulfilled {
		out, err = s.TransitionRequest(ctx, store.TransitionRequest{
			Meta: store.Meta{OperationID: "cleanup-confirm", At: testNow.UnixMilli()}, Key: store.RequestKey(cleanupRequest),
			ExpectedVersion: 2, ExpectedStatus: store.RequestMatching, ExpectedAttemptID: a.AttemptID, To: store.RequestMatched,
		})
		mustOutcome(t, out, err, store.OutcomeOK)
	}
	rec := read(t, ctx, s, store.RequestKey(cleanupRequest))
	state, disposition := store.RequestCancelling, store.ResourceAvailable
	if fulfilled {
		state = store.RequestFulfilling
		if !reusable {
			disposition = store.ResourceConsumed
		}
	}
	at := testNow.Add(time.Minute)
	if expired {
		at = testNow.Add(2 * time.Hour)
		if reusable {
			disposition = store.ResourceOffline
		}
	}
	out, err = s.TransitionRequest(ctx, store.TransitionRequest{
		Meta: store.Meta{OperationID: "cleanup-begin", At: at.UnixMilli()}, Key: rec.Key, ExpectedVersion: rec.Version,
		ExpectedStatus: rec.Request.Status, ExpectedAttemptID: a.AttemptID, To: state, Disposition: disposition,
	})
	mustOutcome(t, out, err, store.OutcomeOK)
	return read(t, ctx, s, rec.Key), claim
}

func cleanupEvents(t *testing.T, ctx context.Context, s store.Store) []store.OutboxEvent {
	t.Helper()
	var all []store.OutboxEvent
	for shard := range s.Shards() {
		rows, err := s.PendingOutbox(ctx, store.OutboxQuery{Shard: shard, Limit: 100})
		if err != nil {
			t.Fatal(err)
		}
		all = append(all, rows...)
	}
	return all
}

func assertCleanTerminal(t *testing.T, ctx context.Context, s store.Store, want store.RequestStatus) store.Record {
	t.Helper()
	r := read(t, ctx, s, store.RequestKey(cleanupRequest))
	res := read(t, ctx, s, store.ResourceKey(cleanupResource))
	cl, err := s.ReadClosure(ctx, res.Key, cleanupAttempt)
	if r.Request.Status != want || err != nil || cl.RequestID != cleanupRequest || cl.Fingerprint != "frozen-cleanup-input" ||
		(res.Resource.ActiveClaim != nil && res.Resource.ActiveClaim.RequestID == cleanupRequest) {
		t.Fatalf("unsafe terminal: request=%+v resource=%+v closure=%+v err=%v", r, res, cl, err)
	}
	return r
}

func TestCleanupCancellationFixturesFenceLateClaims(t *testing.T) {
	for _, tc := range []struct {
		tag  string
		held bool
	}{{"SCH05", false}, {"SCH06", true}, {"SCH09", true}} {
		t.Run(tc.tag, func(t *testing.T) {
			f := schedule(t, tc.tag)
			ctx, raw := testStore(t)
			initial, claim := cleanupFixture(t, ctx, raw, tc.held, false, true, false)
			s := &cleanupTrace{Store: raw}
			s.beforeFinal = func(ctx context.Context, _ store.TransitionRequest) {
				cl, err := raw.ReadClosure(ctx, claim.ResourceKey, claim.Attempt.AttemptID)
				if err != nil || cl.RequestID != cleanupRequest {
					t.Errorf("terminal proposed before exact closure: %+v %v", cl, err)
				}
				if got := read(t, ctx, raw, initial.Key); got.Request.Status != store.RequestCancelling {
					t.Errorf("terminal state before closure/final CAS: %+v", got)
				}
			}
			m, err := New(testConfig(s, 1))
			if err != nil {
				t.Fatal(err)
			}
			if _, err = m.Cleanup(ctx, cleanupRequest); err != nil {
				t.Fatal(err)
			}
			r := assertCleanTerminal(t, ctx, raw, store.RequestCancelled)
			res := read(t, ctx, raw, claim.ResourceKey)
			for _, want := range f.FinalState {
				if want.Entity == "R" && (r.Version != uint64(want.Version) || r.Request.Status != want.ReqStatus) {
					t.Fatalf("fixture request mismatch: %+v want %+v", r, want)
				}
				if want.Entity == "Res" && (res.Version != uint64(want.Version) || res.Resource.Status != want.ResStatus) {
					t.Fatalf("fixture resource mismatch: %+v want %+v", res, want)
				}
			}
			claim.OperationID = "new-delayed-claim"
			out, err := raw.TryClaim(ctx, claim)
			mustOutcome(t, out, err, store.ClaimClosed)
			if tc.held {
				claim.OperationID = "cleanup-claim"
				out, err = raw.TryClaim(ctx, claim)
				mustOutcome(t, out, err, store.ClaimSuccess)
				if !out.Replayed {
					t.Fatal("historical claim was not replayed")
				}
			}
			assertCleanTerminal(t, ctx, raw, store.RequestCancelled)
		})
	}
}

func TestCleanupDispositionsAndAtomicTerminalEvents(t *testing.T) {
	var emitted []store.OutboxEvent
	for _, tc := range []struct {
		name                       string
		fulfill, reusable, expired bool
		disposition                store.ResourceStatus
	}{
		{"cancel", false, true, false, store.ResourceAvailable},
		{"reusable", true, true, false, store.ResourceAvailable},
		{"consumable", true, false, false, store.ResourceConsumed},
		{"expired", true, true, true, store.ResourceOffline},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, raw := testStore(t)
			initial, _ := cleanupFixture(t, ctx, raw, true, tc.fulfill, tc.reusable, tc.expired)
			cfg := testConfig(raw, 1)
			cfg.Now = func() time.Time { return testNow.Add(3 * time.Hour) }
			m, err := New(cfg)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := m.Cleanup(ctx, cleanupRequest); err != nil {
				t.Fatal(err)
			}
			want := store.RequestCancelled
			if tc.fulfill {
				want = store.RequestFulfilled
			}
			r := assertCleanTerminal(t, ctx, raw, want)
			res := read(t, ctx, raw, store.ResourceKey(cleanupResource))
			if res.Resource.Status != tc.disposition || r.Version != initial.Version+1 {
				t.Fatalf("wrong cleanup result: %+v / %+v", r, res)
			}
			rows := cleanupEvents(t, ctx, raw)
			if len(rows) != 1 {
				t.Fatalf("want one atomic terminal event, got %+v", rows)
			}
			e := rows[0]
			var envelope struct {
				EntityVersion uint64 `json:"entityVersion"`
				Payload       struct {
					RequestID      string              `json:"requestId"`
					PreviousStatus store.RequestStatus `json:"previousStatus"`
					NewStatus      store.RequestStatus `json:"newStatus"`
				} `json:"payload"`
			}
			if err := json.Unmarshal(e.Payload, &envelope); err != nil {
				t.Fatal(err)
			}
			route := "request.status.cancelled"
			if tc.fulfill {
				route = "request.status.fulfilled"
			}
			if e.EventType != "request.state_changed" || e.Exchange != "relief.events" || e.RoutingKey != route || e.EntityVersion != r.Version ||
				envelope.EntityVersion != r.Version || envelope.Payload.RequestID != cleanupRequest || envelope.Payload.PreviousStatus != initial.Request.Status || envelope.Payload.NewStatus != want {
				t.Fatalf("terminal event differs from committed state: %+v / %+v", e, envelope)
			}
			validateCleanupEvent(t, e)
			emitted = append(emitted, e)
			for range 3 {
				if _, err := m.Cleanup(ctx, cleanupRequest); err != nil {
					t.Fatal(err)
				}
			}
			if after := read(t, ctx, raw, r.Key); after.Version != r.Version || len(cleanupEvents(t, ctx, raw)) != 1 {
				t.Fatal("repeated cleanup changed terminal state/event")
			}
		})
	}
	if path := os.Getenv("A21_EVENTS_FILE"); path != "" {
		b, err := json.Marshal(emitted)
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, b, 0600); err != nil {
			t.Fatal(err)
		}
	}
}

func TestCleanupUnknownClosePreservesIntentAndCommandAcrossRestart(t *testing.T) {
	for _, applied := range []bool{false, true} {
		t.Run(map[bool]string{false: "before commit", true: "after commit"}[applied], func(t *testing.T) {
			ctx, raw := testStore(t)
			initial, _ := cleanupFixture(t, ctx, raw, true, false, true, false)
			drop := true
			s := &cleanupTrace{Store: memstore.WithFaults(raw, func(c memstore.Call) memstore.Fault {
				if drop && c.Method == "CloseAttempt" {
					return memstore.Fault{DropBefore: !applied, DropResponse: applied}
				}
				return memstore.Fault{}
			})}
			cfg := testConfig(s, 1)
			m, err := New(cfg)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := m.Cleanup(ctx, cleanupRequest); !errors.Is(err, store.ErrUnknownOutcome) {
				t.Fatalf("unknown close = %v", err)
			}
			before := read(t, ctx, raw, initial.Key)
			if before.Version != initial.Version || !reflect.DeepEqual(before.Request, initial.Request) || len(cleanupEvents(t, ctx, raw)) != 0 {
				t.Fatal("unknown close cleared recovery intent or finalized")
			}
			if len(s.closes) != 2 || !reflect.DeepEqual(s.closes[0], s.closes[1]) {
				t.Fatalf("in-call retry changed close command: %+v", s.closes)
			}
			drop = false
			cfg.Now = func() time.Time { return testNow.Add(4 * time.Hour) }
			restarted, err := New(cfg)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := restarted.Cleanup(ctx, cleanupRequest); err != nil {
				t.Fatal(err)
			}
			assertCleanTerminal(t, ctx, raw, store.RequestCancelled)
			for _, cmd := range s.closes {
				if !reflect.DeepEqual(cmd, s.closes[0]) {
					t.Fatalf("clock/restart changed close command: %+v", s.closes)
				}
			}
			if applied && len(s.closes) != 2 {
				t.Fatal("exact existing closure was unnecessarily rewritten")
			}
		})
	}
}

func TestCleanupLostTerminalResponseDoesNotDuplicateEvent(t *testing.T) {
	ctx, raw := testStore(t)
	cleanupFixture(t, ctx, raw, true, true, false, false)
	drop := true
	s := &cleanupTrace{Store: memstore.WithFaults(raw, func(c memstore.Call) memstore.Fault {
		return memstore.Fault{DropResponse: drop && c.Method == "TransitionRequest"}
	})}
	m, err := New(testConfig(s, 1))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := m.Cleanup(ctx, cleanupRequest); !errors.Is(err, store.ErrUnknownOutcome) {
		t.Fatalf("lost terminal response = %v", err)
	}
	terminal := assertCleanTerminal(t, ctx, raw, store.RequestFulfilled)
	if len(s.finals) != 2 || !reflect.DeepEqual(s.finals[0], s.finals[1]) {
		t.Fatalf("terminal retry changed command: %+v", s.finals)
	}
	drop = false
	if _, err := m.Cleanup(ctx, cleanupRequest); err != nil {
		t.Fatal(err)
	}
	if r := read(t, ctx, raw, terminal.Key); r.Version != terminal.Version || len(cleanupEvents(t, ctx, raw)) != 1 {
		t.Fatal("terminal replay duplicated state/event")
	}
}

func TestCleanupCompetingHelpersFinalizeOnce(t *testing.T) {
	ctx, raw := testStore(t)
	cleanupFixture(t, ctx, raw, true, true, true, false)
	arrived, release := make(chan struct{}, 2), make(chan struct{})
	var reads atomic.Int32
	s := &cleanupTrace{Store: raw}
	s.afterRead = func(ctx context.Context, r store.Record) error {
		if r.Key == store.RequestKey(cleanupRequest) && reads.Add(1) <= 2 {
			arrived <- struct{}{}
			select {
			case <-release:
				return nil
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		return nil
	}
	m, err := New(testConfig(s, 1))
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 2)
	for range 2 {
		go func() { _, err := m.Cleanup(ctx, cleanupRequest); done <- err }()
	}
	for range 2 {
		select {
		case <-arrived:
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}
	}
	close(release)
	for range 2 {
		if err := <-done; err != nil {
			t.Fatal(err)
		}
	}
	assertCleanTerminal(t, ctx, raw, store.RequestFulfilled)
	if len(cleanupEvents(t, ctx, raw)) != 1 {
		t.Fatal("competing helpers duplicated terminal event")
	}
}

func TestCleanupNeverReleasesNewOwner(t *testing.T) {
	ctx, raw := testStore(t)
	initial, claim := cleanupFixture(t, ctx, raw, true, false, true, false)
	closed, err := raw.CloseAttempt(ctx, store.CloseAttempt{
		Meta: store.Meta{OperationID: "prior-helper-close", At: initial.UpdatedAt}, ResourceKey: claim.ResourceKey,
		RequestID: cleanupRequest, Attempt: claim.Attempt, Disposition: store.ResourceAvailable, Reason: "cancelled",
	})
	mustOutcome(t, closed, err, store.OutcomeOK)
	other := store.TryClaim{Meta: store.Meta{OperationID: "new-owner-claim", At: testNow.Add(2 * time.Minute).UnixMilli()},
		ResourceKey: claim.ResourceKey, RequestID: "other-request", Type: store.TypeWater,
		Attempt: store.Attempt{AttemptID: "different-attempt", ResourceID: cleanupResource, Fingerprint: "other-input"}}
	out, err := raw.TryClaim(ctx, other)
	mustOutcome(t, out, err, store.ClaimSuccess)
	before := read(t, ctx, raw, claim.ResourceKey)
	m, err := New(testConfig(raw, 1))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := m.Cleanup(ctx, cleanupRequest); err != nil {
		t.Fatal(err)
	}
	after := read(t, ctx, raw, claim.ResourceKey)
	if !reflect.DeepEqual(before, after) {
		t.Fatalf("old cleanup changed the new owner: before=%+v after=%+v", before, after)
	}
	assertCleanTerminal(t, ctx, raw, store.RequestCancelled)
}

func TestCleanupFailedAuthoritativeReadNeverFinalizes(t *testing.T) {
	for _, method := range []string{"ReadRecord", "ReadClosure"} {
		t.Run(method, func(t *testing.T) {
			ctx, raw := testStore(t)
			initial, _ := cleanupFixture(t, ctx, raw, true, false, true, false)
			s := memstore.WithFaults(raw, func(c memstore.Call) memstore.Fault { return memstore.Fault{DropBefore: c.Method == method} })
			m, err := New(testConfig(s, 1))
			if err != nil {
				t.Fatal(err)
			}
			if _, err := m.Cleanup(ctx, cleanupRequest); err == nil {
				t.Fatal("failed read became successful cleanup")
			}
			if got := read(t, ctx, raw, initial.Key); !reflect.DeepEqual(got, initial) {
				t.Fatal("failed read changed cleanup intent")
			}
			if len(cleanupEvents(t, ctx, raw)) != 0 {
				t.Fatal("failed read emitted terminal event")
			}
		})
	}
}

func TestCleanupMatchResumesCleanupState(t *testing.T) {
	ctx, raw := testStore(t)
	cleanupFixture(t, ctx, raw, true, false, true, false)
	m, err := New(testConfig(raw, 1))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := m.Match(ctx, cleanupRequest); err != nil {
		t.Fatal(err)
	}
	assertCleanTerminal(t, ctx, raw, store.RequestCancelled)
}

type historicalClose struct{ store.Store }

func (s historicalClose) CloseAttempt(ctx context.Context, _ store.CloseAttempt) (store.Result, error) {
	return s.Operation(ctx, store.RequestKey(cleanupRequest), "create-request-"+cleanupRequest)
}

func TestCleanupHistoricalOKIsNotClosureProof(t *testing.T) {
	ctx, raw := testStore(t)
	initial, _ := cleanupFixture(t, ctx, raw, true, false, true, false)
	m, err := New(testConfig(historicalClose{raw}, 1))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := m.Cleanup(ctx, cleanupRequest); !errors.Is(err, ErrOwnershipUnknown) {
		t.Fatalf("unproven close = %v", err)
	}
	if got := read(t, ctx, raw, initial.Key); !reflect.DeepEqual(got, initial) {
		t.Fatal("historical OK finalized without closure")
	}
	if len(cleanupEvents(t, ctx, raw)) != 0 {
		t.Fatal("unproven closure emitted a terminal event")
	}
}

func TestCleanupRescheduleWinsBeforeFinalCAS(t *testing.T) {
	ctx, raw := testStore(t)
	initial, _ := cleanupFixture(t, ctx, raw, true, false, true, false)
	s := &cleanupTrace{Store: raw}
	rescheduled := false
	s.beforeFinal = func(ctx context.Context, _ store.TransitionRequest) {
		if rescheduled {
			return
		}
		rescheduled = true
		out, err := raw.RetryWorkflow(ctx, store.RetryWorkflow{
			Meta: store.Meta{OperationID: "cleanup-reschedule", At: testNow.Add(2 * time.Minute).UnixMilli()},
			Key:  initial.Key, ExpectedVersion: initial.Version, NextAttemptAt: testNow.Add(3 * time.Minute).UnixMilli(),
		})
		mustOutcome(t, out, err, store.OutcomeOK)
	}
	m, err := New(testConfig(s, 1))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := m.Cleanup(ctx, cleanupRequest); !errors.Is(err, ErrConcurrentChange) {
		t.Fatalf("lost final CAS = %v", err)
	}
	current := read(t, ctx, raw, initial.Key)
	if current.Request.Status != store.RequestCancelling || current.Request.CurrentAttempt == nil || current.Request.Disposition != initial.Request.Disposition {
		t.Fatal("CAS loss cleared intent/disposition")
	}
	if len(cleanupEvents(t, ctx, raw)) != 0 {
		t.Fatal("losing terminal CAS emitted an event")
	}
	if _, err := m.Cleanup(ctx, cleanupRequest); err != nil {
		t.Fatal(err)
	}
	assertCleanTerminal(t, ctx, raw, store.RequestCancelled)
	if len(s.closes) != 1 || len(cleanupEvents(t, ctx, raw)) != 1 {
		t.Fatal("reschedule repeated release or terminal event")
	}
}

func TestCleanupTerminalCommandSurvivesRestartBeforeCommit(t *testing.T) {
	ctx, raw := testStore(t)
	cleanupFixture(t, ctx, raw, true, false, true, false)
	drop := true
	s := &cleanupTrace{Store: memstore.WithFaults(raw, func(c memstore.Call) memstore.Fault {
		return memstore.Fault{DropBefore: drop && c.Method == "TransitionRequest"}
	})}
	cfg := testConfig(s, 1)
	m, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := m.Cleanup(ctx, cleanupRequest); !errors.Is(err, store.ErrUnknownOutcome) {
		t.Fatalf("terminal failure = %v", err)
	}
	drop = false
	cfg.Now = func() time.Time { return testNow.Add(5 * time.Hour) }
	restarted, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := restarted.Cleanup(ctx, cleanupRequest); err != nil {
		t.Fatal(err)
	}
	if len(s.finals) != 3 {
		t.Fatalf("terminal proposals = %d; want two lost calls plus recovery", len(s.finals))
	}
	for _, c := range s.finals {
		if !reflect.DeepEqual(c, s.finals[0]) {
			t.Fatalf("restart changed terminal command: %+v", s.finals)
		}
	}
	assertCleanTerminal(t, ctx, raw, store.RequestCancelled)
}

func validateCleanupEvent(t *testing.T, row store.OutboxEvent) {
	t.Helper()
	b, err := os.ReadFile("../../contracts/events/request_state_changed.json")
	if err != nil {
		t.Fatal(err)
	}
	var document, value any
	if err := json.Unmarshal(b, &document); err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(row.Payload, &value); err != nil {
		t.Fatal(err)
	}
	c := jsonschema.NewCompiler()
	c.AssertFormat()
	const location = "https://reliefgrid.internal/events/request_state_changed.json"
	if err := c.AddResource(location, document); err != nil {
		t.Fatal(err)
	}
	schema, err := c.Compile(location)
	if err != nil {
		t.Fatal(err)
	}
	if err := schema.Validate(value); err != nil {
		t.Fatalf("actual terminal event violates frozen schema: %v", err)
	}
	t.Log("actual terminal event validates against frozen request.state_changed schema")
}
