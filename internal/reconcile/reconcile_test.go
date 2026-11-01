package reconcile_test

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/ParthMalhotra07/ReliefGrid/internal/fsm/memstore"
	"github.com/ParthMalhotra07/ReliefGrid/internal/geo"
	"github.com/ParthMalhotra07/ReliefGrid/internal/reconcile"
	"github.com/ParthMalhotra07/ReliefGrid/internal/store"
	"github.com/ParthMalhotra07/ReliefGrid/internal/workflow"
	"github.com/google/uuid"
)

var now = time.Date(2026, 11, 1, 12, 0, 0, 0, time.UTC)

func id(name string) string { return uuid.NewSHA1(uuid.NameSpaceURL, []byte(name)).String() }
func fixture(t *testing.T) (context.Context, *memstore.Store, *workflow.Matcher) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	t.Cleanup(cancel)
	s, err := memstore.New(ctx, memstore.WithShards(3))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := s.Close(); err != nil {
			t.Error(err)
		}
	})
	m, err := workflow.New(workflow.Config{Store: s, Now: func() time.Time { return now }, ScanLimit: 2})
	if err != nil {
		t.Fatal(err)
	}
	return ctx, s, m
}
func must(t *testing.T, result store.Result, err error) {
	t.Helper()
	if err != nil || result.Outcome != store.OutcomeOK {
		t.Fatalf("mutation: %+v %v", result, err)
	}
}
func request(t *testing.T, ctx context.Context, s store.Store, name string) string {
	t.Helper()
	key := id(name)
	r := store.Request{ID: key, Type: store.TypeWater, Location: store.Location{}, Geohash: "s000000", PeopleAffected: 1, Urgency: 1}
	res, err := s.CreateEntity(ctx, store.CreateEntity{Meta: store.Meta{OperationID: id("create-" + name), At: now.UnixMilli()}, Key: store.RequestKey(key), Request: &r})
	must(t, res, err)
	return key
}
func read(t *testing.T, ctx context.Context, s store.Store, key string) store.Record {
	t.Helper()
	r, err := s.ReadRecord(ctx, key)
	if err != nil {
		t.Fatal(err)
	}
	return r
}
func engine(t *testing.T, s store.Store, m *workflow.Matcher) *reconcile.Engine {
	t.Helper()
	e, err := reconcile.New(reconcile.Config{Store: s, Matcher: m, Now: func() time.Time { return now }, PageSize: 2, BatchSize: 3})
	if err != nil {
		t.Fatal(err)
	}
	return e
}
func cycle(t *testing.T, ctx context.Context, e *reconcile.Engine) {
	t.Helper()
	for range 100 {
		r, err := e.Step(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if r.Complete {
			return
		}
	}
	t.Fatal("bounded page walk never completed")
}

func TestReconcilerFinishesCleanupWithoutManualMatch(t *testing.T) {
	for _, target := range []store.RequestStatus{store.RequestCancelling, store.RequestFulfilling} {
		t.Run(string(target), func(t *testing.T) {
			ctx, s, m := fixture(t)
			rid := id("resource")
			resource := store.Resource{ID: rid, Type: store.TypeWater, Name: "synthetic", Location: store.Location{}, Geohash: "s000000", Status: store.ResourceAvailable, Reusable: true, AvailableFrom: now.Add(-time.Hour).UnixMilli(), AvailableUntil: now.Add(time.Hour).UnixMilli(), LocationRevision: 1}
			result, err := s.CreateEntity(ctx, store.CreateEntity{Meta: store.Meta{OperationID: id("create-resource"), At: now.UnixMilli()}, Key: store.ResourceKey(rid), Resource: &resource})
			must(t, result, err)
			q := request(t, ctx, s, "cleanup")
			if _, err := m.Match(ctx, q); err != nil {
				t.Fatal(err)
			}
			before := read(t, ctx, s, store.RequestKey(q))
			attempt := *before.Request.CurrentAttempt
			result, err = s.TransitionRequest(ctx, store.TransitionRequest{Meta: store.Meta{OperationID: id("cleanup-" + string(target)), At: now.UnixMilli()}, Key: before.Key, ExpectedVersion: before.Version, ExpectedStatus: before.Request.Status, ExpectedAttemptID: attempt.AttemptID, To: target, Disposition: store.ResourceAvailable})
			must(t, result, err)
			cycle(t, ctx, engine(t, s, m))
			got := read(t, ctx, s, before.Key)
			want := store.RequestCancelled
			if target == store.RequestFulfilling {
				want = store.RequestFulfilled
			}
			if got.Request.Status != want {
				t.Fatalf("reconciler left %s, want %s without manual Match", got.Request.Status, want)
			}
			closure, err := s.ReadClosure(ctx, store.ResourceKey(rid), attempt.AttemptID)
			if err != nil || closure.RequestID != q {
				t.Fatalf("terminal without exact closure: %+v %v", closure, err)
			}
			if r := read(t, ctx, s, store.ResourceKey(rid)); r.Resource.ActiveClaim != nil {
				t.Fatal("terminal request retains a claim")
			}
			late, err := s.TryClaim(ctx, store.TryClaim{Meta: store.Meta{OperationID: id("late"), At: now.UnixMilli()}, ResourceKey: store.ResourceKey(rid), RequestID: q, Attempt: attempt, Type: store.TypeWater})
			if err != nil || late.Outcome != store.ClaimClosed {
				t.Fatalf("late claim: %+v %v", late, err)
			}
		})
	}
}

type scanObserver struct {
	store.Store
	queries []store.ScanQuery
	partial bool
}

func (s *scanObserver) Scan(ctx context.Context, q store.ScanQuery) (store.ScanResult, error) {
	s.queries = append(s.queries, q)
	if s.partial {
		return store.ScanResult{Partial: true, FailedShards: []int{1}}, nil
	}
	return s.Store.Scan(ctx, q)
}
func TestReconcilerPagesEveryShardAndRetainsBoundedBatches(t *testing.T) {
	ctx, s, _ := fixture(t)
	observed := &scanObserver{Store: s}
	m, err := workflow.New(workflow.Config{Store: observed, Now: func() time.Time { return now }, ScanLimit: 2})
	if err != nil {
		t.Fatal(err)
	}
	var ids []string
	for i := range 37 {
		ids = append(ids, request(t, ctx, s, fmt.Sprint("page-", i)))
	}
	e := engine(t, observed, m)
	for range 100 {
		r, err := e.Step(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if r.Attempted > 3 {
			t.Fatalf("batch exceeded: %+v", r)
		}
		if r.Complete {
			break
		}
	}
	for _, q := range ids {
		if r := read(t, ctx, s, store.RequestKey(q)); r.Request.Status != store.RequestWaiting {
			t.Fatalf("paging missed %s: %s", q, r.Request.Status)
		}
	}
	advanced := false
	for _, q := range observed.queries {
		if q.Kind == store.KindRequest && q.Cursor != "" {
			advanced = true
		}
		if q.Limit != 2 {
			t.Fatalf("unbounded scan: %+v", q)
		}
	}
	if !advanced {
		t.Fatal("no opaque continuation cursor was used")
	}
}
func TestReconcilerRejectsPartialScanWithoutMutatingRequests(t *testing.T) {
	ctx, s, m := fixture(t)
	q := request(t, ctx, s, "partial")
	before := read(t, ctx, s, store.RequestKey(q))
	observed := &scanObserver{Store: s, partial: true}
	e := engine(t, observed, m)
	report, err := e.Step(ctx)
	if !errors.Is(err, reconcile.ErrIncompleteScan) || report.Complete {
		t.Fatalf("partial page treated as complete: %+v %v", report, err)
	}
	if got := read(t, ctx, s, before.Key); got.Version != before.Version {
		t.Fatal("partial scan mutated work")
	}
	observed.partial = false
	cycle(t, ctx, e)
	if got := read(t, ctx, s, before.Key); got.Request.Status != store.RequestWaiting {
		t.Fatal("failed shard was not retried")
	}
}

func TestResourceWakeUsesCurrentEligibilityAndOnlyNearbyCompatibleWaiters(t *testing.T) {
	ctx, s, m := fixture(t)
	var ids []string
	for i, input := range []struct {
		kind     store.ResourceType
		location store.Location
	}{{store.TypeWater, store.Location{}}, {store.TypeFood, store.Location{}}, {store.TypeWater, store.Location{Lng: 2}}} {
		q := id(fmt.Sprint("wakeup-", i))
		hash, err := geo.Encode(input.location, 7)
		if err != nil {
			t.Fatal(err)
		}
		req := store.Request{ID: q, Type: input.kind, Location: input.location, Geohash: hash, PeopleAffected: 1, Urgency: 1}
		res, err := s.CreateEntity(ctx, store.CreateEntity{Meta: store.Meta{OperationID: id("create-" + q), At: now.UnixMilli()}, Key: store.RequestKey(q), Request: &req})
		must(t, res, err)
		if _, err := m.Match(ctx, q); err != nil {
			t.Fatal(err)
		}
		ids = append(ids, q)
	}
	before := make([]store.Record, len(ids))
	for i, q := range ids {
		before[i] = read(t, ctx, s, store.RequestKey(q))
	}
	rid := id("new-resource")
	resource := store.Resource{ID: rid, Type: store.TypeWater, Name: "synthetic", Location: store.Location{}, Geohash: "s000000", Status: store.ResourceAvailable, Reusable: true, AvailableFrom: now.Add(-time.Hour).UnixMilli(), AvailableUntil: now.Add(time.Hour).UnixMilli(), LocationRevision: 1}
	res, err := s.CreateEntity(ctx, store.CreateEntity{Meta: store.Meta{OperationID: id("create-new-resource"), At: now.UnixMilli()}, Key: store.ResourceKey(rid), Resource: &resource})
	must(t, res, err)
	e := engine(t, s, m)
	if err := e.WakeResource(ctx, rid, "registration", now.Add(time.Second).UnixMilli()); err != nil {
		t.Fatal(err)
	}
	eligible := read(t, ctx, s, before[0].Key)
	if eligible.Version != before[0].Version+1 || eligible.Request.NextAttemptAt >= before[0].Request.NextAttemptAt {
		t.Fatal("nearby compatible waiter not durably woken")
	}
	for i := 1; i < len(ids); i++ {
		if got := read(t, ctx, s, before[i].Key); got.Version != before[i].Version {
			t.Fatal("incompatible/distant waiter changed")
		}
	}
	owner := request(t, ctx, s, "new-owner")
	if _, err := m.Match(ctx, owner); err != nil {
		t.Fatal(err)
	}
	if err := e.WakeResource(ctx, rid, "duplicate-registration", now.Add(2*time.Second).UnixMilli()); err != nil {
		t.Fatal(err)
	}
	if got := read(t, ctx, s, before[0].Key); got.Version != eligible.Version {
		t.Fatal("old registration treated as current availability")
	}
}
