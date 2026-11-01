package reconcile_test

import (
	"context"
	"errors"
	"github.com/ParthMalhotra07/ReliefGrid/internal/reconcile"
	"github.com/ParthMalhotra07/ReliefGrid/internal/store"
	"testing"
	"time"
)

type scanFailure struct {
	store.Store
	fail    bool
	cursors []string
}

func (s *scanFailure) Scan(ctx context.Context, q store.ScanQuery) (store.ScanResult, error) {
	s.cursors = append(s.cursors, q.Cursor)
	if s.fail {
		return store.ScanResult{}, errors.New("transport scan failure")
	}
	return s.Store.Scan(ctx, q)
}
func TestScanErrorKeepsCursorAndRemainsIncomplete(t *testing.T) {
	ctx, s, m := fixture(t)
	request(t, ctx, s, "scan-error")
	wrapped := &scanFailure{Store: s, fail: true}
	e := engine(t, wrapped, m)
	for range 2 {
		report, err := e.Step(ctx)
		if !errors.Is(err, reconcile.ErrIncompleteScan) || report.Complete || report.Attempted != 0 {
			t.Fatalf("failed scan accepted: %+v %v", report, err)
		}
	}
	if len(wrapped.cursors) != 2 || wrapped.cursors[0] != wrapped.cursors[1] {
		t.Fatal("cursor advanced after scan error")
	}
	wrapped.fail = false
	cycle(t, ctx, e)
}

func TestWakeResourceRejectsPartialPage(t *testing.T) {
	ctx, s, m := fixture(t)
	qid := request(t, ctx, s, "partial-waiter")
	if _, err := m.Match(ctx, qid); err != nil {
		t.Fatal(err)
	}
	rid := id("partial-resource")
	r := store.Resource{ID: rid, Type: store.TypeWater, Name: "synthetic", Location: store.Location{}, Geohash: "s000000", Status: store.ResourceAvailable, Reusable: true, AvailableFrom: now.Add(-time.Hour).UnixMilli(), AvailableUntil: now.Add(time.Hour).UnixMilli(), LocationRevision: 1}
	result, err := s.CreateEntity(ctx, store.CreateEntity{Meta: store.Meta{OperationID: id("partial-resource-create"), At: now.UnixMilli()}, Key: store.ResourceKey(rid), Resource: &r})
	must(t, result, err)
	before := read(t, ctx, s, store.RequestKey(qid))
	e := engine(t, &scanObserver{Store: s, partial: true}, m)
	if err := e.WakeResource(ctx, rid, "resource-event", now.UnixMilli()); !errors.Is(err, reconcile.ErrIncompleteScan) {
		t.Fatalf("partial wake accepted: %v", err)
	}
	if got := read(t, ctx, s, before.Key); got.Version != before.Version {
		t.Fatal("partial wake mutated request")
	}
}
