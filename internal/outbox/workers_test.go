package outbox_test

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ParthMalhotra07/ReliefGrid/internal/fsm/memstore"
	"github.com/ParthMalhotra07/ReliefGrid/internal/outbox"
	"github.com/ParthMalhotra07/ReliefGrid/internal/store"
)

func TestRelayWorkersPollIndependentlyAndCloseOnCancellation(t *testing.T) {
	ctx, s := fixtureStore(t)
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	w := &shardBarrier{Store: s, seen: make(chan int, 3)}
	var created, closed atomic.Int32
	r, err := outbox.New(outbox.Config{Store: w, PublisherFactory: func(context.Context) (outbox.Publisher, error) {
		created.Add(1)
		return &workerPublisher{closed: &closed}, nil
	}})
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- r.Run(ctx) }()
	seen := map[int]bool{}
	for len(seen) < 3 {
		select {
		case shard := <-w.seen:
			seen[shard] = true
		case <-ctx.Done():
			t.Fatalf("one blocked shard starved others: %v", seen)
		}
	}
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatal(err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("relay workers did not stop")
	}
	if created.Load() != 3 || closed.Load() != 3 {
		t.Fatalf("publisher leaked or reconnected on empty poll: created=%d closed=%d", created.Load(), closed.Load())
	}
}

func TestRelayMarkBatchesStayOnEachOfThreeOwningShards(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	s, err := memstore.New(ctx, memstore.WithShards(3))
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := s.Close(); err != nil {
			t.Error(err)
		}
	}()
	seed(t, ctx, s, 24)
	w := &markFault{Store: s}
	r := relay(t, w)
	counts := make([]int, 3)
	for shard := range 3 {
		rows, err := s.PendingOutbox(ctx, store.OutboxQuery{Shard: shard, Limit: 100})
		if err != nil {
			t.Fatal(err)
		}
		counts[shard] = len(rows)
		owned := map[string]bool{}
		for _, e := range rows {
			owned[e.EventID] = true
		}
		p := &publisher{publish: func(_ context.Context, e store.OutboxEvent) error {
			if !owned[e.EventID] {
				t.Fatalf("cross-shard publish: shard=%d event=%s", shard, e.EventID)
			}
			return nil
		}}
		before := len(w.marks)
		report, err := r.Step(ctx, shard, p)
		if err != nil || report.Marked != len(rows) {
			t.Fatalf("shard=%d report=%+v error=%v", shard, report, err)
		}
		for _, mark := range w.marks[before:] {
			if mark.Shard != shard {
				t.Fatalf("cross-shard mark: %+v", mark)
			}
			for _, id := range mark.EventIDs {
				if !owned[id] {
					t.Fatalf("unowned event in mark: %+v", mark)
				}
			}
		}
	}
	if counts[0]+counts[1]+counts[2] != 24 {
		t.Fatalf("lost rows: %v", counts)
	}
}

type shardBarrier struct {
	store.Store
	seen chan int
	once [3]sync.Once
}

func (s *shardBarrier) PendingOutbox(ctx context.Context, q store.OutboxQuery) ([]store.OutboxEvent, error) {
	s.once[q.Shard].Do(func() { s.seen <- q.Shard })
	if q.Shard == 0 {
		<-ctx.Done()
		return nil, ctx.Err()
	}
	return s.Store.PendingOutbox(ctx, q)
}

type workerPublisher struct{ closed *atomic.Int32 }

func (*workerPublisher) Publish(context.Context, store.OutboxEvent) error { return nil }
func (p *workerPublisher) Close() error                                   { p.closed.Add(1); return nil }
