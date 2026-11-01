package outbox_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ParthMalhotra07/ReliefGrid/internal/outbox"
	"github.com/ParthMalhotra07/ReliefGrid/internal/store"
)

func TestRelayFactoryErrorDoesNotCloseTypedNilPublisher(t *testing.T) {
	ctx, s := fixtureStore(t)
	seed(t, ctx, s, 1)
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	var calls, closed atomic.Int32
	relay, err := outbox.New(outbox.Config{Store: s, PublisherFactory: func(context.Context) (outbox.Publisher, error) {
		if calls.Add(1) == 1 {
			// Same Go interface shape as mq.NewPublisher returning (*ConfirmedPublisher)(nil), err.
			var missing *workerPublisher
			return missing, errors.New("broker unavailable before publisher construction")
		}
		return &workerPublisher{closed: &closed}, nil
	}})
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- relay.Run(ctx) }()
	deadline := time.NewTimer(3 * time.Second)
	defer deadline.Stop()
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		rows, err := s.PendingOutbox(ctx, store.OutboxQuery{Shard: 0, Limit: 100})
		if err != nil {
			t.Fatal(err)
		}
		if len(rows) == 0 && calls.Load() >= 4 {
			break
		}
		select {
		case err := <-done:
			t.Fatalf("factory error stopped relay: %v", err)
		case <-deadline.C:
			t.Fatalf("factory retry did not publish/mark pending event: calls=%d rows=%d", calls.Load(), len(rows))
		case <-ticker.C:
		}
	}
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatal(err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("relay workers did not drain")
	}
	if closed.Load() != 3 {
		t.Fatalf("ready publishers closed=%d want 3", closed.Load())
	}
}
