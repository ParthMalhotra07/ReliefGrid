package outbox_test

import (
	"context"
	"reflect"
	"slices"
	"testing"

	"github.com/ParthMalhotra07/ReliefGrid/internal/mq"
	"github.com/ParthMalhotra07/ReliefGrid/internal/store"
)

type frozenBatch struct {
	store.Store
	rows  []store.OutboxEvent
	marks []store.MarkOutboxPublished
}

func (s *frozenBatch) PendingOutbox(context.Context, store.OutboxQuery) ([]store.OutboxEvent, error) {
	return slices.Clone(s.rows), nil
}
func (s *frozenBatch) MarkOutboxPublished(ctx context.Context, cmd store.MarkOutboxPublished) (store.Result, error) {
	s.marks = append(s.marks, cmd)
	return s.Store.MarkOutboxPublished(ctx, cmd)
}

func TestRelayReplicasDeriveIdenticalCanonicalBatchCommands(t *testing.T) {
	ctx, s := fixtureStore(t)
	seed(t, ctx, s, 3)
	rows, err := s.PendingOutbox(ctx, store.OutboxQuery{Shard: 0, Limit: 100})
	if err != nil {
		t.Fatal(err)
	}
	left := &frozenBatch{Store: s, rows: rows}
	rightRows := slices.Clone(rows)
	slices.Reverse(rightRows)
	right := &frozenBatch{Store: s, rows: rightRows}
	for _, snapshot := range []*frozenBatch{left, right} {
		if _, err := relay(t, snapshot).Step(ctx, 0, &publisher{}); err != nil {
			t.Fatal(err)
		}
	}
	if len(left.marks) != 1 || len(right.marks) != 1 || !reflect.DeepEqual(left.marks[0], right.marks[0]) {
		t.Fatalf("two replicas changed canonical batch identity: left=%+v right=%+v", left.marks, right.marks)
	}
	if left.marks[0].At != rows[0].CreatedAt {
		t.Fatalf("mark time is not derived from immutable events: %+v", left.marks[0])
	}
}

func TestRelayNackAndInvalidUrgencyDoNotBlockLaterRows(t *testing.T) {
	for _, kind := range []string{"nack", "invalid urgency"} {
		t.Run(kind, func(t *testing.T) {
			ctx, s := fixtureStore(t)
			seed(t, ctx, s, 3)
			p := &publisher{}
			p.publish = func(context.Context, store.OutboxEvent) error {
				if p.calls != 1 {
					return nil
				}
				if kind == "nack" {
					return mq.ErrNack
				}
				// Same validation boundary as the real publisher, before network I/O.
				_, err := mq.EventPriority([]byte(`{"payload":{"urgency":"BROKEN"}}`))
				return err
			}
			report, err := relay(t, s).Step(ctx, 0, p)
			if err == nil || p.calls != 3 || report.Marked != 2 {
				t.Fatalf("per-row failure stalled later work: report=%+v calls=%d err=%v", report, p.calls, err)
			}
			pending, err := s.PendingOutbox(ctx, store.OutboxQuery{Shard: 0, Limit: 100})
			if err != nil || len(pending) != 1 {
				t.Fatalf("failed row was discarded: %+v %v", pending, err)
			}
		})
	}
}
