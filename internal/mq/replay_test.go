package mq_test

import (
	"context"
	"errors"
	"github.com/ParthMalhotra07/ReliefGrid/internal/mq"
	"github.com/ParthMalhotra07/ReliefGrid/internal/store"
	"testing"
)

type replayPublisher struct {
	ack   *ackLog
	fail  bool
	calls int
	event store.OutboxEvent
}

func (p *replayPublisher) Publish(_ context.Context, e store.OutboxEvent) error {
	if p.ack.acks != 0 {
		p.ack.t.Fatal("dead letter acked before routed confirm")
	}
	p.calls++
	p.event = e
	if p.fail {
		return errors.New("NO_ROUTE")
	}
	return nil
}
func (*replayPublisher) Close() error { return nil }
func TestReplayDeadRequiresValidatedReplacementAndRoutedConfirm(t *testing.T) {
	for _, fail := range []bool{false, true} {
		ack := &ackLog{t: t}
		d := wakeDelivery(t, ack)
		p := &replayPublisher{ack: ack, fail: fail}
		err := mq.ReplayDead(t.Context(), d, d.Body, p)
		if (err != nil) != fail || p.calls != 1 || ack.acks != map[bool]int{true: 0, false: 1}[fail] {
			t.Fatalf("replay fail=%v calls=%d ack=%d err=%v", fail, p.calls, ack.acks, err)
		}
		if p.event.Exchange != "" || p.event.RoutingKey != mq.WakeupQueue {
			t.Fatal("wakeup replay sent to wrong queue")
		}
	}
}
func TestReplayDeadRefusesPoisonAndIdentityChange(t *testing.T) {
	for _, poison := range []bool{true, false} {
		ack := &ackLog{t: t}
		d := wakeDelivery(t, ack)
		replacement := d.Body
		if poison {
			replacement = []byte(`{}`)
		} else {
			d.MessageId = "different"
		}
		p := &replayPublisher{ack: ack}
		if err := mq.ReplayDead(t.Context(), d, replacement, p); err == nil || p.calls != 0 || ack.acks != 0 {
			t.Fatalf("invalid replay consumed dead work: calls=%d ack=%d err=%v", p.calls, ack.acks, err)
		}
	}
}
