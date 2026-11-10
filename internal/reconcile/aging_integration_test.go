//go:build integration && aging

package reconcile_test

import (
	"github.com/ParthMalhotra07/ReliefGrid/internal/mq"
	"github.com/ParthMalhotra07/ReliefGrid/internal/outbox"
	"github.com/ParthMalhotra07/ReliefGrid/internal/store"
	"os"
	"testing"
	"time"
)

func TestAgingImmediateWakeUsesExistingBrokerBinding(t *testing.T) {
	ctx, raw, _ := fixture(t)
	key := agedWaiting(t, ctx, raw, now.Add(-time.Minute))
	clock := now
	e := agingEngine(t, raw, &clock)
	if _, err := e.Step(ctx); err != nil {
		t.Fatal(err)
	}
	address := os.Getenv("MQ_AMQP_URL")
	if address == "" {
		t.Fatal("isolated MQ_AMQP_URL required")
	}
	conn, err := mq.Dial(ctx, address)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close() }()
	if err := mq.Declare(ctx, conn); err != nil {
		t.Fatal(err)
	}
	publisher, err := mq.NewPublisher(ctx, address)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = publisher.Close() }()
	relay, err := outbox.New(outbox.Config{Store: raw})
	if err != nil {
		t.Fatal(err)
	}
	channel, err := conn.Channel()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = channel.Close() }()
	started := time.Now()
	marked := 0
	for shard := 0; shard < raw.Shards(); shard++ {
		report, err := relay.Step(ctx, shard, publisher)
		if err != nil {
			t.Fatal(err)
		}
		marked += report.Marked
	}
	if marked != 2 {
		t.Fatalf("durable routed handoffs=%d want2", marked)
	}
	deadline := started.Add(3 * time.Second)
	for {
		delivery, ok, err := channel.Get(mq.MatchingQueue, false)
		if err != nil {
			t.Fatal(err)
		}
		if ok {
			if time.Since(started) >= 3*time.Second || delivery.RoutingKey != store.AgingWakeRoutingKey || delivery.Priority != 2 {
				t.Fatalf("immediate priority wake not observed: route=%s priority=%d", delivery.RoutingKey, delivery.Priority)
			}
			rec := read(t, ctx, raw, key)
			if rec.Request.EffectiveUrgency != 2 || rec.Request.Urgency != 1 {
				t.Fatal("wake detached from durable priority")
			}
			if err := delivery.Ack(false); err != nil {
				t.Fatal(err)
			}
			t.Logf("immediate routed wake in %s, AMQP priority2, two confirmed durable handoffs", time.Since(started))
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("wake did not arrive before delayed retry tier")
		}
		time.Sleep(10 * time.Millisecond)
	}
}
