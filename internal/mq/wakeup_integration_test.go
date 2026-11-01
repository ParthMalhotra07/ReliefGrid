//go:build integration && a24

package mq_test

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/url"
	"os"
	"testing"
	"time"

	"github.com/ParthMalhotra07/ReliefGrid/internal/fsm/memstore"
	"github.com/ParthMalhotra07/ReliefGrid/internal/mq"
	"github.com/ParthMalhotra07/ReliefGrid/internal/store"
	"github.com/ParthMalhotra07/ReliefGrid/internal/workflow"
)

func wakeURL(t *testing.T, a brokerAdmin) string {
	t.Helper()
	u, err := url.Parse(os.Getenv("MQ_AMQP_URL"))
	if err != nil {
		t.Fatal(err)
	}
	u.Path = "/" + a.vhost
	return u.String()
}
func wakePublisherLive(t *testing.T, ctx context.Context, a brokerAdmin) *mq.ConfirmedPublisher {
	t.Helper()
	p, err := mq.NewPublisher(ctx, wakeURL(t, a))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = p.Close() })
	return p
}
func wakeEvent(t *testing.T, body []byte) store.OutboxEvent {
	t.Helper()
	var e struct {
		EventID   string `json:"eventId"`
		EventType string `json:"eventType"`
	}
	if err := json.Unmarshal(body, &e); err != nil {
		t.Fatal(err)
	}
	return store.OutboxEvent{EventID: e.EventID, EventType: e.EventType, SchemaVersion: 1, Exchange: "relief.events", RoutingKey: "request.status.cancelling", Payload: body}
}

func TestWakeupIntegrationRetryTTLAndControlledPoisonReplay(t *testing.T) {
	ctx, conn, admin := isolatedBroker(t)
	p := wakePublisherLive(t, ctx, admin)
	ch, err := conn.Channel()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = ch.Close() }()
	body := wakeDelivery(t, nil).Body
	r := mq.TransportRetry{PublisherFactory: func(ctx context.Context) (mq.RetryPublisher, error) { return mq.NewPublisher(ctx, wakeURL(t, admin)) }}
	started := time.Now()
	if err := r.RetryWakeup(ctx, wakeDelivery(t, nil)); err != nil {
		t.Fatal(err)
	}
	d := awaitDelivery(t, ctx, ch, mq.WakeupQueue)
	if time.Since(started) < 5*time.Second || d.RoutingKey != "reconcile.retry" || !bytes.Equal(d.Body, body) || d.DeliveryMode != 2 {
		t.Fatalf("wrong wake TTL/route: %+v", d)
	}
	consumer := mq.WakeupConsumer{Handle: func(context.Context, mq.Wakeup) error { return nil }}
	if err := consumer.Process(ctx, d); err != nil {
		t.Fatal(err)
	}
	if _, ok, err := ch.Get(mq.MatchingQueue, false); err != nil || ok {
		t.Fatal("wakeup retry leaked into matching.requests", err)
	}
	poison := wakeEvent(t, body)
	poison.Exchange = ""
	poison.RoutingKey = mq.WakeupQueue
	poison.Payload = []byte(`{}`)
	if err := p.Publish(ctx, poison); err != nil {
		t.Fatal(err)
	}
	d = awaitDelivery(t, ctx, ch, mq.WakeupQueue)
	if err := consumer.Process(ctx, d); err != nil {
		t.Fatal(err)
	}
	dead := awaitDelivery(t, ctx, ch, mq.DeadQueue)
	if err := mq.ReplayDead(ctx, dead, []byte(`{}`), p); err == nil {
		t.Fatal("poison replay was accepted")
	}
	if err := mq.ReplayDead(ctx, dead, body, p); err != nil {
		t.Fatal(err)
	}
	replayed := awaitDelivery(t, ctx, ch, mq.WakeupQueue)
	if !bytes.Equal(replayed.Body, body) {
		t.Fatal("corrected replay changed body")
	}
	if err := consumer.Process(ctx, replayed); err != nil {
		t.Fatal(err)
	}
	if _, ok, err := ch.Get(mq.DeadQueue, false); err != nil || ok {
		t.Fatal("confirmed replay left dead message", err)
	}
	t.Log("wake TTL returns to matching.wakeups; poison rejected; corrected routed replay before dead ack")
}

func TestWakeupIntegrationSurvives25TransientFailures(t *testing.T) {
	// 25 real fixed 5-second TTL hops require >125s; no ordinary test budget changes.
	ctx, conn, admin := isolatedBrokerWithTimeout(t, 180*time.Second)
	p := wakePublisherLive(t, ctx, admin)
	ch, err := conn.Channel()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = ch.Close() }()
	raw, err := memstore.New(ctx, memstore.WithShards(3))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = raw.Close() }()
	result, err := raw.CreateEntity(ctx, store.CreateEntity{Meta: store.Meta{OperationID: "wake-healthy-create", At: 1000}, Key: store.RequestKey(requestID), Request: &store.Request{ID: requestID, Type: store.TypeMedicine, Geohash: "s000000", PeopleAffected: 1, Urgency: 1}})
	if err != nil || result.Outcome != store.OutcomeOK {
		t.Fatal(result, err)
	}
	m, err := workflow.New(workflow.Config{Store: raw, Now: func() time.Time { return time.UnixMilli(1000) }, ScanLimit: 32})
	if err != nil {
		t.Fatal(err)
	}
	failures, deferrals := 0, 0
	retry := mq.TransportRetry{PublisherFactory: func(ctx context.Context) (mq.RetryPublisher, error) {
		deferrals++
		return mq.NewPublisher(ctx, wakeURL(t, admin))
	}}
	consumer := mq.WakeupConsumer{Handle: func(ctx context.Context, w mq.Wakeup) error {
		if failures < 25 {
			failures++
			return store.ErrUnknownOutcome
		}
		return m.Recover(ctx, w.EntityID, 30*time.Second)
	}, Retry: retry.RetryWakeup}
	body := wakeDelivery(t, nil).Body
	if err := p.Publish(ctx, wakeEvent(t, body)); err != nil {
		t.Fatal(err)
	}
	started := time.Now()
	for i := 0; i < 26; i++ {
		d := awaitDelivery(t, ctx, ch, mq.WakeupQueue)
		if !bytes.Equal(d.Body, body) || d.DeliveryMode != 2 {
			t.Fatal("retry changed wake hint")
		}
		if n, ok := d.Headers["x-delivery-count"].(int64); ok && n >= 20 {
			t.Fatal("healthy wake burned delivery limit")
		}
		if err := consumer.Process(ctx, d); err != nil {
			t.Fatal(err)
		}
		rec, err := raw.ReadRecord(ctx, store.RequestKey(requestID))
		if err != nil {
			t.Fatal(err)
		}
		if i < 25 && (rec.Request.Status != store.RequestPending || rec.Request.RetryCount != 0) {
			t.Fatal("transport failure changed business state")
		}
	}
	if time.Since(started) < 125*time.Second || failures != 25 || deferrals != 25 {
		t.Fatal("fixed retry path not exercised", failures, deferrals)
	}
	rec, err := raw.ReadRecord(ctx, store.RequestKey(requestID))
	if err != nil || rec.Request.Status != store.RequestWaiting || rec.Request.RetryCount != 1 {
		t.Fatal("healthy wake did not recover", rec, err)
	}
	if _, ok, err := ch.Get(mq.DeadQueue, false); err != nil || ok {
		t.Fatal("healthy wake parked in dead queue", err)
	}
	var queue struct{ Arguments map[string]any }
	admin.call(t, ctx, http.MethodGet, "/api/queues/"+url.PathEscape(admin.vhost)+"/matching.wakeups", nil, &queue)
	if queue.Arguments["x-delivery-limit"] != float64(20) {
		t.Fatal("delivery limit weakened", queue.Arguments)
	}
	t.Logf("wakeFailures=%d confirmedDeferrals=%d deliveryLimit=20 businessRetryCount=1", failures, deferrals)
}
