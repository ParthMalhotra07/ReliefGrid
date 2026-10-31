//go:build integration && a22

package mq_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/url"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ParthMalhotra07/ReliefGrid/internal/fsm/memstore"
	"github.com/ParthMalhotra07/ReliefGrid/internal/mq"
	"github.com/ParthMalhotra07/ReliefGrid/internal/store"
	"github.com/ParthMalhotra07/ReliefGrid/internal/workflow"
)

func retryBrokerURL(t *testing.T, admin brokerAdmin) string {
	t.Helper()
	u, err := url.Parse(os.Getenv("MQ_AMQP_URL"))
	if err != nil {
		t.Fatal(err)
	}
	u.Path = "/" + admin.vhost
	return u.String()
}

func retryLiveStore(t *testing.T, ctx context.Context) *memstore.Store {
	t.Helper()
	s, err := memstore.New(ctx, memstore.WithShards(3))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := s.Close(); err != nil {
			t.Error(err)
		}
	})
	r, err := s.CreateEntity(ctx, store.CreateEntity{Meta: store.Meta{OperationID: "healthy-created", At: 1000},
		Key: store.RequestKey(requestID), Request: &store.Request{ID: requestID, Type: store.TypeMedicine, PeopleAffected: 4, Urgency: 2, Geohash: "s000000"}})
	if err != nil || r.Outcome != store.OutcomeOK {
		t.Fatalf("create: %+v %v", r, err)
	}
	return s
}

func retryLivePublisher(t *testing.T, ctx context.Context, address string) *mq.ConfirmedPublisher {
	t.Helper()
	p, err := mq.NewPublisher(ctx, address)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = p.Close() })
	return p
}

func publishRetrySource(t *testing.T, ctx context.Context, p *mq.ConfirmedPublisher) []byte {
	t.Helper()
	body := eventDelivery(t, nil).Body
	var e struct {
		EventID string `json:"eventId"`
	}
	if err := json.Unmarshal(body, &e); err != nil {
		t.Fatal(err)
	}
	if err := p.Publish(ctx, store.OutboxEvent{EventID: e.EventID, EventType: "request.created", SchemaVersion: 1,
		Exchange: "relief.requests", RoutingKey: "request.created.medicine", Payload: body}); err != nil {
		t.Fatal(err)
	}
	return body
}

func TestRetryTransportIntegrationSurvives25TransientFailures(t *testing.T) {
	// Twenty-five real fixed 5s TTL hops need at least 125s. This new test's
	// 180s budget is derived from that path; every existing test keeps 30s.
	ctx, conn, admin := isolatedBrokerWithTimeout(t, 180*time.Second)
	address := retryBrokerURL(t, admin)
	raw := retryLiveStore(t, ctx)
	var failRead, failHandle atomic.Bool
	var readFailures, handoffFailures, deferrals atomic.Int32
	s := memstore.WithFaults(raw, func(c memstore.Call) memstore.Fault {
		if c.Method == "ReadRecord" && failRead.Load() {
			readFailures.Add(1)
			return memstore.Fault{DropBefore: true}
		}
		return memstore.Fault{}
	})
	m, err := workflow.New(workflow.Config{Store: s, ScanLimit: 100, Now: func() time.Time { return time.UnixMilli(1000) }})
	if err != nil {
		t.Fatal(err)
	}
	r := mq.TransportRetry{PublisherFactory: func(ctx context.Context) (mq.RetryPublisher, error) {
		deferrals.Add(1)
		return mq.NewPublisher(ctx, address)
	}}
	c := mq.Consumer{Store: s, Handle: func(ctx context.Context, rec store.Record) error {
		if failHandle.Load() {
			handoffFailures.Add(1)
			return store.ErrUnknownOutcome
		}
		return m.Handle(ctx, rec.Request.ID)
	}, Retry: r.Retry}
	p := retryLivePublisher(t, ctx, address)
	body := publishRetrySource(t, ctx, p)
	ch, err := conn.Channel()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = ch.Close() }()
	started := time.Now()
	for i := 0; i < 25; i++ {
		d := awaitDelivery(t, ctx, ch, mq.MatchingQueue)
		if !bytes.Equal(d.Body, body) || d.Priority != 2 || d.DeliveryMode != 2 {
			t.Fatalf("retry changed body/priority/persistence: %+v", d)
		}
		if count, ok := d.Headers["x-delivery-count"].(int64); ok && count >= 20 {
			t.Fatalf("healthy work approached poison delivery limit: %+v", d.Headers)
		}
		failRead.Store(i%2 == 0)
		failHandle.Store(i%2 != 0)
		if err := c.Process(ctx, d); err != nil {
			t.Fatalf("durable deferral %d: %v", i+1, err)
		}
		rec, err := raw.ReadRecord(ctx, store.RequestKey(requestID))
		if err != nil || rec.Request.Status != store.RequestPending || rec.Request.RetryCount != 0 {
			t.Fatalf("transport failures changed business state: %+v %v", rec, err)
		}
	}
	failRead.Store(false)
	failHandle.Store(false)
	d := awaitDelivery(t, ctx, ch, mq.MatchingQueue)
	if time.Since(started) < 125*time.Second {
		t.Fatal("healthy retry bypassed the fixed 5s queue TTL")
	}
	if !bytes.Equal(d.Body, body) || d.Priority != 2 || d.RoutingKey != "request.retry" {
		t.Fatalf("last retry lost immutable hint/priority/route: %+v", d)
	}
	if err := c.Process(ctx, d); err != nil {
		t.Fatal(err)
	}
	rec, err := raw.ReadRecord(ctx, store.RequestKey(requestID))
	if err != nil || rec.Request.Status != store.RequestWaiting || rec.Request.RetryCount != 1 || rec.Request.NextAttemptAt != 6000 {
		t.Fatalf("healthy work failed to recover: %+v %v", rec, err)
	}
	if readFailures.Load()+handoffFailures.Load() != 25 || deferrals.Load() != 25 {
		t.Fatalf("faults not exercised: reads=%d handoffs=%d deferrals=%d", readFailures.Load(), handoffFailures.Load(), deferrals.Load())
	}
	if _, ok, err := ch.Get(mq.DeadQueue, false); err != nil || ok {
		t.Fatalf("healthy work parked as poison: ok=%v err=%v", ok, err)
	}
	var queue struct{ Arguments map[string]any }
	admin.call(t, ctx, http.MethodGet, "/api/queues/"+url.PathEscape(admin.vhost)+"/matching.requests", nil, &queue)
	if queue.Arguments["x-delivery-limit"] != float64(20) {
		t.Fatalf("delivery limit was changed: %+v", queue.Arguments)
	}
	assertLivePolicies(t, ctx, admin)
	t.Logf("readFailures=%d handoffFailures=%d confirmedDeferrals=%d deliveryLimit=20 businessRetryCount=1; healthy work recovered after >20 failures", readFailures.Load(), handoffFailures.Load(), deferrals.Load())
}

func TestRetryTransportIntegrationUnroutableRetainsOriginal(t *testing.T) {
	ctx, conn, admin := isolatedBroker(t)
	address := retryBrokerURL(t, admin)
	raw := retryLiveStore(t, ctx)
	p := retryLivePublisher(t, ctx, address)
	ch, err := conn.Channel()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = ch.Close() }()
	body := publishRetrySource(t, ctx, p)
	d := awaitDelivery(t, ctx, ch, mq.MatchingQueue)
	// The publisher is already declared. Remove only this isolated vhost's
	// empty retry target, so mandatory publishing really returns NO_ROUTE.
	if _, err := ch.QueueDelete("retry.5s", false, false, false); err != nil {
		t.Fatal(err)
	}
	r := mq.TransportRetry{PublisherFactory: func(context.Context) (mq.RetryPublisher, error) { return p, nil }}
	s := memstore.WithFaults(raw, func(c memstore.Call) memstore.Fault { return memstore.Fault{DropBefore: c.Method == "ReadRecord"} })
	c := mq.Consumer{Store: s, Handle: func(context.Context, store.Record) error { t.Fatal("unavailable shard reached handler"); return nil }, Retry: r.Retry}
	if err := c.Process(ctx, d); !errors.Is(err, mq.ErrUnroutable) {
		t.Fatalf("mandatory return = %v", err)
	}
	if err := ch.Close(); err != nil {
		t.Fatal(err)
	}
	recovered, err := conn.Channel()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = recovered.Close() }()
	again := awaitDelivery(t, ctx, recovered, mq.MatchingQueue)
	if !again.Redelivered || !bytes.Equal(again.Body, body) {
		t.Fatal("failed delayed publish disposed the original delivery")
	}
	if err := again.Ack(false); err != nil {
		t.Fatal(err)
	}
	t.Log("mandatory NO_ROUTE leaves original unacknowledged and redeliverable")
}

type lostRetryConfirm struct{ *mq.ConfirmedPublisher }

func (p lostRetryConfirm) Publish(ctx context.Context, e store.OutboxEvent) error {
	if err := p.ConfirmedPublisher.Publish(ctx, e); err != nil {
		return err
	}
	return store.ErrUnknownOutcome
}

func TestRetryTransportIntegrationLostConfirmIsDuplicateSafe(t *testing.T) {
	ctx, conn, admin := isolatedBroker(t)
	address := retryBrokerURL(t, admin)
	raw := retryLiveStore(t, ctx)
	p := retryLivePublisher(t, ctx, address)
	ch, err := conn.Channel()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = ch.Close() }()
	body := publishRetrySource(t, ctx, p)
	d := awaitDelivery(t, ctx, ch, mq.MatchingQueue)
	r := mq.TransportRetry{PublisherFactory: func(ctx context.Context) (mq.RetryPublisher, error) {
		pub, err := mq.NewPublisher(ctx, address)
		if err != nil {
			return nil, err
		}
		return lostRetryConfirm{pub}, nil
	}}
	s := memstore.WithFaults(raw, func(c memstore.Call) memstore.Fault { return memstore.Fault{DropBefore: c.Method == "ReadRecord"} })
	c := mq.Consumer{Store: s, Handle: func(context.Context, store.Record) error { return nil }, Retry: r.Retry}
	if err := c.Process(ctx, d); !errors.Is(err, store.ErrUnknownOutcome) {
		t.Fatalf("lost confirmation = %v", err)
	}
	if err := ch.Close(); err != nil {
		t.Fatal(err)
	}
	recovered, err := conn.Channel()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = recovered.Close() }()
	m, err := workflow.New(workflow.Config{Store: raw, ScanLimit: 100, Now: func() time.Time { return time.UnixMilli(1000) }})
	if err != nil {
		t.Fatal(err)
	}
	healthy := mq.Consumer{Store: raw, Handle: func(ctx context.Context, rec store.Record) error { return m.Handle(ctx, rec.Request.ID) }}
	first := awaitDelivery(t, ctx, recovered, mq.MatchingQueue)
	if !first.Redelivered || !bytes.Equal(first.Body, body) {
		t.Fatal("unknown publish lost the original")
	}
	if err := healthy.Process(ctx, first); err != nil {
		t.Fatal(err)
	}
	before, err := raw.ReadRecord(ctx, store.RequestKey(requestID))
	if err != nil {
		t.Fatal(err)
	}
	copy := awaitDelivery(t, ctx, recovered, mq.MatchingQueue)
	if !bytes.Equal(copy.Body, body) {
		t.Fatal("confirmed copy changed identity/payload")
	}
	if err := healthy.Process(ctx, copy); err != nil {
		t.Fatal(err)
	}
	after, err := raw.ReadRecord(ctx, before.Key)
	if err != nil || after.Version != before.Version || after.Request.RetryCount != 1 {
		t.Fatalf("duplicate delayed hint repeated business transition: %+v %v", after, err)
	}
	t.Log("lost confirm preserves original plus delayed copy; both hints produce one durable business retry")
}
