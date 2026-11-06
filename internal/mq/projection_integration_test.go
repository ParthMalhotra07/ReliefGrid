//go:build integration && a41

package mq_test

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/ParthMalhotra07/ReliefGrid/internal/mq"
	"github.com/ParthMalhotra07/ReliefGrid/internal/store"
	"net/url"
	"os"
	"testing"
	"time"
)

func projectionAddress(t *testing.T, admin brokerAdmin) string {
	t.Helper()
	u, err := url.Parse(os.Getenv("MQ_AMQP_URL"))
	if err != nil {
		t.Fatal(err)
	}
	u.Path = "/" + admin.vhost
	return u.String()
}
func projectionPublisherEvent(t *testing.T) store.OutboxEvent {
	t.Helper()
	body := eventDelivery(t, nil).Body
	var v struct {
		EventID string `json:"eventId"`
	}
	if err := json.Unmarshal(body, &v); err != nil {
		t.Fatal(err)
	}
	return store.OutboxEvent{EventID: v.EventID, EventType: "request.created", SchemaVersion: 1, Exchange: "relief.requests", RoutingKey: "request.created.water", Payload: body}
}
func startProjection(t *testing.T, ctx context.Context, address string) (<-chan []byte, context.CancelFunc, <-chan error) {
	t.Helper()
	run, cancel := context.WithCancel(ctx)
	got := make(chan []byte, 8)
	ready := make(chan bool, 8)
	done := make(chan error, 1)
	go func() {
		done <- mq.ProjectEvents(run, address, func(body []byte) error { got <- append([]byte(nil), body...); return nil }, func(ok bool) { ready <- ok })
	}()
	select {
	case ok := <-ready:
		if !ok {
			t.Fatal("projection did not become ready")
		}
	case <-ctx.Done():
		t.Fatal("projection did not start")
	}
	return got, cancel, done
}
func stopProjection(t *testing.T, ctx context.Context, cancel context.CancelFunc, done <-chan error) {
	t.Helper()
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatal(err)
		}
	case <-ctx.Done():
		t.Error("projection did not stop")
	}
}
func TestProjectionCannotMaskMissingDurableWorkQueue(t *testing.T) {
	ctx, conn, admin := isolatedBroker(t)
	address := projectionAddress(t, admin)
	publisher, err := mq.NewPublisher(ctx, address)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = publisher.Close() }()
	_, cancel, done := startProjection(t, ctx, address)
	defer stopProjection(t, ctx, cancel, done)
	ch, err := conn.Channel()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = ch.Close() }()
	if _, err = ch.QueueDelete(mq.MatchingQueue, false, false, false); err != nil {
		t.Fatal(err)
	}
	if err = publisher.Publish(ctx, projectionPublisherEvent(t)); !errors.Is(err, mq.ErrUnroutable) {
		t.Fatalf("temporary UI subscriber masked missing durable queue: %v", err)
	}
}
func TestProjectionFansOutWithoutConsumingWorkflowDelivery(t *testing.T) {
	ctx, conn, admin := isolatedBroker(t)
	address := projectionAddress(t, admin)
	one, cancelOne, doneOne := startProjection(t, ctx, address)
	defer stopProjection(t, ctx, cancelOne, doneOne)
	two, cancelTwo, doneTwo := startProjection(t, ctx, address)
	defer stopProjection(t, ctx, cancelTwo, doneTwo)
	publisher, err := mq.NewPublisher(ctx, address)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = publisher.Close() }()
	event := projectionPublisherEvent(t)
	if err = publisher.Publish(ctx, event); err != nil {
		t.Fatal(err)
	}
	for _, got := range []<-chan []byte{one, two} {
		select {
		case body := <-got:
			var hint map[string]any
			if err := json.Unmarshal(body, &hint); err != nil {
				t.Fatal(err)
			}
			if hint["eventId"] != event.EventID || hint["payload"] != nil || len(hint) != 4 {
				t.Fatal("projection must contain only committed identity/version hints")
			}
		case <-ctx.Done():
			t.Fatal("per-API projection missing")
		}
	}
	ch, err := conn.Channel()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = ch.Close() }()
	d := awaitDelivery(t, ctx, ch, mq.MatchingQueue)
	if string(d.Body) != string(event.Payload) {
		t.Fatal("workflow delivery lost")
	}
	if err = d.Ack(false); err != nil {
		t.Fatal(err)
	}
	// Cancellation removes only the temporary projection; canonical workflow
	// queues retain their topology. Start/stop is bounded by the root context.
	t.Log("two API copies observed; original work delivery retained")
}
func TestProjectionCancellationDuringUnavailableBrokerIsBounded(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	start := time.Now()
	err := mq.ProjectEvents(ctx, "amqp://127.0.0.1:1", func([]byte) error { return nil }, func(bool) {})
	if !errors.Is(err, context.Canceled) || time.Since(start) > time.Second {
		t.Fatal(err)
	}
}
