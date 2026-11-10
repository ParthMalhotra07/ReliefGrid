//go:build schedules

package workflow

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/ParthMalhotra07/ReliefGrid/internal/mq"
	"github.com/ParthMalhotra07/ReliefGrid/internal/outbox"
	"github.com/ParthMalhotra07/ReliefGrid/internal/store"
	amqp "github.com/rabbitmq/amqp091-go"
)

func brokerForExecution(x *execution) (*amqp.Connection, string) {
	x.t.Helper()
	if x.brokerConnection != nil {
		return x.brokerConnection, x.brokerAddress
	}
	raw := os.Getenv("MQ_AMQP_URL")
	management := os.Getenv("MQ_MANAGEMENT_URL")
	if raw == "" || management == "" {
		x.t.Fatal("SCH15/SCH16 require the pinned disposable broker (MQ_AMQP_URL and MQ_MANAGEMENT_URL)")
	}
	u, err := url.Parse(raw)
	if err != nil || u.User == nil {
		x.t.Fatal("invalid isolated broker URL")
	}
	password, _ := u.User.Password()
	vhost := "a23-" + x.id("broker")
	call := func(ctx context.Context, method, path string, body []byte) {
		x.t.Helper()
		req, err := http.NewRequestWithContext(ctx, method, strings.TrimRight(management, "/")+path, bytes.NewReader(body))
		if err != nil {
			x.t.Fatal(err)
		}
		req.SetBasicAuth(u.User.Username(), password)
		req.Header.Set("Content-Type", "application/json")
		response, err := (&http.Client{Timeout: 5 * time.Second}).Do(req)
		if err != nil {
			x.t.Fatal(err)
		}
		defer func() { _ = response.Body.Close() }()
		if response.StatusCode < 200 || response.StatusCode >= 300 {
			body, _ := io.ReadAll(io.LimitReader(response.Body, 500))
			x.t.Fatalf("broker setup HTTP%d: %s", response.StatusCode, body)
		}
	}
	call(x.ctx, "PUT", "/api/vhosts/"+url.PathEscape(vhost), []byte(`{}`))
	x.t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		call(ctx, "DELETE", "/api/vhosts/"+url.PathEscape(vhost), nil)
	})
	call(x.ctx, "PUT", "/api/permissions/"+url.PathEscape(vhost)+"/"+url.PathEscape(u.User.Username()), []byte(`{"configure":".*","write":".*","read":".*"}`))
	call(x.ctx, "POST", "/api/definitions/"+url.PathEscape(vhost), mq.Definitions())
	u.Path = "/" + vhost
	conn, err := mq.Dial(x.ctx, u.String())
	if err != nil {
		x.t.Fatal(err)
	}
	x.t.Cleanup(func() { _ = conn.Close() })
	if err := mq.Declare(x.ctx, conn); err != nil {
		x.t.Fatal(err)
	}
	x.brokerConnection, x.brokerAddress = conn, u.String()
	return conn, u.String()
}

func retryHint(x *execution, meta store.Meta, version uint64) store.OutboxEvent {
	id := x.id("retry-outbox")
	body, err := json.Marshal(map[string]any{"schemaVersion": 1, "eventId": id, "entityId": x.entities["R"], "entityVersion": version, "eventType": "request.retry", "occurredAt": time.UnixMilli(meta.At).UTC().Format(time.RFC3339Nano), "payload": map[string]any{"requestId": x.entities["R"], "workflowGeneration": 1, "nextAttemptAt": meta.At + 1, "urgency": 1}})
	if err != nil {
		x.t.Fatal(err)
	}
	return store.OutboxEvent{EventID: id, EventType: "request.retry", SchemaVersion: 1, Exchange: "relief.requests", RoutingKey: "request.retry", Payload: body}
}

type unavailablePublisher struct{ address string }

func (p unavailablePublisher) Publish(ctx context.Context, _ store.OutboxEvent) error {
	publisher, err := mq.NewPublisher(ctx, p.address)
	if err != nil {
		return err
	}
	_ = publisher.Close()
	return fmt.Errorf("fault endpoint unexpectedly became available")
}
func (unavailablePublisher) Close() error { return nil }

type observedPublisher struct {
	outbox.Publisher
	seen map[string]bool
}

func (p *observedPublisher) Publish(ctx context.Context, e store.OutboxEvent) error {
	if err := p.Publisher.Publish(ctx, e); err != nil {
		return err
	}
	p.seen[e.EventID] = true
	return nil
}

func executeBrokerOutage(x *execution) {
	_, address := brokerForExecution(x)
	relay, err := outbox.New(outbox.Config{Store: x.s})
	if err != nil {
		x.t.Fatal(err)
	}
	listener, err := (&net.ListenConfig{}).Listen(x.ctx, "tcp", "127.0.0.1:0")
	if err != nil {
		x.t.Fatal(err)
	}
	badAddress := "amqp://guest:guest@" + listener.Addr().String() + "/"
	_ = listener.Close()
	report, outageErr := relay.Step(x.ctx, x.requestShard, unavailablePublisher{address: badAddress})
	failed := outageErr != nil
	if report.Marked != 0 || report.Published != 0 || report.Read == 0 {
		x.t.Fatal("broker outage marked a handoff or did not exercise pending work")
	}
	if !failed || x.read("R").Request.Status != store.RequestRetrying {
		x.t.Fatal("outage did not preserve retry intent")
	}
	publisher, err := mq.NewPublisher(x.ctx, address)
	if err != nil {
		x.t.Fatal(err)
	}
	defer func() { _ = publisher.Close() }()
	observed := &observedPublisher{Publisher: publisher, seen: map[string]bool{}}
	for pass := 0; pass < 100 && !observed.seen[x.id("retry-outbox")]; pass++ {
		if _, err := relay.Step(x.ctx, x.requestShard, observed); err != nil {
			x.t.Fatal(err)
		}
	}
	if !observed.seen[x.id("retry-outbox")] {
		x.t.Fatal("pending retry was not confirmed after transport recovered")
	}
	x.closeRetry()
	x.match(x.attempts["attemptB"])
}

func publishScheduleMessage(t *testing.T, ctx context.Context, ch *amqp.Channel, body []byte) {
	t.Helper()
	if err := ch.Confirm(false); err != nil {
		t.Fatal(err)
	}
	confirmed := ch.NotifyPublish(make(chan amqp.Confirmation, 1))
	if err := ch.PublishWithContext(ctx, "relief.requests", "request.created.water", true, false, amqp.Publishing{DeliveryMode: amqp.Persistent, ContentType: "application/json", Body: body}); err != nil {
		t.Fatal(err)
	}
	select {
	case confirmation := <-confirmed:
		if !confirmation.Ack {
			t.Fatal("publication was nacked")
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
}

func executePoison(x *execution) {
	conn, _ := brokerForExecution(x)
	ch, err := conn.Channel()
	if err != nil {
		x.t.Fatal(err)
	}
	defer func() { _ = ch.Close() }()
	consumer := mq.Consumer{Store: x.s, Handle: func(ctx context.Context, r store.Record) error {
		_, err := x.matcher(x.s).Match(ctx, r.Request.ID)
		return err
	}}
	publishScheduleMessage(x.t, x.ctx, ch, []byte("invalid schedule envelope"))
	delivery, ok, err := ch.Get(mq.MatchingQueue, false)
	if err != nil || !ok {
		x.t.Fatal("poison was not routed", err)
	}
	if err := consumer.Process(x.ctx, delivery); err != nil {
		x.t.Fatal(err)
	}
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		dead, ok, err := ch.Get(mq.DeadQueue, false)
		if err != nil {
			x.t.Fatal(err)
		}
		if ok {
			if string(dead.Body) != "invalid schedule envelope" {
				x.t.Fatal("wrong dead letter")
			}
			if err := dead.Ack(false); err != nil {
				x.t.Fatal(err)
			}
			break
		}
		select {
		case <-ticker.C:
		case <-x.ctx.Done():
			x.t.Fatal("poison never reached durable dead queue")
		}
	}
	schema, err := os.ReadFile("../../contracts/events/request_created.json")
	if err != nil {
		x.t.Fatal(err)
	}
	var document struct{ Examples []map[string]any }
	if err := json.Unmarshal(schema, &document); err != nil || len(document.Examples) == 0 {
		x.t.Fatal("missing created example", err)
	}
	event := document.Examples[0]
	event["entityId"] = x.entities["R"]
	event["eventId"] = x.id("healthy-event")
	event["payload"].(map[string]any)["requestId"] = x.entities["R"]
	body, err := json.Marshal(event)
	if err != nil {
		x.t.Fatal(err)
	}
	publishScheduleMessage(x.t, x.ctx, ch, body)
	delivery, ok, err = ch.Get(mq.MatchingQueue, false)
	if err != nil || !ok {
		x.t.Fatal("healthy hint not routed", err)
	}
	if err := consumer.Process(x.ctx, delivery); err != nil {
		x.t.Fatal(err)
	}
	actual := x.read("R")
	if actual.Request.CurrentAttempt == nil {
		x.t.Fatal("healthy traffic did not progress after poison")
	}
	x.attempts["attemptA"] = *actual.Request.CurrentAttempt
}
