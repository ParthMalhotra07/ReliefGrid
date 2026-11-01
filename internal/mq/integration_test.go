//go:build integration

package mq_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/ParthMalhotra07/ReliefGrid/internal/fsm/memstore"
	"github.com/ParthMalhotra07/ReliefGrid/internal/mq"
	"github.com/ParthMalhotra07/ReliefGrid/internal/store"
	"github.com/google/uuid"
	amqp "github.com/rabbitmq/amqp091-go"
)

func TestIntegrationRetryTTLDeadLetterRoutePreservesPriority(t *testing.T) {
	ctx, conn, management := isolatedBroker(t)
	ch, err := conn.Channel()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = ch.Close() }()
	input := eventDelivery(t, nil)
	started := time.Now()
	publishConfirmed(t, ctx, ch, "relief.retry", "delay.5s", input.Body, 3)
	d := awaitDelivery(t, ctx, ch, "matching.requests")
	if time.Since(started) < 5*time.Second {
		t.Fatal("message bypassed fixed 5-second queue TTL")
	}
	if d.RoutingKey != "request.retry" || d.Priority != 3 || d.DeliveryMode != amqp.Persistent || !bytes.Equal(d.Body, input.Body) {
		t.Fatalf("retry route corrupted message: route=%s priority=%d mode=%d body=%s", d.RoutingKey, d.Priority, d.DeliveryMode, d.Body)
	}
	deaths, ok := d.Headers["x-death"].([]any)
	if !ok || len(deaths) == 0 {
		t.Fatalf("no dead-letter provenance: %+v", d.Headers)
	}
	death, ok := deaths[0].(amqp.Table)
	if !ok || death["queue"] != "retry.5s" || death["reason"] != "expired" {
		t.Fatalf("wrong dead-letter hop: %+v", d.Headers)
	}
	if err := d.Ack(false); err != nil {
		t.Fatal(err)
	}
	assertLivePolicies(t, ctx, management)
	t.Log("TTL -> relief.requests/request.retry -> matching.requests; priority=3; reason=expired")
}

func TestIntegrationMalformedDeliveryReachesMatchingDead(t *testing.T) {
	ctx, conn, _ := isolatedBroker(t)
	s, err := memstore.New(ctx, memstore.WithShards(3))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = s.Close() }()
	c := mq.Consumer{Store: s, Handle: func(context.Context, store.Record) error {
		t.Error("poison reached business handler")
		return errors.New("poison")
	}}
	consumerCtx, cancel := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() { done <- c.Consume(consumerCtx, conn) }()
	defer func() {
		cancel()
		select {
		case err := <-done:
			if err != nil && !errors.Is(err, context.Canceled) {
				t.Error(err)
			}
		case <-ctx.Done():
			t.Error("consumer did not stop")
		}
	}()
	ch, err := conn.Channel()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = ch.Close() }()
	publishConfirmed(t, ctx, ch, "relief.requests", "request.created.water", []byte("malformed-event"), 1)
	d := awaitDelivery(t, ctx, ch, "matching.dead")
	if string(d.Body) != "malformed-event" {
		t.Fatalf("wrong poison body: %q", d.Body)
	}
	deaths, ok := d.Headers["x-death"].([]any)
	if !ok || len(deaths) == 0 {
		t.Fatalf("poison has no rejection provenance: %+v", d.Headers)
	}
	death, ok := deaths[0].(amqp.Table)
	if !ok || death["reason"] != "rejected" || death["queue"] != "matching.requests" {
		t.Fatalf("wrong poison route: %+v", d.Headers)
	}
	if err := d.Ack(false); err != nil {
		t.Fatal(err)
	}
	t.Log("malformed -> reject(requeue=false) -> relief.dlx -> matching.dead")
}

func TestIntegrationMissingRequestDeadLettersWithoutRestart(t *testing.T) {
	ctx, conn, _ := isolatedBroker(t)
	s, err := memstore.New(ctx, memstore.WithShards(3))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = s.Close() }()
	ch, err := conn.Channel()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = ch.Close() }()
	publishConfirmed(t, ctx, ch, "relief.requests", "request.created.medicine", eventDelivery(t, nil).Body, 2)
	d := awaitDelivery(t, ctx, ch, "matching.requests")
	c := mq.Consumer{Store: s, Handle: func(context.Context, store.Record) error {
		t.Fatal("missing request reached handler")
		return nil
	}}
	if err := c.Process(ctx, d); err != nil {
		t.Fatalf("missing request restarts consumer: %v", err)
	}
	dead := awaitDelivery(t, ctx, ch, "matching.dead")
	deaths, ok := dead.Headers["x-death"].([]any)
	if !ok || len(deaths) != 1 {
		t.Fatalf("unexpected death history: %+v", dead.Headers)
	}
	death, ok := deaths[0].(amqp.Table)
	if !ok || death["reason"] != "rejected" || death["queue"] != "matching.requests" || death["count"] != int64(1) {
		t.Fatalf("absence exhausted delivery limit instead of one rejection: %+v", dead.Headers)
	}
	if err := dead.Ack(false); err != nil {
		t.Fatal(err)
	}
	t.Log("missing request -> one reject(requeue=false) -> matching.dead; no consumer restart")
}

func TestIntegrationDomainEventsRouteToDurableWakeups(t *testing.T) {
	ctx, conn, admin := isolatedBroker(t)
	ch, err := conn.Channel()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = ch.Close() }()
	for _, key := range []string{"resource.registered.medicine", "resource.state_changed", "match.confirmed", "request.status.cancelled"} {
		body := []byte("durable wakeup " + key)
		publishConfirmed(t, ctx, ch, "relief.events", key, body, 2)
		d := awaitDelivery(t, ctx, ch, "matching.wakeups")
		if d.RoutingKey != key || d.DeliveryMode != amqp.Persistent || !bytes.Equal(d.Body, body) {
			t.Fatalf("wakeup changed: %+v", d)
		}
		if err := d.Ack(false); err != nil {
			t.Fatal(err)
		}
	}
	assertLivePolicies(t, ctx, admin)
	t.Log("all four relief.events patterns: mandatory routed confirm -> durable quorum matching.wakeups")
}

func TestIntegrationManualAckPrefetchAndInterruptedHandoff(t *testing.T) {
	ctx, conn, admin := isolatedBroker(t)
	s, err := memstore.New(ctx, memstore.WithShards(3))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = s.Close() }()
	r, err := s.CreateEntity(ctx, store.CreateEntity{
		Meta: store.Meta{OperationID: "create-live-consumer", At: 1}, Key: store.RequestKey(requestID),
		Request: &store.Request{ID: requestID, Type: store.TypeMedicine, PeopleAffected: 1, Urgency: 2, Geohash: "s000000"},
	})
	if err != nil || r.Outcome != store.OutcomeOK {
		t.Fatalf("fixture: %+v %v", r, err)
	}
	entered := make(chan struct{}, 1)
	c := mq.Consumer{Store: s, Handle: func(ctx context.Context, _ store.Record) error {
		entered <- struct{}{}
		<-ctx.Done()
		return ctx.Err()
	}}
	consumerCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- c.Consume(consumerCtx, conn) }()
	ch, err := conn.Channel()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = ch.Close() }()
	publishConfirmed(t, ctx, ch, "relief.requests", "request.created.medicine", eventDelivery(t, nil).Body, 2)
	select {
	case <-entered:
	case <-ctx.Done():
		t.Fatal("valid delivery never reached handler")
	}
	var consumers []struct {
		AckRequired bool `json:"ack_required"`
		Prefetch    int  `json:"prefetch_count"`
	}
	// The management database learns consumer metadata asynchronously. Await
	// its appearance within the original test context before checking the
	// same exact protocol settings; handler entry already proves delivery.
	metadataPoll := time.NewTicker(20 * time.Millisecond)
	defer metadataPoll.Stop()
	for {
		admin.call(t, ctx, http.MethodGet, "/api/consumers/"+url.PathEscape(admin.vhost), nil, &consumers)
		if len(consumers) != 0 {
			break
		}
		select {
		case <-ctx.Done():
			t.Fatal("consumer metadata did not appear within the original deadline")
		case <-metadataPoll.C:
		}
	}
	if len(consumers) != 1 || !consumers[0].AckRequired || consumers[0].Prefetch != 8 {
		t.Fatalf("manual ack / prefetch mismatch: %+v", consumers)
	}
	cancel()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("interrupted handoff reported success")
		}
	case <-ctx.Done():
		t.Fatal("consumer failed to drain")
	}
	d := awaitDelivery(t, ctx, ch, "matching.requests")
	if !d.Redelivered {
		t.Fatal("interrupted delivery was lost or acknowledged before handoff")
	}
	if err := d.Ack(false); err != nil {
		t.Fatal(err)
	}
	t.Log("prefetch=8; manual ack=true; interrupted handoff redelivered")
}

type brokerAdmin struct{ endpoint, user, password, vhost string }

func isolatedBroker(t *testing.T) (context.Context, *amqp.Connection, brokerAdmin) {
	t.Helper()
	return isolatedBrokerWithTimeout(t, 30*time.Second)
}

func isolatedBrokerWithTimeout(t *testing.T, budget time.Duration) (context.Context, *amqp.Connection, brokerAdmin) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), budget)
	t.Cleanup(cancel)
	amqpURL := os.Getenv("MQ_AMQP_URL")
	endpoint := os.Getenv("MQ_MANAGEMENT_URL")
	if amqpURL == "" || endpoint == "" {
		t.Fatal("live integration requires MQ_AMQP_URL and MQ_MANAGEMENT_URL; no broker tests are skipped")
	}
	u, err := url.Parse(amqpURL)
	if err != nil {
		t.Fatal(err)
	}
	if u.User == nil {
		t.Fatal("test AMQP URL requires isolated-broker administrator credentials")
	}
	password, _ := u.User.Password()
	a := brokerAdmin{endpoint: strings.TrimRight(endpoint, "/"), user: u.User.Username(), password: password, vhost: "a1.3-test-" + uuid.NewString()}
	a.call(t, ctx, http.MethodPut, "/api/vhosts/"+url.PathEscape(a.vhost), []byte(`{}`), nil)
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cleanupCancel()
		a.call(t, cleanupCtx, http.MethodDelete, "/api/vhosts/"+url.PathEscape(a.vhost), nil, nil)
	})
	a.call(t, ctx, http.MethodPut, "/api/permissions/"+url.PathEscape(a.vhost)+"/"+url.PathEscape(a.user), []byte(`{"configure":".*","write":".*","read":".*"}`), nil)
	a.call(t, ctx, http.MethodPost, "/api/definitions/"+url.PathEscape(a.vhost), mq.Definitions(), nil)
	u.Path = "/" + a.vhost
	conn, err := mq.Dial(ctx, u.String())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	if err := mq.Declare(ctx, conn); err != nil {
		t.Fatal(err)
	}
	if err := mq.Declare(ctx, conn); err != nil {
		t.Fatalf("topology redeclaration not idempotent: %v", err)
	}
	return ctx, conn, a
}

func (a brokerAdmin) call(t *testing.T, ctx context.Context, method, path string, body []byte, result any) {
	t.Helper()
	req, err := http.NewRequestWithContext(ctx, method, a.endpoint+path, bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	req.SetBasicAuth(a.user, a.password)
	req.Header.Set("Content-Type", "application/json")
	client := http.Client{Timeout: 5 * time.Second}
	response, err := client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = response.Body.Close() }()
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		b, _ := io.ReadAll(io.LimitReader(response.Body, 2048))
		t.Fatalf("broker admin %s %s: %s %s", method, path, response.Status, b)
	}
	if result != nil {
		if err := json.NewDecoder(response.Body).Decode(result); err != nil {
			t.Fatal(err)
		}
	}
}

func publishConfirmed(t *testing.T, ctx context.Context, ch *amqp.Channel, exchange, key string, body []byte, priority uint8) {
	t.Helper()
	if err := ch.Confirm(false); err != nil {
		t.Fatal(err)
	}
	returns := ch.NotifyReturn(make(chan amqp.Return, 1))
	confirmation, err := ch.PublishWithDeferredConfirmWithContext(ctx, exchange, key, true, false, amqp.Publishing{
		DeliveryMode: amqp.Persistent, Priority: priority, ContentType: "application/json", MessageId: uuid.NewString(), Body: body,
	})
	if err != nil {
		t.Fatal(err)
	}
	if confirmation == nil {
		t.Fatal("publisher confirmation missing")
	}
	ack, err := confirmation.WaitContext(ctx)
	if err != nil || !ack {
		t.Fatalf("publish confirm=%v err=%v", ack, err)
	}
	select {
	case returned := <-returns:
		t.Fatalf("mandatory return: %s", returned.ReplyText)
	default:
	}
}

func awaitDelivery(t *testing.T, ctx context.Context, ch *amqp.Channel, queue string) amqp.Delivery {
	t.Helper()
	poll := time.NewTicker(20 * time.Millisecond)
	defer poll.Stop()
	for {
		d, ok, err := ch.Get(queue, false)
		if err != nil {
			t.Fatal(err)
		}
		if ok {
			return d
		}
		select {
		case <-ctx.Done():
			t.Fatalf("waiting for %s: %v", queue, ctx.Err())
		case <-poll.C:
		}
	}
}

func assertLivePolicies(t *testing.T, ctx context.Context, a brokerAdmin) {
	t.Helper()
	for _, tc := range []struct {
		queue, dlx, route string
		ttl               int
	}{
		{"retry.5s", "relief.requests", "request.retry", 5000},
		{"retry.30s", "relief.requests", "request.retry", 30000},
		{"retry.120s", "relief.requests", "request.retry", 120000},
		{"matching.requests", "relief.dlx", "", 0},
		{"matching.wakeups", "relief.dlx", "", 0},
	} {
		var q struct {
			Type      string
			Arguments map[string]any
			Policy    string
			Effective map[string]any `json:"effective_policy_definition"`
			Members   []string
		}
		// Management statistics publish asynchronously. Await their presence
		// inside the original test context, then assert the exact policy below.
		metadataPoll := time.NewTicker(20 * time.Millisecond)
		for {
			a.call(t, ctx, http.MethodGet, "/api/queues/"+url.PathEscape(a.vhost)+"/"+url.PathEscape(tc.queue), nil, &q)
			if q.Policy != "" && len(q.Effective) != 0 {
				break
			}
			select {
			case <-ctx.Done():
				metadataPoll.Stop()
				t.Fatalf("%s policy metadata did not appear within original deadline: %v", tc.queue, ctx.Err())
			case <-metadataPoll.C:
			}
		}
		metadataPoll.Stop()
		if q.Type != "quorum" || len(q.Members) != 1 || q.Policy == "" {
			t.Errorf("%s quorum/policy: %+v", tc.queue, q)
		}
		for key, want := range map[string]any{"dead-letter-strategy": "at-least-once", "overflow": "reject-publish", "dead-letter-exchange": tc.dlx} {
			if q.Effective[key] != want {
				t.Errorf("%s effective %s=%v want %v", tc.queue, key, q.Effective[key], want)
			}
		}
		if tc.ttl > 0 && (q.Arguments["x-message-ttl"] != float64(tc.ttl) || q.Effective["dead-letter-routing-key"] != tc.route) {
			t.Errorf("%s retry TTL/route: %+v", tc.queue, q)
		}
	}
}
