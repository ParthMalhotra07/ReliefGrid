//go:build integration

package outbox_test

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/ParthMalhotra07/ReliefGrid/internal/mq"
	"github.com/ParthMalhotra07/ReliefGrid/internal/outbox"
	"github.com/ParthMalhotra07/ReliefGrid/internal/store"
	"github.com/ParthMalhotra07/ReliefGrid/internal/workflow"
	"github.com/google/uuid"
	amqp "github.com/rabbitmq/amqp091-go"
)

func TestIntegrationUnroutableConfirmNeverMarksAndNextPublishSucceeds(t *testing.T) {
	b := liveBroker(t)
	ctx, s := fixtureStore(t)
	seed(t, ctx, s, 1)
	rows, err := s.PendingOutbox(ctx, store.OutboxQuery{Shard: 0, Limit: 100})
	if err != nil {
		t.Fatal(err)
	}
	p := b.publisher(t)
	defer func() { _ = p.Close() }()
	r := relay(t, s)
	unroutable := &rewriteRoute{Publisher: p}
	if report, err := r.Step(ctx, 0, unroutable); !errors.Is(err, mq.ErrUnroutable) || report.Marked != 0 {
		t.Fatalf("unroutable marked: %+v %v", report, err)
	}
	pending, err := s.PendingOutbox(ctx, store.OutboxQuery{Shard: 0, Limit: 100})
	if err != nil || len(pending) != 1 {
		t.Fatalf("unroutable lost: %d %v", len(pending), err)
	}
	if report, err := r.Step(ctx, 0, p); err != nil || report.Marked != 1 {
		t.Fatalf("publisher poisoned by mandatory return: %+v %v", report, err)
	}
	conn, ch := b.channel(t)
	defer func() { _ = conn.CloseDeadline(time.Now().Add(time.Second)) }()
	d := receive(t, b.ctx, ch)
	if d.MessageId != rows[0].EventID || !bytes.Equal(d.Body, rows[0].Payload) || d.Priority != 2 || d.DeliveryMode != amqp.Persistent {
		t.Fatalf("message changed: %+v", d)
	}
	if err := d.Ack(false); err != nil {
		t.Fatal(err)
	}
	t.Log("mandatory return + broker ack leaves row pending; same session routes retry; only routed confirm marks")
}

type rewriteRoute struct{ outbox.Publisher }

func (p *rewriteRoute) Publish(ctx context.Context, e store.OutboxEvent) error {
	e.RoutingKey = "no.subscriber"
	return p.Publisher.Publish(ctx, e)
}

func TestIntegrationBrokerStopsMidBatchAndPendingRowsRecover(t *testing.T) {
	b := liveBroker(t)
	broker := os.Getenv("A14_BROKER_ID")
	if broker == "" {
		t.Fatal("A1.4 acceptance requires its own labelled disposable Docker broker")
	}
	label := docker(t, b.ctx, "inspect", "--format", `{{ index .Config.Labels "reliefgrid.goal" }}`, broker)
	if strings.TrimSpace(label) != "A1.4" {
		t.Fatal("refusing to stop a broker not owned by the A1.4 verifier")
	}
	ctx, s := fixtureStore(t)
	seed(t, ctx, s, 3)
	p := b.publisher(t)
	defer func() { _ = p.Close() }()
	stopped := false
	t.Cleanup(func() {
		if stopped {
			docker(t, b.ctx, "start", broker)
			b.ready(t)
		}
	})
	r := relay(t, s)
	report, err := r.Step(ctx, 0, &stopAfterFirst{Publisher: p, stop: func() { docker(t, b.ctx, "stop", "--time", "1", broker); stopped = true }})
	if err == nil || report.Published != 1 || report.Marked != 1 {
		t.Fatalf("mid-batch outcome: %+v %v", report, err)
	}
	pending, err := s.PendingOutbox(ctx, store.OutboxQuery{Shard: 0, Limit: 100})
	if err != nil || len(pending) != 2 {
		t.Fatalf("unconfirmed rows lost: %d %v", len(pending), err)
	}
	docker(t, b.ctx, "start", broker)
	stopped = false
	b.ready(t)
	p2 := b.publisher(t)
	defer func() { _ = p2.Close() }()
	if report, err := r.Step(b.ctx, 0, p2); err != nil || report.Marked != 2 {
		t.Fatalf("restart replay: %+v %v", report, err)
	}
	conn, ch := b.channel(t)
	defer func() { _ = conn.CloseDeadline(time.Now().Add(time.Second)) }()
	seen := map[string]bool{}
	for range 3 {
		d := receive(t, b.ctx, ch)
		seen[d.MessageId] = true
		if d.DeliveryMode != amqp.Persistent || d.Priority != 2 {
			t.Fatalf("restart lost properties: %+v", d)
		}
		if err := d.Ack(false); err != nil {
			t.Fatal(err)
		}
	}
	if len(seen) != 3 {
		t.Fatalf("lost or duplicated recovery messages: %v", seen)
	}
	t.Log("broker stopped after first routed confirm: marked=1 pending=2; after restart marked=2 received=3 persistent")
}

type stopAfterFirst struct {
	outbox.Publisher
	calls int
	stop  func()
}

func (p *stopAfterFirst) Publish(ctx context.Context, e store.OutboxEvent) error {
	err := p.Publisher.Publish(ctx, e)
	p.calls++
	if err == nil && p.calls == 1 {
		p.stop()
	}
	return err
}

func TestIntegrationCrashAfterConfirmsReplaysWithoutRepeatingDurableWorkflow(t *testing.T) {
	b := liveBroker(t)
	ctx, s := fixtureStore(t)
	rows := seed(t, ctx, s, 3)
	p := b.publisher(t)
	defer func() { _ = p.Close() }()
	lost := &dropMark{Store: s}
	if report, err := relay(t, lost).Step(ctx, 0, p); !errors.Is(err, store.ErrUnknownOutcome) || report.Published != 3 || report.Marked != 0 {
		t.Fatalf("crash boundary: %+v %v", report, err)
	}
	// Discard the relay's in-memory pending command, as a process crash would.
	if report, err := relay(t, s).Step(ctx, 0, p); err != nil || report.Marked != 3 {
		t.Fatalf("new process replay: %+v %v", report, err)
	}
	fixedNow := func() time.Time { return time.UnixMilli(1793318400000) }
	m, err := workflow.New(workflow.Config{Store: s, ScanLimit: 100, Now: fixedNow})
	if err != nil {
		t.Fatal(err)
	}
	c := mq.Consumer{Store: s, Handle: func(ctx context.Context, rec store.Record) error { _, err := m.Match(ctx, rec.Request.ID); return err }}
	conn, ch := b.channel(t)
	defer func() { _ = conn.CloseDeadline(time.Now().Add(time.Second)) }()
	seen := map[string]int{}
	for range 6 {
		d := receive(t, b.ctx, ch)
		seen[d.MessageId]++
		if err := c.Process(ctx, d); err != nil {
			t.Fatal(err)
		}
	}
	for _, e := range rows {
		if seen[e.EventID] != 2 {
			t.Fatalf("not replayed exactly twice: %+v", seen)
		}
	}
	pending, err := s.PendingOutbox(ctx, store.OutboxQuery{Shard: 0, Limit: 100})
	if err != nil || len(pending) != 3 {
		t.Fatalf("expected one durable retry per request: %+v %v", pending, err)
	}
	for _, e := range pending {
		if e.EventType != "request.retry" {
			t.Fatalf("create not marked or duplicate retry: %+v", e)
		}
		rec, err := s.ReadRecord(ctx, e.EntityKey)
		if err != nil || rec.Request.Status != store.RequestWaiting || rec.Request.RetryCount != 1 {
			t.Fatalf("duplicate repeated workflow: %+v %v", rec, err)
		}
	}
	q, err := ch.QueueDeclarePassive(mq.MatchingQueue, true, false, false, false, nil)
	if err != nil || q.Messages != 0 {
		t.Fatalf("delivery not durably acknowledged: %+v %v", q, err)
	}
	t.Log("confirm-before-mark process loss: 3 events replayed as 6 deliveries, 3 durable retry handoffs, retryCount=1 each")
}

type dropMark struct{ store.Store }

func (*dropMark) MarkOutboxPublished(context.Context, store.MarkOutboxPublished) (store.Result, error) {
	return store.Result{}, store.ErrUnknownOutcome
}

type broker struct {
	ctx                                      context.Context
	address, endpoint, user, password, vhost string
}

func liveBroker(t *testing.T) *broker {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 90*time.Second)
	t.Cleanup(cancel)
	u, err := url.Parse(os.Getenv("MQ_AMQP_URL"))
	if err != nil || u.User == nil || os.Getenv("MQ_MANAGEMENT_URL") == "" {
		t.Fatal("run make verify-A1.4 for an isolated pinned broker; no live tests are skipped")
	}
	password, _ := u.User.Password()
	b := &broker{ctx: ctx, endpoint: strings.TrimRight(os.Getenv("MQ_MANAGEMENT_URL"), "/"), user: u.User.Username(), password: password, vhost: "a1.4-" + uuid.NewString()}
	u.Path = "/" + b.vhost
	b.address = u.String()
	b.refreshPorts(t)
	b.admin(t, ctx, http.MethodPut, "/api/vhosts/"+url.PathEscape(b.vhost), []byte(`{}`))
	b.admin(t, ctx, http.MethodPut, "/api/permissions/"+url.PathEscape(b.vhost)+"/"+url.PathEscape(b.user), []byte(`{"configure":".*","write":".*","read":".*"}`))
	b.admin(t, ctx, http.MethodPost, "/api/definitions/"+url.PathEscape(b.vhost), mq.Definitions())
	t.Cleanup(func() {
		cleanup, stop := context.WithTimeout(context.Background(), 5*time.Second)
		defer stop()
		b.admin(t, cleanup, http.MethodDelete, "/api/vhosts/"+url.PathEscape(b.vhost), nil)
	})
	return b
}

func (b broker) publisher(t *testing.T) *mq.ConfirmedPublisher {
	t.Helper()
	p, err := mq.NewPublisher(b.ctx, b.address)
	if err != nil {
		t.Fatal(err)
	}
	return p
}
func (b broker) channel(t *testing.T) (*amqp.Connection, *amqp.Channel) {
	t.Helper()
	conn, err := mq.Dial(b.ctx, b.address)
	if err != nil {
		t.Fatal(err)
	}
	ch, err := conn.Channel()
	if err != nil {
		_ = conn.CloseDeadline(time.Now())
		t.Fatal(err)
	}
	return conn, ch
}

func (b broker) request(ctx context.Context, method, path string, body []byte) (int, []byte, error) {
	req, err := http.NewRequestWithContext(ctx, method, b.endpoint+path, bytes.NewReader(body))
	if err != nil {
		return 0, nil, err
	}
	req.SetBasicAuth(b.user, b.password)
	req.Header.Set("Content-Type", "application/json")
	client := http.Client{Timeout: 2 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return 0, nil, err
	}
	defer func() { _ = resp.Body.Close() }()
	data, err := io.ReadAll(io.LimitReader(resp.Body, 8192))
	return resp.StatusCode, data, err
}
func (b broker) admin(t *testing.T, ctx context.Context, method, path string, body []byte) {
	t.Helper()
	status, data, err := b.request(ctx, method, path, body)
	if err != nil || status < 200 || status >= 300 {
		t.Fatalf("broker admin %s %s: status=%d error=%v body=%s", method, path, status, err, data)
	}
}
func (b *broker) ready(t *testing.T) {
	t.Helper()
	b.refreshPorts(t)
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for {
		status, _, err := b.request(b.ctx, http.MethodGet, "/api/overview", nil)
		if err == nil && status == 200 {
			return
		}
		select {
		case <-b.ctx.Done():
			t.Fatalf("broker restart: %v", b.ctx.Err())
		case <-ticker.C:
		}
	}
}

// Docker can reassign ephemeral host ports on restart. Re-discover only the
// verifier-owned container; all persistence/delivery assertions stay unchanged.
func (b *broker) refreshPorts(t *testing.T) {
	t.Helper()
	id := os.Getenv("A14_BROKER_ID")
	if id == "" {
		return
	}
	u, err := url.Parse(b.address)
	if err != nil {
		t.Fatal(err)
	}
	u.Host = strings.TrimSpace(docker(t, b.ctx, "port", id, "5672/tcp"))
	b.address = u.String()
	b.endpoint = "http://" + strings.TrimSpace(docker(t, b.ctx, "port", id, "15672/tcp"))
}

func receive(t *testing.T, ctx context.Context, ch *amqp.Channel) amqp.Delivery {
	t.Helper()
	ticker := time.NewTicker(20 * time.Millisecond)
	defer ticker.Stop()
	for {
		d, ok, err := ch.Get(mq.MatchingQueue, false)
		if err != nil {
			t.Fatal(err)
		}
		if ok {
			return d
		}
		select {
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		case <-ticker.C:
		}
	}
}
func docker(t *testing.T, ctx context.Context, args ...string) string {
	t.Helper()
	data, err := exec.CommandContext(ctx, "docker", args...).CombinedOutput()
	if err != nil {
		t.Fatalf("docker %s: %v %s", args[0], err, data)
	}
	return string(data)
}
