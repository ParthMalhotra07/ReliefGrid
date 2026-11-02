package mq

import (
	"context"
	"embed"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/ParthMalhotra07/ReliefGrid/internal/store"
	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/santhosh-tekuri/jsonschema/v6"
)

//go:embed schemas/*.json
var wakeSchemaFiles embed.FS
var wakeSchemas = sync.OnceValues(func() (map[string]*jsonschema.Schema, error) {
	schemas := make(map[string]*jsonschema.Schema)
	for kind, name := range map[string]string{"request.state_changed": "request_state_changed", "resource.registered": "resource_registered", "resource.state_changed": "resource_state_changed", "match.proposed": "match_proposed"} {
		raw, err := wakeSchemaFiles.ReadFile("schemas/" + name + ".json")
		if err != nil {
			return nil, err
		}
		var document any
		if err := json.Unmarshal(raw, &document); err != nil {
			return nil, err
		}
		compiler := jsonschema.NewCompiler()
		compiler.AssertFormat()
		location := "https://reliefgrid.internal/events/" + name + ".json"
		if err := compiler.AddResource(location, document); err != nil {
			return nil, err
		}
		schema, err := compiler.Compile(location)
		if err != nil {
			return nil, err
		}
		schemas[kind] = schema
	}
	return schemas, nil
})

// Wakeup has transport identity only. Handle must reread current authoritative
// state; payload status, resource ownership and event version grant nothing.
type Wakeup struct {
	EventID, EntityID, EventType string
	At                           int64
}
type WakeupConsumer struct {
	Handle                   func(context.Context, Wakeup) error
	Retry                    func(context.Context, amqp.Delivery) error
	ObserveDead              func(int)
	CallTimeout, WorkTimeout time.Duration
}

func parseWakeEvent(body []byte) (event, error) {
	e, err := decodeEnvelope(body)
	if err != nil {
		return e, err
	}
	schemas, err := wakeSchemas()
	if err != nil {
		return e, err
	}
	if schema, ok := schemas[e.EventType]; ok {
		var document any
		if err := json.Unmarshal(body, &document); err != nil {
			return e, err
		}
		if err := schema.Validate(document); err != nil {
			return e, fmt.Errorf("%s schema: %w", e.EventType, err)
		}
	} else if e.EventType == "match.completed" {
		if err := exactFields(e.Payload, "requestId", "attemptId", "resourceId", "fingerprint"); err != nil {
			return e, err
		}
		var p struct {
			RequestID   string `json:"requestId"`
			AttemptID   string `json:"attemptId"`
			ResourceID  string `json:"resourceId"`
			Fingerprint string `json:"fingerprint"`
		}
		if err := json.Unmarshal(e.Payload, &p); err != nil {
			return e, err
		}
		if !validUUID(p.RequestID) || !validUUID(p.AttemptID) || !validUUID(p.ResourceID) || p.Fingerprint == "" {
			return e, fmt.Errorf("invalid completed match identity")
		}
	} else {
		return e, fmt.Errorf("unsupported wakeup event %q", e.EventType)
	}
	var p struct {
		RequestID  string `json:"requestId"`
		ResourceID string `json:"resourceId"`
	}
	if err := json.Unmarshal(e.Payload, &p); err != nil {
		return e, err
	}
	payloadID := p.RequestID
	if e.EventType == "resource.registered" || e.EventType == "resource.state_changed" {
		payloadID = p.ResourceID
	}
	if payloadID != e.EntityID {
		return e, fmt.Errorf("wakeup payload identity differs from entityId")
	}
	return e, nil
}
func (c WakeupConsumer) timeouts() (time.Duration, time.Duration) {
	call, work := c.CallTimeout, c.WorkTimeout
	if call == 0 {
		call = callTimeout
	}
	if work == 0 {
		work = 15 * time.Second
	}
	return call, work
}
func (c WakeupConsumer) validate() error {
	call, work := c.timeouts()
	if c.Handle == nil || call <= 0 || work <= 0 {
		return fmt.Errorf("wakeup consumer requires handler and positive deadlines")
	}
	_, err := wakeSchemas()
	return err
}
func (c WakeupConsumer) Process(ctx context.Context, d amqp.Delivery) error {
	if err := c.validate(); err != nil {
		return err
	}
	if d.Acknowledger == nil {
		return fmt.Errorf("wakeup delivery has no acknowledger")
	}
	call, work := c.timeouts()
	ctx, cancel := context.WithTimeout(ctx, work)
	defer cancel()
	if err := ctx.Err(); err != nil {
		return err
	}
	e, err := parseWakeEvent(d.Body)
	if err != nil {
		if rejectErr := d.Reject(false); rejectErr != nil {
			return fmt.Errorf("reject wakeup poison: %w", rejectErr)
		}
		slog.WarnContext(ctx, "rejected malformed wakeup", "messageId", d.MessageId, "reason", err)
		return nil
	}
	occurred, err := time.Parse(time.RFC3339Nano, e.OccurredAt)
	if err != nil {
		return err
	}
	err = c.Handle(ctx, Wakeup{EventID: e.EventID, EntityID: e.EntityID, EventType: e.EventType, At: occurred.UnixMilli()})
	if errors.Is(err, store.ErrNotFound) {
		if rejectErr := d.Reject(false); rejectErr != nil {
			return fmt.Errorf("reject absent wakeup entity: %w", rejectErr)
		}
		slog.WarnContext(ctx, "rejected wakeup for absent entity", "eventId", e.EventID, "reason", err)
		return nil
	}
	if err != nil {
		if c.Retry == nil {
			return fmt.Errorf("wakeup durable handoff: %w", err)
		}
		retryCtx, retryCancel := context.WithTimeout(ctx, call)
		retryErr := c.Retry(retryCtx, d)
		retryCancel()
		if retryErr != nil {
			return fmt.Errorf("defer wakeup (%w): %w", err, retryErr)
		}
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := d.Ack(false); err != nil {
		return fmt.Errorf("ack wakeup after durable handoff: %w", err)
	}
	return nil
}

// Consume holds the same unacknowledged delivery while a retry publisher is
// unavailable. It does not burn the quorum delivery limit with reconnects.
// A connection/channel failure still requeues its outstanding deliveries.
func (c WakeupConsumer) Consume(ctx context.Context, conn *amqp.Connection) error {
	if err := c.validate(); err != nil {
		return err
	}
	if conn == nil {
		return fmt.Errorf("wakeup consumer requires connection")
	}
	call, work := c.timeouts()
	setup, cancel := context.WithTimeout(ctx, call)
	stop := closeOnCancel(setup, conn)
	ch, err := conn.Channel()
	if err != nil {
		stop()
		cancel()
		return fmt.Errorf("wakeup channel: %w", err)
	}
	defer func() {
		cleanup, done := context.WithTimeout(context.Background(), call)
		defer done()
		stop := closeOnCancel(cleanup, conn)
		defer stop()
		_ = ch.Close()
	}()
	if err := ch.Qos(Prefetch, 0, false); err != nil {
		stop()
		cancel()
		return fmt.Errorf("wakeup prefetch: %w", err)
	}
	deliveries, err := ch.Consume(WakeupQueue, "", false, false, false, false, nil)
	stop()
	cancel()
	if err != nil {
		return fmt.Errorf("consume wakeups: %w", err)
	}
	inspect := func() error {
		bounded, done := context.WithTimeout(ctx, call)
		defer done()
		stop := closeOnCancel(bounded, conn)
		defer stop()
		q, err := ch.QueueDeclarePassive(DeadQueue, true, false, false, false, amqp.Table{"x-queue-type": "quorum"})
		if err != nil {
			return err
		}
		if c.ObserveDead != nil {
			c.ObserveDead(q.Messages)
		}
		if q.Messages > 0 {
			slog.WarnContext(ctx, "matching.dead requires controlled replay", "ready", q.Messages, "reason", "see per-event rejection reason and dead-letter headers")
		}
		return nil
	}
	if err := inspect(); err != nil {
		return fmt.Errorf("inspect dead queue: %w", err)
	}
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			if err := inspect(); err != nil {
				return fmt.Errorf("inspect dead queue: %w", err)
			}
		case d, ok := <-deliveries:
			if !ok {
				return fmt.Errorf("wakeup delivery channel closed")
			}
			originalAck := d.Acknowledger
			backoff := 100 * time.Millisecond
			for {
				if err := ctx.Err(); err != nil {
					return err
				}
				// Only the admitted attempt survives shutdown. A failed retry
				// stays on the same unacked delivery while admission is open.
				deliveryCtx, finish := context.WithTimeout(context.WithoutCancel(ctx), work)
				d.Acknowledger = boundedAcknowledger{Acknowledger: originalAck, ctx: deliveryCtx, conn: conn, timeout: call}
				err := c.Process(deliveryCtx, d)
				finish()
				if err == nil {
					break
				}
				if ctx.Err() != nil {
					return ctx.Err()
				}
				if conn.IsClosed() || ch.IsClosed() {
					return err
				}
				slog.WarnContext(ctx, "wakeup handoff held unacknowledged", "messageId", d.MessageId, "reason", err, "backoff", backoff)
				timer := time.NewTimer(backoff)
				select {
				case <-ctx.Done():
					timer.Stop()
					return ctx.Err()
				case <-timer.C:
				}
				backoff = min(2*backoff, 5*time.Second)
			}
		}
	}
}
