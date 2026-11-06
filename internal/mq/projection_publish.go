package mq

import (
	"context"
	"encoding/json"
	"log/slog"
	"time"

	"github.com/ParthMalhotra07/ReliefGrid/internal/obs"
	"github.com/ParthMalhotra07/ReliefGrid/internal/store"
	amqp "github.com/rabbitmq/amqp091-go"
)

// ProjectionExchange is intentionally separate from all mandatory workflow
// routes. Its non-durable fanout/temporary queues are disposable UI hints.
const ProjectionExchange = "relief.projections"

func (p *ConfirmedPublisher) startProjections(ctx context.Context, address string) {
	ctx, p.projectionCancel = context.WithCancel(ctx)
	p.projections = make(chan amqp.Publishing, 64)
	p.projectionDone = make(chan struct{})
	go func() {
		defer close(p.projectionDone)
		backoff := 100 * time.Millisecond
		for ctx.Err() == nil {
			_ = p.projectConnection(ctx, address)
			if ctx.Err() == nil {
				slog.DebugContext(ctx, "UI projection publisher reconnecting; snapshots remain authoritative")
			}
			if ctx.Err() != nil {
				return
			}
			timer := time.NewTimer(backoff)
			select {
			case <-ctx.Done():
				timer.Stop()
				return
			case <-timer.C:
			}
			backoff = min(5*time.Second, backoff*2)
		}
	}()
}

// offerProjection runs only after the PRIMARY routed positive confirm. It
// neither blocks nor changes that result. The bounded worker has its own
// transport, so UI failures cannot close the workflow publisher connection.
func (p *ConfirmedPublisher) offerProjection(ctx context.Context, event store.OutboxEvent) {
	if p.projections == nil || len(event.Payload) > 1<<20 {
		return
	}
	var hint struct {
		EventID       string `json:"eventId"`
		EntityID      string `json:"entityId"`
		EntityVersion uint64 `json:"entityVersion"`
		EventType     string `json:"eventType"`
	}
	if json.Unmarshal(event.Payload, &hint) != nil || hint.EventID != event.EventID || hint.EntityID == "" || hint.EntityVersion == 0 {
		return
	}
	body, err := json.Marshal(hint)
	if err != nil {
		return
	}
	headers := amqp.Table{}
	obs.InjectAMQP(ctx, headers)
	msg := amqp.Publishing{DeliveryMode: amqp.Transient, ContentType: "application/json", MessageId: hint.EventID, Type: hint.EventType, Body: body, Headers: headers}
	select {
	case p.projections <- msg:
	default:
	} // snapshots/reconnect recover missed hints
}
func (p *ConfirmedPublisher) projectConnection(ctx context.Context, address string) error {
	conn, err := Dial(ctx, address)
	if err != nil {
		return err
	}
	defer func() { _ = conn.CloseDeadline(time.Now().Add(time.Second)) }()
	setup, cancel := context.WithTimeout(ctx, 5*time.Second)
	stop := closeOnCancel(setup, conn)
	defer cancel()
	defer stop()
	ch, err := conn.Channel()
	if err != nil {
		return err
	}
	if err = ch.ExchangeDeclare(ProjectionExchange, "fanout", false, false, false, false, nil); err != nil {
		return err
	}
	stop()
	cancel()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case message := <-p.projections:
			call, done := context.WithTimeout(ctx, time.Second)
			unblock := closeOnCancel(call, conn)
			err = ch.PublishWithContext(call, ProjectionExchange, "", false, false, message)
			unblock()
			done()
			if err != nil {
				return err
			}
		}
	}
}
