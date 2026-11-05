//go:build integration && a32

package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ParthMalhotra07/ReliefGrid/internal/mq"
	amqp "github.com/rabbitmq/amqp091-go"
)

// Use the real supervisor and transport, not Consumer.Consume in isolation.
func TestSupervisorKeepsAdmittedTransportThroughCancellation(t *testing.T) {
	address := os.Getenv("MQ_AMQP_URL")
	if address == "" {
		t.Fatal("MQ_AMQP_URL required (scripts/verify-A3.2.sh supplies an isolated pinned broker)")
	}
	ctx, finish := context.WithTimeout(t.Context(), 10*time.Second)
	defer finish()
	admission, cancel := context.WithCancel(ctx)
	defer cancel()
	entered := make(chan struct{})
	done := make(chan error, 1)
	handoff := make(chan error, 1)
	var connection *amqp.Connection
	var connected atomic.Bool
	consume := func(work context.Context, conn *amqp.Connection) error {
		connection = conn
		// Bound all AMQP RPCs even if the broker stalls during the test.
		stop := context.AfterFunc(ctx, func() { _ = conn.CloseDeadline(time.Now()) })
		defer stop()
		ch, err := conn.Channel()
		if err != nil {
			return err
		}
		defer func() { _ = ch.Close() }()
		q, err := ch.QueueDeclare("", false, true, true, false, nil)
		if err != nil {
			return err
		}
		if err = ch.PublishWithContext(ctx, "", q.Name, true, false, amqp.Publishing{Body: []byte("admitted handoff")}); err != nil {
			return err
		}
		delivery, ok, err := ch.Get(q.Name, false)
		if err != nil {
			return err
		}
		if !ok {
			return fmt.Errorf("admitted message absent")
		}
		close(entered)
		select {
		case <-work.Done():
		case <-ctx.Done():
			return ctx.Err()
		}
		// Keep an admitted delivery through a measurable cancellation window;
		// repeated real RPCs fail if Dial's lifetime is tied to admission.
		timer := time.NewTimer(150 * time.Millisecond)
		defer timer.Stop()
		for {
			if conn.IsClosed() {
				err = fmt.Errorf("supervisor closed transport before admitted handoff")
				handoff <- err
				return err
			}
			if _, err = ch.QueueInspect(q.Name); err != nil {
				handoff <- err
				return err
			}
			select {
			case <-timer.C:
				err = delivery.Ack(false)
				handoff <- err
				return err
			default:
			}
		}
	}
	go func() { done <- superviseConsumer(admission, address, mq.MatchingQueue, consume, connected.Store) }()
	select {
	case <-entered:
	case err := <-done:
		t.Fatalf("supervisor did not enter: %v", err)
	case <-ctx.Done():
		t.Fatal("supervisor did not admit delivery")
	}
	if !connected.Load() {
		t.Fatal("supervisor never reported connected")
	}
	cancel()
	select {
	case err := <-handoff:
		if err != nil {
			t.Fatalf("admitted handoff lost transport: %v", err)
		}
	case <-ctx.Done():
		t.Fatal("handoff did not finish")
	}
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("supervisor exit=%v", err)
		}
	case <-ctx.Done():
		t.Fatal("supervisor did not join")
	}
	if connected.Load() || !connection.IsClosed() {
		t.Fatal("supervisor did not clear readiness and close after drain")
	}
	t.Log("SIGTERM kept the admitted AMQP transport usable until ack, then closed it")
}
