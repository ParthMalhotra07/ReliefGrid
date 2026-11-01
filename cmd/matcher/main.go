// matcher runs the per-shard transactional outbox relay and durable matching
// consumer. Application images, probes and Helm deployment are owned by A1.5.
package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/ParthMalhotra07/ReliefGrid/internal/coordclient"
	"github.com/ParthMalhotra07/ReliefGrid/internal/mq"
	"github.com/ParthMalhotra07/ReliefGrid/internal/outbox"
	"github.com/ParthMalhotra07/ReliefGrid/internal/store"
	"github.com/ParthMalhotra07/ReliefGrid/internal/workflow"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	// Only an external shutdown suppresses the error. Cancelling the sibling
	// worker after a fatal relay failure must not turn that failure into exit 0.
	address := os.Getenv("HTTP_ADDR")
	if address == "" {
		address = ":8081"
	}
	if err := run(ctx, os.Getenv("COORDINATOR_URL"), os.Getenv("AMQP_URL"), address); err != nil && ctx.Err() == nil {
		slog.Error("matcher stopped", "error", err)
		os.Exit(1)
	}
}

func run(ctx context.Context, coordinatorURL, amqpURL, healthAddress string) error {
	if coordinatorURL == "" || amqpURL == "" {
		return fmt.Errorf("COORDINATOR_URL and AMQP_URL are required")
	}
	s, err := coordclient.New(ctx, coordinatorURL)
	if err != nil {
		return fmt.Errorf("matcher coordinator: %w", err)
	}
	m, err := workflow.New(workflow.Config{Store: s, ScanLimit: 100})
	if err != nil {
		return err
	}
	r, err := outbox.New(outbox.Config{Store: s, PublisherFactory: func(ctx context.Context) (outbox.Publisher, error) { return mq.NewPublisher(ctx, amqpURL) }})
	if err != nil {
		return err
	}
	c := mq.Consumer{Store: s, Handle: func(ctx context.Context, rec store.Record) error { _, err := m.Match(ctx, rec.Request.ID); return err }}
	listener, err := (&net.ListenConfig{}).Listen(ctx, "tcp", healthAddress)
	if err != nil {
		return fmt.Errorf("matcher health listen: %w", err)
	}
	state := &healthState{}
	state.initialized.Store(true)
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	done := make(chan error, 3)
	go func() {
		state.relayRunning.Store(true)
		err := r.Run(ctx)
		state.relayRunning.Store(false)
		done <- err
	}()
	go func() { done <- consume(ctx, amqpURL, c, state) }()
	go func() { done <- serveHealth(ctx, listener, state) }()
	first := <-done
	cancel()
	second := <-done
	third := <-done
	return errors.Join(first, second, third)
}

// A failed handoff remains unacked. Closing its dedicated connection requeues
// it, and bounded reconnect backoff avoids a tight redelivery loop. Business
// cleanup and state-aware recovery remain with A2.2/reconcilers.
func consume(ctx context.Context, address string, c mq.Consumer, state *healthState) error {
	defer state.consumerConnected.Store(false)
	backoff := 100 * time.Millisecond
	for ctx.Err() == nil {
		started := time.Now()
		conn, err := mq.Dial(ctx, address)
		if err == nil {
			err = mq.Declare(ctx, conn)
			if err == nil {
				state.consumerConnected.Store(true)
				err = c.Consume(ctx, conn)
			}
			state.consumerConnected.Store(false)
			_ = conn.CloseDeadline(time.Now().Add(5 * time.Second))
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if time.Since(started) >= 5*time.Second {
			backoff = 100 * time.Millisecond
		}
		slog.WarnContext(ctx, "matching consumer reconnect", "error", err, "backoff", backoff)
		timer := time.NewTimer(backoff)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
		backoff = min(2*backoff, 5*time.Second)
	}
	return ctx.Err()
}
