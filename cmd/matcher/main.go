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

	"github.com/ParthMalhotra07/ReliefGrid/internal/appmetrics"
	"github.com/ParthMalhotra07/ReliefGrid/internal/coordclient"
	"github.com/ParthMalhotra07/ReliefGrid/internal/mq"
	"github.com/ParthMalhotra07/ReliefGrid/internal/obs"
	"github.com/ParthMalhotra07/ReliefGrid/internal/outbox"
	"github.com/ParthMalhotra07/ReliefGrid/internal/reconcile"
	"github.com/ParthMalhotra07/ReliefGrid/internal/store"
	"github.com/ParthMalhotra07/ReliefGrid/internal/workflow"
	amqp "github.com/rabbitmq/amqp091-go"
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
	err := withTracing(ctx, "matcher", func() error {
		return run(ctx, os.Getenv("COORDINATOR_URL"), os.Getenv("AMQP_URL"), address, os.Getenv("METRICS_ADDR"))
	})
	if err != nil && ctx.Err() == nil {
		slog.Error("matcher stopped", "error", err)
		os.Exit(1)
	}
}

func run(ctx context.Context, coordinatorURL, amqpURL, healthAddress, metricsAddress string) (runErr error) {
	if coordinatorURL == "" || amqpURL == "" {
		return fmt.Errorf("COORDINATOR_URL and AMQP_URL are required")
	}
	aging := time.Duration(0)
	if value := os.Getenv("AGING_INTERVAL"); value != "" {
		var parseErr error
		aging, parseErr = time.ParseDuration(value)
		if parseErr != nil || aging < 0 || (aging > 0 && aging < time.Millisecond) {
			return fmt.Errorf("invalid AGING_INTERVAL: expected zero or duration at least 1ms")
		}
	}
	s, err := coordclient.New(ctx, coordinatorURL, coordclient.WithInternalAuthFile(os.Getenv("INTERNAL_AUTH_TOKEN_FILE")))
	if err != nil {
		return fmt.Errorf("matcher coordinator: %w", err)
	}
	var backend store.Store = s
	var observer workflow.Observer
	if metricsAddress != "" {
		metrics, err := appmetrics.New(s, "matcher", nil)
		if err != nil {
			return err
		}
		stop, err := metrics.Start(ctx, metricsAddress)
		if err != nil {
			return err
		}
		defer func() { runErr = errors.Join(runErr, stop()) }()
		backend = metrics.Store()
		observer = metrics
	}
	m, err := workflow.New(workflow.Config{Store: backend, ScanLimit: 100, Observer: observer})
	if err != nil {
		return err
	}
	r, err := outbox.New(outbox.Config{Store: backend, PublisherFactory: func(ctx context.Context) (outbox.Publisher, error) { return mq.NewPublisher(ctx, amqpURL) }})
	if err != nil {
		return err
	}
	transportRetry := mq.TransportRetry{PublisherFactory: func(ctx context.Context) (mq.RetryPublisher, error) { return mq.NewPublisher(ctx, amqpURL) }}
	c := mq.Consumer{Store: backend, Handle: func(ctx context.Context, rec store.Record) error { return m.Handle(ctx, rec.Request.ID) }, Retry: transportRetry.Retry}
	recovery, err := reconcile.New(reconcile.Config{Store: backend, Matcher: m, AgingInterval: aging})
	if err != nil {
		return err
	}
	wakeups := mq.WakeupConsumer{Handle: func(ctx context.Context, w mq.Wakeup) error {
		return recovery.Wake(ctx, w.EntityID, w.EventID, w.EventType, w.At)
	}, Retry: transportRetry.RetryWakeup}
	listener, err := (&net.ListenConfig{}).Listen(ctx, "tcp", healthAddress)
	if err != nil {
		return fmt.Errorf("matcher health listen: %w", err)
	}
	state := &healthState{}
	state.initialized.Store(true)
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	done := make(chan error, 5)
	go func() {
		state.relayRunning.Store(true)
		err := r.Run(ctx)
		state.relayRunning.Store(false)
		done <- err
	}()
	go func() { done <- consume(ctx, amqpURL, c, state) }()
	go func() {
		done <- superviseConsumer(ctx, amqpURL, mq.WakeupQueue, wakeups.Consume, state.wakeupConnected.Store)
	}()
	go func() {
		state.reconcilerRunning.Store(true)
		err := recovery.Run(ctx)
		state.reconcilerRunning.Store(false)
		done <- err
	}()
	go func() { done <- serveHealth(ctx, listener, state) }()
	first := <-done
	cancel()
	results := []error{first}
	for range 4 {
		results = append(results, <-done)
	}
	return errors.Join(results...)
}

// A failed handoff remains unacked. Closing its dedicated connection requeues
// it, and bounded reconnect backoff avoids a tight redelivery loop. Successful
// retry handoffs are acknowledged; periodic recovery runs independently.
func consume(ctx context.Context, address string, c mq.Consumer, state *healthState) error {
	return superviseConsumer(ctx, address, mq.MatchingQueue, c.Consume, state.consumerConnected.Store)
}
func superviseConsumer(ctx context.Context, address, queue string, consumeRun func(context.Context, *amqp.Connection) error, connected func(bool)) error {
	defer connected(false)
	backoff := 100 * time.Millisecond
	for ctx.Err() == nil {
		started := time.Now()
		// The dedicated transport must outlive admission cancellation so an
		// admitted delivery can finish its bounded durable handoff and ack.
		connectionCtx, closeConnection := context.WithCancel(context.WithoutCancel(ctx))
		conn, err := mq.Dial(connectionCtx, address)
		if err == nil {
			err = mq.Declare(ctx, conn)
			if err == nil {
				connected(true)
				err = consumeRun(ctx, conn)
			}
			connected(false)
			_ = conn.CloseDeadline(time.Now().Add(5 * time.Second))
		}
		closeConnection()
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if time.Since(started) >= 5*time.Second {
			backoff = 100 * time.Millisecond
		}
		slog.WarnContext(ctx, "consumer reconnect", "queue", queue, "error", err, "backoff", backoff)
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

// withTracing runs fn with tracing set up from the standard OTEL_*
// variables (S4.4): with no OTLP endpoint only the W3C propagator, so
// traceparent still flows through the relay and consumers. Spans are
// flushed when fn returns.
func withTracing(ctx context.Context, service string, fn func() error) error {
	enabled := obs.EnabledFromEnv()
	shutdown, err := obs.Init(ctx, obs.Config{ServiceName: service, Enabled: enabled})
	if err != nil {
		return err
	}
	slog.Info("tracing", "enabled", enabled)
	err = fn()
	flushCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if serr := shutdown(flushCtx); serr != nil {
		slog.Warn("flushing traces", "err", serr)
	}
	return err
}
