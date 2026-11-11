// Command drain-outbox finishes committed handoffs after the demo stops all
// workflow admission. It reuses the production routed-confirm relay and never
// consumes a request hint or changes a workflow state. A publish the broker
// refuses is retried for a short window (retryWindow).
package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/ParthMalhotra07/ReliefGrid/internal/coordclient"
	"github.com/ParthMalhotra07/ReliefGrid/internal/mq"
	"github.com/ParthMalhotra07/ReliefGrid/internal/outbox"
)

// stepper is the part of the relay drain uses (outbox.Relay.Step).
type stepper interface {
	Step(ctx context.Context, shard int, p outbox.Publisher) (outbox.Report, error)
}

// result is the drain's stdout: the relay's totals and how many steps were
// retried after the broker refused a publish.
type result struct {
	outbox.Report
	Retries int
}

// retryWindow bounds the retries of refused publishes; retryPause is the
// wait between steps after one.
const (
	retryWindow = 20 * time.Second
	retryPause  = 500 * time.Millisecond
)

func drain(ctx context.Context) (err error) {
	client, err := coordclient.New(ctx, os.Getenv("COORDINATOR_URL"))
	if err != nil {
		return fmt.Errorf("drain coordinator: %w", err)
	}
	publisher, err := mq.NewPublisher(ctx, os.Getenv("AMQP_URL"))
	if err != nil {
		return fmt.Errorf("drain publisher: %w", err)
	}
	defer func() { err = errors.Join(err, publisher.Close()) }()
	relay, err := outbox.New(outbox.Config{Store: client})
	if err != nil {
		return err
	}
	total, err := drainLoop(ctx, relay, client.Shards(), publisher, retryWindow, time.Sleep)
	if err != nil {
		return err
	}
	return json.NewEncoder(os.Stdout).Encode(total)
}

func drainLoop(ctx context.Context, relay stepper, shards int, p outbox.Publisher, window time.Duration, sleep func(time.Duration)) (result, error) {
	return drainLoopAt(ctx, relay, shards, p, window, sleep, time.Now)
}

// drainLoopAt steps every shard until a full pass reads and marks nothing.
// A publish the broker refuses (a full queue under reject-publish) is
// retried for up to window from the first refusal, while the consumers
// still empty the queue; any other error, or a refusal past the window,
// ends the drain.
func drainLoopAt(ctx context.Context, relay stepper, shards int, p outbox.Publisher, window time.Duration, sleep func(time.Duration), now func() time.Time) (result, error) {
	var total result
	var firstRefusal time.Time
	for ctx.Err() == nil {
		idle, refused := true, false
		for shard := range shards {
			report, err := relay.Step(ctx, shard, p)
			total.Read += report.Read
			total.Published += report.Published
			total.Marked += report.Marked
			if err != nil {
				if !errors.Is(err, mq.ErrNack) {
					return total, fmt.Errorf("drain shard %d: %w", shard, err)
				}
				if firstRefusal.IsZero() {
					firstRefusal = now()
				}
				if now().Sub(firstRefusal) >= window {
					return total, fmt.Errorf("drain shard %d: refused for %s: %w", shard, window, err)
				}
				refused = true
			}
			idle = idle && report.Read == 0 && report.Marked == 0
		}
		if refused {
			total.Retries++
			sleep(retryPause)
			continue
		}
		if idle {
			return total, nil
		}
	}
	return total, ctx.Err()
}

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Second)
	defer cancel()
	if err := drain(ctx); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
