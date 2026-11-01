// Command drain-outbox finishes committed handoffs after the demo stops all
// workflow admission. It reuses the production routed-confirm relay and never
// consumes a request hint or changes a workflow state.
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
	total := outbox.Report{}
	for ctx.Err() == nil {
		idle := true
		for shard := range client.Shards() {
			report, err := relay.Step(ctx, shard, publisher)
			if err != nil {
				return fmt.Errorf("drain shard %d: %w", shard, err)
			}
			idle = idle && report.Read == 0 && report.Marked == 0
			total.Read += report.Read
			total.Published += report.Published
			total.Marked += report.Marked
		}
		if idle {
			return json.NewEncoder(os.Stdout).Encode(total)
		}
	}
	return ctx.Err()
}

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := drain(ctx); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
