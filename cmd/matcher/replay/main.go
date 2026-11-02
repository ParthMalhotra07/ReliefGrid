// Command replay inspects matching.dead or, with explicit confirmation, replays
// one corrected event. It never automatically loops through poison messages.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"github.com/ParthMalhotra07/ReliefGrid/internal/mq"
	"os"
	"time"
)

func run() error {
	confirm := flag.Bool("confirm", false, "publish one corrected event and acknowledge its matching dead original")
	replacement := flag.String("replacement", "", "corrected event JSON file (required with --confirm)")
	expected := flag.String("message-id", "", "exact event ID at the head of matching.dead")
	flag.Parse()
	var body []byte
	if *confirm {
		var err error
		body, err = os.ReadFile(*replacement)
		if err != nil {
			return err
		}
		e, err := mq.ReplayEnvelope(body)
		if err != nil {
			return err
		}
		if *expected == "" || e.EventID != *expected {
			return fmt.Errorf("--message-id must equal corrected eventId")
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	conn, err := mq.Dial(ctx, os.Getenv("AMQP_URL"))
	if err != nil {
		return err
	}
	defer func() { _ = conn.CloseDeadline(time.Now().Add(5 * time.Second)) }()
	if !*confirm {
		count, err := mq.DeadReady(ctx, conn)
		if err != nil {
			return err
		}
		return json.NewEncoder(os.Stdout).Encode(map[string]any{"queue": mq.DeadQueue, "ready": count, "replayConfirmed": false})
	}
	p, err := mq.NewPublisher(ctx, os.Getenv("AMQP_URL"))
	if err != nil {
		return err
	}
	defer func() { _ = p.Close() }()
	report, err := mq.ReplayOne(ctx, conn, body, *expected, p)
	if encodeErr := json.NewEncoder(os.Stdout).Encode(report); encodeErr != nil {
		return encodeErr
	}
	return err
}
func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
