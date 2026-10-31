// Package mq owns the durable application messaging topology and manual-ack
// consumption. A delivery is a hint; store.Store is authoritative.
package mq

import (
	"bytes"
	"context"
	_ "embed"
	"encoding/json"
	"fmt"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

const (
	MatchingQueue = "matching.requests"
	WakeupQueue   = "matching.wakeups"
	DeadQueue     = "matching.dead"
	Prefetch      = 8
	callTimeout   = 5 * time.Second
)

//go:embed definitions.json
var definitions []byte

// Definitions returns an independent copy of the committed RabbitMQ definitions.
// Helm imports these after node startup, including the safety policies. Runtime
// declarations use identical arguments, so a missing policy cannot silently
// downgrade dead-letter safety. Changing these arguments needs a queue migration.
func Definitions() []byte { return bytes.Clone(definitions) }

type topology struct {
	Exchanges []struct {
		Name, Type                    string
		Durable, AutoDelete, Internal bool
		Arguments                     map[string]any
	}
	Queues []struct {
		Name                string
		Durable, AutoDelete bool
		Arguments           map[string]any
	}
	Bindings []struct {
		Source, Destination string
		RoutingKey          string `json:"routing_key"`
		Arguments           map[string]any
	}
}

// Declare idempotently creates the committed exchanges, queues and bindings.
// The connection must be exclusively owned during setup: cancellation or an RPC
// deadline closes it to unblock AMQP methods that have no context parameter.
func Declare(ctx context.Context, conn *amqp.Connection) error {
	ctx, cancel := context.WithTimeout(ctx, callTimeout)
	defer cancel()
	if err := ctx.Err(); err != nil {
		return err
	}
	if conn == nil {
		return fmt.Errorf("mq topology requires a connection")
	}
	stop := closeOnCancel(ctx, conn)
	defer stop()
	var d topology
	decoder := json.NewDecoder(bytes.NewReader(definitions))
	decoder.UseNumber()
	if err := decoder.Decode(&d); err != nil {
		return fmt.Errorf("decode mq topology: %w", err)
	}
	ch, err := conn.Channel()
	if err != nil {
		return fmt.Errorf("open topology channel: %w", err)
	}
	defer func() { _ = ch.Close() }()
	for _, e := range d.Exchanges {
		args, err := table(e.Arguments)
		if err != nil {
			return err
		}
		if err := ch.ExchangeDeclare(e.Name, e.Type, e.Durable, e.AutoDelete, e.Internal, false, args); err != nil {
			return fmt.Errorf("declare exchange %s: %w", e.Name, err)
		}
	}
	for _, q := range d.Queues {
		args, err := table(q.Arguments)
		if err != nil {
			return err
		}
		if _, err := ch.QueueDeclare(q.Name, q.Durable, q.AutoDelete, false, false, args); err != nil {
			return fmt.Errorf("declare queue %s: %w", q.Name, err)
		}
	}
	for _, b := range d.Bindings {
		args, err := table(b.Arguments)
		if err != nil {
			return err
		}
		if err := ch.QueueBind(b.Destination, b.RoutingKey, b.Source, false, args); err != nil {
			return fmt.Errorf("bind %s/%s to %s: %w", b.Source, b.RoutingKey, b.Destination, err)
		}
	}
	return nil
}

func table(values map[string]any) (amqp.Table, error) {
	result := make(amqp.Table, len(values))
	for k, v := range values {
		if n, ok := v.(json.Number); ok {
			i, err := n.Int64()
			if err != nil {
				return nil, fmt.Errorf("mq argument %s: %w", k, err)
			}
			v = i
		}
		result[k] = v
	}
	return result, nil
}
