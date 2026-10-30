package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"net"
	"os"
	"os/signal"
	"slices"
	"syscall"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

const (
	spikeExchange = "reliefgrid.spike"
	spikeQueue    = "reliefgrid.spike.durable"
	spikeRoute    = "persistent"
)

func main() {
	mode := flag.String("mode", "", "publish, consume, or priority")
	url := flag.String("url", "amqp://guest:guest@127.0.0.1:5672/", "AMQP URL")
	count := flag.Int("count", 100, "number of messages")
	flag.Parse()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	ctx, cancel := context.WithTimeout(ctx, 3*time.Minute)
	defer cancel()

	var err error
	switch *mode {
	case "publish":
		err = publish(ctx, *url, *count)
	case "consume":
		err = consume(ctx, *url, *count)
	case "priority":
		err = probePriority(ctx, *url)
	default:
		err = fmt.Errorf("unknown -mode %q (want publish, consume, or priority)", *mode)
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func open(ctx context.Context, url string) (*amqp.Connection, *amqp.Channel, error) {
	dialer := &net.Dialer{Timeout: 10 * time.Second}
	conn, err := amqp.DialConfig(url, amqp.Config{
		Heartbeat: 10 * time.Second,
		Dial: func(network, address string) (net.Conn, error) {
			transport, err := dialer.DialContext(ctx, network, address)
			if err != nil {
				return nil, err
			}
			// DialContext only bounds TCP establishment. Keep the context
			// effective during AMQP handshaking and subsequent channel RPCs.
			stop := context.AfterFunc(ctx, func() { _ = transport.Close() })
			return &contextConn{Conn: transport, stop: stop}, nil
		},
	})
	if err != nil {
		return nil, nil, fmt.Errorf("dial RabbitMQ: %w", err)
	}
	ch, err := conn.Channel()
	if err != nil {
		_ = conn.Close()
		return nil, nil, fmt.Errorf("open channel: %w", err)
	}
	return conn, ch, nil
}

type contextConn struct {
	net.Conn
	stop func() bool
}

func (conn *contextConn) Close() error {
	conn.stop()
	return conn.Conn.Close()
}

func declareSpikeTopology(ch *amqp.Channel) error {
	if err := ch.ExchangeDeclare(spikeExchange, "direct", true, false, false, false, nil); err != nil {
		return fmt.Errorf("declare exchange: %w", err)
	}
	if _, err := ch.QueueDeclare(spikeQueue, true, false, false, false, quorumArgs()); err != nil {
		return fmt.Errorf("declare queue: %w", err)
	}
	if err := ch.QueueBind(spikeQueue, spikeRoute, spikeExchange, false, nil); err != nil {
		return fmt.Errorf("bind queue: %w", err)
	}
	return nil
}

func quorumArgs() amqp.Table {
	return amqp.Table{
		"x-queue-type":                "quorum",
		"x-quorum-initial-group-size": int32(1),
	}
}

func publish(ctx context.Context, url string, count int) error {
	if count < 1 {
		return errors.New("count must be positive")
	}
	conn, ch, err := open(ctx, url)
	if err != nil {
		return err
	}
	defer func() { _ = conn.Close() }()
	defer func() { _ = ch.Close() }()

	if err := declareSpikeTopology(ch); err != nil {
		return err
	}
	if _, err := ch.QueuePurge(spikeQueue, false); err != nil {
		return fmt.Errorf("purge queue: %w", err)
	}
	if err := ch.Confirm(false); err != nil {
		return fmt.Errorf("enable publisher confirms: %w", err)
	}
	returned := ch.NotifyReturn(make(chan amqp.Return, count))
	confirms := make([]*amqp.DeferredConfirmation, 0, count)
	for i := 0; i < count; i++ {
		confirmation, err := ch.PublishWithDeferredConfirmWithContext(
			ctx,
			spikeExchange,
			spikeRoute,
			true,
			false,
			amqp.Publishing{
				DeliveryMode: amqp.Persistent,
				Priority:     1, // NORMAL; zero omits the property and acts as 4 on this broker
				ContentType:  "text/plain",
				MessageId:    fmt.Sprintf("a0.3-%03d", i),
				Body:         []byte(fmt.Sprintf("reliefgrid-message-%03d", i)),
			},
		)
		if err != nil {
			return fmt.Errorf("publish message %d: %w", i, err)
		}
		confirms = append(confirms, confirmation)
	}
	for i, confirmation := range confirms {
		acknowledged, err := confirmation.WaitContext(ctx)
		if err != nil {
			return fmt.Errorf("wait for confirm %d: %w", i, err)
		}
		if !acknowledged {
			return fmt.Errorf("message %d was negatively acknowledged", i)
		}
	}
	select {
	case unroutable := <-returned:
		return fmt.Errorf("mandatory message %q was returned: %s", unroutable.MessageId, unroutable.ReplyText)
	default:
	}
	fmt.Printf("confirmed=%d\n", count)
	return nil
}

func consume(ctx context.Context, url string, count int) error {
	if count < 1 {
		return errors.New("count must be positive")
	}
	conn, ch, err := open(ctx, url)
	if err != nil {
		return err
	}
	defer func() { _ = conn.Close() }()
	defer func() { _ = ch.Close() }()

	if err := declareSpikeTopology(ch); err != nil {
		return err
	}
	if err := ch.Qos(8, 0, false); err != nil {
		return fmt.Errorf("set prefetch: %w", err)
	}
	deliveries, err := ch.ConsumeWithContext(ctx, spikeQueue, "a0.3-consumer", false, false, false, false, nil)
	if err != nil {
		return fmt.Errorf("start consumer: %w", err)
	}
	seen := make(map[string]struct{}, count)
	for len(seen) < count {
		select {
		case <-ctx.Done():
			return fmt.Errorf("consume %d of %d messages: %w", len(seen), count, ctx.Err())
		case delivery, ok := <-deliveries:
			if !ok {
				return fmt.Errorf("delivery channel closed after %d of %d messages", len(seen), count)
			}
			if delivery.DeliveryMode != amqp.Persistent {
				return fmt.Errorf("message %q is not persistent", delivery.MessageId)
			}
			if delivery.Priority != 1 {
				return fmt.Errorf("message %q priority %d, want explicit NORMAL=1", delivery.MessageId, delivery.Priority)
			}
			if _, duplicate := seen[delivery.MessageId]; duplicate {
				return fmt.Errorf("duplicate message %q", delivery.MessageId)
			}
			seen[delivery.MessageId] = struct{}{}
			if err := delivery.Ack(false); err != nil {
				return fmt.Errorf("ack message %q: %w", delivery.MessageId, err)
			}
		}
	}
	fmt.Printf("received=%d\n", len(seen))
	return nil
}

func probePriority(ctx context.Context, url string) error {
	// amqp091-go v1.16.0 omits the priority property for its zero value.
	// Quorum queues treat an absent property as effective priority 4, so
	// test that case separately from explicit nonzero priorities.
	for _, probe := range []struct {
		name      string
		published []uint8
		want      []uint8
	}{
		{name: "strict", published: []uint8{1, 7, 31, 15, 2}, want: []uint8{31, 15, 7, 2, 1}},
		{name: "urgency", published: []uint8{1, 3, 2}, want: []uint8{3, 2, 1}},
		{name: "omitted-default-4", published: []uint8{1, 0, 7}, want: []uint8{7, 0, 1}},
	} {
		if err := probePriorityCase(ctx, url, probe.name, probe.published, probe.want); err != nil {
			return err
		}
	}
	return nil
}

func probePriorityCase(ctx context.Context, url, name string, published, want []uint8) error {
	conn, ch, err := open(ctx, url)
	if err != nil {
		return err
	}
	defer func() { _ = conn.Close() }()
	defer func() { _ = ch.Close() }()

	queue := fmt.Sprintf("reliefgrid.spike.priority.%s.%d", name, time.Now().UnixNano())
	if _, err := ch.QueueDeclare(queue, true, false, false, false, quorumArgs()); err != nil {
		return fmt.Errorf("declare priority quorum queue: %w", err)
	}
	defer func() { _, _ = ch.QueueDelete(queue, false, false, false) }()
	if err := ch.Confirm(false); err != nil {
		return fmt.Errorf("enable priority confirms: %w", err)
	}

	returned := ch.NotifyReturn(make(chan amqp.Return, len(published)))
	confirms := make([]*amqp.DeferredConfirmation, 0, len(published))
	for _, priority := range published {
		confirmation, err := ch.PublishWithDeferredConfirmWithContext(ctx, "", queue, true, false, amqp.Publishing{
			DeliveryMode: amqp.Persistent,
			ContentType:  "text/plain",
			MessageId:    fmt.Sprintf("priority-%02d", priority),
			Priority:     priority,
			Body:         []byte(fmt.Sprintf("priority-%02d", priority)),
		})
		if err != nil {
			return fmt.Errorf("publish priority %d: %w", priority, err)
		}
		confirms = append(confirms, confirmation)
	}
	for i, confirmation := range confirms {
		acknowledged, err := confirmation.WaitContext(ctx)
		if err != nil {
			return fmt.Errorf("wait for priority confirm %d: %w", i, err)
		}
		if !acknowledged {
			return fmt.Errorf("priority message %d was negatively acknowledged", i)
		}
	}
	select {
	case unroutable := <-returned:
		return fmt.Errorf("priority message %q was returned: %s", unroutable.MessageId, unroutable.ReplyText)
	default:
	}

	if err := ch.Qos(1, 0, false); err != nil {
		return fmt.Errorf("set priority prefetch: %w", err)
	}
	deliveries, err := ch.ConsumeWithContext(ctx, queue, "a0.3-priority", false, false, false, false, nil)
	if err != nil {
		return fmt.Errorf("start priority consumer: %w", err)
	}
	observed := make([]uint8, 0, len(published))
	for len(observed) < len(published) {
		select {
		case <-ctx.Done():
			return fmt.Errorf("consume priority probe: %w", ctx.Err())
		case delivery, ok := <-deliveries:
			if !ok {
				return errors.New("priority delivery channel closed")
			}
			id := fmt.Sprintf("priority-%02d", delivery.Priority)
			if delivery.MessageId != id || string(delivery.Body) != id || delivery.DeliveryMode != amqp.Persistent {
				return fmt.Errorf("priority %s delivery %q has unexpected identity, body or persistence", name, delivery.MessageId)
			}
			observed = append(observed, delivery.Priority)
			if err := delivery.Ack(false); err != nil {
				return fmt.Errorf("ack priority %d: %w", delivery.Priority, err)
			}
		}
	}
	if !slices.Equal(observed, want) {
		return fmt.Errorf("priority %s order %v (want %v)", name, observed, want)
	}
	fmt.Printf("priority=%s:%v\n", name, observed)
	return nil
}
