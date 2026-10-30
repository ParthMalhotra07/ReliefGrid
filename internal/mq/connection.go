package mq

import (
	"context"
	"fmt"
	"net"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

// Dial opens a non-recovering connection owned by ctx. TCP setup and AMQP
// handshaking are bounded by five seconds. Cancelling ctx closes the transport
// for the rest of its lifetime, including stalled protocol writes and reads.
// Use separate connections for consumers and publishers; reconnect with backoff.
func Dial(ctx context.Context, address string) (*amqp.Connection, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	dialCtx, cancel := context.WithTimeout(ctx, callTimeout)
	defer cancel()
	var transport net.Conn
	var stopLifetime func() bool
	conn, err := amqp.DialConfig(address, amqp.Config{
		Heartbeat: 10 * time.Second,
		Dial: func(network, address string) (net.Conn, error) {
			d := net.Dialer{Timeout: callTimeout}
			raw, err := d.DialContext(dialCtx, network, address)
			if err != nil {
				return nil, err
			}
			if deadline, ok := dialCtx.Deadline(); ok {
				if err := raw.SetDeadline(deadline); err != nil {
					_ = raw.Close()
					return nil, err
				}
			}
			stopLifetime = context.AfterFunc(ctx, func() { _ = raw.Close() })
			transport = &contextConn{Conn: raw, stop: stopLifetime}
			return transport, nil
		},
	})
	if err != nil {
		if transport != nil {
			_ = transport.Close()
		}
		return nil, fmt.Errorf("dial RabbitMQ: %w", err)
	}
	if err := transport.SetDeadline(time.Time{}); err != nil {
		_ = conn.CloseDeadline(time.Now())
		return nil, fmt.Errorf("clear AMQP handshake deadline: %w", err)
	}
	if err := ctx.Err(); err != nil {
		_ = conn.CloseDeadline(time.Now())
		return nil, err
	}
	return conn, nil
}

type contextConn struct {
	net.Conn
	stop func() bool
}

func (c *contextConn) Close() error { c.stop(); return c.Conn.Close() }

func closeOnCancel(ctx context.Context, conn *amqp.Connection) func() bool {
	return context.AfterFunc(ctx, func() { _ = conn.CloseDeadline(time.Now()) })
}
