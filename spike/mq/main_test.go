package main

import (
	"context"
	"io"
	"net"
	"testing"
	"time"
)

func TestOpenCancellationInterruptsStalledHandshake(t *testing.T) {
	listener, err := net.ListenTCP("tcp", &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = listener.Close() }()
	deadline := time.Now().Add(5 * time.Second)
	if err := listener.SetDeadline(deadline); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithDeadline(context.Background(), deadline)
	defer cancel()
	result := make(chan error, 1)
	go func() {
		conn, _, err := open(ctx, "amqp://guest:guest@"+listener.Addr().String()+"/")
		if conn != nil {
			_ = conn.Close()
		}
		result <- err
	}()
	server, err := listener.Accept()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = server.Close() }()
	if err := server.SetReadDeadline(deadline); err != nil {
		t.Fatal(err)
	}
	// The protocol header proves TCP DialContext has finished. The peer then
	// deliberately withholds connection.start, blocking the AMQP handshake.
	var header [8]byte
	if _, err := io.ReadFull(server, header[:]); err != nil {
		t.Fatal(err)
	}
	cancel()
	select {
	case err := <-result:
		if err == nil {
			t.Fatal("stalled handshake unexpectedly succeeded")
		}
	case <-time.After(time.Until(deadline)):
		t.Fatal("AMQP handshake ignored context cancellation after TCP connected")
	}
}
