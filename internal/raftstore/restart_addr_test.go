//go:build linux || darwin

package raftstore

import (
	"context"
	"errors"
	"net"
	"syscall"
	"testing"
)

// #58: while a node is down its port stays reserved for it. Nothing else can
// bind it (as another process taking an ephemeral port would), a dial is
// refused as by a dead pod, and the node reopens on the same address.
func TestKilledNodeKeepsItsAddress(t *testing.T) {
	c := newTestCluster(t, 1, testProfile)
	addr := c.nodes[0].Addr()
	c.kill(0)

	ln, err := (&net.ListenConfig{}).Listen(context.Background(), "tcp", c.cfgs[0].BindAddr)
	if err == nil {
		_ = ln.Close()
		t.Fatalf("another listener bound %s while the node was down", c.cfgs[0].BindAddr)
	}
	if !errors.Is(err, syscall.EADDRINUSE) {
		t.Fatalf("bind while the node was down: %v, want EADDRINUSE", err)
	}
	conn, err := (&net.Dialer{}).DialContext(context.Background(), "tcp", c.cfgs[0].BindAddr)
	if err == nil {
		_ = conn.Close()
		t.Fatal("the port of a killed node accepted a connection")
	}
	if !errors.Is(err, syscall.ECONNREFUSED) {
		t.Fatalf("dial while the node was down: %v, want ECONNREFUSED", err)
	}

	c.open(0)
	if got := c.nodes[0].Addr(); got != addr {
		t.Fatalf("address after restart = %s, want %s", got, addr)
	}
	if !listening(c.cfgs[0].BindAddr) {
		t.Fatal("the restarted node is not listening")
	}
}
