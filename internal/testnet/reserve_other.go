//go:build !linux && !darwin

package raftstore

import (
	"context"
	"net"
	"testing"
)

// reservePort without SO_REUSEPORT: the port is picked and released, so a
// restart can still lose it to another process (issue #58). CI runs on Linux.
func reservePort(t testing.TB) (port string, listen func(ctx context.Context, network, address string) (net.Listener, error)) {
	t.Helper()
	ln, err := (&net.ListenConfig{}).Listen(context.Background(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	_, port, _ = net.SplitHostPort(ln.Addr().String())
	_ = ln.Close()
	return port, nil
}
