//go:build !linux && !darwin

package testnet

import (
	"context"
	"net"
	"testing"
)

// Supported reports whether Reserve holds the port. Without SO_REUSEPORT it
// does not: CI runs on Linux.
const Supported = false

// Reserve without SO_REUSEPORT picks the port and releases it, so another
// process can still take it before the code under test binds it. listen is
// nil: use net.ListenConfig.Listen.
func Reserve(t testing.TB) (port string, listen ListenFunc) {
	t.Helper()
	ln, err := (&net.ListenConfig{}).Listen(context.Background(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	_, port, _ = net.SplitHostPort(ln.Addr().String())
	_ = ln.Close()
	return port, nil
}
