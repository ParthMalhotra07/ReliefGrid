package testnet_test

import (
	"errors"
	"net"
	"syscall"
	"testing"

	"github.com/ParthMalhotra07/ReliefGrid/internal/testnet"
)

// A reserved port cannot be bound by anyone else and refuses connections,
// and listen opens it, again after the first listener closes.
func TestReserveHoldsThePortForListen(t *testing.T) {
	if !testnet.Supported {
		t.Skip("Reserve needs SO_REUSEPORT (linux, darwin)")
	}
	port, listen := testnet.Reserve(t)
	addr := "127.0.0.1:" + port
	if ln, err := (&net.ListenConfig{}).Listen(t.Context(), "tcp", addr); !errors.Is(err, syscall.EADDRINUSE) {
		if err == nil {
			_ = ln.Close()
		}
		t.Fatalf("plain bind of a reserved port: %v, want EADDRINUSE", err)
	}
	if c, err := (&net.Dialer{}).DialContext(t.Context(), "tcp", addr); !errors.Is(err, syscall.ECONNREFUSED) {
		if err == nil {
			_ = c.Close()
		}
		t.Fatalf("dial of a reserved port: %v, want ECONNREFUSED", err)
	}
	for range 2 {
		ln, err := listen(t.Context(), "tcp", addr)
		if err != nil {
			t.Fatalf("listen on the reserved port: %v", err)
		}
		c, err := (&net.Dialer{}).DialContext(t.Context(), "tcp", addr)
		if err != nil {
			t.Fatalf("dial the listener: %v", err)
		}
		_ = c.Close()
		_ = ln.Close()
	}
}
