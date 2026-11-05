// Package testnet holds loopback ports for tests (issues #58, #75).
//
// Reserve picks a port and keeps it bound for the whole test with a socket
// that never listens, instead of picking a port and releasing it. In
// between, another process could otherwise take the port and the code under
// test would fail to bind it, or a test that checks the port is closed would
// dial the other process. While held, a bind gets EADDRINUSE and a dial is
// refused, as for a pod that is not serving. The code under test opens its
// listener on the reserved port through the returned listen function.
//
// Only tests import this package.
package testnet

import (
	"context"
	"net"
)

// ListenFunc has the signature of net.ListenConfig.Listen.
type ListenFunc = func(ctx context.Context, network, address string) (net.Listener, error)
