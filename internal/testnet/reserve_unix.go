//go:build linux || darwin

package raftstore

import (
	"context"
	"net"
	"strconv"
	"syscall"
	"testing"

	"golang.org/x/sys/unix"
)

// reservePort holds a loopback port for the whole test (issue #58): a socket
// bound with SO_REUSEPORT that never listens, so nothing else can bind the
// port, and a dial is refused as by a dead pod while the node is down. The
// returned listen opens the node's listener on the same port through
// SO_REUSEPORT, and can do so again after every restart.
func reservePort(t testing.TB) (port string, listen func(ctx context.Context, network, address string) (net.Listener, error)) {
	t.Helper()
	fd, err := unix.Socket(unix.AF_INET, unix.SOCK_STREAM, 0)
	if err != nil {
		t.Fatalf("reserve port: socket: %v", err)
	}
	t.Cleanup(func() { _ = unix.Close(fd) })
	unix.CloseOnExec(fd)
	if err := unix.SetsockoptInt(fd, unix.SOL_SOCKET, unix.SO_REUSEPORT, 1); err != nil {
		t.Fatalf("reserve port: SO_REUSEPORT: %v", err)
	}
	if err := unix.Bind(fd, &unix.SockaddrInet4{Addr: [4]byte{127, 0, 0, 1}}); err != nil {
		t.Fatalf("reserve port: bind: %v", err)
	}
	sa, err := unix.Getsockname(fd)
	if err != nil {
		t.Fatalf("reserve port: getsockname: %v", err)
	}
	lc := net.ListenConfig{Control: func(_, _ string, c syscall.RawConn) error {
		var serr error
		if err := c.Control(func(fd uintptr) {
			serr = unix.SetsockoptInt(int(fd), unix.SOL_SOCKET, unix.SO_REUSEPORT, 1)
		}); err != nil {
			return err
		}
		return serr
	}}
	return strconv.Itoa(sa.(*unix.SockaddrInet4).Port), lc.Listen
}
