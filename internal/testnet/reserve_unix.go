//go:build linux || darwin

package testnet

import (
	"net"
	"strconv"
	"syscall"
	"testing"

	"golang.org/x/sys/unix"
)

// Supported reports whether Reserve holds the port. Where it is false,
// Reserve picks and releases the port, as before #58.
const Supported = true

// Reserve holds a loopback port for the rest of the test: a socket bound with
// SO_REUSEPORT that never listens, so nothing else can bind the port and a
// dial is refused. listen opens a listener on the same port through
// SO_REUSEPORT, and can do so again after a restart.
func Reserve(t testing.TB) (port string, listen ListenFunc) {
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
	return strconv.Itoa(sa.(*unix.SockaddrInet4).Port), reusePortListen
}

var reusePortListen = (&net.ListenConfig{Control: func(_, _ string, c syscall.RawConn) error {
	var serr error
	if err := c.Control(func(fd uintptr) {
		serr = unix.SetsockoptInt(int(fd), unix.SOL_SOCKET, unix.SO_REUSEPORT, 1)
	}); err != nil {
		return err
	}
	return serr
}}).Listen
