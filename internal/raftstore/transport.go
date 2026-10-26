package raftstore

import (
	"context"
	"fmt"
	"net"
	"strconv"
	"time"

	"github.com/hashicorp/go-hclog"
	"github.com/hashicorp/raft"
)

// raft.NewTCPTransport only accepts an advertise address that is a
// *net.TCPAddr with a concrete IP (tcp_transport.go in v1.8.0), so it cannot
// advertise a stable DNS name. dnsStreamLayer replaces it: Addr() returns the
// pod's DNS name, which NetworkTransport.LocalAddr() hands to followers as
// the leader address, and Dial resolves the peer's name on every dial, so a
// peer recreated with a new pod IP is found once cluster DNS serves it.

// dnsAddr is a net.Addr whose String() is a host:port resolved only when dialled.
type dnsAddr string

func (dnsAddr) Network() string  { return "tcp" }
func (a dnsAddr) String() string { return string(a) }

type dnsStreamLayer struct {
	net.Listener
	advertise dnsAddr
}

func (s *dnsStreamLayer) Addr() net.Addr { return s.advertise }

// Dial resolves address through the system resolver each time; nothing on
// the path caches the answer.
func (s *dnsStreamLayer) Dial(address raft.ServerAddress, timeout time.Duration) (net.Conn, error) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	var d net.Dialer
	return d.DialContext(ctx, "tcp", string(address))
}

// newDNSTransport listens on bind and advertises advertise. A port of 0 in
// advertise is replaced with the bound port.
func newDNSTransport(bind, advertise string, logger hclog.Logger) (*raft.NetworkTransport, error) {
	host, port, err := net.SplitHostPort(advertise)
	if err != nil || host == "" {
		return nil, fmt.Errorf("raftstore: advertise address %q must be host:port", advertise)
	}
	var lc net.ListenConfig
	ln, err := lc.Listen(context.Background(), "tcp", bind)
	if err != nil {
		return nil, fmt.Errorf("raftstore: listen %s: %w", bind, err)
	}
	if port == "0" {
		advertise = net.JoinHostPort(host, strconv.Itoa(ln.Addr().(*net.TCPAddr).Port))
	}
	stream := &dnsStreamLayer{Listener: ln, advertise: dnsAddr(advertise)}
	return raft.NewNetworkTransportWithConfig(&raft.NetworkTransportConfig{
		Stream:  stream,
		MaxPool: 3,
		Timeout: 10 * time.Second,
		Logger:  logger,
	}), nil
}
