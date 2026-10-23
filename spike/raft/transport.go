package main

import (
	"context"
	"fmt"
	"io"
	"net"
	"time"

	"github.com/hashicorp/raft"
)

// raft.NewTCPTransport only accepts an advertise address that is a *net.TCPAddr
// with a concrete IP (tcp_transport.go in v1.8.0), so it cannot advertise a stable
// DNS name. dnsStreamLayer is the replacement: Addr() returns the pod's FQDN, which
// NetworkTransport.LocalAddr() hands to followers as the leader address, and Dial
// takes the peer's ServerAddress (also an FQDN) and resolves it on every dial.

// dnsAddr is a net.Addr whose String() is a host:port that is resolved only when dialled.
type dnsAddr string

func (dnsAddr) Network() string  { return "tcp" }
func (a dnsAddr) String() string { return string(a) }

type dnsStreamLayer struct {
	net.Listener
	advertise dnsAddr
}

func (s *dnsStreamLayer) Addr() net.Addr { return s.advertise }

// Dial resolves address through the system resolver each time. Go's resolver keeps
// no cache, so a peer that restarted with a new pod IP is found as soon as cluster
// DNS serves the new record.
func (s *dnsStreamLayer) Dial(address raft.ServerAddress, timeout time.Duration) (net.Conn, error) {
	d := net.Dialer{Timeout: timeout}
	return d.Dial("tcp", string(address))
}

func newDNSTransport(bind, advertise string, logOutput io.Writer) (*raft.NetworkTransport, error) {
	var lc net.ListenConfig
	ln, err := lc.Listen(context.Background(), "tcp", bind)
	if err != nil {
		return nil, fmt.Errorf("listen raft %s: %w", bind, err)
	}
	stream := &dnsStreamLayer{Listener: ln, advertise: dnsAddr(advertise)}
	return raft.NewNetworkTransport(stream, 3, 10*time.Second, logOutput), nil
}
