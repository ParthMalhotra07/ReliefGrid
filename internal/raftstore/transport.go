package raftstore

import (
	"context"
	"fmt"
	"net"
	"strconv"
	"sync"
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

// commitFence is the leader's commit index as this replica last learned it:
// the highest LeaderCommitIndex carried by an AppendEntries of the newest
// term seen. A deposed leader's append (a lower term) never replaces it.
type commitFence struct {
	mu     sync.Mutex
	term   uint64
	commit uint64
}

func (f *commitFence) observe(term, commit uint64) {
	f.mu.Lock()
	defer f.mu.Unlock()
	switch {
	case term > f.term:
		f.term, f.commit = term, commit
	case term == f.term && commit > f.commit:
		f.commit = commit
	}
}

func (f *commitFence) get() (term, commit uint64) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.term, f.commit
}

// fenceTransport is the Raft transport with every incoming AppendEntries
// observed on the way to Raft, to learn the leader's commit index (issue
// #22). Heartbeats take NetworkTransport's fast path and never pass here;
// they carry no commit index anyway (net_transport.go in v1.8.0), while
// every append does (replication.go), at least every CommitTimeout.
// Embedding keeps the optional interfaces Raft looks for (WithPreVote,
// WithClose).
type fenceTransport struct {
	*raft.NetworkTransport
	fence     *commitFence
	consumeCh chan raft.RPC
	done      chan struct{}
	closeOnce sync.Once
	before    func(raft.RPC) // tests only
}

func newFenceTransport(t *raft.NetworkTransport, fence *commitFence, before func(raft.RPC)) *fenceTransport {
	ft := &fenceTransport{NetworkTransport: t, fence: fence, consumeCh: make(chan raft.RPC), done: make(chan struct{}), before: before}
	go ft.forward()
	return ft
}

func (t *fenceTransport) forward() {
	in := t.NetworkTransport.Consumer()
	for {
		select {
		case <-t.done:
			return
		case rpc := <-in:
			if t.before != nil {
				t.before(rpc)
			}
			if ae, ok := rpc.Command.(*raft.AppendEntriesRequest); ok {
				t.fence.observe(ae.Term, ae.LeaderCommitIndex)
			}
			select {
			case t.consumeCh <- rpc:
			case <-t.done:
				return
			}
		}
	}
}

// Consumer is what Raft reads incoming RPCs from.
func (t *fenceTransport) Consumer() <-chan raft.RPC { return t.consumeCh }

// Close stops forwarding and closes the network transport.
func (t *fenceTransport) Close() error {
	t.closeOnce.Do(func() { close(t.done) })
	return t.NetworkTransport.Close()
}
