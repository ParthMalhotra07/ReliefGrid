package raftstore

import (
	"context"
	"errors"
	"fmt"
	"io"
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

// newDNSTransport listens on bind through listen (net.ListenConfig.Listen if
// nil) and advertises advertise. A port of 0 in advertise is replaced with the
// bound port.
func newDNSTransport(bind, advertise string, listen func(ctx context.Context, network, address string) (net.Listener, error), logger hclog.Logger) (*raft.NetworkTransport, error) {
	host, port, err := net.SplitHostPort(advertise)
	if err != nil || host == "" {
		return nil, fmt.Errorf("raftstore: advertise address %q must be host:port", advertise)
	}
	if listen == nil {
		listen = (&net.ListenConfig{}).Listen
	}
	ln, err := listen(context.Background(), "tcp", bind)
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
// the newest term seen, and the highest LeaderCommitIndex carried by any
// AppendEntries. Every leader's commit index is really committed, so the
// highest is kept across terms: a new leader advances its own only once its
// no-op commits, and until then may advertise less than the old leader
// committed. A deposed leader's append (a lower term) never sets the term.
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
		f.term, f.commit = term, max(f.commit, commit)
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
	acks      ackTracker
	// Tests only (Config.dropSend, Config.afterSend); set before Raft starts.
	dropSend  func(target raft.ServerAddress, req any) bool
	afterSend func(target raft.ServerAddress, req any)
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
			if ae, ok := rpc.Command.(*raft.AppendEntriesRequest); ok {
				t.fence.observe(ae.Term, ae.LeaderCommitIndex)
			}
			if t.before != nil {
				t.before(rpc)
			}
			select {
			case t.consumeCh <- rpc:
			case <-t.done:
				return
			}
		}
	}
}

// Raft enables pre-vote and closes the transport on Shutdown only through
// these optional interfaces, which the embedded NetworkTransport provides.
var (
	_ raft.WithPreVote = (*fenceTransport)(nil)
	_ raft.WithClose   = (*fenceTransport)(nil)
)

// Consumer is what Raft reads incoming RPCs from.
func (t *fenceTransport) Consumer() <-chan raft.RPC { return t.consumeCh }

// Close stops forwarding and closes the network transport.
func (t *fenceTransport) Close() error {
	t.closeOnce.Do(func() { close(t.done) })
	return t.NetworkTransport.Close()
}

// errDropped is what a test's dropSend makes an outgoing RPC fail with.
var errDropped = errors.New("raftstore: rpc dropped by test")

// send runs one outgoing RPC through the test hooks, if any.
func (t *fenceTransport) send(target raft.ServerAddress, req any, rpc func() error) error {
	if t.dropSend != nil && t.dropSend(target, req) {
		return errDropped
	}
	if err := rpc(); err != nil {
		return err
	}
	if t.afterSend != nil {
		t.afterSend(target, req)
	}
	return nil
}

// AppendEntries carries heartbeats as well as appends (replication.go in
// v1.8.0 calls it for both). A successful answer is recorded with the time
// the request was sent, for the read fence.
func (t *fenceTransport) AppendEntries(id raft.ServerID, target raft.ServerAddress, args *raft.AppendEntriesRequest, resp *raft.AppendEntriesResponse) error {
	sent := time.Now()
	err := t.send(target, args, func() error { return t.NetworkTransport.AppendEntries(id, target, args, resp) })
	if err == nil && resp.Success {
		t.acks.record(id, args.Term, sent)
	}
	return err
}

// ackTracker keeps, per peer, the newest AppendEntries request that peer
// accepted: its term and when it was sent (S4.1).
//
// raft v1.8.0's VerifyLeader counts any successful response that arrives
// after it is registered (replication.go:437, future.go:274-293), even one
// to a request sent before the read began; a response delayed past an
// election could confirm a deposed leader. The fence therefore also needs
// a quorum of acceptances of requests sent after the read began. A
// follower that accepted a request of term T has not voted in a later
// term, so no later leader can have committed anything before that.
// Pipelined appends are not recorded: missing acknowledgements only make
// the check stricter, and VerifyLeader's heartbeats always come here.
type ackTracker struct {
	mu   sync.Mutex
	acks map[raft.ServerID]ack
}

type ack struct {
	term uint64
	sent time.Time // local monotonic clock; only ever compared locally
}

func (a *ackTracker) record(id raft.ServerID, term uint64, sent time.Time) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.acks == nil {
		a.acks = map[raft.ServerID]ack{}
	}
	if old, ok := a.acks[id]; ok && (old.term > term || old.term == term && !sent.After(old.sent)) {
		return
	}
	a.acks[id] = ack{term: term, sent: sent}
}

// acceptedSince reports whether peer id accepted a request of term that
// was sent at or after since.
func (a *ackTracker) acceptedSince(id raft.ServerID, term uint64, since time.Time) bool {
	a.mu.Lock()
	defer a.mu.Unlock()
	k, ok := a.acks[id]
	return ok && k.term == term && !k.sent.Before(since)
}

// AppendEntriesPipeline is off while a test drops RPCs, so that every
// append goes through AppendEntries; Raft falls back on this error
// (replication.go in v1.8.0).
func (t *fenceTransport) AppendEntriesPipeline(id raft.ServerID, target raft.ServerAddress) (raft.AppendPipeline, error) {
	if t.dropSend != nil {
		return nil, raft.ErrPipelineReplicationNotSupported
	}
	return t.NetworkTransport.AppendEntriesPipeline(id, target)
}

func (t *fenceTransport) RequestVote(id raft.ServerID, target raft.ServerAddress, args *raft.RequestVoteRequest, resp *raft.RequestVoteResponse) error {
	return t.send(target, args, func() error { return t.NetworkTransport.RequestVote(id, target, args, resp) })
}

func (t *fenceTransport) RequestPreVote(id raft.ServerID, target raft.ServerAddress, args *raft.RequestPreVoteRequest, resp *raft.RequestPreVoteResponse) error {
	return t.send(target, args, func() error { return t.NetworkTransport.RequestPreVote(id, target, args, resp) })
}

func (t *fenceTransport) InstallSnapshot(id raft.ServerID, target raft.ServerAddress, args *raft.InstallSnapshotRequest, resp *raft.InstallSnapshotResponse, data io.Reader) error {
	return t.send(target, args, func() error { return t.NetworkTransport.InstallSnapshot(id, target, args, resp, data) })
}

func (t *fenceTransport) TimeoutNow(id raft.ServerID, target raft.ServerAddress, args *raft.TimeoutNowRequest, resp *raft.TimeoutNowResponse) error {
	return t.send(target, args, func() error { return t.NetworkTransport.TimeoutNow(id, target, args, resp) })
}
