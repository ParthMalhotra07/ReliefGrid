package raftstore

import (
	"errors"
	"io"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"fmt"
	"github.com/hashicorp/raft"
	"net"
	"os"
)

// In raft v1.8.0 a heartbeat is the one RPC the transport hands to Raft
// outside its main loop: NetworkTransport runs processHeartbeat on the
// connection's goroutine, which checks shutdownCh once and can then write
// the term to the stable store. Raft's Shutdown waits only for its own
// goroutines, and NetworkTransport.Close only closes the listener, so
// Node.Close could close BoltDB under a heartbeat still in appendEntries:
// "panic: failed to save current term: database not open" (CI run
// 41980396785). These tests pin fenceTransport.Close as the barrier.

// heartbeatPair is a fenceTransport serving heartbeats through handler, and
// a client transport that sends them over a pooled connection.
func heartbeatPair(t *testing.T, handler func(raft.RPC)) (*fenceTransport, *raft.NetworkTransport) {
	t.Helper()
	nt, err := raft.NewTCPTransport("127.0.0.1:0", nil, 2, time.Second, io.Discard)
	if err != nil {
		t.Fatal(err)
	}
	ft := newFenceTransport(nt, &commitFence{}, nil, nil)
	ft.SetHeartbeatHandler(handler)
	client, err := raft.NewTCPTransport("127.0.0.1:0", nil, 2, time.Second, io.Discard)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = ft.Close()
		_ = client.Close()
	})
	return ft, client
}

// sendHeartbeat sends a request NetworkTransport classifies as a heartbeat
// (term set, leader address set, no entries, prev/commit zero).
func sendHeartbeat(client *raft.NetworkTransport, target raft.ServerAddress) error {
	var resp raft.AppendEntriesResponse
	return client.AppendEntries("leader", target, &raft.AppendEntriesRequest{
		RPCHeader: raft.RPCHeader{Addr: []byte("leader")}, Term: 9,
	}, &resp)
}

func TestCloseWaitsForInFlightHeartbeat(t *testing.T) {
	entered, release := make(chan struct{}), make(chan struct{})
	var finished atomic.Bool
	ft, client := heartbeatPair(t, func(rpc raft.RPC) {
		close(entered)
		<-release // Raft still inside appendEntries
		finished.Store(true)
		rpc.Respond(&raft.AppendEntriesResponse{}, nil)
	})
	go func() { _ = sendHeartbeat(client, ft.LocalAddr()) }()
	<-entered

	closed := make(chan struct{})
	go func() { _ = ft.Close(); close(closed) }()
	select {
	case <-closed:
		close(release)
		t.Fatal("Close returned while a heartbeat that started before it was still running; the stores would close under it")
	case <-time.After(300 * time.Millisecond):
	}
	close(release)
	select {
	case <-closed:
	case <-time.After(5 * time.Second):
		t.Fatal("Close did not return after the in-flight heartbeat finished")
	}
	if !finished.Load() {
		t.Fatal("Close returned before the in-flight heartbeat finished")
	}
}

func TestHeartbeatAfterCloseIsRejected(t *testing.T) {
	var calls atomic.Int32
	ft, client := heartbeatPair(t, func(rpc raft.RPC) {
		calls.Add(1)
		rpc.Respond(&raft.AppendEntriesResponse{}, nil)
	})
	// The first heartbeat leaves a pooled connection whose server-side
	// goroutine outlives NetworkTransport.Close: the path a live peer uses.
	if err := sendHeartbeat(client, ft.LocalAddr()); err != nil {
		t.Fatal(err)
	}
	if err := ft.Close(); err != nil {
		t.Fatal(err)
	}
	err := sendHeartbeat(client, ft.LocalAddr())
	if n := calls.Load(); n != 1 {
		t.Fatalf("heartbeat after Close reached Raft's handler: calls=%d, want 1", n)
	}
	// The gate answers ErrTransportShutdown, but NetworkTransport's own
	// closed shutdownCh races that answer in its response select and may
	// drop the connection instead (net_transport.go:744-760, v1.8.0).
	// A dropped connection reads as EOF or, under load, as a reset (ECONNRESET), wrapped in a
	// msgpack decode error that exposes its cause only through Cause().
	if err == nil || (err.Error() != raft.ErrTransportShutdown.Error() && !closedConn(err)) {
		t.Fatalf("heartbeat after Close: error %v, want %v or a closed connection", err, raft.ErrTransportShutdown)
	}
}

// closedConn reports whether err, followed through Unwrap and through the msgpack codec's
// Cause, ends in a closed connection: EOF or a connection reset by the peer.
func closedConn(err error) bool {
	for range 16 { // a bound, in case a Cause() chain loops
		if err == nil {
			return false
		}
		if errors.Is(err, io.EOF) || errors.Is(err, syscall.ECONNRESET) {
			return true
		}
		var c interface{ Cause() error }
		if errors.As(err, &c) {
			err = c.Cause()
			continue
		}
		err = errors.Unwrap(err)
	}
	return false
}

// The gate itself, without the network's response race: after Close every
// heartbeat is answered ErrTransportShutdown and never reaches Raft.
func TestGateRefusesHeartbeatAfterClose(t *testing.T) {
	var calls atomic.Int32
	ft, _ := heartbeatPair(t, func(raft.RPC) {})
	gate := ft.gateHeartbeat(func(raft.RPC) { calls.Add(1) })
	if err := ft.Close(); err != nil {
		t.Fatal(err)
	}
	respCh := make(chan raft.RPCResponse, 1)
	gate(raft.RPC{Command: &raft.AppendEntriesRequest{Term: 9}, RespChan: respCh})
	if n := calls.Load(); n != 0 {
		t.Fatalf("heartbeat after Close reached Raft's handler: calls=%d", n)
	}
	select {
	case resp := <-respCh:
		if !errors.Is(resp.Error, raft.ErrTransportShutdown) {
			t.Fatalf("heartbeat after Close: error %v, want %v", resp.Error, raft.ErrTransportShutdown)
		}
	default:
		t.Fatal("heartbeat after Close got no response")
	}
}

// The in-flight heartbeat stands in for Raft's: after Shutdown it unblocks
// on shutdownCh and returns. Close waits for it and must still return.
func TestCloseReturnsWhenHeartbeatReleasedByShutdown(t *testing.T) {
	entered, shutdownCh := make(chan struct{}), make(chan struct{})
	ft, client := heartbeatPair(t, func(rpc raft.RPC) {
		close(entered)
		<-shutdownCh
		rpc.Respond(nil, raft.ErrRaftShutdown)
	})
	go func() { _ = sendHeartbeat(client, ft.LocalAddr()) }()
	<-entered

	closed := make(chan struct{})
	go func() { _ = ft.Close(); close(closed) }()
	close(shutdownCh)
	select {
	case <-closed:
	case <-time.After(5 * time.Second):
		t.Fatal("Close deadlocked on an in-flight heartbeat released by Raft shutdown")
	}
}

// Real Raft: a peer at a higher term keeps heartbeating a node while it
// closes. Each heartbeat makes appendEntries persist the term. Close must
// return within a bound and nothing may write to the closed stores.
func TestNodeCloseUnderHeartbeatsFromNewerTerm(t *testing.T) {
	c := newTestCluster(t, 1, ProfileDemo)
	n := c.nodes[0]
	client, err := raft.NewTCPTransport("127.0.0.1:0", nil, 4, time.Second, io.Discard)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = client.Close() }()

	stop := make(chan struct{})
	var wg sync.WaitGroup
	for w := range 4 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for term := uint64(2 + w); ; term += 4 {
				select {
				case <-stop:
					return
				default:
				}
				var resp raft.AppendEntriesResponse
				_ = client.AppendEntries("leader", n.Addr(), &raft.AppendEntriesRequest{
					RPCHeader: raft.RPCHeader{Addr: []byte("leader")}, Term: term,
				}, &resp)
			}
		}()
	}
	// Heartbeats are reaching appendEntries: they have persisted a newer term.
	deadline := time.Now().Add(waitFor)
	for n.raft.Stats()["term"] == "0" {
		if time.Now().After(deadline) {
			t.Fatal("no heartbeat reached Raft before Close")
		}
		time.Sleep(10 * time.Millisecond)
	}

	closed := make(chan error, 1)
	go func() { closed <- n.Close() }()
	select {
	case err := <-closed:
		if err != nil {
			t.Errorf("Close: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("Node.Close did not return while heartbeats were arriving")
	}
	c.nodes[0] = nil
	time.Sleep(200 * time.Millisecond) // heartbeats after Close
	close(stop)
	wg.Wait()
}

// causeErr has the msgpack codec error's shape: a message and Cause(), no Unwrap.
type causeErr struct{ cause error }

func (e causeErr) Error() string { return "msgpack decode error [pos 69]: " + e.cause.Error() }
func (e causeErr) Cause() error  { return e.cause }

// The flake seen under make check load (14 Nov): the dropped connection surfaced as a reset
// inside a decode error. closedConn must see through it, and only to a closed connection.
func TestClosedConnSeesThroughTheCodecError(t *testing.T) {
	reset := &net.OpError{Op: "read", Net: "tcp", Err: os.NewSyscallError("read", syscall.ECONNRESET)}
	for _, c := range []struct {
		err  error
		want bool
	}{
		{causeErr{reset}, true},
		{causeErr{io.EOF}, true},
		{fmt.Errorf("wrapped: %w", causeErr{reset}), true},
		{causeErr{errors.New("unexpected response")}, false},
		{errors.New("connection refused"), false},
	} {
		if got := closedConn(c.err); got != c.want {
			t.Errorf("closedConn(%v) = %v, want %v", c.err, got, c.want)
		}
	}
}
