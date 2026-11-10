package raftstore

import (
	"context"
	"errors"
	"io"
	"net"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hashicorp/go-hclog"
	"github.com/hashicorp/raft"
)

func TestDNSTransportAdvertisesName(t *testing.T) {
	const name = "storage-1-0.storage-1.reliefgrid.svc.cluster.local:7000"
	trans, err := newDNSTransport("127.0.0.1:0", name, nil, hclog.NewNullLogger())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = trans.Close() }()
	if got := string(trans.LocalAddr()); got != name {
		t.Fatalf("LocalAddr = %q, want the DNS name", got)
	}
}

func TestDNSTransportAdvertisesBoundPortForZero(t *testing.T) {
	trans, err := newDNSTransport("127.0.0.1:0", "localhost:0", nil, hclog.NewNullLogger())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = trans.Close() }()
	addr := string(trans.LocalAddr())
	host, port, err := net.SplitHostPort(addr)
	if err != nil || host != "localhost" || port == "0" || strings.Contains(addr, "127.0.0.1") {
		t.Fatalf("LocalAddr = %q, want localhost:<bound port>", addr)
	}
}

func TestDNSStreamLayerDialsByName(t *testing.T) {
	var lc net.ListenConfig
	ln, err := lc.Listen(context.Background(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = ln.Close() }()
	go func() {
		if c, err := ln.Accept(); err == nil {
			_ = c.Close()
		}
	}()
	// A name, not an IP: the layer must resolve it at dial time.
	addr := net.JoinHostPort("localhost", strconv.Itoa(ln.Addr().(*net.TCPAddr).Port))
	s := &dnsStreamLayer{advertise: "unused:1"}
	c, err := s.Dial(raft.ServerAddress(addr), time.Second)
	if err != nil {
		t.Fatalf("Dial(%s): %v", addr, err)
	}
	_ = c.Close()
}

// readObservedConn signals the actual handleConn read, rather than merely
// Accept returning. A Close entered after Read returns observes handleConn's
// deferred cleanup, including when transport shutdown first closes the socket.
type readObservedConn struct {
	net.Conn
	started, handlerClosed chan struct{}
	readOnce, closeOnce    sync.Once
	readReturned           atomic.Bool
}

func (c *readObservedConn) Read(p []byte) (int, error) {
	c.readOnce.Do(func() { close(c.started) })
	n, err := c.Conn.Read(p)
	c.readReturned.Store(true)
	return n, err
}

func (c *readObservedConn) Close() error {
	fromHandler := c.readReturned.Load()
	err := c.Conn.Close()
	if fromHandler {
		c.closeOnce.Do(func() { close(c.handlerClosed) })
	}
	return err
}

type readObservedListener struct {
	net.Listener
	started, handlerClosed chan struct{}
}

func (l *readObservedListener) Accept() (net.Conn, error) {
	c, err := l.Listener.Accept()
	if err != nil {
		return nil, err
	}
	return &readObservedConn{Conn: c, started: l.started, handlerClosed: l.handlerClosed}, nil
}

func TestCloseUnblocksAcceptedConnectionRead(t *testing.T) {
	started, handlerClosed := make(chan struct{}), make(chan struct{})
	listen := func(ctx context.Context, network, address string) (net.Listener, error) {
		ln, err := (&net.ListenConfig{}).Listen(ctx, network, address)
		if err != nil {
			return nil, err
		}
		return &readObservedListener{Listener: ln, started: started, handlerClosed: handlerClosed}, nil
	}
	nt, err := newDNSTransport("127.0.0.1:0", "127.0.0.1:0", listen, hclog.NewNullLogger())
	if err != nil {
		t.Fatal(err)
	}
	ft := newFenceTransport(nt, &commitFence{}, nil, nil)
	t.Cleanup(func() { _ = ft.Close() })
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	peer, err := (&net.Dialer{}).DialContext(ctx, "tcp", string(ft.LocalAddr()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = peer.Close() // release the baseline's blocked read even on failure
		select {
		case <-handlerClosed:
		case <-time.After(5 * time.Second):
			t.Error("handler did not clean up after peer close")
		}
	})
	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("accepted connection handler never entered Read")
	}
	if err := ft.Close(); err != nil {
		t.Fatal(err)
	}
	// Keep the peer open: shutdown itself must release the handler's I/O.
	if err := peer.SetReadDeadline(time.Now().Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	var b [1]byte
	if _, err := peer.Read(b[:]); !errors.Is(err, io.EOF) {
		t.Errorf("accepted connection after transport Close: %v, want EOF", err)
	}
	select {
	case <-handlerClosed:
	case <-time.After(time.Second):
		t.Error("handleConn did not exit through its deferred connection Close")
	}
}

// Hold a successful listener Accept across stream Close to exercise the
// registration race without scheduling sleeps or repeated probabilistic dials.
type heldAcceptListener struct {
	net.Listener
	conn             net.Conn
	entered, release chan struct{}
}

func (l *heldAcceptListener) Accept() (net.Conn, error) {
	close(l.entered)
	<-l.release
	return l.conn, nil
}

func (l *heldAcceptListener) Close() error { return nil }

func TestDNSStreamLayerAcceptRacingClose(t *testing.T) {
	server, peer := net.Pipe()
	defer func() { _ = server.Close(); _ = peer.Close() }()
	ln := &heldAcceptListener{conn: server, entered: make(chan struct{}), release: make(chan struct{})}
	s := &dnsStreamLayer{Listener: ln}
	result := make(chan error, 1)
	go func() {
		c, err := s.Accept()
		if c != nil {
			_ = c.Close()
		}
		result <- err
	}()
	select {
	case <-ln.entered:
	case <-time.After(time.Second):
		t.Fatal("Accept did not start")
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	close(ln.release)
	select {
	case err := <-result:
		if !errors.Is(err, net.ErrClosed) {
			t.Fatalf("Accept after Close: %v, want net.ErrClosed", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Accept did not return")
	}
	read := make(chan error, 1)
	go func() {
		var b [1]byte
		_, err := peer.Read(b[:])
		read <- err
	}()
	select {
	case err := <-read:
		if !errors.Is(err, io.EOF) {
			t.Fatalf("rejected socket: %v, want EOF", err)
		}
	case <-time.After(time.Second):
		t.Fatal("rejected socket remained open")
	}
}

func TestDNSStreamLayerHandlerCloseUnregisters(t *testing.T) {
	server, peer := net.Pipe()
	defer func() { _ = server.Close(); _ = peer.Close() }()
	ln := &heldAcceptListener{conn: server, entered: make(chan struct{}), release: make(chan struct{})}
	close(ln.release)
	s := &dnsStreamLayer{Listener: ln}
	defer func() { _ = s.Close() }()
	c, err := s.Accept()
	if err != nil {
		t.Fatal(err)
	}
	if len(s.accepted) != 1 {
		t.Fatal("accepted connection was not tracked")
	}
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
	if len(s.accepted) != 0 {
		t.Fatal("handler Close retained the connection")
	}
}
