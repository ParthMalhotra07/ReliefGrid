package raftstore

import (
	"context"
	"net"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/hashicorp/go-hclog"
	"github.com/hashicorp/raft"
)

func TestDNSTransportAdvertisesName(t *testing.T) {
	const name = "storage-1-0.storage-1.reliefgrid.svc.cluster.local:7000"
	trans, err := newDNSTransport("127.0.0.1:0", name, hclog.NewNullLogger())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = trans.Close() }()
	if got := string(trans.LocalAddr()); got != name {
		t.Fatalf("LocalAddr = %q, want the DNS name", got)
	}
}

func TestDNSTransportAdvertisesBoundPortForZero(t *testing.T) {
	trans, err := newDNSTransport("127.0.0.1:0", "localhost:0", hclog.NewNullLogger())
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
