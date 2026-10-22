package main

import (
	"context"
	"io"
	"net"
	"strconv"
	"testing"
	"time"

	"github.com/hashicorp/raft"
)

func TestDNSTransportAdvertisesName(t *testing.T) {
	trans, err := newDNSTransport("127.0.0.1:0", "storage-spike-0.storage-spike.ns.svc.cluster.local:7000", io.Discard)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = trans.Close() }()
	if got := string(trans.LocalAddr()); got != "storage-spike-0.storage-spike.ns.svc.cluster.local:7000" {
		t.Fatalf("LocalAddr = %q, want the DNS name", got)
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
