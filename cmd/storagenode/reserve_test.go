package main

import (
	"errors"
	"net"
	"syscall"
	"testing"
	"time"

	"github.com/ParthMalhotra07/ReliefGrid/internal/testnet"
)

// #75: the gate tests' addresses stay reserved from the moment they are
// picked until run listens on them. In between, another process cannot bind
// them (it would make run fail with "address already in use"), and a dial is
// refused (an accepted one would be that other process, and the
// unadmitted-disk check would test it, not run). run then opens all three
// listeners on the held ports.
func TestGateTestAddrsStayReservedUntilRun(t *testing.T) {
	if !testnet.Supported {
		t.Skip("testnet.Reserve needs SO_REUSEPORT (linux, darwin)")
	}
	raftAddr, dataAddr, mgmtAddr, listen := reserveAddrs(t)
	addrs := map[string]string{"raft": raftAddr, "data": dataAddr, "mgmt": mgmtAddr}
	for name, a := range addrs {
		ln, err := (&net.ListenConfig{}).Listen(t.Context(), "tcp", a)
		if err == nil {
			_ = ln.Close()
			t.Fatalf("another listener bound the %s address %s between selection and run", name, a)
		}
		if !errors.Is(err, syscall.EADDRINUSE) {
			t.Fatalf("bind %s address %s before run: %v, want EADDRINUSE", name, a, err)
		}
		conn, err := (&net.Dialer{}).DialContext(t.Context(), "tcp", a)
		if err == nil {
			_ = conn.Close()
			t.Fatalf("the reserved %s address %s accepted a connection before run", name, a)
		}
		if !errors.Is(err, syscall.ECONNREFUSED) {
			t.Fatalf("dial %s address %s before run: %v, want ECONNREFUSED", name, a, err)
		}
	}

	cfg := testConfig(t, raftAddr, dataAddr, mgmtAddr, listen)
	cfg.AdmissionOff = true
	startRun(t, cfg)
	deadline := time.Now().Add(10 * time.Second)
	for !open(raftAddr) || !open(dataAddr) || !open(mgmtAddr) {
		if time.Now().After(deadline) {
			t.Fatalf("run did not listen on every reserved address: raft %v, data %v, mgmt %v",
				open(raftAddr), open(dataAddr), open(mgmtAddr))
		}
		time.Sleep(20 * time.Millisecond)
	}
}
