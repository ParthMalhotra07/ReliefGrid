package main

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"maps"
	"net"
	"net/http"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ParthMalhotra07/ReliefGrid/internal/raftstore"
)

type manifestData struct {
	v atomic.Pointer[map[string]string]
}

func (m *manifestData) set(d map[string]string) { m.v.Store(&d) }

func (m *manifestData) Get(context.Context) (map[string]string, error) { return *m.v.Load(), nil }

func freeAddr(t *testing.T) string {
	t.Helper()
	ln, err := (&net.ListenConfig{}).Listen(context.Background(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	a := ln.Addr().String()
	_ = ln.Close()
	return a
}

func open(addr string) bool {
	c, err := (&net.Dialer{Timeout: 200 * time.Millisecond}).DialContext(context.Background(), "tcp", addr)
	if err != nil {
		return false
	}
	_ = c.Close()
	return true
}

func mgmtStatus(t *testing.T, addr string) map[string]any {
	t.Helper()
	req, _ := http.NewRequestWithContext(t.Context(), http.MethodGet, "http://"+addr+"/mgmt/status", nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil
	}
	defer func() { _ = resp.Body.Close() }()
	var st map[string]any
	_ = json.NewDecoder(resp.Body).Decode(&st)
	return st
}

// #20 / M7, end to end in the process: a storage node whose empty disk is
// not admitted serves only management. Its Raft and data ports stay closed
// (nothing can reach the old voter's stable address) until the manifest
// records admitted[<podName>] = <incarnation>; then both open.
func TestRunKeepsAnUnadmittedDiskOnManagementOnly(t *testing.T) {
	root := t.TempDir()
	raftAddr, dataAddr, mgmtAddr := freeAddr(t), freeAddr(t), freeAddr(t)
	_, raftPort, _ := net.SplitHostPort(raftAddr)
	src := &manifestData{}
	src.set(map[string]string{"groupId": "shard-1", "shard": "1", "phase": "BOOTSTRAPPED",
		"peerIds": `["storage-shard-1-0/aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa","storage-shard-1-1/bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb"]`})
	cfg := config{
		Raft: raftstore.Config{PodName: "storage-shard-1-2", GroupID: "shard-1", Shard: 1,
			DataDir: filepath.Join(root, "pvc"), FSMDir: filepath.Join(root, "fsm"),
			BindAddr: raftAddr, AdvertiseAddr: "localhost:" + raftPort, Profile: raftstore.ProfileDemo},
		HTTPAddr: dataAddr, MgmtAddr: mgmtAddr, RingPath: filepath.Join("..", "..", "contracts", "ring.yaml"),
		RequestTimeout: time.Second, Admission: src, AdmissionPoll: 10 * time.Millisecond,
	}
	_, cfg.HTTPPort, _ = net.SplitHostPort(dataAddr)
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- run(ctx, cfg) }()
	t.Cleanup(func() {
		cancel()
		if err := <-done; err != nil {
			t.Errorf("run: %v", err)
		}
	})

	var st map[string]any
	deadline := time.Now().Add(10 * time.Second)
	for st = mgmtStatus(t, mgmtAddr); st == nil || st["state"] != "Unadmitted"; st = mgmtStatus(t, mgmtAddr) {
		if time.Now().After(deadline) {
			t.Fatalf("management never reported Unadmitted: %v", st)
		}
		time.Sleep(20 * time.Millisecond)
	}
	for range 10 {
		if open(raftAddr) || open(dataAddr) {
			t.Fatalf("an unadmitted disk opened a port: raft %v, data %v", open(raftAddr), open(dataAddr))
		}
		time.Sleep(20 * time.Millisecond)
	}

	inc, _ := st["incarnation"].(string)
	d := maps.Clone(*src.v.Load()) // the source has handed out the old map
	d["admitted"] = `{"storage-shard-1-2":"` + inc + `"}`
	src.set(d)
	for !open(raftAddr) || !open(dataAddr) {
		if time.Now().After(deadline) {
			t.Fatalf("not opened after admission: raft %v, data %v; status %v", open(raftAddr), open(dataAddr), mgmtStatus(t, mgmtAddr))
		}
		time.Sleep(20 * time.Millisecond)
	}
	if st := mgmtStatus(t, mgmtAddr); st["state"] == "Unadmitted" || st["incarnation"] != inc {
		t.Fatalf("after admission: %v", st)
	}
}

// The gate cannot be configured away by accident: production needs the
// manifest's name; only ADMISSION=off (tests, local runs) skips it.
func TestLoadConfigAdmission(t *testing.T) {
	base := map[string]string{
		"POD_NAME": "storage-shard-1-2", "SHARD": "1", "DATA_DIR": "/data", "FSM_DIR": "/fsm",
		"RAFT_ADVERTISE": "storage-shard-1-2.storage-shard-1.reliefgrid.svc.cluster.local:7000",
	}
	env := func(over map[string]string) func(string) string {
		return func(k string) string {
			if v, ok := over[k]; ok {
				return v
			}
			return base[k]
		}
	}
	if _, err := loadConfig(env(nil)); err == nil {
		t.Fatal("no MANIFEST_NAME and no ADMISSION=off accepted")
	}
	cfg, err := loadConfig(env(map[string]string{"MANIFEST_NAME": "storage-shard-1-manifest", "POD_NAMESPACE": "reliefgrid"}))
	if err != nil || cfg.ManifestName != "storage-shard-1-manifest" || cfg.Namespace != "reliefgrid" || cfg.AdmissionPoll != 2*time.Second {
		t.Fatalf("with MANIFEST_NAME: %+v, %v", cfg, err)
	}
	if cfg, err := loadConfig(env(map[string]string{"ADMISSION": "off"})); err != nil || cfg.Admission != nil || cfg.ManifestName != "" {
		t.Fatalf("ADMISSION=off: %+v, %v", cfg, err)
	}
	if _, err := loadConfig(env(map[string]string{"ADMISSION": "maybe"})); err == nil {
		t.Fatal("ADMISSION=maybe accepted")
	}
}

// ADMISSION=off (tests and local runs only; the chart cannot set it) is
// announced loudly at startup, and the disk opens Raft at once.
func TestRunWarnsLoudlyWhenAdmissionIsOff(t *testing.T) {
	var logs syncBuffer
	prev := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&logs, nil)))
	t.Cleanup(func() { slog.SetDefault(prev) })

	root := t.TempDir()
	raftAddr, dataAddr, mgmtAddr := freeAddr(t), freeAddr(t), freeAddr(t)
	_, raftPort, _ := net.SplitHostPort(raftAddr)
	cfg := config{
		Raft: raftstore.Config{PodName: "storage-shard-1-2", GroupID: "shard-1", Shard: 1,
			DataDir: filepath.Join(root, "pvc"), FSMDir: filepath.Join(root, "fsm"),
			BindAddr: raftAddr, AdvertiseAddr: "localhost:" + raftPort, Profile: raftstore.ProfileDemo},
		HTTPAddr: dataAddr, MgmtAddr: mgmtAddr, RingPath: filepath.Join("..", "..", "contracts", "ring.yaml"),
		RequestTimeout: time.Second, AdmissionOff: true,
	}
	_, cfg.HTTPPort, _ = net.SplitHostPort(dataAddr)
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- run(ctx, cfg) }()
	t.Cleanup(func() {
		cancel()
		if err := <-done; err != nil {
			t.Errorf("run: %v", err)
		}
	})
	deadline := time.Now().Add(10 * time.Second)
	for !open(raftAddr) {
		if time.Now().After(deadline) {
			t.Fatal("Raft never opened with ADMISSION=off")
		}
		time.Sleep(20 * time.Millisecond)
	}
	if out := logs.String(); !strings.Contains(out, "level=WARN") || !strings.Contains(out, "ADMISSION=off") || !strings.Contains(out, "DISABLED") {
		t.Fatalf("no loud warning:\n%s", out)
	}
}

type syncBuffer struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (s *syncBuffer) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.Write(p)
}

func (s *syncBuffer) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.String()
}
