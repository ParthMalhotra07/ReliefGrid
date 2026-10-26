package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

// fakeStore is a ConfigMap store with resourceVersion compare-and-swap.
type fakeStore struct {
	mu   sync.Mutex
	data map[string]map[string]string
	rv   map[string]int
}

func newFakeStore(ms ...manifest) *fakeStore {
	s := &fakeStore{data: map[string]map[string]string{}, rv: map[string]int{}}
	for _, m := range ms {
		s.data[m.Name], s.rv[m.Name] = m.data(), 1
	}
	return s
}

func (s *fakeStore) Get(_ context.Context, name string) (manifest, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	d, ok := s.data[name]
	if !ok {
		return manifest{}, fmt.Errorf("configmap %s not found", name)
	}
	return parseManifest(name, strconv.Itoa(s.rv[name]), d)
}

func (s *fakeStore) Update(_ context.Context, m manifest) (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if m.ResourceVersion != strconv.Itoa(s.rv[m.Name]) {
		return "", errConflict
	}
	s.rv[m.Name]++
	s.data[m.Name] = m.data()
	return strconv.Itoa(s.rv[m.Name]), nil
}

func (s *fakeStore) phase(name string) string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.data[name]["phase"]
}

// fakeCluster is the storage nodes of several shards, keyed by mgmt address.
// A bootstrap forms the group at once: every member gets state and the
// target leads.
type fakeCluster struct {
	mu         sync.Mutex
	nodes      map[string]*nodeStatus
	bootstraps map[string]int // by target address
	failNext   error          // returned (once) by the next Bootstrap, after it took effect
}

func newFakeCluster(ms ...manifest) *fakeCluster {
	c := &fakeCluster{nodes: map[string]*nodeStatus{}, bootstraps: map[string]int{}}
	for _, m := range ms {
		for i, p := range m.Peers {
			addr, _ := mgmtAddr(p.Address, "8081")
			c.nodes[addr] = &nodeStatus{
				PodName: p.PodName, GroupID: m.GroupID, Shard: m.Shard, Address: p.Address, RingVersion: m.RingVersion,
				ServerID: fmt.Sprintf("%s/inc-%d", p.PodName, i), State: "Follower",
			}
		}
	}
	return c
}

func (c *fakeCluster) Status(_ context.Context, addr string) (nodeStatus, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	n, ok := c.nodes[addr]
	if !ok {
		return nodeStatus{}, fmt.Errorf("%s unreachable", addr)
	}
	return *n, nil
}

func (c *fakeCluster) Bootstrap(_ context.Context, addr string, voters []peerVoter) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	target := c.nodes[addr]
	if target.HasState {
		return errHasState
	}
	c.bootstraps[addr]++
	for _, n := range c.nodes {
		for _, v := range voters {
			if n.ServerID == v.ID {
				n.HasState, n.CaughtUp, n.LeaderID, n.Term = true, true, target.ServerID, 2
			}
		}
	}
	target.State, target.CommitIndex = "Leader", 2
	if err := c.failNext; err != nil {
		c.failNext = nil
		return err
	}
	return nil
}

func (c *fakeCluster) wipe(m manifest) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for i, p := range m.Peers {
		addr, _ := mgmtAddr(p.Address, "8081")
		n := c.nodes[addr]
		*n = nodeStatus{PodName: n.PodName, GroupID: n.GroupID, Shard: n.Shard, Address: n.Address, RingVersion: n.RingVersion,
			ServerID: fmt.Sprintf("%s/new-%d", p.PodName, i), State: "Follower"}
	}
}

func (c *fakeCluster) totalBootstraps() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	n := 0
	for _, v := range c.bootstraps {
		n += v
	}
	return n
}

func shardManifest(shard int) manifest {
	m := manifest{Name: fmt.Sprintf("storage-shard-%d-manifest", shard), GroupID: fmt.Sprintf("shard-%d", shard), Shard: shard, RingVersion: 1, Phase: phasePending}
	for i := range 3 {
		pod := fmt.Sprintf("storage-shard-%d-%d", shard, i)
		m.Peers = append(m.Peers, peer{PodName: pod, Address: fmt.Sprintf("%s.storage-shard-%d.reliefgrid.svc.cluster.local:7000", pod, shard)})
	}
	return m
}

func newTestBootstrapper(store manifestStore, peers peerClient, logs io.Writer) *bootstrapper {
	return &bootstrapper{
		store: store, peers: peers, mgmtPort: "8081", poll: time.Millisecond, call: time.Second,
		log: slog.New(slog.NewTextHandler(logs, nil)),
	}
}

func TestBootstrapFreshInstall(t *testing.T) {
	ms := []manifest{shardManifest(0), shardManifest(1), shardManifest(2)}
	store, cluster := newFakeStore(ms...), newFakeCluster(ms...)
	var logs bytes.Buffer
	if err := newTestBootstrapper(store, cluster, &logs).run(t.Context(), names(ms)); err != nil {
		t.Fatalf("run: %v\n%s", err, logs.String())
	}
	if got := cluster.totalBootstraps(); got != 3 {
		t.Fatalf("%d bootstraps, want one per shard", got)
	}
	for _, m := range ms {
		got, _ := store.Get(t.Context(), m.Name)
		if got.Phase != phaseBootstrapped || len(got.PeerIDs) != 3 || got.PeerIDs[1] != m.Peers[1].PodName+"/inc-1" {
			t.Fatalf("%s: %+v", m.Name, got)
		}
	}

	// A re-run (every helm upgrade) only verifies.
	logs.Reset()
	if err := newTestBootstrapper(store, cluster, &logs).run(t.Context(), names(ms)); err != nil {
		t.Fatalf("re-run: %v", err)
	}
	if got := cluster.totalBootstraps(); got != 3 {
		t.Fatalf("re-run bootstrapped again: %d", got)
	}
	if strings.Count(logs.String(), "verified") != 3 {
		t.Fatalf("re-run logs:\n%s", logs.String())
	}
}

func TestBootstrapKilledDuringHoldResumes(t *testing.T) {
	ms := []manifest{shardManifest(0), shardManifest(1)}
	store, cluster := newFakeStore(ms...), newFakeCluster(ms...)
	var logs syncBuffer
	b := newTestBootstrapper(store, cluster, &logs)
	b.hold = time.Hour
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- b.run(ctx, names(ms)) }()
	for !strings.Contains(logs.String(), "holding") {
		time.Sleep(time.Millisecond)
	}
	cancel() // kubectl delete job
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("killed run: %v", err)
	}
	if p0, p1 := store.phase(ms[0].Name), store.phase(ms[1].Name); p0 != phaseBootstrapping || p1 != phasePending {
		t.Fatalf("phases at the kill: %s %s", p0, p1)
	}

	var logs2 bytes.Buffer
	if err := newTestBootstrapper(store, cluster, &logs2).run(t.Context(), names(ms)); err != nil {
		t.Fatalf("re-run: %v\n%s", err, logs2.String())
	}
	if got := cluster.totalBootstraps(); got != 2 {
		t.Fatalf("%d bootstraps after the re-run, want 2 (one per shard)", got)
	}
	if !strings.Contains(logs2.String(), "shard-0: group already initialized") {
		t.Fatalf("re-run logs:\n%s", logs2.String())
	}
	for _, m := range ms {
		if p := store.phase(m.Name); p != phaseBootstrapped {
			t.Fatalf("%s: %s", m.Name, p)
		}
	}
}

func TestBootstrapRefusesWipedGroup(t *testing.T) {
	m := shardManifest(0)
	store, cluster := newFakeStore(m), newFakeCluster(m)
	var logs bytes.Buffer
	if err := newTestBootstrapper(store, cluster, &logs).run(t.Context(), []string{m.Name}); err != nil {
		t.Fatal(err)
	}
	cluster.wipe(m) // every PVC deleted; the manifest survives
	err := newTestBootstrapper(store, cluster, &logs).run(t.Context(), []string{m.Name})
	if !errors.Is(err, errRefused) {
		t.Fatalf("run over wiped disks: %v, want refused", err)
	}
	if got := cluster.totalBootstraps(); got != 1 {
		t.Fatalf("%d bootstraps, want 1", got)
	}
}

func TestBootstrapUnknownOutcomeIsNotRetriedBlindly(t *testing.T) {
	m := shardManifest(0)
	store, cluster := newFakeStore(m), newFakeCluster(m)
	cluster.failNext = errors.New("context deadline exceeded") // took effect, response lost
	var logs bytes.Buffer
	if err := newTestBootstrapper(store, cluster, &logs).run(t.Context(), []string{m.Name}); err != nil {
		t.Fatalf("run: %v\n%s", err, logs.String())
	}
	if got := cluster.totalBootstraps(); got != 1 {
		t.Fatalf("%d bootstraps, want 1", got)
	}
	if store.phase(m.Name) != phaseBootstrapped {
		t.Fatal("not BOOTSTRAPPED")
	}
}

func names(ms []manifest) []string {
	var n []string
	for _, m := range ms {
		n = append(n, m.Name)
	}
	return n
}

// syncBuffer is a log sink read while another goroutine writes it.
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
