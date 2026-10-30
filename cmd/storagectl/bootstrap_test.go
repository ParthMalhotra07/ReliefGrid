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
	mu        sync.Mutex
	data      map[string]map[string]string
	rv        map[string]int
	managedBy map[string]string // the app.kubernetes.io/managed-by label
	created   int
}

func newFakeStore(ms ...manifest) *fakeStore {
	s := &fakeStore{data: map[string]map[string]string{}, rv: map[string]int{}, managedBy: map[string]string{}}
	for _, m := range ms {
		s.plant(m, managedByStoragectl)
	}
	return s
}

// plant writes m as an existing ConfigMap with the given managed-by label
// ("" for none), as anything with create on configmaps could.
func (s *fakeStore) plant(m manifest, managedBy string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.data[m.Name], s.rv[m.Name], s.managedBy[m.Name] = m.data(), 1, managedBy
}

func (s *fakeStore) Get(_ context.Context, name string) (manifest, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	d, ok := s.data[name]
	if !ok {
		return manifest{}, fmt.Errorf("configmap %s not found", name)
	}
	m, err := parseManifest(name, strconv.Itoa(s.rv[name]), d)
	m.ManagedBy = s.managedBy[name]
	return m, err
}

func (s *fakeStore) Create(_ context.Context, m manifest) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.data[m.Name]; ok {
		return errExists
	}
	s.data[m.Name], s.rv[m.Name], s.managedBy[m.Name] = m.data(), 1, managedByStoragectl
	s.created++
	return nil
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
	calls      map[string]int // Status and Bootstrap calls, by address
	failNext   error          // returned (once) by the next Bootstrap, after it took effect
	reject     error          // returned by every Bootstrap, which takes no effect
}

func newFakeCluster(ms ...manifest) *fakeCluster {
	c := &fakeCluster{nodes: map[string]*nodeStatus{}, bootstraps: map[string]int{}, calls: map[string]int{}}
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
	c.calls[addr]++
	n, ok := c.nodes[addr]
	if !ok {
		return nodeStatus{}, fmt.Errorf("%s unreachable", addr)
	}
	return *n, nil
}

func (c *fakeCluster) Bootstrap(_ context.Context, addr string, voters []peerVoter) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.calls[addr]++
	if c.reject != nil {
		return c.reject
	}
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
	// The bootstrap configuration (index 1), committed with the leader's
	// no-op, as the leader reports it in /mgmt/status (#21).
	target.Configuration = &raftConfiguration{Index: 1, Committed: true}
	for _, v := range voters {
		target.Configuration.Servers = append(target.Configuration.Servers, configServer{ID: v.ID, Address: v.Address, Suffrage: suffrageVoter})
	}
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

// leaders is the distinct leader IDs the shard's nodes name: one group has
// exactly one.
func (c *fakeCluster) leaders(m manifest) map[string]bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	ids := map[string]bool{}
	for _, p := range m.Peers {
		addr, _ := mgmtAddr(p.Address, "8081")
		ids[c.nodes[addr].LeaderID] = true
	}
	return ids
}

// callsTo is the number of Status and Bootstrap calls to m's peers.
func (c *fakeCluster) callsTo(m manifest) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	n := 0
	for _, p := range m.Peers {
		addr, _ := mgmtAddr(p.Address, "8081")
		n += c.calls[addr]
	}
	return n
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
	store, cluster := newFakeStore(), newFakeCluster(ms...) // no manifests yet: the Job creates them
	var logs bytes.Buffer
	if err := newTestBootstrapper(store, cluster, &logs).run(t.Context(), ms); err != nil {
		t.Fatalf("run: %v\n%s", err, logs.String())
	}
	if got := cluster.totalBootstraps(); got != 3 {
		t.Fatalf("%d bootstraps, want one per shard", got)
	}
	if store.created != 3 {
		t.Fatalf("%d manifests created, want 3", store.created)
	}
	for _, m := range ms {
		got, _ := store.Get(t.Context(), m.Name)
		if got.Phase != phaseBootstrapped || len(got.PeerIDs) != 3 || got.PeerIDs[1] != m.Peers[1].PodName+"/inc-1" {
			t.Fatalf("%s: %+v", m.Name, got)
		}
	}

	// A re-run (every helm upgrade) only verifies.
	logs.Reset()
	if err := newTestBootstrapper(store, cluster, &logs).run(t.Context(), ms); err != nil {
		t.Fatalf("re-run: %v", err)
	}
	if got := cluster.totalBootstraps(); got != 3 {
		t.Fatalf("re-run bootstrapped again: %d", got)
	}
	if strings.Count(logs.String(), "verified") != 3 || store.created != 3 {
		t.Fatalf("re-run created %d manifests; logs:\n%s", store.created, logs.String())
	}
}

func TestBootstrapKilledDuringHoldResumes(t *testing.T) {
	ms := []manifest{shardManifest(0), shardManifest(1)}
	store, cluster := newFakeStore(), newFakeCluster(ms...)
	var logs syncBuffer
	b := newTestBootstrapper(store, cluster, &logs)
	b.hold = time.Hour
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- b.run(ctx, ms) }()
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
	if err := newTestBootstrapper(store, cluster, &logs2).run(t.Context(), ms); err != nil {
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
	if err := newTestBootstrapper(store, cluster, &logs).run(t.Context(), []manifest{m}); err != nil {
		t.Fatal(err)
	}
	cluster.wipe(m) // every PVC deleted; the manifest survives
	err := newTestBootstrapper(store, cluster, &logs).run(t.Context(), []manifest{m})
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
	if err := newTestBootstrapper(store, cluster, &logs).run(t.Context(), []manifest{m}); err != nil {
		t.Fatalf("run: %v\n%s", err, logs.String())
	}
	if got := cluster.totalBootstraps(); got != 1 {
		t.Fatalf("%d bootstraps, want 1", got)
	}
	if store.phase(m.Name) != phaseBootstrapped {
		t.Fatal("not BOOTSTRAPPED")
	}
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

// The spec the chart passes is only used to create a missing manifest: an
// existing one (here BOOTSTRAPPED, then every disk wiped) is never reset to
// PENDING, whatever the Job's spec says (helm rollback can't regress it).
func TestBootstrapExistingManifestWinsOverSpec(t *testing.T) {
	m := shardManifest(0)
	store, cluster := newFakeStore(), newFakeCluster(m)
	var logs bytes.Buffer
	if err := newTestBootstrapper(store, cluster, &logs).run(t.Context(), []manifest{m}); err != nil {
		t.Fatal(err)
	}
	cluster.wipe(m)
	err := newTestBootstrapper(store, cluster, &logs).run(t.Context(), []manifest{shardManifest(0)})
	if !errors.Is(err, errRefused) {
		t.Fatalf("run over wiped disks with a PENDING spec: %v, want refused", err)
	}
	if got := store.phase(m.Name); got != phaseBootstrapped {
		t.Fatalf("manifest phase %s, want BOOTSTRAPPED", got)
	}
	if got := cluster.totalBootstraps(); got != 1 {
		t.Fatalf("%d bootstraps, want 1", got)
	}
}

// A 400/401/403 from /mgmt/bootstrap is a definite answer: exit at once
// (code 1, not a refusal) instead of re-reading until the deadline.
func TestBootstrapRejectedRequestFailsAtOnce(t *testing.T) {
	m := shardManifest(0)
	store, cluster := newFakeStore(), newFakeCluster(m)
	cluster.reject = fmt.Errorf("%w: HTTP 401: operator bearer token required", errRejected)
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	var logs bytes.Buffer
	err := newTestBootstrapper(store, cluster, &logs).run(ctx, []manifest{m})
	if !errors.Is(err, errRejected) || errors.Is(err, errRefused) || ctx.Err() != nil {
		t.Fatalf("run: %v (ctx %v), want errRejected at once\n%s", err, ctx.Err(), logs.String())
	}
	if got := cluster.totalBootstraps(); got != 0 {
		t.Fatalf("%d bootstraps took effect", got)
	}
}

// Two Jobs (an upgrade while the previous revision's Job still runs) race on
// one store from nothing: the manifests' create and compare-and-swap leave
// exactly one group per shard.
func TestConcurrentBootstrappersFormOneGroup(t *testing.T) {
	for i := range 50 {
		ms := []manifest{shardManifest(0), shardManifest(1)}
		store, cluster := newFakeStore(), newFakeCluster(ms...)
		var wg sync.WaitGroup
		errs := make([]error, 2)
		start := make(chan struct{})
		for j := range errs {
			wg.Go(func() {
				<-start
				errs[j] = newTestBootstrapper(store, cluster, io.Discard).run(t.Context(), ms)
			})
		}
		close(start)
		wg.Wait()
		for j, err := range errs {
			if err != nil {
				t.Fatalf("round %d, bootstrapper %d: %v", i, j, err)
			}
		}
		if got := cluster.totalBootstraps(); got != len(ms) {
			t.Fatalf("round %d: %d bootstraps took effect, want one per shard", i, got)
		}
		if store.created != len(ms) {
			t.Fatalf("round %d: %d manifests created, want %d", i, store.created, len(ms))
		}
		for _, m := range ms {
			if l := cluster.leaders(m); len(l) != 1 || l[""] {
				t.Fatalf("round %d, %s: leaders %v, want one group", i, m.GroupID, l)
			}
			if p := store.phase(m.Name); p != phaseBootstrapped {
				t.Fatalf("round %d, %s: %s", i, m.Name, p)
			}
		}
	}
}

// A manifest planted before the Job runs (or left by an older chart) is not
// the record of this chart's group: the Job refuses before it talks to any
// peer, so a planted peer list never receives the operator token.
func TestBootstrapRefusesPlantedManifest(t *testing.T) {
	planted := func(edit func(*manifest)) manifest {
		m := shardManifest(1)
		edit(&m)
		return m
	}
	for _, tc := range []struct {
		name      string
		m         manifest
		managedBy string
		want      string
	}{
		{"wrong peers", planted(func(m *manifest) {
			for i := range m.Peers {
				m.Peers[i].Address = fmt.Sprintf("evil-%d.attacker.reliefgrid.svc.cluster.local:7000", i)
			}
		}), managedByStoragectl, "peers"},
		{"wrong pod names", planted(func(m *manifest) { m.Peers[2].PodName = "storage-shard-9-9" }), managedByStoragectl, "peers"},
		{"wrong groupId", planted(func(m *manifest) { m.GroupID = "shard-9" }), managedByStoragectl, "groupId"},
		{"wrong shard", planted(func(m *manifest) { m.Shard = 9 }), managedByStoragectl, "shard"},
		{"wrong ringVersion", planted(func(m *manifest) { m.RingVersion = 7 }), managedByStoragectl, "ringVersion"},
		{"missing label", shardManifest(1), "", "managed-by"},
		{"Helm-owned (an older chart)", shardManifest(1), "Helm", "managed-by"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			specs := []manifest{shardManifest(0), shardManifest(1)}
			store := newFakeStore()
			store.plant(tc.m, tc.managedBy)
			cluster := newFakeCluster(specs[0], specs[1], tc.m) // the planted peers answer, consistently
			var logs bytes.Buffer
			err := newTestBootstrapper(store, cluster, &logs).run(t.Context(), specs)
			if !errors.Is(err, errRefused) || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("run: %v, want refused naming %q\n%s", err, tc.want, logs.String())
			}
			if n := cluster.callsTo(tc.m); n != 0 {
				t.Fatalf("%d calls to the planted peers, want 0", n)
			}
			if n := cluster.callsTo(specs[0]) + cluster.callsTo(specs[1]); n != 0 {
				t.Fatalf("%d calls to the chart's peers before every manifest was checked, want 0", n)
			}
			if got, _ := store.Get(t.Context(), tc.m.Name); got.Phase != phasePending || got.ResourceVersion != "1" {
				t.Fatalf("planted manifest changed: %+v", got)
			}
		})
	}
}

// hookPeers runs hook once, at the first status call: after the bootstrapper
// read the manifest and before it reads any peer, the window in which a
// second Job can move the manifest on.
type hookPeers struct {
	peerClient
	once sync.Once
	hook func()
}

func (h *hookPeers) Status(ctx context.Context, addr string) (nodeStatus, error) {
	h.once.Do(h.hook)
	return h.peerClient.Status(ctx, addr)
}

// A second Job records BOOTSTRAPPING and bootstraps the group between this
// run's manifest read and its status reads. The PENDING it read is stale, so
// it must decide again and verify the group, not refuse.
func TestStaleRefusalDecidesAgain(t *testing.T) {
	m := shardManifest(0)
	store, cluster := newFakeStore(m), newFakeCluster(m)
	peers := &hookPeers{peerClient: cluster, hook: func() {
		cur, err := store.Get(t.Context(), m.Name)
		if err != nil {
			t.Error(err)
			return
		}
		cur.Phase = phaseBootstrapping
		voters := make([]peerVoter, len(cur.Peers))
		for i, p := range cur.Peers {
			cur.PeerIDs = append(cur.PeerIDs, fmt.Sprintf("%s/inc-%d", p.PodName, i))
			voters[i] = peerVoter{ID: cur.PeerIDs[i], Address: p.Address}
		}
		if _, err := store.Update(t.Context(), cur); err != nil {
			t.Error(err)
		}
		addr, _ := mgmtAddr(cur.Peers[0].Address, "8081")
		if err := cluster.Bootstrap(t.Context(), addr, voters); err != nil {
			t.Error(err)
		}
	}}
	var logs bytes.Buffer
	if err := newTestBootstrapper(store, peers, &logs).run(t.Context(), []manifest{m}); err != nil {
		t.Fatalf("run: %v\n%s", err, logs.String())
	}
	if p := store.phase(m.Name); p != phaseBootstrapped {
		t.Fatalf("phase %s, want BOOTSTRAPPED", p)
	}
	if got := cluster.totalBootstraps(); got != 1 {
		t.Fatalf("%d bootstraps, want 1", got)
	}
	if !strings.Contains(logs.String(), "group already initialized") {
		t.Fatalf("logs:\n%s", logs.String())
	}
}

// Wiped disks under a BOOTSTRAPPED manifest, and an unrelated write (an M7
// admission) lands between the manifest read and the status reads. The run
// decides again from the new version and still refuses.
func TestMovedManifestStillRefusesWipedGroup(t *testing.T) {
	m := shardManifest(0)
	store, cluster := newFakeStore(m), newFakeCluster(m)
	if err := newTestBootstrapper(store, cluster, io.Discard).run(t.Context(), []manifest{m}); err != nil {
		t.Fatal(err)
	}
	cluster.wipe(m)
	before := cluster.callsTo(m)
	peers := &hookPeers{peerClient: cluster, hook: func() {
		cur, err := store.Get(t.Context(), m.Name)
		if err != nil {
			t.Error(err)
			return
		}
		if _, err := store.Update(t.Context(), cur); err != nil { // same phase, new resourceVersion
			t.Error(err)
		}
	}}
	err := newTestBootstrapper(store, peers, io.Discard).run(t.Context(), []manifest{m})
	if !errors.Is(err, errRefused) || !strings.Contains(err.Error(), "no reachable peer has Raft state") {
		t.Fatalf("run: %v, want refused over wiped disks", err)
	}
	if n := cluster.callsTo(m) - before; n != 2*len(m.Peers) {
		t.Fatalf("%d status calls, want two rounds (%d): the moved manifest must be decided again", n, 2*len(m.Peers))
	}
	if got := cluster.totalBootstraps(); got != 1 {
		t.Fatalf("%d bootstraps, want 1", got)
	}
	if p := store.phase(m.Name); p != phaseBootstrapped {
		t.Fatalf("phase %s, want BOOTSTRAPPED", p)
	}
}
