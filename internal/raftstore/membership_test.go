package raftstore

import (
	"context"
	"errors"
	"fmt"
	"net"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/hashicorp/raft"

	"github.com/ParthMalhotra07/ReliefGrid/internal/store"
)

func listening(addr string) bool {
	c, err := (&net.Dialer{Timeout: 200 * time.Millisecond}).DialContext(context.Background(), "tcp", addr)
	if err != nil {
		return false
	}
	_ = c.Close()
	return true
}

func voters(c Configuration) []raft.ServerID {
	var ids []raft.ServerID
	for _, s := range c.Servers {
		if s.Suffrage == raft.Voter.String() {
			ids = append(ids, s.ID)
		}
	}
	slices.Sort(ids)
	return ids
}

func ids(servers []raft.Server) []raft.ServerID {
	var out []raft.ServerID
	for _, s := range servers {
		out = append(out, s.ID)
	}
	slices.Sort(out)
	return out
}

// M7: Open runs Config.Admit after the disk's identity exists and before the
// Raft transport opens. While Admit waits, nothing listens on the Raft port;
// an Admit error fails Open with nothing opened, and the identity stays, so
// the same incarnation waits again on the next start.
func TestOpenWaitsForAdmissionBeforeTransport(t *testing.T) {
	root := t.TempDir()
	port, listen := reservePort(t)
	cfg := Config{
		PodName: "storage-1-0", GroupID: "shard-1", Shard: 1,
		DataDir: root + "/pvc", FSMDir: root + "/emptydir",
		BindAddr: "127.0.0.1:" + port, AdvertiseAddr: "localhost:" + port, Profile: testProfile,
		listen: listen,
	}
	type admitCall struct {
		id       Identity
		hasState bool
	}
	calls := make(chan admitCall, 1)
	release := make(chan error)
	cfg.Admit = func(ctx context.Context, id Identity, hasState bool) error {
		calls <- admitCall{id, hasState}
		select {
		case err := <-release:
			return err
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	// Refused: Open fails and leaves nothing listening.
	errNo := errors.New("not admitted")
	opened := make(chan error, 1)
	go func() {
		n, err := Open(context.Background(), cfg)
		if n != nil {
			_ = n.Close()
		}
		opened <- err
	}()
	first := <-calls
	if first.hasState || first.id.Incarnation == "" || first.id.PodName != "storage-1-0" {
		t.Fatalf("Admit got %+v, want a new identity with no Raft state", first)
	}
	if listening(cfg.BindAddr) {
		t.Fatal("the Raft port listens while admission is pending")
	}
	release <- errNo
	if err := <-opened; !errors.Is(err, errNo) {
		t.Fatalf("Open after a refused admission: %v, want %v", err, errNo)
	}
	if listening(cfg.BindAddr) {
		t.Fatal("the Raft port listens after a refused admission")
	}

	// Admitted: the same incarnation, and the transport opens.
	go func() {
		n, err := Open(context.Background(), cfg)
		if err == nil {
			t.Cleanup(func() { _ = n.Close() })
		}
		opened <- err
	}()
	second := <-calls
	if second.id.Incarnation != first.id.Incarnation {
		t.Fatalf("incarnation changed across a refused start: %s -> %s", first.id.Incarnation, second.id.Incarnation)
	}
	release <- nil
	if err := <-opened; err != nil {
		t.Fatalf("Open after admission: %v", err)
	}
	if !listening(cfg.BindAddr) {
		t.Fatal("the Raft port is closed after admission")
	}
}

// A restart of a disk with Raft state calls Admit with hasState, so the
// policy can let ordinary restarts through.
func TestAdmitSeesRaftStateOnRestart(t *testing.T) {
	c := newBootstrappedCluster(t, testProfile)
	li, _ := c.waitLeader()
	fi, _ := c.follower(li)
	var got []bool
	c.cfgs[fi].Admit = func(_ context.Context, _ Identity, hasState bool) error {
		got = append(got, hasState)
		return nil
	}
	c.restart(fi)
	if !slices.Equal(got, []bool{true}) {
		t.Fatalf("Admit hasState on restart = %v, want [true]", got)
	}
}

// #21: every replica reports the group's configuration with its index, and
// whether it is committed (at most one change is ever uncommitted, so the
// latest is committed once its index is within the commit index).
func TestConfigurationReportedCommitted(t *testing.T) {
	c := newBootstrappedCluster(t, testProfile)
	_, l := c.waitLeader()
	mustApply(t, l, cas("op-1", "kv:a", 0, 1))
	want := ids(c.servers())
	deadline := time.Now().Add(waitFor)
	for {
		ok := true
		for _, n := range c.nodes {
			cfg := n.Status().Configuration
			ok = ok && cfg.Committed && cfg.Index > 0 && slices.Equal(voters(cfg), want)
		}
		if ok {
			break
		}
		if time.Now().After(deadline) {
			for i, n := range c.nodes {
				t.Logf("node %d: %+v", i, n.Status().Configuration)
			}
			t.Fatalf("not every replica reports the committed three-voter configuration %v", want)
		}
		time.Sleep(10 * time.Millisecond)
	}
	got, err := l.Configuration(testCtx(t))
	if err != nil || !got.Committed || !slices.Equal(voters(got), want) {
		t.Fatalf("leader Configuration = %+v, %v", got, err)
	}
}

// Membership changes go through the leader only, and only from the
// configuration the caller inspected (prevIndex): a stale or zero index is
// refused with ErrConfigurationChanged and changes nothing.
func TestMembershipChangeGuards(t *testing.T) {
	c := newBootstrappedCluster(t, testProfile)
	li, l := c.waitLeader()
	_, f := c.follower(li)
	ctx := testCtx(t)
	cfg, err := l.Configuration(ctx)
	if err != nil {
		t.Fatal(err)
	}
	victim := f.Identity().ServerID()
	if _, err := f.RemoveServer(ctx, victim, cfg.Index); !errors.Is(err, ErrNotLeader) {
		t.Fatalf("RemoveServer on a follower: %v, want ErrNotLeader", err)
	}
	// raft reads prevIndex 0 as "no check"; raftstore never does.
	for _, prev := range []uint64{0, cfg.Index + 1} {
		if _, err := l.RemoveServer(ctx, victim, prev); !errors.Is(err, ErrConfigurationChanged) {
			t.Fatalf("RemoveServer with prevIndex %d (latest %d): %v, want ErrConfigurationChanged", prev, cfg.Index, err)
		}
	}
	after, err := l.Configuration(ctx)
	if err != nil || after.Index != cfg.Index || len(voters(after)) != 3 {
		t.Fatalf("configuration after refused changes: %+v, %v", after, err)
	}
}

// The replacement sequence on real Raft (v3.1 §11): with the old voter
// stopped, remove it through the leader; then add the new incarnation as a
// nonvoter, let it catch up, promote it. Each change names the index it
// was decided from, and the group commits a write at every step.
func TestReplaceSequenceOnRealRaft(t *testing.T) {
	c := newTestCluster(t, 4, testProfile)
	ctx := testCtx(t)
	three := c.servers()[:3]
	if err := c.nodes[0].Bootstrap(ctx, three); err != nil {
		t.Fatal(err)
	}
	c.kill(3) // the replacement waits outside the group
	li, l := c.waitLeader()
	old := 0
	if old == li {
		old = 1
	}
	for i := range 5 {
		mustApply(t, l, cas(fmt.Sprintf("op-a-%d", i), "kv:a", uint64(i), i+1))
	}
	oldID := c.nodes[old].Identity().ServerID()
	c.kill(old)

	cfg, err := l.Configuration(ctx)
	if err != nil {
		t.Fatal(err)
	}
	removedAt, err := l.RemoveServer(ctx, oldID, cfg.Index)
	if err != nil {
		t.Fatalf("RemoveServer: %v", err)
	}
	cfg, err = l.Configuration(ctx)
	if err != nil || !cfg.Committed || cfg.Index != removedAt || len(voters(cfg)) != 2 || slices.Contains(voters(cfg), oldID) {
		t.Fatalf("after removal: %+v, %v", cfg, err)
	}
	mustApply(t, l, cas("op-b", "kv:a", 5, 6)) // two voters still commit

	c.open(3)
	newID, newAddr := c.nodes[3].Identity().ServerID(), c.nodes[3].Addr()
	addedAt, err := l.AddNonvoter(ctx, newID, newAddr, cfg.Index)
	if err != nil {
		t.Fatalf("AddNonvoter: %v", err)
	}
	deadline := time.Now().Add(waitFor)
	for s := c.nodes[3].Status(); !s.CaughtUp || s.AppliedIndex < l.Status().AppliedIndex; s = c.nodes[3].Status() {
		if time.Now().After(deadline) {
			t.Fatalf("the nonvoter never caught up: %+v", s)
		}
		time.Sleep(10 * time.Millisecond)
	}
	if _, err := l.AddVoter(ctx, newID, newAddr, addedAt); err != nil {
		t.Fatalf("AddVoter: %v", err)
	}
	cfg, err = l.Configuration(ctx)
	want := []raft.ServerID{newID}
	for _, s := range three {
		if s.ID != oldID {
			want = append(want, s.ID)
		}
	}
	slices.Sort(want)
	if err != nil || !cfg.Committed || !slices.Equal(voters(cfg), want) {
		t.Fatalf("after promotion: %+v, %v, want voters %v", cfg, err, want)
	}
	mustApply(t, l, cas("op-c", "kv:a", 6, 7))
}

// The configuration's index is cached while the entry it came from is
// compacted into a snapshot: once the log no longer holds it, the index is
// the newest snapshot's ConfigurationIndex, and Status() must not rescan the
// whole retained log (load profile: 10240 trailing entries) on every call.
func TestConfigIndexCachedOnACompactedLog(t *testing.T) {
	if testing.Short() {
		t.Skip("writes past the load profile's TrailingLogs")
	}
	p := testProfile
	p.TrailingLogs = ProfileLoad.TrailingLogs
	c := newTestCluster(t, 1, p)
	ctx := testCtx(t)
	n := c.nodes[0]
	if err := n.Bootstrap(ctx, c.servers()); err != nil {
		t.Fatal(err)
	}
	c.waitLeader()
	writes := int(p.TrailingLogs) + 500
	wctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute) // ~10k writes, slower under -race
	defer cancel()
	var wg sync.WaitGroup
	errs := make(chan error, 32)
	for w := range 32 {
		wg.Go(func() {
			for i := w; i < writes; i += 32 {
				cmd := store.CreateEntity{Meta: store.Meta{OperationID: fmt.Sprintf("op-%d", i), At: t0}, Key: fmt.Sprintf("kv:c-%d", i), Value: []byte("1")}
				if _, err := n.Apply(wctx, cmd); err != nil {
					errs <- err
					return
				}
			}
		})
	}
	wg.Wait()
	close(errs)
	if err := <-errs; err != nil {
		t.Fatal(err)
	}
	if _, err := n.Snapshot(wctx); err != nil {
		t.Fatal(err)
	}
	first, _ := n.logs.FirstIndex()
	metas, err := n.snaps.List()
	if err != nil || len(metas) == 0 || first <= metas[0].ConfigurationIndex {
		t.Fatalf("the configuration entry %v is not compacted (first log index %d): %v", metas, first, err)
	}
	want := metas[0].ConfigurationIndex

	if got := n.Status().Configuration; got.Index != want || !got.Committed {
		t.Fatalf("Status configuration %+v, want index %d (the snapshot's), committed", got, want)
	}
	before := n.cfgScan.reads.Load()
	start := time.Now()
	for range 50 {
		if got := n.Status().Configuration; got.Index != want {
			t.Fatalf("Status configuration index %d, want %d", got.Index, want)
		}
	}
	elapsed := time.Since(start)
	if reads := n.cfgScan.reads.Load() - before; reads > 50*2 {
		t.Fatalf("50 Status calls read %d log entries looking for the configuration: the cache is not used", reads)
	}
	if elapsed > 2*time.Second {
		t.Fatalf("50 Status calls took %s", elapsed)
	}
}
