package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"
)

// The replacement under test: shard 1's storage-shard-1-2 lost its disk. Its
// pod is back under the same name and address with a new, empty incarnation,
// waiting unadmitted; storage-shard-1-0 leads, storage-shard-1-1 follows.
const (
	repPod = "storage-shard-1-2"
	oldID  = "storage-shard-1-2/inc-2"
	newInc = "new-2"
	newID  = "storage-shard-1-2/new-2"
)

func bootstrappedManifest() manifest {
	m := shardManifest(1)
	m.Phase = phaseBootstrapped
	for i, p := range m.Peers {
		m.PeerIDs = append(m.PeerIDs, fmt.Sprintf("%s/inc-%d", p.PodName, i))
	}
	return m
}

// fakeGroup is one Raft group as storagectl sees it through management
// endpoints: statuses with the committed configuration, and membership
// changes on the leader, refused from a stale prevIndex.
type fakeGroup struct {
	mu      sync.Mutex
	m       manifest
	store   *fakeStore
	nodes   map[string]*nodeStatus // by mgmt address
	down    map[string]bool        // unreachable
	leader  string                 // mgmt address
	config  []configServer
	index   uint64
	catchUp int // status reads the new member needs before it is caught up

	// lose makes the next change of that op apply and then fail, as a lost
	// response; flipAfter moves leadership to the other old voter right
	// after that op applies.
	lose      map[string]bool
	flipAfter string

	ops       []membershipChange // every applied change, in order
	maxVoters int
}

func newFakeGroup(t *testing.T, m manifest, store *fakeStore) *fakeGroup {
	g := &fakeGroup{m: m, store: store, nodes: map[string]*nodeStatus{}, down: map[string]bool{}, index: 10, catchUp: 3, lose: map[string]bool{}}
	for i, p := range m.Peers {
		addr, _ := mgmtAddr(p.Address, "8081")
		n := &nodeStatus{PodName: p.PodName, GroupID: m.GroupID, Shard: m.Shard, Address: p.Address, RingVersion: m.RingVersion,
			ServerID: m.PeerIDs[i], State: "Follower", HasState: true, CaughtUp: true, Term: 3, CommitIndex: 40}
		if i == 0 {
			n.State, g.leader = "Leader", addr
		}
		g.nodes[addr] = n
		g.config = append(g.config, configServer{ID: m.PeerIDs[i], Address: p.Address, Suffrage: suffrageVoter})
	}
	g.maxVoters = 3
	g.wipe(repPod)
	t.Cleanup(func() {
		if t.Failed() {
			t.Logf("ops: %+v\nconfig at %d: %+v", g.ops, g.index, g.config)
		}
	})
	return g
}

func (g *fakeGroup) addrOf(pod string) string {
	for _, p := range g.m.Peers {
		if p.PodName == pod {
			a, _ := mgmtAddr(p.Address, "8081")
			return a
		}
	}
	return ""
}

// wipe brings pod back on a new, empty disk: management only, unadmitted.
func (g *fakeGroup) wipe(pod string) {
	n := g.nodes[g.addrOf(pod)]
	*n = nodeStatus{PodName: n.PodName, GroupID: n.GroupID, Shard: n.Shard, Address: n.Address, RingVersion: n.RingVersion,
		ServerID: pod + "/" + newInc, State: "Unadmitted"}
}

func (g *fakeGroup) member(id string) (configServer, bool) {
	for _, s := range g.config {
		if s.ID == id {
			return s, true
		}
	}
	return configServer{}, false
}

func (g *fakeGroup) Status(_ context.Context, addr string) (nodeStatus, error) {
	g.mu.Lock()
	defer g.mu.Unlock()
	n, ok := g.nodes[addr]
	if !ok || g.down[addr] {
		return nodeStatus{}, fmt.Errorf("%s unreachable", addr)
	}
	leaderID := g.nodes[g.leader].ServerID
	if n.ServerID == newID {
		// The new disk opens Raft only once admitted, and holds the group's
		// state only once the leader replicates to it.
		cur, _ := g.store.Get(context.Background(), g.m.Name)
		_, inConfig := g.member(newID)
		if cur.Admitted[repPod] == newInc && inConfig {
			n.State, n.HasState, n.LeaderID, n.Term = "Follower", true, leaderID, g.nodes[g.leader].Term
			if g.catchUp > 0 {
				g.catchUp--
			} else {
				n.CaughtUp = true
			}
		}
	}
	out := *n
	if out.HasState {
		out.LeaderID = leaderID
		out.Configuration = &raftConfiguration{Index: g.index, Committed: true, Servers: slices.Clone(g.config)}
	}
	return out, nil
}

func (g *fakeGroup) Bootstrap(context.Context, string, []peerVoter) error {
	return errors.New("replace never bootstraps")
}

func (g *fakeGroup) Membership(_ context.Context, addr string, c membershipChange) (uint64, error) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.down[addr] {
		return 0, fmt.Errorf("%s unreachable", addr)
	}
	if addr != g.leader {
		return 0, fmt.Errorf("%w: %s", errNotLeader, g.nodes[addr].PodName)
	}
	if c.PrevIndex != g.index {
		return 0, fmt.Errorf("%w: since %d (latest is %d)", errConfigChanged, c.PrevIndex, g.index)
	}
	switch c.Op {
	case opRemove:
		i := slices.IndexFunc(g.config, func(s configServer) bool { return s.ID == c.ID })
		if i < 0 {
			return 0, fmt.Errorf("%s is not a member", c.ID)
		}
		g.config = slices.Delete(g.config, i, i+1)
	case opAddNonvoter:
		if _, ok := g.member(c.ID); ok {
			return 0, fmt.Errorf("%s is already a member", c.ID)
		}
		g.config = append(g.config, configServer{ID: c.ID, Address: c.Address, Suffrage: suffrageNonvoter})
	case opAddVoter:
		i := slices.IndexFunc(g.config, func(s configServer) bool { return s.ID == c.ID })
		if i < 0 {
			g.config = append(g.config, configServer{ID: c.ID, Address: c.Address, Suffrage: suffrageVoter})
		} else {
			g.config[i].Suffrage = suffrageVoter
		}
	default:
		return 0, fmt.Errorf("unknown op %q", c.Op)
	}
	g.index++
	g.ops = append(g.ops, c)
	v := 0
	for _, s := range g.config {
		if s.Suffrage == suffrageVoter {
			v++
		}
	}
	g.maxVoters = max(g.maxVoters, v)
	if g.flipAfter == c.Op {
		g.flipAfter = ""
		g.nodes[g.leader].State = "Follower"
		g.leader = g.addrOf("storage-shard-1-1")
		g.nodes[g.leader].State = "Leader"
		for _, n := range g.nodes {
			n.Term++
		}
	}
	if g.lose[c.Op] {
		delete(g.lose, c.Op)
		return 0, errors.New("context deadline exceeded (the change may have committed)")
	}
	return g.index, nil
}

// admissionCheckingStore fails the test if admitted[<pod>] is ever written
// while the old ID is still in the group's configuration (M7: only after
// RemoveServer(old) has committed).
type admissionCheckingStore struct {
	*fakeStore
	t *testing.T
	g *fakeGroup
}

func (s admissionCheckingStore) Update(ctx context.Context, m manifest) (string, error) {
	if m.Admitted[repPod] == newInc {
		s.g.mu.Lock()
		_, still := s.g.member(oldID)
		s.g.mu.Unlock()
		if still {
			s.t.Errorf("admitted %s=%s while %s is still in the configuration", repPod, newInc, oldID)
		}
	}
	return s.fakeStore.Update(ctx, m)
}

func newTestReplacer(store manifestStore, g *fakeGroup, logs io.Writer) *replacer {
	return &replacer{
		store: store, peers: g, members: g, mgmtPort: "8081", poll: time.Millisecond, call: time.Second,
		log: slog.New(slog.NewTextHandler(logs, nil)),
	}
}

type world struct {
	store *fakeStore
	g     *fakeGroup
	r     *replacer
	logs  *bytes.Buffer
}

func newWorld(t *testing.T) world {
	m := bootstrappedManifest()
	store := newFakeStore(m)
	g := newFakeGroup(t, m, store)
	var logs bytes.Buffer
	return world{store: store, g: g, r: newTestReplacer(admissionCheckingStore{store, t, g}, g, &logs), logs: &logs}
}

// done checks the end state of a completed replacement.
func (w world) done(t *testing.T) {
	t.Helper()
	m, err := w.store.Get(t.Context(), bootstrappedManifest().Name)
	if err != nil {
		t.Fatal(err)
	}
	wantIDs := []string{"storage-shard-1-0/inc-0", "storage-shard-1-1/inc-1", newID}
	if !slices.Equal(m.PeerIDs, wantIDs) || m.Replace != nil || m.Admitted[repPod] != newInc || !slices.Contains(m.Removed, oldID) || m.Phase != phaseBootstrapped {
		t.Fatalf("manifest after replace: peerIds %v, replace %+v, admitted %v, removed %v, phase %s\nlogs:\n%s",
			m.PeerIDs, m.Replace, m.Admitted, m.Removed, m.Phase, w.logs)
	}
	w.g.mu.Lock()
	defer w.g.mu.Unlock()
	var voters []string
	for _, s := range w.g.config {
		if s.Suffrage != suffrageVoter {
			t.Fatalf("%s is still a %s", s.ID, s.Suffrage)
		}
		voters = append(voters, s.ID)
	}
	slices.Sort(voters)
	if !slices.Equal(voters, wantIDs) {
		t.Fatalf("voters %v, want %v", voters, wantIDs)
	}
	if w.g.maxVoters > 3 {
		t.Fatalf("the group had %d voters at some point", w.g.maxVoters)
	}
	for _, op := range w.g.ops {
		if op.Op == opRemove && op.ID != oldID || op.Op != opRemove && op.ID != newID {
			t.Fatalf("unexpected change %+v", op)
		}
	}
}

func (w world) countOps(op string) int {
	w.g.mu.Lock()
	defer w.g.mu.Unlock()
	n := 0
	for _, c := range w.g.ops {
		if c.Op == op {
			n++
		}
	}
	return n
}

// The whole controlled replacement (v3.1 §11): remove the old voter, admit
// the new incarnation, add it as a nonvoter, wait for catch-up, promote it,
// verify three voters and record them as the manifest's peerIds (#21).
func TestReplaceCompletes(t *testing.T) {
	w := newWorld(t)
	if err := w.r.run(t.Context(), bootstrappedManifest(), repPod); err != nil {
		t.Fatalf("replace: %v\n%s", err, w.logs)
	}
	w.done(t)
	if w.countOps(opRemove) != 1 || w.countOps(opAddNonvoter) != 1 || w.countOps(opAddVoter) != 1 {
		t.Fatalf("ops %+v, want one remove, one add-nonvoter, one add-voter", w.g.ops)
	}
	for _, phase := range []string{replacePlanned, replaceRemoved, replaceAdmitted, replaceNonvoter, replaceVoter} {
		if !strings.Contains(w.logs.String(), "phase="+phase) {
			t.Fatalf("no phase %s logged:\n%s", phase, w.logs)
		}
	}
	// A re-run finds the work done and changes nothing.
	ops := len(w.g.ops)
	if err := w.r.run(t.Context(), bootstrappedManifest(), repPod); err != nil {
		t.Fatalf("re-run: %v", err)
	}
	if len(w.g.ops) != ops {
		t.Fatalf("re-run changed membership: %+v", w.g.ops[ops:])
	}
	w.done(t)
}

// A tool crash right after every step (its effect applied, the next record
// not yet written) is resumed by the next run to the same end state, with
// no second removal, no fourth voter and no change to another member.
func TestReplaceResumesAfterCrashAtEveryStep(t *testing.T) {
	if len(replaceSteps) < 8 {
		t.Fatalf("replaceSteps = %v: want every phase boundary", replaceSteps)
	}
	for _, step := range replaceSteps {
		t.Run(step, func(t *testing.T) {
			w := newWorld(t)
			w.r.crashAt = step
			err := w.r.run(t.Context(), bootstrappedManifest(), repPod)
			if !errors.Is(err, errCrash) {
				t.Fatalf("first run: %v, want the injected crash at %s\n%s", err, step, w.logs)
			}
			w.r.crashAt = ""
			if err := w.r.run(t.Context(), bootstrappedManifest(), repPod); err != nil {
				t.Fatalf("resumed run: %v\n%s", err, w.logs)
			}
			w.done(t)
			if w.countOps(opRemove) != 1 || w.countOps(opAddNonvoter) > 1 || w.countOps(opAddVoter) != 1 {
				t.Fatalf("ops after resume %+v", w.g.ops)
			}
		})
	}
}

// A lost response to a change that did commit is settled by re-reading the
// configuration, never by assuming it failed (§11 step 2).
func TestReplaceLostResponses(t *testing.T) {
	for _, op := range []string{opRemove, opAddNonvoter, opAddVoter} {
		t.Run(op, func(t *testing.T) {
			w := newWorld(t)
			w.g.lose[op] = true
			if err := w.r.run(t.Context(), bootstrappedManifest(), repPod); err != nil {
				t.Fatalf("replace: %v\n%s", err, w.logs)
			}
			w.done(t)
			if w.countOps(op) != 1 {
				t.Fatalf("%s applied %d times: %+v", op, w.countOps(op), w.g.ops)
			}
		})
	}
}

// A leader change between phases: the next change goes to the new leader.
func TestReplaceFollowsALeaderChange(t *testing.T) {
	for _, op := range []string{opRemove, opAddNonvoter} {
		t.Run("after "+op, func(t *testing.T) {
			w := newWorld(t)
			w.g.flipAfter = op
			if err := w.r.run(t.Context(), bootstrappedManifest(), repPod); err != nil {
				t.Fatalf("replace: %v\n%s", err, w.logs)
			}
			w.done(t)
		})
	}
}

// Every precondition is checked before the first change, and a refusal
// changes nothing.
func TestReplaceRefuses(t *testing.T) {
	for _, tc := range []struct {
		name  string
		setup func(w world)
		why   string
	}{
		{"the old voter still answers", func(w world) {
			n := w.g.nodes[w.g.addrOf(repPod)]
			n.ServerID, n.State, n.HasState, n.CaughtUp = oldID, "Follower", true, true
		}, "still running"},
		{"the replacement disk is not empty", func(w world) {
			w.g.nodes[w.g.addrOf(repPod)].HasState = true
		}, "not empty"},
		{"the replacement is unreachable", func(w world) { w.g.down[w.g.addrOf(repPod)] = true }, "unreachable"},
		{"only one other voter is healthy", func(w world) { w.g.down[w.g.addrOf("storage-shard-1-1")] = true }, "two healthy voters"},
		{"the other voter is behind", func(w world) { w.g.nodes[w.g.addrOf("storage-shard-1-1")].CaughtUp = false }, "two healthy voters"},
		{"another replacement is in progress", func(w world) {
			m, _ := w.store.Get(context.Background(), bootstrappedManifest().Name)
			m.Replace = &replaceRecord{PodName: "storage-shard-1-1", OldID: "storage-shard-1-1/inc-1", Phase: replacePlanned}
			if _, err := w.store.Update(context.Background(), m); err != nil {
				panic(err)
			}
		}, "in progress"},
		{"the group is not bootstrapped", func(w world) {
			m, _ := w.store.Get(context.Background(), bootstrappedManifest().Name)
			m.Phase = phaseBootstrapping
			if _, err := w.store.Update(context.Background(), m); err != nil {
				panic(err)
			}
		}, "BOOTSTRAPPED"},
		{"the pod is not a peer", func(w world) {}, "not a peer"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			w := newWorld(t)
			tc.setup(w)
			pod := repPod
			if tc.why == "not a peer" {
				pod = "storage-shard-1-9"
			}
			err := w.r.run(t.Context(), bootstrappedManifest(), pod)
			if !errors.Is(err, errRefused) || !strings.Contains(err.Error(), tc.why) {
				t.Fatalf("replace: %v, want a refusal naming %q\n%s", err, tc.why, w.logs)
			}
			if len(w.g.ops) != 0 {
				t.Fatalf("a refused replace changed membership: %+v", w.g.ops)
			}
			if m, _ := w.store.Get(context.Background(), bootstrappedManifest().Name); m.Admitted[repPod] == newInc {
				t.Fatal("a refused replace admitted the new disk")
			}
		})
	}
}

// If a voter is lost after the removal, the two-voter group cannot commit:
// replace stops and waits rather than adding a member it cannot commit, and
// finishes once the voter is back (§11 safety boundaries).
func TestReplaceWaitsWhenAVoterIsLostMidway(t *testing.T) {
	w := newWorld(t)
	w.r.crashAt = stepRecordedRemoved
	if err := w.r.run(t.Context(), bootstrappedManifest(), repPod); !errors.Is(err, errCrash) {
		t.Fatalf("first run: %v", err)
	}
	w.r.crashAt = ""
	w.g.down[w.g.addrOf("storage-shard-1-1")] = true
	ctx, cancel := context.WithTimeout(t.Context(), 100*time.Millisecond)
	defer cancel()
	if err := w.r.run(ctx, bootstrappedManifest(), repPod); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("with one voter lost: %v, want it to wait until the deadline", err)
	}
	if w.countOps(opAddNonvoter) != 0 {
		t.Fatalf("added a member with one of two voters down: %+v", w.g.ops)
	}
	delete(w.g.down, w.g.addrOf("storage-shard-1-1"))
	if err := w.r.run(t.Context(), bootstrappedManifest(), repPod); err != nil {
		t.Fatalf("after the voter returned: %v\n%s", err, w.logs)
	}
	w.done(t)
}
