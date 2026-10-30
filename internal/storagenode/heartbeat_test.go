package storagenode

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/ParthMalhotra07/ReliefGrid/internal/internalapi"
	"github.com/ParthMalhotra07/ReliefGrid/internal/raftstore"
	"github.com/ParthMalhotra07/ReliefGrid/internal/store"
)

const testIncarnation = "a81d4e2c-6f3b-4a9e-8c57-1d0e9b3f7c24"

// fakeCoordinator records registrations and heartbeats.
type fakeCoordinator struct {
	mu         sync.Mutex
	order      []string // "register" or "heartbeat", in arrival order
	regs       []internalapi.NodeRegistration
	beats      []internalapi.Heartbeat
	failFirstN int // answer the first n registrations with 503
	forget     int // answer the next n heartbeats with 404 (coordinator restarted)
	// strict answers a heartbeat carrying configuration with 400, as a
	// coordinator from before #21 does (it decodes with DisallowUnknownFields).
	strict   bool
	rejected int
	srv      *httptest.Server
}

func newFakeCoordinator(t *testing.T) *fakeCoordinator {
	c := &fakeCoordinator{}
	mux := http.NewServeMux()
	mux.HandleFunc("POST /internal/nodes/register", func(w http.ResponseWriter, r *http.Request) {
		var reg internalapi.NodeRegistration
		body, _ := io.ReadAll(r.Body)
		if err := json.Unmarshal(body, &reg); err != nil {
			t.Errorf("register body %q: %v", body, err)
		}
		c.mu.Lock()
		c.order = append(c.order, "register")
		c.regs = append(c.regs, reg)
		fail := len(c.regs) <= c.failFirstN
		c.mu.Unlock()
		if fail {
			writeError(w, http.StatusServiceUnavailable, internalapi.NOLEADER, "starting")
			return
		}
		writeJSON(w, http.StatusOK, internalapi.RegisterResponse{Accepted: true, Admitted: true})
	})
	mux.HandleFunc("POST /internal/nodes/heartbeat", func(w http.ResponseWriter, r *http.Request) {
		var hb internalapi.Heartbeat
		body, _ := io.ReadAll(r.Body)
		if err := json.Unmarshal(body, &hb); err != nil {
			t.Errorf("heartbeat body %q: %v", body, err)
		}
		c.mu.Lock()
		if c.strict && hb.Configuration != nil {
			c.rejected++
			c.mu.Unlock()
			writeError(w, http.StatusBadRequest, internalapi.INVALID, `heartbeat: json: unknown field "configuration"`)
			return
		}
		c.order = append(c.order, "heartbeat")
		c.beats = append(c.beats, hb)
		forgot := c.forget > 0
		if forgot {
			c.forget--
		}
		c.mu.Unlock()
		if forgot {
			writeError(w, http.StatusNotFound, internalapi.NOTFOUND, "unknown node")
			return
		}
		// Hints naming another leader must change nothing in Raft.
		writeJSON(w, http.StatusOK, internalapi.HeartbeatResponse{RingVersion: 1, Leaders: []internalapi.ShardLeader{
			{Shard: testShard, LeaderId: "storage-1-2/" + testIncarnation, Address: "elsewhere:8080", Term: 99},
		}})
	})
	c.srv = httptest.NewServer(mux)
	t.Cleanup(c.srv.Close)
	return c
}

func (c *fakeCoordinator) snapshot() ([]string, []internalapi.NodeRegistration, []internalapi.Heartbeat) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]string(nil), c.order...), append([]internalapi.NodeRegistration(nil), c.regs...), append([]internalapi.Heartbeat(nil), c.beats...)
}

func heartbeatNode(t *testing.T) *fakeNode {
	n := newFakeNode(t)
	n.status = raftstore.Status{
		Identity: raftstore.Identity{PodName: testPod, Incarnation: testIncarnation, GroupID: "shard-1", Shard: testShard},
		ServerID: "storage-1-1/" + testIncarnation, State: "Follower", Term: 7,
		LeaderID: "storage-1-0/" + testIncarnation, LeaderAddr: "storage-1-0:7000", CommitIndex: 12, AppliedIndex: 11, HasState: true, CaughtUp: true,
	}
	return n
}

// runHeartbeats runs RunHeartbeats until stop is called and returns its error.
func runHeartbeats(t *testing.T, n *fakeNode, c *fakeCoordinator) (stop func() error) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	s := &Server{s: newTestServer(t, n)}
	errc := make(chan error, 1)
	go func() {
		errc <- s.RunHeartbeats(ctx, HeartbeatConfig{CoordinatorURL: c.srv.URL, Address: "storage-1-1.storage-1-headless:8080", Interval: 20 * time.Millisecond})
	}()
	return func() error {
		cancel()
		select {
		case err := <-errc:
			return err
		case <-time.After(5 * time.Second):
			t.Fatal("RunHeartbeats did not return after cancel")
			return nil
		}
	}
}

func eventually(t *testing.T, what string, cond func() bool) {
	t.Helper()
	for deadline := time.Now().Add(5 * time.Second); time.Now().Before(deadline); time.Sleep(10 * time.Millisecond) {
		if cond() {
			return
		}
	}
	t.Fatalf("timed out waiting for %s", what)
}

// A replica registers first, then heartbeats its Raft position and local
// record counts. The coordinator's leader hints change nothing in Raft.
func TestRunHeartbeatsRegistersThenReports(t *testing.T) {
	n := heartbeatNode(t)
	n.seed(t, store.CreateEntity{Meta: meta("c-r"), Key: "res:r1", Resource: &store.Resource{ID: "r1", Type: store.TypeWater, Status: store.ResourceAvailable, AvailableFrom: t0, AvailableUntil: t0 + 1}})
	n.seed(t, store.CreateEntity{Meta: meta("c-q"), Key: "req:q1", Request: &store.Request{ID: "q1", Type: store.TypeWater, Status: store.RequestPending}})
	n.seed(t, store.CompareAndSet{Meta: meta("c-k"), Key: "kv:k", Value: json.RawMessage(`1`)})
	n.seed(t, store.CompareAndSet{Meta: meta("c-d"), Key: "kv:d", Value: json.RawMessage(`1`)})
	n.seed(t, store.CompareAndSet{Meta: meta("d-d"), Key: "kv:d", ExpectedVersion: 1, Delete: true})
	c := newFakeCoordinator(t)
	stop := runHeartbeats(t, n, c)
	eventually(t, "two heartbeats", func() bool { _, _, b := c.snapshot(); return len(b) >= 2 })
	if err := stop(); err != nil {
		t.Fatalf("RunHeartbeats: %v", err)
	}

	order, regs, beats := c.snapshot()
	if order[0] != "register" || len(regs) != 1 {
		t.Fatalf("calls = %v, want one register first", order)
	}
	want := internalapi.NodeRegistration{NodeId: testPod, GroupId: "shard-1", ShardId: testShard, Incarnation: testIncarnation,
		Address: "storage-1-1.storage-1-headless:8080", ProtocolVersion: 1, SchemaVersion: 1}
	if regs[0] != want {
		t.Fatalf("registration = %+v\nwant          %+v", regs[0], want)
	}
	hb := beats[len(beats)-1]
	if hb.NodeId != testPod || hb.GroupId != "shard-1" || hb.ShardId != testShard || hb.Incarnation != testIncarnation ||
		hb.Health != internalapi.HealthOk || hb.Term != 7 || hb.CommitIndex != 12 || hb.AppliedIndex != 11 ||
		hb.LeaderId == nil || *hb.LeaderId != "storage-1-0/"+testIncarnation {
		t.Fatalf("heartbeat = %+v", hb)
	}
	if hb.RecordCounts == nil || *hb.RecordCounts != (internalapi.RecordCounts{Request: 1, Resource: 1, Kv: 1, Tombstones: 1}) {
		t.Fatalf("recordCounts = %+v", hb.RecordCounts)
	}
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.booted != nil || len(n.applied) != 0 {
		t.Fatalf("heartbeats changed Raft: booted %v, applied %v", n.booted, n.applied)
	}
	if len(n.modes) == 0 || n.modes[0] != raftstore.ReadStale {
		t.Fatalf("counts read modes = %v, want a local (stale) read: a heartbeat never goes through the log", n.modes)
	}
}

// Registration is retried until accepted; no heartbeat is sent before.
func TestRunHeartbeatsRetriesRegistration(t *testing.T) {
	n := heartbeatNode(t)
	c := newFakeCoordinator(t)
	c.failFirstN = 2
	stop := runHeartbeats(t, n, c)
	eventually(t, "a heartbeat", func() bool { _, _, b := c.snapshot(); return len(b) >= 1 })
	if err := stop(); err != nil {
		t.Fatalf("RunHeartbeats: %v", err)
	}
	order, regs, _ := c.snapshot()
	if len(regs) != 3 || order[0] != "register" || order[1] != "register" || order[2] != "register" || order[3] != "heartbeat" {
		t.Fatalf("calls = %v, want three registers then heartbeats", order)
	}
}

// A replica that knows no leader omits leaderId, and a replica whose FSM has
// not caught up reports degraded health; a failed counts read omits the
// counts rather than reporting zeros.
func TestHeartbeatWithoutLeaderOrCounts(t *testing.T) {
	n := heartbeatNode(t)
	n.status.LeaderID, n.status.LeaderAddr, n.status.CaughtUp = "", "", false
	n.readErr = io.ErrUnexpectedEOF
	c := newFakeCoordinator(t)
	stop := runHeartbeats(t, n, c)
	eventually(t, "a heartbeat", func() bool { _, _, b := c.snapshot(); return len(b) >= 1 })
	if err := stop(); err != nil {
		t.Fatalf("RunHeartbeats: %v", err)
	}
	_, _, beats := c.snapshot()
	if hb := beats[0]; hb.LeaderId != nil || hb.Health != internalapi.HealthDegraded || hb.RecordCounts != nil {
		t.Fatalf("heartbeat = %+v, want no leaderId, degraded, no recordCounts", hb)
	}
}

// A coordinator that no longer knows the replica (404: it restarted and its
// table is in memory) gets a new registration before the next heartbeat.
func TestHeartbeatNotFoundRegistersAgain(t *testing.T) {
	n := heartbeatNode(t)
	c := newFakeCoordinator(t)
	c.forget = 1
	stop := runHeartbeats(t, n, c)
	eventually(t, "a second registration and a heartbeat after it", func() bool {
		order, _, _ := c.snapshot()
		return len(order) >= 4
	})
	if err := stop(); err != nil {
		t.Fatalf("RunHeartbeats: %v", err)
	}
	order, _, _ := c.snapshot()
	if want := []string{"register", "heartbeat", "register", "heartbeat"}; !slices.Equal(order[:4], want) {
		t.Fatalf("calls = %v, want %v", order, want)
	}
}

// Heartbeats carry the committed configuration (#21), but a coordinator
// from before #21 rejects that unknown field with 400. The replica then
// heartbeats without it (that coordinator admits every node anyway), so
// upgrading the storage nodes before the coordinator is not an outage.
func TestHeartbeatFallsBackForACoordinatorWithoutConfiguration(t *testing.T) {
	n := heartbeatNode(t)
	n.status.Configuration = raftstore.Configuration{Index: 3, Committed: true,
		Servers: []raftstore.Server{{ID: "storage-1-1/" + testIncarnation, Address: "storage-1-1:7000", Suffrage: "Voter"}}}
	c := newFakeCoordinator(t)
	c.strict = true
	stop := runHeartbeats(t, n, c)
	eventually(t, "accepted heartbeats", func() bool { _, _, b := c.snapshot(); return len(b) >= 3 })
	if err := stop(); err != nil {
		t.Fatalf("RunHeartbeats: %v", err)
	}
	c.mu.Lock()
	rejected := c.rejected
	c.mu.Unlock()
	if rejected != 1 {
		t.Fatalf("%d heartbeats rejected, want only the first", rejected)
	}

	// A coordinator that knows the field gets it.
	c2 := newFakeCoordinator(t)
	stop = runHeartbeats(t, n, c2)
	eventually(t, "a heartbeat", func() bool { _, _, b := c2.snapshot(); return len(b) >= 1 })
	_ = stop()
	if _, _, beats := c2.snapshot(); beats[0].Configuration == nil || beats[0].Configuration.Index != 3 {
		t.Fatalf("heartbeat to a current coordinator: %+v", beats[0].Configuration)
	}
}

// The fallback must not latch: a coordinator upgraded in place starts
// accepting the configuration, and the replica offers it again on its
// periodic retry (ConfigRetry) without a restart, clearing the fallback.
func TestHeartbeatOffersConfigurationAgainAfterTheCoordinatorUpgrades(t *testing.T) {
	n := heartbeatNode(t)
	n.status.Configuration = raftstore.Configuration{Index: 3, Committed: true,
		Servers: []raftstore.Server{{ID: "storage-1-1/" + testIncarnation, Address: "storage-1-1:7000", Suffrage: "Voter"}}}
	c := newFakeCoordinator(t)
	c.strict = true
	ctx, cancel := context.WithCancel(context.Background())
	s := &Server{s: newTestServer(t, n)}
	errc := make(chan error, 1)
	go func() {
		errc <- s.RunHeartbeats(ctx, HeartbeatConfig{CoordinatorURL: c.srv.URL, Address: "storage-1-1.storage-1-headless:8080",
			Interval: 20 * time.Millisecond, ConfigRetry: 100 * time.Millisecond})
	}()
	defer func() {
		cancel()
		if err := <-errc; err != nil {
			t.Errorf("RunHeartbeats: %v", err)
		}
	}()
	eventually(t, "heartbeats without configuration", func() bool { _, _, b := c.snapshot(); return len(b) >= 2 })

	c.mu.Lock()
	c.strict = false // upgraded in place
	c.mu.Unlock()
	eventually(t, "a heartbeat with configuration again", func() bool {
		_, _, beats := c.snapshot()
		return len(beats) > 0 && beats[len(beats)-1].Configuration != nil
	})
	// Cleared: every heartbeat carries it from now on, not only the retries.
	c.mu.Lock()
	start := len(c.beats)
	c.mu.Unlock()
	eventually(t, "five more heartbeats", func() bool { _, _, b := c.snapshot(); return len(b) >= start+5 })
	_, _, beats := c.snapshot()
	for i, hb := range beats[start : start+5] {
		if hb.Configuration == nil {
			t.Fatalf("heartbeat %d after the upgrade has no configuration: the fallback latched", start+i)
		}
	}
}

// A re-registration (the coordinator restarted, possibly upgraded) offers
// the configuration again at once, not only after ConfigRetry.
func TestReRegistrationOffersConfigurationAgain(t *testing.T) {
	n := heartbeatNode(t)
	n.status.Configuration = raftstore.Configuration{Index: 3, Committed: true,
		Servers: []raftstore.Server{{ID: "storage-1-1/" + testIncarnation, Address: "storage-1-1:7000", Suffrage: "Voter"}}}
	c := newFakeCoordinator(t)
	c.strict = true
	ctx, cancel := context.WithCancel(context.Background())
	s := &Server{s: newTestServer(t, n)}
	errc := make(chan error, 1)
	go func() {
		errc <- s.RunHeartbeats(ctx, HeartbeatConfig{CoordinatorURL: c.srv.URL, Address: "storage-1-1.storage-1-headless:8080",
			Interval: 20 * time.Millisecond, ConfigRetry: time.Hour})
	}()
	defer func() {
		cancel()
		if err := <-errc; err != nil {
			t.Errorf("RunHeartbeats: %v", err)
		}
	}()
	eventually(t, "heartbeats without configuration", func() bool { _, _, b := c.snapshot(); return len(b) >= 2 })
	c.mu.Lock()
	c.strict, c.forget = false, 1 // a restarted, upgraded coordinator
	c.mu.Unlock()
	eventually(t, "re-registration, then a heartbeat with configuration", func() bool {
		order, regs, beats := c.snapshot()
		return len(regs) >= 2 && order[len(order)-1] == "heartbeat" && beats[len(beats)-1].Configuration != nil
	})
}
