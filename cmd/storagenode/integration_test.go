package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/hashicorp/go-hclog"
	"github.com/hashicorp/raft"

	"github.com/ParthMalhotra07/ReliefGrid/internal/internalapi"
	"github.com/ParthMalhotra07/ReliefGrid/internal/raftstore"
)

const waitFor = 10 * time.Second

var fastProfile = raftstore.Profile{
	Name: "test", SnapshotThreshold: 1 << 30, TrailingLogs: 64, SnapshotInterval: time.Hour,
	HeartbeatTimeout: 200 * time.Millisecond, ElectionTimeout: 200 * time.Millisecond, LeaderLeaseTimeout: 100 * time.Millisecond,
}

// replica is one in-process storage node: a raftstore.Node behind its data
// and management listeners.
type replica struct {
	pod        string
	node       *raftstore.Node
	data, mgmt *httptest.Server
}

func (r *replica) dataHost() string { return strings.TrimPrefix(r.data.URL, "http://") }

func (r *replica) stop(t *testing.T) {
	t.Helper()
	if r.node == nil {
		return
	}
	r.data.Close()
	r.mgmt.Close()
	if err := r.node.Close(); err != nil {
		t.Errorf("close %s: %v", r.pod, err)
	}
	r.node = nil
}

func startReplicas(t *testing.T) []*replica {
	t.Helper()
	rg := loadRing(t)
	hosts := map[raft.ServerAddress]string{} // Raft address -> data host, filled before any request
	var reps []*replica
	for i := range 3 {
		pod := fmt.Sprintf("storage-1-%d", i)
		root := t.TempDir()
		ctx, cancel := context.WithTimeout(context.Background(), waitFor)
		n, err := raftstore.Open(ctx, raftstore.Config{
			PodName: pod, GroupID: "shard-1", Shard: testShard,
			DataDir: filepath.Join(root, "pvc"), FSMDir: filepath.Join(root, "emptydir"),
			BindAddr: "127.0.0.1:0", AdvertiseAddr: "localhost:0", Profile: fastProfile,
			Logger: hclog.New(&hclog.LoggerOptions{Name: pod, Level: hclog.Warn}),
		})
		cancel()
		if err != nil {
			t.Fatalf("open %s: %v", pod, err)
		}
		s := &server{node: n, ring: rg, shard: testShard, podName: pod, timeout: 3 * time.Second,
			leaderHTTP: func(a raft.ServerAddress) string { return hosts[a] }, operatorToken: testToken}
		r := &replica{pod: pod, node: n, data: httptest.NewServer(s.dataRoutes()), mgmt: httptest.NewServer(s.mgmtRoutes())}
		hosts[n.Addr()] = r.dataHost()
		reps = append(reps, r)
	}
	t.Cleanup(func() {
		for _, r := range reps {
			r.stop(t)
		}
	})
	return reps
}

// httpDo calls a live server and returns the response as a recorder, so the
// same decode and expectError helpers apply.
func httpDo(t *testing.T, method, url string, header map[string]string, body any) *httptest.ResponseRecorder {
	t.Helper()
	var reader io.Reader
	if body != nil {
		raw, err := json.Marshal(body)
		if err != nil {
			t.Fatal(err)
		}
		reader = bytes.NewReader(raw)
	}
	ctx, cancel := context.WithTimeout(context.Background(), waitFor)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, method, url, reader)
	if err != nil {
		t.Fatal(err)
	}
	for k, v := range header {
		req.Header.Set(k, v)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("%s %s: %v", method, url, err)
	}
	defer func() { _ = resp.Body.Close() }()
	rec := httptest.NewRecorder()
	for k, v := range resp.Header {
		rec.Header()[k] = v
	}
	rec.WriteHeader(resp.StatusCode)
	if _, err := io.Copy(rec.Body, resp.Body); err != nil {
		t.Fatal(err)
	}
	return rec
}

func statusOf(t *testing.T, r *replica) statusResponse {
	t.Helper()
	rec := httpDo(t, http.MethodGet, r.mgmt.URL+"/mgmt/status", nil, nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("%s status: %d %s", r.pod, rec.Code, rec.Body.String())
	}
	return decodeJSON[statusResponse](t, rec)
}

// waitLeader polls /mgmt/status until one live replica is the leader.
func waitLeader(t *testing.T, reps []*replica) *replica {
	t.Helper()
	deadline := time.Now().Add(waitFor)
	for time.Now().Before(deadline) {
		for _, r := range reps {
			if r.node != nil && statusOf(t, r).State == raft.Leader.String() {
				return r
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("no leader within %s", waitFor)
	return nil
}

// Verify (S1.3): three storage nodes over HTTP. Bootstrap through the
// management endpoint, write through the leader, kill it, and read the
// write through the new leader; followers answer NOT_LEADER with a hint and
// serve stale reads only on request.
func TestStorageNodesWriteSurvivesLeaderKill(t *testing.T) {
	reps := startReplicas(t)
	var servers []bootstrapServer
	for _, r := range reps {
		servers = append(servers, bootstrapServer{ID: string(r.node.Identity().ServerID()), Address: string(r.node.Addr())})
	}
	if rec := httpDo(t, http.MethodPost, reps[0].mgmt.URL+"/mgmt/bootstrap", operatorAuth, bootstrapRequest{Servers: servers}); rec.Code != http.StatusOK {
		t.Fatalf("bootstrap: %d %s", rec.Code, rec.Body.String())
	}
	leader := waitLeader(t, reps)
	if rec := httpDo(t, http.MethodPost, leader.mgmt.URL+"/mgmt/bootstrap", operatorAuth, bootstrapRequest{Servers: servers}); rec.Code != http.StatusConflict {
		t.Fatalf("re-bootstrap: %d, want 409", rec.Code)
	}

	key, _ := keyOn(t, loadRing(t), testShard, "", "kv:demo")
	rec := httpDo(t, http.MethodPut, leader.data.URL+"/internal/data/"+key, map[string]string{"If-None-Match": "*"},
		map[string]any{"operationId": "op-1", "at": t0, "ringVersion": 1, "value": 1})
	if rec.Code != http.StatusOK || rec.Header().Get("ETag") != `"1"` {
		t.Fatalf("write through leader: %d %s (ETag %q)", rec.Code, rec.Body.String(), rec.Header().Get("ETag"))
	}

	var follower *replica
	for _, r := range reps {
		if r != leader {
			follower = r
			break
		}
	}
	rec = httpDo(t, http.MethodPut, follower.data.URL+"/internal/data/"+key, map[string]string{"If-Match": `"1"`},
		map[string]any{"operationId": "op-2", "at": t0, "ringVersion": 1, "value": 2})
	e := expectError(t, rec, http.StatusServiceUnavailable, internalapi.NOTLEADER)
	if e.LeaderHint == nil || e.LeaderHint.NodeId != leader.pod || e.LeaderHint.Address != leader.dataHost() {
		t.Fatalf("leaderHint = %+v, want %s at %s", e.LeaderHint, leader.pod, leader.dataHost())
	}

	leader.stop(t)
	newLeader := waitLeader(t, reps)
	rec = httpDo(t, http.MethodGet, newLeader.data.URL+"/internal/data/"+key, nil, nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("read through new leader: %d %s", rec.Code, rec.Body.String())
	}
	resp := decodeJSON[internalapi.RecordResponse](t, rec)
	if resp.Record.Version != 1 || string(resp.Record.Value) != "1" || resp.Read.Consistency != "strong" ||
		resp.Read.StalePossible || resp.Read.ServedBy != newLeader.pod || rec.Header().Get("ETag") != `"1"` {
		t.Fatalf("read = %+v", resp)
	}

	var other *replica
	for _, r := range reps {
		if r.node != nil && r != newLeader {
			other = r
		}
	}
	expectError(t, httpDo(t, http.MethodGet, other.data.URL+"/internal/data/"+key, nil, nil), http.StatusServiceUnavailable, internalapi.NOTLEADER)
	deadline := time.Now().Add(waitFor)
	for {
		rec = httpDo(t, http.MethodGet, other.data.URL+"/internal/data/"+key+"?consistency=stale", nil, nil)
		if rec.Code == http.StatusOK {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("stale read on follower: %d %s", rec.Code, rec.Body.String())
		}
		time.Sleep(20 * time.Millisecond)
	}
	if s := decodeJSON[internalapi.RecordResponse](t, rec); !s.Read.StalePossible || s.Read.Consistency != "stale" || s.Read.ServedBy != other.pod {
		t.Fatalf("stale read = %+v", s.Read)
	}

	deadline = time.Now().Add(waitFor)
	for httpDo(t, http.MethodGet, newLeader.mgmt.URL+"/readyz", nil, nil).Code != http.StatusOK {
		if time.Now().After(deadline) {
			t.Fatalf("new leader never ready: %+v", statusOf(t, newLeader))
		}
		time.Sleep(20 * time.Millisecond)
	}
}
