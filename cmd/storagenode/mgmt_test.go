package main

import (
	"errors"
	"fmt"
	"net/http"
	"reflect"
	"testing"
	"time"

	"github.com/hashicorp/raft"

	"github.com/ParthMalhotra07/ReliefGrid/internal/internalapi"
	"github.com/ParthMalhotra07/ReliefGrid/internal/raftstore"
)

func TestMgmtStatus(t *testing.T) {
	n := newFakeNode(t)
	n.status = raftstore.Status{
		Identity: raftstore.Identity{PodName: testPod, Incarnation: "inc", GroupID: "shard-1", Shard: testShard},
		ServerID: "storage-1-1/inc", Address: "storage-1-1:7000", State: "Follower", Term: 3,
		LeaderID: "storage-1-0/x", LeaderAddr: "storage-1-0:7000", CommitIndex: 12, LastLogIndex: 12,
		RaftAppliedIndex: 12, AppliedIndex: 10, HasState: true,
	}
	rec := do(t, newTestServer(t, n).mgmtRoutes(), call{method: http.MethodGet, path: "/mgmt/status"})
	if rec.Code != http.StatusOK {
		t.Fatalf("status %d: %s", rec.Code, rec.Body.String())
	}
	got := decodeJSON[statusResponse](t, rec)
	want := statusResponse{PodName: testPod, Incarnation: "inc", GroupID: "shard-1", Shard: testShard, ServerID: "storage-1-1/inc",
		Address: "storage-1-1:7000", State: "Follower", Term: 3, LeaderID: "storage-1-0/x", LeaderAddr: "storage-1-0:7000",
		CommitIndex: 12, LastLogIndex: 12, RaftAppliedIndex: 12, AppliedIndex: 10, HasState: true, RingVersion: 1}
	if got != want {
		t.Fatalf("status = %+v\nwant     %+v", got, want)
	}
}

func TestMgmtBootstrap(t *testing.T) {
	body := bootstrapRequest{Servers: []bootstrapServer{
		{ID: "storage-1-0/a", Address: "storage-1-0:7000"},
		{ID: "storage-1-1/b", Address: "storage-1-1:7000"},
		{ID: "storage-1-2/c", Address: "storage-1-2:7000"},
	}}
	t.Run("ok", func(t *testing.T) {
		n := newFakeNode(t)
		rec := do(t, newTestServer(t, n).mgmtRoutes(), call{method: http.MethodPost, path: "/mgmt/bootstrap", header: operatorAuth, body: body})
		if rec.Code != http.StatusOK {
			t.Fatalf("status %d: %s", rec.Code, rec.Body.String())
		}
		want := []raft.Server{
			{Suffrage: raft.Voter, ID: "storage-1-0/a", Address: "storage-1-0:7000"},
			{Suffrage: raft.Voter, ID: "storage-1-1/b", Address: "storage-1-1:7000"},
			{Suffrage: raft.Voter, ID: "storage-1-2/c", Address: "storage-1-2:7000"},
		}
		if !reflect.DeepEqual(n.booted, want) {
			t.Fatalf("bootstrapped %+v, want %+v", n.booted, want)
		}
	})
	t.Run("existing state is 409", func(t *testing.T) {
		n := newFakeNode(t)
		n.bootErr = raftstore.ErrHasState
		rec := do(t, newTestServer(t, n).mgmtRoutes(), call{method: http.MethodPost, path: "/mgmt/bootstrap", header: operatorAuth, body: body})
		if rec.Code != http.StatusConflict {
			t.Fatalf("status %d, want 409", rec.Code)
		}
	})
	t.Run("configuration without this node is 400", func(t *testing.T) {
		n := newFakeNode(t)
		n.bootErr = fmt.Errorf("%w: it does not include this node", raftstore.ErrBadBootstrap)
		rec := do(t, newTestServer(t, n).mgmtRoutes(), call{method: http.MethodPost, path: "/mgmt/bootstrap", header: operatorAuth, body: body})
		if rec.Code != http.StatusBadRequest {
			t.Fatalf("status %d (%s), want 400", rec.Code, rec.Body.String())
		}
	})
	t.Run("other failure is 500", func(t *testing.T) {
		n := newFakeNode(t)
		n.bootErr = errors.New("configuration does not include this node")
		rec := do(t, newTestServer(t, n).mgmtRoutes(), call{method: http.MethodPost, path: "/mgmt/bootstrap", header: operatorAuth, body: body})
		if rec.Code != http.StatusInternalServerError {
			t.Fatalf("status %d, want 500", rec.Code)
		}
	})
	for name, bad := range map[string]any{
		"empty":         bootstrapRequest{},
		"malformed":     "{",
		"missing id":    bootstrapRequest{Servers: []bootstrapServer{{Address: "a:1"}}},
		"missing addr":  bootstrapRequest{Servers: []bootstrapServer{{ID: "x/y"}}},
		"unknown field": map[string]any{"servers": body.Servers, "force": true},
	} {
		t.Run(name, func(t *testing.T) {
			n := newFakeNode(t)
			rec := do(t, newTestServer(t, n).mgmtRoutes(), call{method: http.MethodPost, path: "/mgmt/bootstrap", header: operatorAuth, body: bad})
			if rec.Code != http.StatusBadRequest || n.booted != nil {
				t.Fatalf("status %d, booted %v; want 400 and no bootstrap", rec.Code, n.booted)
			}
		})
	}
}

// v3.1 §11: bootstrap is restricted to the operator from day one. Status,
// health and readiness stay open.
func TestMgmtBootstrapRequiresOperatorToken(t *testing.T) {
	body := bootstrapRequest{Servers: []bootstrapServer{{ID: "storage-1-0/a", Address: "storage-1-0:7000"}}}
	cases := []struct {
		name       string
		configured string
		header     map[string]string
		status     int
	}{
		{"no token configured", "", operatorAuth, http.StatusForbidden},
		{"no Authorization header", testToken, nil, http.StatusUnauthorized},
		{"wrong token", testToken, map[string]string{"Authorization": "Bearer nope"}, http.StatusUnauthorized},
		{"token as a prefix", testToken, map[string]string{"Authorization": "Bearer " + testToken + "x"}, http.StatusUnauthorized},
		{"basic scheme", testToken, map[string]string{"Authorization": "Basic " + testToken}, http.StatusUnauthorized},
		{"bare token", testToken, map[string]string{"Authorization": testToken}, http.StatusUnauthorized},
		{"lower-case scheme", testToken, map[string]string{"Authorization": "bearer " + testToken}, http.StatusOK},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			n := newFakeNode(t)
			s := newTestServer(t, n)
			s.operatorToken = tc.configured
			rec := do(t, s.mgmtRoutes(), call{method: http.MethodPost, path: "/mgmt/bootstrap", header: tc.header, body: body})
			if rec.Code != tc.status {
				t.Fatalf("status %d (%s), want %d", rec.Code, rec.Body.String(), tc.status)
			}
			if tc.status != http.StatusOK && n.booted != nil {
				t.Fatal("bootstrap ran without a valid operator token")
			}
			if tc.status == http.StatusUnauthorized && rec.Header().Get("WWW-Authenticate") == "" {
				t.Fatal("401 without WWW-Authenticate")
			}
			if code, ok := map[int]internalapi.ErrorCode{http.StatusUnauthorized: internalapi.UNAUTHORIZED, http.StatusForbidden: internalapi.FORBIDDEN}[tc.status]; ok {
				if e := expectError(t, rec, tc.status, code); e.Retryable || e.OutcomeUnknown || e.Message == "" {
					t.Fatalf("error body = %+v, want %s, not retryable, with a message", e, code)
				}
			}
		})
	}
	// The open endpoints need no token, even with none configured.
	n := newFakeNode(t)
	s := newTestServer(t, n)
	s.operatorToken = ""
	n.status.CaughtUp = true
	for _, path := range []string{"/mgmt/status", "/healthz", "/readyz"} {
		if rec := do(t, s.mgmtRoutes(), call{method: http.MethodGet, path: path}); rec.Code != http.StatusOK {
			t.Fatalf("GET %s without a token = %d, want 200", path, rec.Code)
		}
	}
}

func TestHealthAndReadiness(t *testing.T) {
	n := newFakeNode(t)
	h := newTestServer(t, n).mgmtRoutes()
	if rec := do(t, h, call{method: http.MethodGet, path: "/healthz"}); rec.Code != http.StatusOK {
		t.Fatalf("healthz %d", rec.Code)
	}
	if rec := do(t, h, call{method: http.MethodGet, path: "/readyz"}); rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("readyz with the FSM behind = %d, want 503", rec.Code)
	}
	n.status.CaughtUp = true
	if rec := do(t, h, call{method: http.MethodGet, path: "/readyz"}); rec.Code != http.StatusOK {
		t.Fatalf("readyz caught up = %d, want 200", rec.Code)
	}
}

func TestLoadConfig(t *testing.T) {
	base := map[string]string{
		"POD_NAME": "storage-1-2", "SHARD": "1", "DATA_DIR": "/data", "FSM_DIR": "/fsm",
		"RAFT_ADVERTISE": "storage-1-2.storage-1.reliefgrid.svc.cluster.local:7000", "OPERATOR_TOKEN": "s3cret",
	}
	env := func(m map[string]string) func(string) string { return func(k string) string { return m[k] } }

	cfg, err := loadConfig(env(base))
	if err != nil {
		t.Fatalf("loadConfig: %v", err)
	}
	r := cfg.Raft
	if r.PodName != "storage-1-2" || r.Shard != 1 || r.GroupID != "shard-1" || r.DataDir != "/data" || r.FSMDir != "/fsm" ||
		r.BindAddr != ":7000" || r.AdvertiseAddr != base["RAFT_ADVERTISE"] || r.Profile != raftstore.ProfileDemo {
		t.Fatalf("raft config = %+v", r)
	}
	if cfg.HTTPAddr != ":8080" || cfg.MgmtAddr != ":8081" || cfg.HTTPPort != "8080" ||
		cfg.RingPath != "/etc/reliefgrid/ring.yaml" || cfg.RequestTimeout != 3*time.Second || cfg.OperatorToken != "s3cret" {
		t.Fatalf("config = %+v", cfg)
	}

	over := map[string]string{"PROFILE": "load", "HTTP_ADDR": ":9090", "GROUP_ID": "g1", "REQUEST_TIMEOUT_MS": "1500"}
	m := map[string]string{}
	for k, v := range base {
		m[k] = v
	}
	for k, v := range over {
		m[k] = v
	}
	cfg, err = loadConfig(env(m))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Raft.Profile != raftstore.ProfileLoad || cfg.HTTPPort != "9090" || cfg.Raft.GroupID != "g1" || cfg.RequestTimeout != 1500*time.Millisecond {
		t.Fatalf("overrides not applied: %+v", cfg)
	}

	for _, missing := range []string{"POD_NAME", "SHARD", "DATA_DIR", "FSM_DIR", "RAFT_ADVERTISE"} {
		m := map[string]string{}
		for k, v := range base {
			if k != missing {
				m[k] = v
			}
		}
		if _, err := loadConfig(env(m)); err == nil {
			t.Errorf("missing %s accepted", missing)
		}
	}
	for k, v := range map[string]string{"SHARD": "-1", "PROFILE": "fast", "REQUEST_TIMEOUT_MS": "soon", "HTTP_ADDR": "8080"} {
		m := map[string]string{}
		for bk, bv := range base {
			m[bk] = bv
		}
		m[k] = v
		if _, err := loadConfig(env(m)); err == nil {
			t.Errorf("%s=%q accepted", k, v)
		}
	}
}

func TestHTTPAddrFor(t *testing.T) {
	got := httpAddrFor("storage-1-0.storage-1.reliefgrid.svc.cluster.local:7000", "8080")
	if want := "storage-1-0.storage-1.reliefgrid.svc.cluster.local:8080"; got != want {
		t.Fatalf("httpAddrFor = %q, want %q", got, want)
	}
}
