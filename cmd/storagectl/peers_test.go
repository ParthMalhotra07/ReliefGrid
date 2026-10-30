package main

import (
	"encoding/json"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestHTTPPeersBootstrapStatusCodes(t *testing.T) {
	for _, tt := range []struct {
		code               int
		rejected, hasState bool
		ok                 bool
	}{
		{http.StatusOK, false, false, true},
		{http.StatusConflict, false, true, false},
		{http.StatusBadRequest, true, false, false},
		{http.StatusUnauthorized, true, false, false},
		{http.StatusForbidden, true, false, false},
		{http.StatusInternalServerError, false, false, false}, // outcome unknown
	} {
		t.Run(http.StatusText(tt.code), func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(tt.code)
			}))
			defer srv.Close()
			err := newHTTPPeers("tok", time.Second).Bootstrap(t.Context(), strings.TrimPrefix(srv.URL, "http://"), []peerVoter{{ID: "a/1", Address: "a:7000"}})
			if (err == nil) != tt.ok || errors.Is(err, errRejected) != tt.rejected || errors.Is(err, errHasState) != tt.hasState {
				t.Fatalf("HTTP %d: %v", tt.code, err)
			}
		})
	}
}

func TestDial(t *testing.T) {
	var lc net.ListenConfig
	ln, err := lc.Listen(t.Context(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	if err := dial(t.Context(), ln.Addr().String()); err != nil {
		t.Fatalf("dial a listener: %v", err)
	}
	addr := ln.Addr().String()
	_ = ln.Close()
	if err := dial(t.Context(), addr); err == nil {
		t.Fatal("dial a closed port succeeded")
	}
}

// The membership client sends the change with the operator token and maps
// the leader's answers: 409 NOT_LEADER and 409 CONFIG_CHANGED are definite
// refusals (re-read and retry); anything else unanswered is unknown.
func TestHTTPPeersMembership(t *testing.T) {
	for _, tt := range []struct {
		status    int
		body      string
		notLeader bool
		changed   bool
		ok        bool
	}{
		{http.StatusOK, `{"index":12}`, false, false, true},
		{http.StatusConflict, `{"error":"NOT_LEADER: storage-shard-1-1 is not the leader"}`, true, false, false},
		{http.StatusConflict, `{"error":"CONFIG_CHANGED: since 11 (latest is 12)"}`, false, true, false},
		{http.StatusServiceUnavailable, `{"error":"outcome unknown; re-read the configuration"}`, false, false, false},
	} {
		t.Run(http.StatusText(tt.status)+" "+tt.body, func(t *testing.T) {
			var got membershipChange
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path != "/mgmt/membership" || r.Header.Get("Authorization") != "Bearer tok" {
					t.Errorf("%s %s auth %q", r.Method, r.URL.Path, r.Header.Get("Authorization"))
				}
				_ = json.NewDecoder(r.Body).Decode(&got)
				w.WriteHeader(tt.status)
				_, _ = w.Write([]byte(tt.body))
			}))
			defer srv.Close()
			change := membershipChange{Op: opRemove, ID: "storage-shard-1-2/inc-2", PrevIndex: 11}
			idx, err := newHTTPPeers("tok", time.Second).Membership(t.Context(), strings.TrimPrefix(srv.URL, "http://"), change)
			if (err == nil) != tt.ok || errors.Is(err, errNotLeader) != tt.notLeader || errors.Is(err, errConfigChanged) != tt.changed {
				t.Fatalf("HTTP %d: %v", tt.status, err)
			}
			if tt.ok && idx != 12 || got != change {
				t.Fatalf("index %d, sent %+v", idx, got)
			}
		})
	}
}
