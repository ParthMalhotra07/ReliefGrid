package main

import (
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
