package main

import (
	"context"
	"github.com/ParthMalhotra07/ReliefGrid/internal/auth"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestInternalAuthSenderIsValidatedBeforeCoordinatorIO(t *testing.T) {
	for _, tc := range []struct {
		name, token       string
		missing, disabled bool
	}{
		{name: "valid", token: "fixture-app-sender"},
		{name: "missing", missing: true}, {name: "empty"},
		{name: "newline-is-not-trimmed", token: "fixture-app-sender\n"},
		{name: "malformed", token: "fixture,invalid"}, {name: "disabled", disabled: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var calls atomic.Int32
			upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls.Add(1)
				if r.URL.Path != "/internal/ring" {
					t.Errorf("unexpected initial path %s", r.URL.Path)
				}
				want := "fixture-app-sender"
				if tc.disabled {
					want = ""
				}
				if got := r.Header.Get(auth.Header); got != want {
					t.Errorf("initial request credential present/correct=%v, want true", got == want)
				}
				if r.Header.Get("Authorization") != "" {
					t.Error("operator credential sent")
				}
				w.WriteHeader(http.StatusServiceUnavailable)
			}))
			defer upstream.Close()
			path := filepath.Join(t.TempDir(), "sender-token")
			if !tc.missing && !tc.disabled {
				if err := os.WriteFile(path, []byte(tc.token), 0o600); err != nil {
					t.Fatal(err)
				}
			}
			if tc.disabled {
				path = ""
			}
			ctx, cancel := context.WithTimeout(t.Context(), time.Second)
			defer cancel()
			t.Setenv("INTERNAL_AUTH_TOKEN_FILE", path)
			err := run(ctx, upstream.URL, "amqp://fixture:fixture@127.0.0.1:1/", "127.0.0.1:0", "")
			if err == nil {
				t.Fatal("fake ring refusal should stop startup")
			}
			wantCalls := int32(0)
			if tc.name == "valid" || tc.disabled {
				wantCalls = 1
			}
			if calls.Load() != wantCalls {
				t.Errorf("coordinator calls=%d, want %d", calls.Load(), wantCalls)
			}
			if tc.token != "" && strings.Contains(err.Error(), tc.token) {
				t.Error("credential disclosed in error")
			}
		})
	}
}
