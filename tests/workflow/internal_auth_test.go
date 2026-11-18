//go:build schedules

package workflow

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ParthMalhotra07/ReliefGrid/internal/auth"
	"github.com/ParthMalhotra07/ReliefGrid/internal/internalapi"
	"github.com/ParthMalhotra07/ReliefGrid/internal/ring"
	"github.com/ParthMalhotra07/ReliefGrid/internal/store"
)

func scheduleAuthFile(t *testing.T, value string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "sender-token")
	if err := os.WriteFile(path, []byte(value), 0600); err != nil {
		t.Fatal(err)
	}
	return path
}

func scheduleAuthRing(t *testing.T) internalapi.Ring {
	t.Helper()
	r, err := ring.Load("../../contracts/ring.yaml")
	if err != nil {
		t.Fatal(err)
	}
	return internalapi.Ring{RingVersion: r.Version(), Shards: r.Shards(), VnodesPerShard: r.VNodesPerShard(), Hash: internalapi.Xxhash64, Checksum: r.Checksum()}
}

// Exercise actual cluster fixture startup, including the very first ring lookup
// and the subsequent mutation. No live coordinator or broker is involved.
func TestScheduleClusterStartupInternalAuth(t *testing.T) {
	for _, token := range []string{"", "app-schedule-credential"} {
		t.Run(map[bool]string{true: "enabled", false: "disabled"}[token != ""], func(t *testing.T) {
			file := ""
			if token != "" {
				file = scheduleAuthFile(t, token)
			}
			t.Setenv("INTERNAL_AUTH_TOKEN_FILE", file)
			t.Setenv("SCHEDULE_BACKEND", "cluster")
			var calls atomic.Int32
			wantRing := scheduleAuthRing(t)
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls.Add(1)
				if r.Header.Get(auth.Header) != token || r.Header.Get("Authorization") != "" {
					t.Error("cluster startup sent the wrong credential class")
					w.WriteHeader(http.StatusUnauthorized)
					return
				}
				if r.URL.Path == "/internal/ring" {
					_ = json.NewEncoder(w).Encode(wantRing)
					return
				}
				if r.Method != http.MethodPut || !strings.HasPrefix(r.URL.Path, "/internal/data/req:") {
					t.Error("unexpected startup route")
				}
				_ = json.NewEncoder(w).Encode(internalapi.MutationResponse{Result: store.Result{Outcome: store.OutcomeOK}})
			}))
			defer srv.Close()
			t.Setenv("COORDINATOR_URL", srv.URL)
			fixture := Schedules[0]
			fixture.InitialState = nil
			x := newExecution(t, fixture, 1, "auth-startup")
			if x.s == nil || calls.Load() != 2 {
				t.Fatalf("startup calls=%d, want ring then create", calls.Load())
			}
		})
	}
}

func TestScheduleRingProbeInternalAuth(t *testing.T) {
	for _, token := range []string{"", "app-schedule-credential"} {
		t.Run(map[bool]string{true: "enabled", false: "disabled"}[token != ""], func(t *testing.T) {
			file := ""
			if token != "" {
				file = scheduleAuthFile(t, token)
			}
			t.Setenv("INTERNAL_AUTH_TOKEN_FILE", file)
			want := scheduleAuthRing(t)
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Header.Get(auth.Header) != token || r.Header.Get("Authorization") != "" {
					t.Error("ring probe has wrong credential class")
					w.WriteHeader(401)
					return
				}
				_ = json.NewEncoder(w).Encode(want)
			}))
			defer srv.Close()
			t.Setenv("COORDINATOR_URL", srv.URL)
			got := scheduleRing(&execution{t: t, ctx: t.Context()})
			if got.Checksum != want.Checksum {
				t.Fatal("ring changed")
			}
		})
	}
}

func TestScheduleAuthFilesFailBeforeNetwork(t *testing.T) {
	var calls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { calls.Add(1) }))
	defer srv.Close()
	for _, value := range []string{"", "secret\n", "secret,other", "secret with space"} {
		t.Run("invalid-"+strings.ReplaceAll(value, "\n", "newline"), func(t *testing.T) {
			t.Setenv("INTERNAL_AUTH_TOKEN_FILE", scheduleAuthFile(t, value))
			if _, err := scheduleCoordinatorClient(t.Context(), srv.URL); err == nil {
				t.Fatal("invalid client credential accepted")
			}
			if _, err := scheduleHTTPClient(); err == nil {
				t.Fatal("invalid raw-HTTP credential accepted")
			} else if value != "" && strings.Contains(err.Error(), value) {
				t.Fatal("credential appeared in error")
			}
		})
	}
	t.Setenv("INTERNAL_AUTH_TOKEN_FILE", filepath.Join(t.TempDir(), "missing"))
	if _, err := scheduleCoordinatorClient(t.Context(), srv.URL); err == nil {
		t.Fatal("missing client credential accepted")
	}
	if _, err := scheduleHTTPClient(); err == nil {
		t.Fatal("missing raw-HTTP credential accepted")
	}
	if calls.Load() != 0 {
		t.Fatalf("invalid credentials caused %d requests", calls.Load())
	}
}

func TestScheduleAuthRedirectCompatibility(t *testing.T) {
	for _, enabled := range []bool{false, true} {
		t.Run(map[bool]string{true: "enabled", false: "disabled"}[enabled], func(t *testing.T) {
			file := ""
			if enabled {
				file = scheduleAuthFile(t, "app-schedule-credential")
			}
			t.Setenv("INTERNAL_AUTH_TOKEN_FILE", file)
			var followed atomic.Int32
			target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { followed.Add(1); w.WriteHeader(200) }))
			defer target.Close()
			source := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { http.Redirect(w, r, target.URL, http.StatusFound) }))
			defer source.Close()
			client, err := scheduleHTTPClient()
			if err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithTimeout(t.Context(), time.Second)
			defer cancel()
			req, err := http.NewRequestWithContext(ctx, http.MethodGet, source.URL+"/internal/ring", nil)
			if err != nil {
				t.Fatal(err)
			}
			response, err := client.Do(req)
			if response != nil {
				_ = response.Body.Close()
			}
			if enabled {
				if err == nil || followed.Load() != 0 {
					t.Fatal("credentialed redirect followed")
				}
			} else if err != nil || followed.Load() != 1 || response.StatusCode != 200 {
				t.Fatalf("disabled legacy redirect changed: %v", err)
			}
		})
	}
}

func TestScheduleMemoryBackendIgnoresClusterCredential(t *testing.T) {
	t.Setenv("SCHEDULE_BACKEND", "m8")
	t.Setenv("INTERNAL_AUTH_TOKEN_FILE", filepath.Join(t.TempDir(), "missing"))
	fixture := Schedules[0]
	fixture.InitialState = nil
	if x := newExecution(t, fixture, 1, "auth-memory"); x.s == nil {
		t.Fatal("memory backend unexpectedly needs internal auth")
	}
}
