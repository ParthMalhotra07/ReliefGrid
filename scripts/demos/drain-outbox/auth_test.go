package main

import (
	"encoding/json"
	"github.com/ParthMalhotra07/ReliefGrid/internal/auth"
	"github.com/ParthMalhotra07/ReliefGrid/internal/internalapi"
	"github.com/ParthMalhotra07/ReliefGrid/internal/ring"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
)

func TestOperationalSenderToken(t *testing.T) {
	rg, err := ring.Load("../../../contracts/ring.yaml")
	if err != nil {
		t.Fatal(err)
	}
	var calls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		if r.Header.Get(auth.Header) != "operational-token" {
			t.Error("missing sender credential")
		}
		_ = json.NewEncoder(w).Encode(internalapi.Ring{RingVersion: rg.Version(), Hash: internalapi.Xxhash64, Shards: rg.Shards(), VnodesPerShard: rg.VNodesPerShard(), Checksum: rg.Checksum()})
	}))
	defer srv.Close()
	path := filepath.Join(t.TempDir(), "sender-token")
	t.Setenv("INTERNAL_AUTH_TOKEN_FILE", path)
	t.Setenv("COORDINATOR_URL", srv.URL)
	t.Setenv("AMQP_URL", ":invalid")

	for _, value := range []string{"bad\n", "operational-token"} {
		if err := os.WriteFile(path, []byte(value), 0600); err != nil {
			t.Fatal(err)
		}
		err := drain(t.Context())
		if err == nil {
			t.Fatal("invalid publisher unexpectedly connected")
		}
		if value == "operational-token" && !strings.Contains(err.Error(), "drain publisher:") {
			t.Fatalf("ring load: %v", err)
		}

	}
	if calls.Load() != 1 {
		t.Fatalf("invalid credentials sent: %d calls", calls.Load())
	}
}
