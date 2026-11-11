package main

import (
	"context"
	"errors"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/ParthMalhotra07/ReliefGrid/internal/coordinator"
	"github.com/ParthMalhotra07/ReliefGrid/internal/ring"
)

func TestLoadConfig(t *testing.T) {
	env := func(m map[string]string) func(string) string { return func(k string) string { return m[k] } }

	cfg, err := loadConfig(env(map[string]string{"OPERATOR_TOKEN": "s3cret"}))
	if err != nil {
		t.Fatalf("loadConfig: %v", err)
	}
	c := cfg.Coordinator
	if cfg.HTTPAddr != ":8080" || cfg.MetricsAddr != ":9090" || cfg.RingPath != "/etc/reliefgrid/ring.yaml" || c.OperatorToken != "s3cret" ||
		c.SuspectAfter != 3*time.Second || c.DownAfter != 5*time.Second || c.Budget != 4*time.Second || c.AttemptTimeout != 3500*time.Millisecond {
		t.Fatalf("defaults = %+v", cfg)
	}

	cfg, err = loadConfig(env(map[string]string{"HTTP_ADDR": ":9000", "RING_PATH": "/r.yaml",
		"SUSPECT_AFTER_MS": "1000", "DOWN_AFTER_MS": "2000", "BUDGET_MS": "1500", "ATTEMPT_TIMEOUT_MS": "700"}))
	if err != nil {
		t.Fatal(err)
	}
	c = cfg.Coordinator
	if cfg.HTTPAddr != ":9000" || cfg.RingPath != "/r.yaml" || c.SuspectAfter != time.Second || c.DownAfter != 2*time.Second ||
		c.Budget != 1500*time.Millisecond || c.AttemptTimeout != 700*time.Millisecond {
		t.Fatalf("overrides = %+v", cfg)
	}

	for k, v := range map[string]string{
		"SUSPECT_AFTER_MS": "soon", "DOWN_AFTER_MS": "-1", "BUDGET_MS": "0", "ATTEMPT_TIMEOUT_MS": "x", "HTTP_ADDR": "8080",
	} {
		if _, err := loadConfig(env(map[string]string{k: v})); err == nil {
			t.Errorf("%s=%q accepted", k, v)
		}
	}
	// Plan §14: one attempt fits in the budget, which stays under 5 s HTTP.
	for name, m := range map[string]map[string]string{
		"budget at 5s":               {"BUDGET_MS": "5000"},
		"attempt longer than budget": {"BUDGET_MS": "3000", "ATTEMPT_TIMEOUT_MS": "3500"},
	} {
		if _, err := loadConfig(env(m)); err == nil {
			t.Errorf("%s accepted", name)
		}
	}
	// A short budget without ATTEMPT_TIMEOUT_MS caps the attempt at it.
	cfg, err = loadConfig(env(map[string]string{"BUDGET_MS": "2000"}))
	if err != nil || cfg.Coordinator.AttemptTimeout != 2*time.Second {
		t.Fatalf("BUDGET_MS=2000 alone: attempt %s, %v; want 2s", cfg.Coordinator.AttemptTimeout, err)
	}
	// SUSPECT must come before DOWN.
	if _, err := loadConfig(env(map[string]string{"SUSPECT_AFTER_MS": "5000", "DOWN_AFTER_MS": "3000"})); err == nil {
		t.Error("SUSPECT_AFTER_MS >= DOWN_AFTER_MS accepted")
	}
}

// Plan §09: every restart loads exactly the published ring or refuses to start.
func TestRunRefusesRingChecksumMismatch(t *testing.T) {
	good, err := os.ReadFile("../../contracts/ring.yaml")
	if err != nil {
		t.Fatal(err)
	}
	bad := strings.Replace(string(good), "vnodesPerShard: 128", "vnodesPerShard: 127", 1)
	path := filepath.Join(t.TempDir(), "ring.yaml")
	if err := os.WriteFile(path, []byte(bad), 0o600); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	ready := make(chan string, 1)
	err = run(ctx, config{HTTPAddr: "127.0.0.1:0", RingPath: path}, ready)
	if !errors.Is(err, ring.ErrChecksumMismatch) {
		t.Fatalf("run with a tampered ring: err = %v, want ErrChecksumMismatch", err)
	}
	select {
	case addr := <-ready:
		t.Fatalf("served on %s with a tampered ring", addr)
	default:
	}
}

func TestRunServesTheLoadedRing(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	tokens := mountedAuth(t)
	ready := make(chan string, 1)
	errc := make(chan error, 1)
	go func() {
		errc <- run(ctx, config{AppAuthFiles: tokens, NodeAuthFiles: tokens, NodeSenderTokenFile: tokens.Token, HTTPAddr: "127.0.0.1:0", RingPath: "../../contracts/ring.yaml", Coordinator: coordinator.Config{OperatorToken: "t"}}, ready)
	}()
	var addr string
	select {
	case addr = <-ready:
	case err := <-errc:
		t.Fatalf("run: %v", err)
	case <-time.After(5 * time.Second):
		t.Fatal("not serving after 5s")
	}
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, "http://"+addr+"/internal/ring", nil)
	req.Header.Set("X-ReliefGrid-Internal-Token", "test-token")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET /internal/ring = %d", resp.StatusCode)
	}
	for _, tc := range []struct {
		path   string
		status int
	}{{"/healthz", 200}, {"/internal/ring", 401}} {
		req, _ := http.NewRequestWithContext(ctx, "GET", "http://"+addr+tc.path, nil)
		response, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		_ = response.Body.Close()
		if response.StatusCode != tc.status {
			t.Fatalf("%s=%d", tc.path, response.StatusCode)
		}
	}
	cancel()
	if err := <-errc; err != nil {
		t.Fatalf("run after cancel: %v", err)
	}
}
