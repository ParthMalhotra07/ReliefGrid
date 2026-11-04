package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/ParthMalhotra07/ReliefGrid/internal/fsm/memstore"
	"github.com/ParthMalhotra07/ReliefGrid/internal/store"
)

func TestRunServesRealFSMAndStops(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	s, err := memstore.New(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := s.Close(); err != nil {
			t.Error(err)
		}
	})
	cfg, err := loadConfig(func(string) string { return "" })
	if err != nil {
		t.Fatal(err)
	}
	cfg.addr = "127.0.0.1:0"
	ready, result, finished := make(chan string, 1), make(chan error, 1), make(chan struct{})
	go func() {
		result <- run(ctx, cfg, s, ready)
		close(finished)
	}()
	t.Cleanup(func() {
		cancel()
		select {
		case <-finished:
		case <-time.After(apiReadinessPropagation + 5*time.Second):
			t.Error("API process did not shut down")
		}
	})
	var addr string
	select {
	case addr = <-ready:
	case <-finished:
		t.Fatalf("startup failed: %v", <-result)
	case <-time.After(5 * time.Second):
		t.Fatal("API did not listen")
	}
	requestCtx, requestCancel := context.WithTimeout(ctx, 5*time.Second)
	defer requestCancel()
	req, err := http.NewRequestWithContext(requestCtx, "POST", "http://"+addr+"/api/v1/requests", strings.NewReader(`{"requestType":"WATER","location":{"lat":0,"lng":0},"details":"Synthetic process test"}`))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/json")
	response, err := (&http.Client{Timeout: 5 * time.Second}).Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = response.Body.Close() }()
	hostname, err := os.Hostname()
	if err != nil {
		t.Fatal(err)
	}
	if got := response.Header.Get("X-ReliefGrid-Pod"); got != hostname {
		t.Errorf("served pod %q, want %q", got, hostname)
	}
	var body struct {
		ID string `json:"requestId"`
	}
	if err := json.NewDecoder(response.Body).Decode(&body); err != nil || response.StatusCode != 201 {
		t.Fatalf("process response: status=%d, %v", response.StatusCode, err)
	}
	rec, err := s.ReadRecord(requestCtx, store.RequestKey(body.ID))
	if err != nil || rec.Request == nil || rec.Request.Status != store.RequestPending {
		t.Fatalf("HTTP response not backed by real FSM: %+v, %v", rec, err)
	}
	cancel()
	select {
	case <-finished:
	case <-time.After(apiReadinessPropagation + 5*time.Second):
		t.Fatal("API process ignored cancellation")
	}
	if err := <-result; err != nil {
		t.Fatal(err)
	}
}

func TestProductionStartupConnectsToCoordinator(t *testing.T) {
	paths := make(chan string, 1)
	coordinator := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		paths <- r.URL.Path
		w.WriteHeader(503)
	}))
	t.Cleanup(coordinator.Close)
	cfg, err := loadConfig(func(key string) string {
		if key == "COORDINATOR_URL" {
			return coordinator.URL
		}
		return ""
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	if err := run(ctx, cfg, nil, nil); err == nil {
		t.Fatal("started without a coordinator ring")
	}
	select {
	case path := <-paths:
		if path != "/internal/ring" {
			t.Fatalf("unexpected coordinator bootstrap route %q", path)
		}
	default:
		t.Fatal("production startup bypassed coordclient")
	}
}

func TestConfigRefusesUnboundedCallsAndBadCoordinator(t *testing.T) {
	for _, env := range []map[string]string{
		{"BUDGET_MS": "0"}, {"CALL_TIMEOUT_MS": "-1"}, {"BUDGET_MS": "50", "CALL_TIMEOUT_MS": "100"},
		{"COORDINATOR_URL": "file:///tmp/store"}, {"COORDINATOR_URL": "https://user:password@example.invalid"},
		{"HTTP_ADDR": "not-an-address"},
	} {
		if _, err := loadConfig(func(key string) string { return env[key] }); err == nil {
			t.Fatalf("accepted invalid process config: %#v", env)
		}
	}
}

// Hold a real FSM create across process cancellation. A graceful stop must let
// this admitted request commit and return its original HTTP result.
type drainStore struct {
	store.Store
	entered, release chan struct{}
}

func (s *drainStore) CreateEntity(ctx context.Context, c store.CreateEntity) (store.Result, error) {
	close(s.entered)
	<-s.release
	return s.Store.CreateEntity(ctx, c)
}
func TestShutdownDrainsAdmittedCreate(t *testing.T) {
	s, err := memstore.New(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = s.Close() }()
	held := &drainStore{Store: s, entered: make(chan struct{}), release: make(chan struct{})}
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	cfg, err := loadConfig(func(string) string { return "" })
	if err != nil {
		t.Fatal(err)
	}
	cfg.addr = "127.0.0.1:0"
	ready, stopped := make(chan string, 1), make(chan error, 1)
	go func() { stopped <- run(ctx, cfg, held, ready) }()
	var addr string
	select {
	case addr = <-ready:
	case <-time.After(5 * time.Second):
		t.Fatal("not listening")
	}
	result := make(chan int, 1)
	go func() {
		req, err := http.NewRequestWithContext(t.Context(), "POST", "http://"+addr+"/api/v1/requests", strings.NewReader(`{"requestType":"WATER","location":{"lat":0,"lng":0},"details":"Synthetic draining request"}`))
		if err != nil {
			result <- 0
			return
		}
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Idempotency-Key", "271d725c-2f34-411e-92ea-496456f5da92")
		response, err := (&http.Client{Timeout: 5 * time.Second}).Do(req)
		if err != nil {
			result <- 0
			return
		}
		defer func() { _ = response.Body.Close() }()
		result <- response.StatusCode
	}()
	select {
	case <-held.entered:
	case <-time.After(5 * time.Second):
		close(held.release)
		t.Fatal("create never entered")
	}
	cancel()
	close(held.release)
	select {
	case status := <-result:
		if status != http.StatusCreated {
			t.Errorf("admitted create lost on SIGTERM: HTTP %d, want201", status)
		}
	case <-time.After(5 * time.Second):
		t.Error("create did not finish")
	}
	select {
	case err := <-stopped:
		if err != nil {
			t.Error(err)
		}
	case <-time.After(apiReadinessPropagation + 5*time.Second):
		t.Fatal("API did not join after drain")
	}
}
