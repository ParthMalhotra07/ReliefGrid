package api

import (
	"context"
	"encoding/json"
	"github.com/ParthMalhotra07/ReliefGrid/internal/fsm/memstore"
	"github.com/google/uuid"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func projectionBody(t *testing.T) []byte {
	t.Helper()
	b, err := json.Marshal(map[string]any{"schemaVersion": 1, "eventId": uuid.NewString(), "entityId": uuid.NewString(), "entityVersion": 3, "eventType": "request.state_changed", "payload": map[string]string{"details": "private details"}})
	if err != nil {
		t.Fatal(err)
	}
	return b
}
func TestEventHubHintsOmitPayloadAndPreserveIdentity(t *testing.T) {
	h := NewEventHub()
	c, stop := h.subscribe()
	defer stop()
	body := projectionBody(t)
	if err := h.Publish(body); err != nil {
		t.Fatal(err)
	}
	p := <-c.messages
	var v map[string]any
	if err := json.Unmarshal(p.data, &v); err != nil {
		t.Fatal(err)
	}
	if p.kind != "change" || v["eventId"] != p.id || len(v) != 4 || v["payload"] != nil {
		t.Fatalf("unexpected projection: %+v %s", p, p.data)
	}
	for _, bad := range [][]byte{[]byte(`{}`), []byte(`{"eventId":"bad"}`), make([]byte, (1<<20)+1)} {
		if err := h.Publish(bad); err == nil {
			t.Fatal("invalid envelope accepted")
		}
	}
}
func TestEventHubDisconnectsSlowClientsWithoutBlockingOthers(t *testing.T) {
	h := NewEventHub()
	slow, stopSlow := h.subscribe()
	defer stopSlow()
	fast, stopFast := h.subscribe()
	defer stopFast()
	for range 17 {
		if err := h.Publish(projectionBody(t)); err != nil {
			t.Fatal(err)
		}
		select {
		case <-fast.messages:
		default:
			t.Fatal("fast subscriber lost a hint")
		}
	}
	select {
	case <-slow.done:
	default:
		t.Fatal("slow subscriber did not disconnect for reconnect refresh")
	}
	h.SetAvailable(true)
	p := <-fast.messages
	if p.kind != "refresh" {
		t.Fatal(p.kind)
	}
	h.SetAvailable(false)
	p = <-fast.messages
	if p.kind != "unavailable" {
		t.Fatal(p.kind)
	}
}
func TestSSEStreamContextEndsBeforeShutdownDrain(t *testing.T) {
	store, err := memstore.New(t.Context(), memstore.WithShards(3))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = store.Close() }()
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	s, err := New(Config{Store: store, CallerNamespace: "dispatcher", StreamContext: ctx})
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { defer close(done); s.ServeHTTP(w, r) }))
	defer server.Close()
	req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, server.URL+"/api/v1/events", nil)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := server.Client().Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("SSE kept API shutdown waiting")
	}
}
