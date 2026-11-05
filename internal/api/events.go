package api

import (
	"crypto/subtle"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
)

// EventHub is a bounded, per-API-pod hint projection, never a workflow owner or
// replay log. A slow client is disconnected and must refresh on reconnect.
type EventHub struct {
	mu        sync.Mutex
	clients   map[*eventClient]struct{}
	available bool
}
type eventClient struct {
	messages chan projection
	done     chan struct{}
}
type projection struct {
	id, kind string
	data     []byte
}

func NewEventHub() *EventHub { return &EventHub{clients: map[*eventClient]struct{}{}} }
func (h *EventHub) subscribe() (*eventClient, func()) {
	h.mu.Lock()
	defer h.mu.Unlock()
	c := &eventClient{messages: make(chan projection, 16), done: make(chan struct{})}
	h.clients[c] = struct{}{}
	return c, func() {
		h.mu.Lock()
		defer h.mu.Unlock()
		if _, ok := h.clients[c]; ok {
			delete(h.clients, c)
			close(c.done)
		}
	}
}
func (h *EventHub) broadcast(p projection) {
	for c := range h.clients {
		select {
		case c.messages <- p:
		default:
			delete(h.clients, c)
			close(c.done)
		}
	}
}

// Publish accepts only bounded envelopes with real event/entity identities.
// Payload details are not forwarded: clients fetch current public records.
func (h *EventHub) Publish(body []byte) error {
	if len(body) > 1<<20 {
		return fmt.Errorf("projection envelope too large")
	}
	var v struct {
		EventID       string `json:"eventId"`
		EntityID      string `json:"entityId"`
		EntityVersion uint64 `json:"entityVersion"`
		EventType     string `json:"eventType"`
	}
	if err := json.Unmarshal(body, &v); err != nil {
		return fmt.Errorf("projection envelope: %w", err)
	}
	if id, err := uuid.Parse(v.EventID); err != nil || id == uuid.Nil {
		return fmt.Errorf("projection event ID invalid")
	}
	if id, err := uuid.Parse(v.EntityID); err != nil || id == uuid.Nil {
		return fmt.Errorf("projection entity ID invalid")
	}
	if v.EntityVersion == 0 || v.EventType == "" || len(v.EventType) > 80 || strings.ContainsAny(v.EventType, "\r\n") {
		return fmt.Errorf("projection envelope metadata invalid")
	}
	data, err := json.Marshal(v)
	if err != nil {
		return fmt.Errorf("encode projection: %w", err)
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	h.broadcast(projection{v.EventID, "change", data})
	return nil
}

// SetAvailable makes missed broker history explicit. Reconnection asks every
// client to reread authoritative state; no lossless delivery is promised.
func (h *EventHub) SetAvailable(ok bool) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.available == ok {
		return
	}
	h.available = ok
	kind := "unavailable"
	if ok {
		kind = "refresh"
	}
	h.broadcast(projection{uuid.NewString(), kind, []byte(`{"reason":"projection_connection_changed"}`)})
}
func (h *EventHub) isAvailable() bool { h.mu.Lock(); defer h.mu.Unlock(); return h.available }
func writeProjection(w http.ResponseWriter, p projection) error {
	_, err := fmt.Fprintf(w, "id: %s\nevent: %s\ndata: %s\n\n", p.id, p.kind, p.data)
	return err
}
func (s *Server) events(w http.ResponseWriter, r *http.Request) error {
	if s.cfg.StreamToken != "" {
		bearer, ok := strings.CutPrefix(r.Header.Get("Authorization"), "Bearer ")
		if !ok || bearer == "" {
			return issue(401, "UNAUTHORIZED", "stream credentials required")
		}
		if subtle.ConstantTimeCompare([]byte(bearer), []byte(s.cfg.StreamToken)) != 1 {
			return issue(403, "FORBIDDEN", "stream access denied")
		}
	}
	c, unsubscribe := s.cfg.Events.subscribe()
	defer unsubscribe()
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("X-Accel-Buffering", "no")
	controller := http.NewResponseController(w)
	flush := func(p *projection) error {
		if err := controller.SetWriteDeadline(time.Now().Add(s.cfg.Budget)); err != nil {
			return fmt.Errorf("SSE write deadline: %w", err)
		}
		var err error
		if p == nil {
			_, err = fmt.Fprint(w, ": heartbeat\n\n")
		} else {
			err = writeProjection(w, *p)
		}
		if err != nil {
			return err
		}
		return controller.Flush()
	}
	if err := flush(nil); err != nil {
		return nil
	}
	// A Last-Event-ID identifies a duplicate, not a replay cursor. Always refresh.
	p := projection{uuid.NewString(), "refresh", []byte(`{"reason":"connected"}`)}
	if err := flush(&p); err != nil {
		return nil
	}
	if !s.cfg.Events.isAvailable() {
		p = projection{uuid.NewString(), "unavailable", []byte(`{"reason":"projection_unavailable"}`)}
		if err := flush(&p); err != nil {
			return nil
		}
	}
	ticker := time.NewTicker(s.cfg.HeartbeatInterval)
	defer ticker.Stop()
	for {
		select {
		case <-r.Context().Done():
			return nil
		case <-s.cfg.StreamContext.Done():
			return nil
		case <-c.done:
			return nil
		case p := <-c.messages:
			if err := flush(&p); err != nil {
				return nil
			}
		case <-ticker.C:
			if err := flush(nil); err != nil {
				return nil
			}
		}
	}
}
