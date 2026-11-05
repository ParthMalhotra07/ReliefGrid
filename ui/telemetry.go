package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"sync"
	"time"
)

type queueSnapshot struct {
	Available      bool      `json:"available"`
	SampledAt      time.Time `json:"sampledAt"`
	Ready          *int64    `json:"ready"`
	Unacknowledged *int64    `json:"unacknowledged"`
	Dead           *int64    `json:"dead"`
}
type queueTelemetry struct {
	base   *url.URL
	client *http.Client
	mu     sync.Mutex
	cached queueSnapshot
	until  time.Time
}

func newQueueTelemetry(address string) (*queueTelemetry, error) {
	t := &queueTelemetry{client: &http.Client{Timeout: 2 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}}
	if address == "" {
		return t, nil
	}
	u, err := url.Parse(address)
	if err != nil || u.Host == "" || (u.Scheme != "http" && u.Scheme != "https") || u.RawQuery != "" || u.Fragment != "" || u.Path != "" && u.Path != "/" {
		return nil, fmt.Errorf("UI_BROKER_MANAGEMENT_URL must be an HTTP(S) origin")
	}
	t.base = u
	return t, nil
}
func (t *queueTelemetry) sample(ctx context.Context) queueSnapshot {
	t.mu.Lock()
	defer t.mu.Unlock()
	if time.Now().Before(t.until) {
		return t.cached
	}
	out := queueSnapshot{SampledAt: time.Now().UTC()}
	defer func() { t.cached = out; t.until = time.Now().Add(10 * time.Second) }()
	if t.base == nil {
		return out
	}
	ctx, cancel := context.WithTimeout(ctx, 1500*time.Millisecond)
	defer cancel()
	type stats struct {
		Ready    *int64 `json:"messages_ready"`
		Unacked  *int64 `json:"messages_unacknowledged"`
		Messages *int64 `json:"messages"`
	}
	var values [2]stats
	var errs [2]error
	var wg sync.WaitGroup
	for i, name := range []string{"matching.requests", "matching.dead"} {
		wg.Go(func() {
			u := *t.base
			u.Path = "/api/queues///" + name
			u.RawPath = "/api/queues/%2F/" + name
			user := u.User
			u.User = nil
			req, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
			if err != nil {
				errs[i] = err
				return
			}
			if user != nil {
				password, _ := user.Password()
				req.SetBasicAuth(user.Username(), password)
			}
			resp, err := t.client.Do(req)
			if err != nil {
				errs[i] = err
				return
			}
			defer func() { _ = resp.Body.Close() }()
			if resp.StatusCode != http.StatusOK {
				errs[i] = fmt.Errorf("queue observation unavailable")
				return
			}
			raw, err := io.ReadAll(io.LimitReader(resp.Body, 65537))
			if err != nil || len(raw) > 65536 {
				errs[i] = fmt.Errorf("queue observation incomplete")
				return
			}
			errs[i] = json.Unmarshal(raw, &values[i])
		})
	}
	wg.Wait()
	if errs[0] != nil || errs[1] != nil {
		return out
	}
	if values[0].Ready == nil || values[0].Unacked == nil || values[1].Messages == nil || *values[0].Ready < 0 || *values[0].Unacked < 0 || *values[1].Messages < 0 {
		return out
	}
	out.Available = true
	out.Ready = values[0].Ready
	out.Unacknowledged = values[0].Unacked
	out.Dead = values[1].Messages
	return out
}
func (t *queueTelemetry) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	// Credentials and broker addresses remain server-side; only bounded aggregate
	// observations are published by this UI endpoint, outside /api/v1.
	_ = json.NewEncoder(w).Encode(t.sample(r.Context()))
}
