package report

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"slices"
	"strings"
	"sync"
	"time"
)

// Status is one created request's state after the run, read through the
// public API: the acknowledged-write check (every 201 must still be
// readable) and the matched/waiting split.
type Status struct {
	ID         string `json:"id"`
	HTTPStatus int    `json:"httpStatus"`
	Status     string `json:"status,omitempty"`
	CreatedAt  string `json:"createdAt,omitempty"`
	UpdatedAt  string `json:"updatedAt,omitempty"`
	Err        string `json:"err,omitempty"`
}

// CollectStatuses GETs /api/v1/requests/{id} for every id, a few at a time.
func CollectStatuses(ctx context.Context, apiURL string, ids []string, parallel int) []Status {
	client := &http.Client{Timeout: 10 * time.Second}
	out := make([]Status, len(ids))
	var wg sync.WaitGroup
	sem := make(chan struct{}, max(1, parallel))
	for i, id := range ids {
		wg.Add(1)
		sem <- struct{}{}
		go func() {
			defer func() { <-sem; wg.Done() }()
			out[i] = fetch(ctx, client, apiURL, id)
		}()
	}
	wg.Wait()
	return out
}

func fetch(ctx context.Context, c *http.Client, apiURL, id string) Status {
	s := Status{ID: id}
	for attempt := range 3 { // a transient 503 is retried; a 404 is a lost write
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, strings.TrimRight(apiURL, "/")+"/api/v1/requests/"+id, nil)
		if err != nil {
			s.Err = err.Error()
			return s
		}
		resp, err := c.Do(req)
		if err != nil {
			s.Err = err.Error()
		} else {
			b, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
			_ = resp.Body.Close()
			s.HTTPStatus, s.Err = resp.StatusCode, ""
			if resp.StatusCode == http.StatusOK {
				var body struct{ Status, CreatedAt, UpdatedAt string }
				if err := json.Unmarshal(b, &body); err != nil {
					s.Err = err.Error()
				} else {
					s.Status, s.CreatedAt, s.UpdatedAt = body.Status, body.CreatedAt, body.UpdatedAt
				}
				return s
			}
			if resp.StatusCode == http.StatusNotFound {
				return s
			}
		}
		time.Sleep(time.Duration(attempt+1) * 500 * time.Millisecond)
	}
	return s
}

// WriteStatuses writes one JSON line per status.
func WriteStatuses(path string, ss []Status) error {
	f, err := os.Create(path) //nolint:gosec // the run directory's own file
	if err != nil {
		return err
	}
	enc := json.NewEncoder(f)
	for _, s := range ss {
		if err := enc.Encode(s); err != nil {
			_ = f.Close()
			return err
		}
	}
	return f.Close()
}

// ReadStatuses reads WriteStatuses' file.
func ReadStatuses(path string) ([]Status, error) {
	f, err := os.Open(path) //nolint:gosec // the run directory's own file
	if err != nil {
		return nil, err
	}
	defer func() { _ = f.Close() }()
	var out []Status
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		var s Status
		if err := json.Unmarshal(sc.Bytes(), &s); err != nil {
			return nil, fmt.Errorf("report: %s: %w", path, err)
		}
		out = append(out, s)
	}
	return out, sc.Err()
}

// Match is the report's matched/waiting split. Latency is server-side:
// updatedAt - createdAt of each request that ended MATCHED (the load never
// moves a request past MATCHED, so updatedAt is when it matched), and it
// covers matched requests only; waiting ones are counted, not timed.
type Match struct {
	Label   string         `json:"label"`
	Matched int            `json:"matched"`
	Waiting int            `json:"waiting"`
	Other   map[string]int `json:"other"` // any other final status, by status
	P50Ms   float64        `json:"p50Ms"`
	P95Ms   float64        `json:"p95Ms"`
	P99Ms   float64        `json:"p99Ms"`
}

// waitingStates are the states of a request still without a resource.
var waitingStates = []string{"PENDING", "WAITING", "MATCHING", "RETRYING"}

// Summarise splits statuses into matched and waiting, and reports every
// acknowledged create that could not be read back (lost or unreadable).
func Summarise(ss []Status) (m Match, lost []Status) {
	m = Match{Label: "matched only", Other: map[string]int{}}
	var lat []float64
	for _, s := range ss {
		switch {
		case s.HTTPStatus != http.StatusOK || s.Err != "":
			lost = append(lost, s)
		case s.Status == "MATCHED":
			m.Matched++
			c, err1 := time.Parse(time.RFC3339Nano, s.CreatedAt)
			u, err2 := time.Parse(time.RFC3339Nano, s.UpdatedAt)
			if err1 == nil && err2 == nil {
				lat = append(lat, float64(u.Sub(c).Microseconds())/1000)
			}
		case slices.Contains(waitingStates, s.Status):
			m.Waiting++
		default:
			m.Other[s.Status]++
		}
	}
	slices.Sort(lat)
	m.P50Ms, m.P95Ms, m.P99Ms = Quantile(lat, 0.5), Quantile(lat, 0.95), Quantile(lat, 0.99)
	return m, lost
}
