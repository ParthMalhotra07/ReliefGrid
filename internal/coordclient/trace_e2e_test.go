package coordclient_test

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"

	"github.com/ParthMalhotra07/ReliefGrid/internal/api"
	"github.com/ParthMalhotra07/ReliefGrid/internal/obs"
	"github.com/ParthMalhotra07/ReliefGrid/internal/store"
)

// One public create is one trace across the whole storage path: the API's
// server span → coordclient.CreateEntity → the coordinator's routed call →
// coordinator.forward → the storage node's data route → raft.apply, each a
// descendant of the API span; and the committed outbox event carries the
// API span's context, so the relay's producer span joins the same trace.
// The API is wired as cmd/api wires it: obs.Handler around api.New.
func TestCreateIsOneTraceFromAPIToRaftApply(t *testing.T) {
	sr := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSampler(sdktrace.AlwaysSample()), sdktrace.WithSpanProcessor(sr))
	t.Cleanup(func() { _ = tp.Shutdown(context.Background()) })
	c := newTracedCluster(t, nil, tp)

	h, err := api.New(api.Config{Store: c.client, CallerNamespace: "trace-e2e", Budget: 5 * time.Second, CallTimeout: 4 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	srv := httptest.NewServer(obs.Handler(h, obs.WithTracerProvider(tp)))
	t.Cleanup(srv.Close)
	ctx, cancel := context.WithTimeout(t.Context(), clusterWait)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, srv.URL+"/api/v1/requests",
		strings.NewReader(`{"requestType":"WATER","location":{"lat":0,"lng":0},"details":"Synthetic trace e2e"}`))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	var created struct {
		ID string `json:"requestId"`
	}
	err = json.NewDecoder(resp.Body).Decode(&created)
	_ = resp.Body.Close()
	if err != nil || resp.StatusCode != http.StatusCreated {
		t.Fatalf("create: %d %v", resp.StatusCode, err)
	}

	// raft.apply's ancestors, nearest first, must be exactly this chain, and
	// the chain's root is the API server span. The API span ends after the
	// response is written, so wait for the chain to close.
	type hop struct {
		name string
		kind trace.SpanKind
	}
	wantChain := []hop{
		{"raft.apply", trace.SpanKindInternal},
		{"PUT /internal/data/{key}", trace.SpanKindServer}, // storage node
		{"coordinator.forward", trace.SpanKindClient},
		{"PUT /internal/data/{key}", trace.SpanKindServer}, // coordinator
		{"coordclient.CreateEntity", trace.SpanKindClient},
		{"POST /api/v1/requests", trace.SpanKindServer}, // API
	}
	var chain []sdktrace.ReadOnlySpan
	for deadline := time.Now().Add(2 * time.Second); ; time.Sleep(10 * time.Millisecond) {
		chain = applyAncestry(sr.Ended())
		if len(chain) > 0 && !chain[len(chain)-1].Parent().IsValid() || time.Now().After(deadline) {
			break
		}
	}
	got := make([]hop, len(chain))
	for i, s := range chain {
		got[i] = hop{s.Name(), s.SpanKind()}
	}
	if fmt.Sprint(got) != fmt.Sprint(wantChain) {
		for _, s := range sr.Ended() {
			t.Logf("recorded %q kind %v trace %s parent %s", s.Name(), s.SpanKind(), s.SpanContext().TraceID(), s.Parent().SpanID())
		}
		t.Errorf("raft.apply ancestry %v, want %v", got, wantChain)
	}
	if len(chain) == 0 {
		t.Fatal("no single raft.apply span")
	}
	root := chain[len(chain)-1]
	if root.Parent().IsValid() {
		t.Errorf("API span has parent %s, want the trace root", root.Parent().SpanID())
	}

	shard, err := c.rg.Owner(store.RequestKey(created.ID))
	if err != nil {
		t.Fatal(err)
	}
	rows, err := c.client.PendingOutbox(ctx, store.OutboxQuery{Shard: shard, Limit: 100})
	if err != nil {
		t.Fatal(err)
	}
	want := fmt.Sprintf("00-%s-%s-01", root.SpanContext().TraceID(), root.SpanContext().SpanID())
	var env struct {
		Traceparent string `json:"traceparent"`
	}
	for _, row := range rows {
		if row.EventType == "request.created" {
			if err := json.Unmarshal(row.Payload, &env); err != nil {
				t.Fatal(err)
			}
		}
	}
	if env.Traceparent != want {
		t.Errorf("request.created traceparent %q, want the API span %q", env.Traceparent, want)
	}
}

// applyAncestry is the one raft.apply span recorded and its ancestors,
// nearest first, as far as the recorded spans reach; nil unless exactly one
// raft.apply was recorded.
func applyAncestry(spans []sdktrace.ReadOnlySpan) []sdktrace.ReadOnlySpan {
	byID := map[trace.SpanID]sdktrace.ReadOnlySpan{}
	var apply []sdktrace.ReadOnlySpan
	for _, s := range spans {
		byID[s.SpanContext().SpanID()] = s
		if s.Name() == "raft.apply" {
			apply = append(apply, s)
		}
	}
	if len(apply) != 1 {
		return nil
	}
	var chain []sdktrace.ReadOnlySpan
	for s := apply[0]; s != nil; s = byID[s.Parent().SpanID()] {
		chain = append(chain, s)
		if !s.Parent().IsValid() {
			break
		}
	}
	return chain
}
