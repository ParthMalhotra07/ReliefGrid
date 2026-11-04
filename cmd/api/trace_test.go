package main

import (
	"context"
	"net/http"
	"strings"
	"testing"
	"time"

	"go.opentelemetry.io/otel"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"

	"github.com/ParthMalhotra07/ReliefGrid/internal/fsm/memstore"
)

const incomingTrace = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"

// The process serves the public API under an obs.Handler server span that
// continues the caller's trace and is named by route; probes are not traced
// (kubelet would fill Jaeger). The binary uses the global provider, which
// obs.Init installs when tracing is enabled.
func TestAPIRoutesAreServerSpansProbesAreNot(t *testing.T) {
	sr := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSampler(sdktrace.AlwaysSample()), sdktrace.WithSpanProcessor(sr))
	prev := otel.GetTracerProvider()
	otel.SetTracerProvider(tp)
	t.Cleanup(func() { otel.SetTracerProvider(prev); _ = tp.Shutdown(context.Background()) })

	ctx, cancel := context.WithCancel(t.Context())
	s, err := memstore.New(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	cfg, err := loadConfig(func(string) string { return "" })
	if err != nil {
		t.Fatal(err)
	}
	cfg.addr = "127.0.0.1:0"
	ready, finished := make(chan string, 1), make(chan struct{})
	go func() { _ = run(ctx, cfg, s, ready); close(finished) }()
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
	case <-time.After(5 * time.Second):
		t.Fatal("API did not listen")
	}
	client := &http.Client{Timeout: 5 * time.Second}
	for _, path := range []string{"/healthz", "/readyz"} {
		probe, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://"+addr+path, nil)
		if err != nil {
			t.Fatal(err)
		}
		resp, err := client.Do(probe)
		if err != nil {
			t.Fatal(err)
		}
		_ = resp.Body.Close()
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, "http://"+addr+"/api/v1/requests",
		strings.NewReader(`{"requestType":"WATER","location":{"lat":0,"lng":0},"details":"Synthetic trace test"}`))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("traceparent", incomingTrace)
	resp, err := client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("create: %d", resp.StatusCode)
	}

	// A server span ends after its response is written: wait for the
	// create's (bounded), then count every server span recorded.
	var servers []sdktrace.ReadOnlySpan
	for deadline := time.Now().Add(2 * time.Second); ; time.Sleep(10 * time.Millisecond) {
		servers = servers[:0]
		created := false
		for _, sp := range sr.Ended() {
			if sp.SpanKind() == trace.SpanKindServer {
				servers = append(servers, sp)
				created = created || sp.Parent().SpanID().String() == "00f067aa0ba902b7"
			}
		}
		if created || time.Now().After(deadline) {
			break
		}
	}
	if len(servers) != 1 {
		names := make([]string, len(servers))
		for i, sp := range servers {
			names[i] = sp.Name()
		}
		t.Fatalf("server spans %q, want exactly one for the create (probes untraced)", names)
	}
	sp := servers[0]
	if sp.Name() != "POST /api/v1/requests" {
		t.Errorf("server span %q, want POST /api/v1/requests", sp.Name())
	}
	if got := sp.SpanContext().TraceID().String(); got != "4bf92f3577b34da6a3ce929d0e0e4736" || sp.Parent().SpanID().String() != "00f067aa0ba902b7" {
		t.Errorf("server span %s parent %s, want a child of the incoming %s", got, sp.Parent().SpanID(), incomingTrace)
	}
}
