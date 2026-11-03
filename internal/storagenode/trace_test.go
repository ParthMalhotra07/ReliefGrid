package storagenode

import (
	"context"
	"net/http"
	"sync"
	"testing"

	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"

	"github.com/ParthMalhotra07/ReliefGrid/internal/store"
)

const incomingTrace = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"

// ctxNode is a fakeNode that remembers the span context each Apply ran under.
type ctxNode struct {
	*fakeNode
	mu    sync.Mutex
	spans []trace.SpanContext
}

func (n *ctxNode) Apply(ctx context.Context, cmd any) (store.Result, error) {
	n.mu.Lock()
	n.spans = append(n.spans, trace.SpanContextFromContext(ctx))
	n.mu.Unlock()
	return n.fakeNode.Apply(ctx, cmd)
}

func tracedServer(t *testing.T) (*server, *ctxNode, *tracetest.SpanRecorder) {
	t.Helper()
	sr := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSampler(sdktrace.AlwaysSample()), sdktrace.WithSpanProcessor(sr))
	t.Cleanup(func() { _ = tp.Shutdown(context.Background()) })
	n := &ctxNode{fakeNode: newFakeNode(t)}
	s := newTestServer(t, n)
	s.tracerProvider = tp
	return s, n, sr
}

// A data route continues the coordinator's trace in a server span named by
// route pattern, and the command is proposed under it, so raft.apply
// (raftstore) is its child.
func TestDataRouteServerSpanParentsTheProposal(t *testing.T) {
	s, n, sr := tracedServer(t)
	kvKey, _ := keyOn(t, s.ring, testShard, "", "kv:trace")
	rec := do(t, s.dataRoutes(), call{method: http.MethodPut, path: "/internal/data/" + kvKey,
		header: map[string]string{"If-None-Match": "*", "traceparent": incomingTrace},
		body:   map[string]any{"operationId": "op-t", "at": t0, "ringVersion": 1, "value": 1}})
	if rec.Code != http.StatusOK && rec.Code != http.StatusCreated {
		t.Fatalf("PUT: %d %s", rec.Code, rec.Body.String())
	}

	ended := sr.Ended()
	if len(ended) != 1 {
		t.Fatalf("recorded %d spans, want 1 server span", len(ended))
	}
	sp := ended[0]
	in := trace.SpanContextFromContext(propagation.TraceContext{}.Extract(context.Background(),
		propagation.MapCarrier{"traceparent": incomingTrace}))
	if sp.SpanKind() != trace.SpanKindServer || sp.Name() != "PUT /internal/data/{key}" {
		t.Errorf("span %q kind %v, want server span PUT /internal/data/{key}", sp.Name(), sp.SpanKind())
	}
	if sp.Parent().SpanID() != in.SpanID() || sp.SpanContext().TraceID() != in.TraceID() {
		t.Errorf("server span parent %s/%s, want the incoming %s/%s", sp.SpanContext().TraceID(), sp.Parent().SpanID(), in.TraceID(), in.SpanID())
	}
	n.mu.Lock()
	defer n.mu.Unlock()
	if len(n.spans) != 1 || n.spans[0].SpanID() != sp.SpanContext().SpanID() {
		t.Errorf("Apply ran under %v, want the server span %s", n.spans, sp.SpanContext().SpanID())
	}
}

// Management routes are not traced: kubelet probes would fill Jaeger.
func TestMgmtRoutesAreNotTraced(t *testing.T) {
	s, _, sr := tracedServer(t)
	do(t, s.mgmtRoutes(), call{method: http.MethodGet, path: "/healthz"})
	do(t, s.mgmtRoutes(), call{method: http.MethodGet, path: "/readyz"})
	if got := sr.Ended(); len(got) != 0 {
		t.Errorf("mgmt routes recorded %d spans, want 0", len(got))
	}
}
