package obs_test

import (
	"context"
	"crypto/sha256"
	"fmt"
	"net/http"
	"net/http/httptest"
	"regexp"
	"testing"

	amqp "github.com/rabbitmq/amqp091-go"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"
	"go.opentelemetry.io/otel/trace/noop"

	"github.com/ParthMalhotra07/ReliefGrid/internal/obs"
)

// The event contracts' traceparent pattern (internal/mq/event.go).
var traceparentRE = regexp.MustCompile(`^00-[0-9a-f]{32}-[0-9a-f]{16}-[0-9a-f]{2}$`)

const incoming = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"

func remote(t *testing.T, tp string) trace.SpanContext {
	t.Helper()
	sc := trace.SpanContextFromContext(propagation.TraceContext{}.Extract(context.Background(),
		propagation.MapCarrier{"traceparent": tp}))
	if !sc.IsValid() {
		t.Fatalf("bad test traceparent %q", tp)
	}
	return sc
}

func recorder(t *testing.T) (*tracetest.SpanRecorder, *sdktrace.TracerProvider) {
	t.Helper()
	sr := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSampler(sdktrace.AlwaysSample()), sdktrace.WithSpanProcessor(sr))
	t.Cleanup(func() { _ = tp.Shutdown(context.Background()) })
	return sr, tp
}

func only(t *testing.T, sr *tracetest.SpanRecorder) sdktrace.ReadOnlySpan {
	t.Helper()
	ended := sr.Ended()
	if len(ended) != 1 {
		var names []string
		for _, s := range ended {
			names = append(names, s.Name())
		}
		t.Fatalf("ended spans = %q, want exactly one", names)
	}
	return ended[0]
}

func attr(s sdktrace.ReadOnlySpan, key attribute.Key) (attribute.Value, bool) {
	for _, kv := range s.Attributes() {
		if kv.Key == key {
			return kv.Value, true
		}
	}
	return attribute.Value{}, false
}

// restoreGlobals puts back the global provider and propagator Init changes.
func restoreGlobals(t *testing.T) {
	t.Helper()
	tp, prop := otel.GetTracerProvider(), otel.GetTextMapPropagator()
	t.Cleanup(func() {
		otel.SetTracerProvider(tp)
		otel.SetTextMapPropagator(prop)
	})
}

// --- Init ---------------------------------------------------------------

// With tracing off, Init installs only the W3C propagator: traceparent still
// flows through HTTP and AMQP, but no SDK runs and nothing is exported.
func TestInitDisabledInstallsOnlyTheW3CPropagator(t *testing.T) {
	restoreGlobals(t)
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator()) // the library default: propagates nothing
	shutdown, err := obs.Init(t.Context(), obs.Config{ServiceName: "test", Enabled: false})
	if err != nil {
		t.Fatalf("Init: %v", err)
	}
	if err := shutdown(t.Context()); err != nil {
		t.Errorf("shutdown: %v", err)
	}
	if _, ok := otel.GetTracerProvider().(*sdktrace.TracerProvider); ok {
		t.Error("tracing off installed the SDK tracer provider")
	}
	ctx := trace.ContextWithRemoteSpanContext(context.Background(), remote(t, incoming))
	carrier := propagation.MapCarrier{}
	otel.GetTextMapPropagator().Inject(ctx, carrier)
	if carrier["traceparent"] != incoming {
		t.Errorf("global propagator injected traceparent %q, want %q", carrier["traceparent"], incoming)
	}
}

// With tracing on, spans reach the exporter and the sampler comes from the
// standard OTEL_TRACES_SAMPLER / OTEL_TRACES_SAMPLER_ARG variables.
func TestInitEnabledExportsAndHonoursSamplerEnv(t *testing.T) {
	for _, tc := range []struct {
		ratio       string
		wantSampled bool
	}{{"1.0", true}, {"0", false}} {
		t.Run("ratio="+tc.ratio, func(t *testing.T) {
			restoreGlobals(t)
			t.Setenv("OTEL_TRACES_SAMPLER", "parentbased_traceidratio")
			t.Setenv("OTEL_TRACES_SAMPLER_ARG", tc.ratio)
			exp := keepSpans{tracetest.NewInMemoryExporter()}
			shutdown, err := obs.Init(t.Context(), obs.Config{ServiceName: "test", Enabled: true, Exporter: exp})
			if err != nil {
				t.Fatalf("Init: %v", err)
			}
			_, root := otel.Tracer("test").Start(context.Background(), "root")
			root.End()
			// parentbased: a sampled remote parent is followed whatever the ratio.
			ctx := trace.ContextWithRemoteSpanContext(context.Background(), remote(t, incoming))
			_, child := otel.Tracer("test").Start(ctx, "child")
			child.End()
			if err := shutdown(t.Context()); err != nil {
				t.Fatalf("shutdown: %v", err)
			}
			if root.SpanContext().IsSampled() != tc.wantSampled {
				t.Errorf("root sampled = %v, want %v", root.SpanContext().IsSampled(), tc.wantSampled)
			}
			if !child.SpanContext().IsSampled() {
				t.Error("child of a sampled remote parent was not sampled")
			}
			var names []string
			for _, s := range exp.GetSpans() {
				names = append(names, s.Name)
			}
			want := 1
			if tc.wantSampled {
				want = 2
			}
			if len(names) != want {
				t.Errorf("exported %q, want %d spans", names, want)
			}
		})
	}
}

// keepSpans keeps exported spans readable after shutdown (the in-memory
// exporter's Shutdown clears them).
type keepSpans struct{ *tracetest.InMemoryExporter }

func (keepSpans) Shutdown(context.Context) error { return nil }

// Tracing is on when an OTLP endpoint is configured and the SDK is not
// disabled; the Helm chart sets the endpoint only with tracing.enabled.
func TestEnabledFromEnv(t *testing.T) {
	for _, tc := range []struct {
		name                       string
		endpoint, traces, disabled string
		want                       bool
	}{
		{"nothing set", "", "", "", false},
		{"endpoint", "http://otel-collector:4318", "", "", true},
		{"traces endpoint", "", "http://otel-collector:4318", "", true},
		{"disabled wins", "http://otel-collector:4318", "", "true", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", tc.endpoint)
			t.Setenv("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT", tc.traces)
			t.Setenv("OTEL_SDK_DISABLED", tc.disabled)
			if got := obs.EnabledFromEnv(); got != tc.want {
				t.Errorf("EnabledFromEnv() = %v, want %v", got, tc.want)
			}
		})
	}
}

// --- Traceparent ----------------------------------------------------------

func TestTraceparentUsesTheSpanInContext(t *testing.T) {
	sr, tp := recorder(t)
	_ = sr
	ctx, span := tp.Tracer("test").Start(context.Background(), "s")
	defer span.End()
	sc := span.SpanContext()
	want := fmt.Sprintf("00-%s-%s-01", sc.TraceID(), sc.SpanID())
	if got := obs.Traceparent(ctx, "op-1"); got != want {
		t.Errorf("Traceparent = %q, want %q", got, want)
	}
}

// A remote parent alone (tracing off: the noop provider keeps it) is passed
// through unchanged, sampled flag included.
func TestTraceparentPassesARemoteParentThrough(t *testing.T) {
	for _, tp := range []string{incoming, "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-00"} {
		ctx := trace.ContextWithRemoteSpanContext(context.Background(), remote(t, tp))
		if got := obs.Traceparent(ctx, "op-1"); got != tp {
			t.Errorf("Traceparent = %q, want %q", got, tp)
		}
	}
}

// With no span context the value is derived from seed (the operationId), so
// a retry of the same operation writes the same outbox row. The derivation
// matches the one internal/api and internal/workflow use today, unsampled.
func TestTraceparentFallsBackToTheSeed(t *testing.T) {
	d := sha256.Sum256([]byte("op-1"))
	want := fmt.Sprintf("00-%x-%x-00", d[:16], d[16:24])
	got := obs.Traceparent(context.Background(), "op-1")
	if got != want {
		t.Errorf("Traceparent = %q, want %q", got, want)
	}
	if !traceparentRE.MatchString(got) {
		t.Errorf("Traceparent %q does not match the event contract", got)
	}
	if again := obs.Traceparent(context.Background(), "op-1"); again != got {
		t.Errorf("Traceparent is not deterministic: %q then %q", got, again)
	}
}

// --- HTTP -------------------------------------------------------------------

// The server wrapper continues the caller's trace and names the span by
// route pattern, never by raw path (bounded cardinality).
func TestHandlerContinuesTheIncomingTrace(t *testing.T) {
	sr, tp := recorder(t)
	mux := http.NewServeMux()
	var inner trace.SpanContext
	mux.HandleFunc("PUT /internal/data/{key}", func(w http.ResponseWriter, r *http.Request) {
		inner = trace.SpanContextFromContext(r.Context())
		w.WriteHeader(http.StatusOK)
	})
	h := obs.Handler(mux, obs.WithTracerProvider(tp))
	req := httptest.NewRequestWithContext(t.Context(), http.MethodPut, "/internal/data/kv:a", nil)
	req.Header.Set("traceparent", incoming)
	h.ServeHTTP(httptest.NewRecorder(), req)

	s := only(t, sr)
	parent := remote(t, incoming)
	if s.SpanKind() != trace.SpanKindServer {
		t.Errorf("kind = %v, want server", s.SpanKind())
	}
	if s.Parent().SpanID() != parent.SpanID() || s.SpanContext().TraceID() != parent.TraceID() {
		t.Errorf("span parent %s/%s, want %s/%s", s.SpanContext().TraceID(), s.Parent().SpanID(), parent.TraceID(), parent.SpanID())
	}
	if s.Name() != "PUT /internal/data/{key}" {
		t.Errorf("name = %q, want the route pattern", s.Name())
	}
	if inner.SpanID() != s.SpanContext().SpanID() {
		t.Error("the handler's context does not carry the server span")
	}
	if v, ok := attr(s, "http.response.status_code"); !ok || v.AsInt64() != 200 {
		t.Errorf("http.response.status_code = %v (%v), want 200", v.AsInt64(), ok)
	}
}

func TestHandlerWithoutTraceparentStartsARoot(t *testing.T) {
	sr, tp := recorder(t)
	h := obs.Handler(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {}), obs.WithTracerProvider(tp))
	h.ServeHTTP(httptest.NewRecorder(), httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/x", nil))
	if s := only(t, sr); s.Parent().IsValid() {
		t.Errorf("span has parent %s, want a root", s.Parent().SpanID())
	}
}

func TestHandlerMarks5xxAsError(t *testing.T) {
	sr, tp := recorder(t)
	h := obs.Handler(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
	}), obs.WithTracerProvider(tp))
	h.ServeHTTP(httptest.NewRecorder(), httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/x", nil))
	if s := only(t, sr); s.Status().Code != codes.Error {
		t.Errorf("status = %v, want Error", s.Status())
	}
}

// With tracing off (noop provider), the wrapper still hands the caller's
// trace context to the handler, so it reaches outbox rows unchanged.
func TestHandlerWithTracingOffPassesContextThrough(t *testing.T) {
	var got string
	h := obs.Handler(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got = obs.Traceparent(r.Context(), "op-1")
	}), obs.WithTracerProvider(noop.NewTracerProvider()))
	req := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/x", nil)
	req.Header.Set("traceparent", incoming)
	h.ServeHTTP(httptest.NewRecorder(), req)
	if got != incoming {
		t.Errorf("Traceparent in handler = %q, want %q", got, incoming)
	}
}

// The client wrapper records a client span and sends its context, so the
// server's span is that client span's child.
func TestTransportInjectsTheClientSpan(t *testing.T) {
	sr, tp := recorder(t)
	var sent string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		sent = r.Header.Get("traceparent")
	}))
	defer srv.Close()
	client := &http.Client{Transport: obs.Transport(http.DefaultTransport, obs.WithTracerProvider(tp))}

	ctx, parent := tp.Tracer("test").Start(context.Background(), "caller")
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, srv.URL+"/internal/ring", nil)
	resp, err := client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()
	parent.End()

	var cs sdktrace.ReadOnlySpan
	for _, s := range sr.Ended() {
		if s.SpanKind() == trace.SpanKindClient {
			cs = s
		}
	}
	if cs == nil {
		t.Fatal("no client span recorded")
	}
	if cs.Parent().SpanID() != parent.SpanContext().SpanID() {
		t.Errorf("client span parent = %s, want the caller %s", cs.Parent().SpanID(), parent.SpanContext().SpanID())
	}
	want := fmt.Sprintf("00-%s-%s-01", cs.SpanContext().TraceID(), cs.SpanContext().SpanID())
	if sent != want {
		t.Errorf("server received traceparent %q, want the client span's %q", sent, want)
	}
}

// --- AMQP -------------------------------------------------------------------

func TestAMQPHeadersRoundTrip(t *testing.T) {
	ctx := trace.ContextWithRemoteSpanContext(context.Background(), remote(t, incoming))
	headers := amqp.Table{"x-other": int32(1)}
	obs.InjectAMQP(ctx, headers)
	if headers["traceparent"] != incoming {
		t.Fatalf("headers[traceparent] = %v, want %q", headers["traceparent"], incoming)
	}
	if headers["x-other"] != int32(1) {
		t.Error("InjectAMQP dropped an unrelated header")
	}
	got := trace.SpanContextFromContext(obs.ExtractAMQP(context.Background(), headers))
	if got.TraceID() != remote(t, incoming).TraceID() || !got.IsRemote() {
		t.Errorf("extracted %v, want the injected remote context", got)
	}
}

// A missing, malformed or non-string header extracts nothing (and never
// panics): the consumer starts a root.
func TestExtractAMQPIgnoresBadHeaders(t *testing.T) {
	for _, h := range []amqp.Table{nil, {}, {"traceparent": "nope"}, {"traceparent": int64(5)}, {"traceparent": []byte(incoming)}} {
		if sc := trace.SpanContextFromContext(obs.ExtractAMQP(context.Background(), h)); sc.IsValid() {
			t.Errorf("ExtractAMQP(%v) = %v, want no span context", h, sc)
		}
	}
}

// The relay's first publish of an outbox row continues the trace stored in
// the row; a retry is separated in time, so it links to it instead.
func TestStartPublishChildFirstThenLinkOnRetry(t *testing.T) {
	stored := remote(t, incoming)
	for _, tc := range []struct {
		name  string
		retry bool
	}{{"first", false}, {"retry", true}} {
		t.Run(tc.name, func(t *testing.T) {
			sr, tp := recorder(t)
			ctx, span := obs.StartPublish(context.Background(), incoming, tc.retry, obs.WithTracerProvider(tp),
				obs.WithAttributes(attribute.String("messaging.message.id", "ev-1")))
			headers := amqp.Table{}
			obs.InjectAMQP(ctx, headers)
			span.End()

			s := only(t, sr)
			if s.SpanKind() != trace.SpanKindProducer {
				t.Errorf("kind = %v, want producer", s.SpanKind())
			}
			if v, _ := attr(s, "messaging.message.id"); v.AsString() != "ev-1" {
				t.Errorf("messaging.message.id = %q, want ev-1", v.AsString())
			}
			if want := fmt.Sprintf("00-%s-%s-01", s.SpanContext().TraceID(), s.SpanContext().SpanID()); headers["traceparent"] != want {
				t.Errorf("published traceparent %v, want the publish span's %q", headers["traceparent"], want)
			}
			if !tc.retry {
				if s.Parent().SpanID() != stored.SpanID() || s.SpanContext().TraceID() != stored.TraceID() {
					t.Errorf("first publish parent %s, want the stored %s", s.Parent().SpanID(), stored.SpanID())
				}
				if len(s.Links()) != 0 {
					t.Errorf("first publish has %d links, want 0", len(s.Links()))
				}
				return
			}
			if s.Parent().IsValid() || s.SpanContext().TraceID() == stored.TraceID() {
				t.Errorf("retry publish is a child of %s in trace %s, want a new root", s.Parent().SpanID(), s.SpanContext().TraceID())
			}
			if len(s.Links()) != 1 || s.Links()[0].SpanContext.SpanID() != stored.SpanID() {
				t.Errorf("retry publish links = %v, want one link to the stored context", s.Links())
			}
		})
	}
}

// A stored traceparent that does not parse starts a root with no link.
func TestStartPublishWithBadStoredContext(t *testing.T) {
	sr, tp := recorder(t)
	_, span := obs.StartPublish(context.Background(), "garbage", false, obs.WithTracerProvider(tp))
	span.End()
	if s := only(t, sr); s.Parent().IsValid() || len(s.Links()) != 0 {
		t.Errorf("parent %v, links %v; want a root with no links", s.Parent(), s.Links())
	}
}

// A first delivery continues the publisher's trace; a redelivery links to it.
func TestStartConsumeChildFirstThenLinkOnRedelivery(t *testing.T) {
	sent := remote(t, incoming)
	for _, redelivered := range []bool{false, true} {
		t.Run(fmt.Sprint("redelivered=", redelivered), func(t *testing.T) {
			sr, tp := recorder(t)
			ctx, span := obs.StartConsume(context.Background(), amqp.Table{"traceparent": incoming}, redelivered, obs.WithTracerProvider(tp))
			if trace.SpanContextFromContext(ctx).SpanID() != span.SpanContext().SpanID() {
				t.Error("returned context does not carry the consume span")
			}
			span.End()
			s := only(t, sr)
			if s.SpanKind() != trace.SpanKindConsumer {
				t.Errorf("kind = %v, want consumer", s.SpanKind())
			}
			if !redelivered {
				if s.Parent().SpanID() != sent.SpanID() || len(s.Links()) != 0 {
					t.Errorf("first delivery parent %s links %v, want child of %s with no links", s.Parent().SpanID(), s.Links(), sent.SpanID())
				}
				return
			}
			if s.Parent().IsValid() || len(s.Links()) != 1 || s.Links()[0].SpanContext.SpanID() != sent.SpanID() {
				t.Errorf("redelivery parent %v links %v, want a root linked to %s", s.Parent(), s.Links(), sent.SpanID())
			}
		})
	}
}
