package obs

import (
	"context"
	"crypto/sha256"
	"fmt"
	"net/http"
	"os"
	"strings"

	amqp "github.com/rabbitmq/amqp091-go"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
)

// scope names the instrumentation in every span this repository records.
const scope = "github.com/ParthMalhotra07/ReliefGrid"

// propagator is W3C Trace Context, the only format ReliefGrid carries (HTTP
// headers, outbox rows, AMQP headers). The helpers use it directly, so they
// propagate even if Init was never called.
var propagator propagation.TextMapPropagator = propagation.TraceContext{}

// Config configures Init.
type Config struct {
	// ServiceName is service.name unless OTEL_SERVICE_NAME overrides it.
	ServiceName string
	// Enabled installs the SDK and an exporter; false installs only the
	// W3C propagator.
	Enabled bool
	// Exporter replaces the OTLP/HTTP exporter (tests, benchmarks); nil
	// exports with OTLP/HTTP as the standard OTEL_EXPORTER_OTLP_* variables
	// say.
	Exporter sdktrace.SpanExporter
}

// Init installs the W3C propagator globally and, with tracing on, the SDK
// tracer provider: batched export, the sampler from OTEL_TRACES_SAMPLER and
// OTEL_TRACES_SAMPLER_ARG (parentbased_always_on if unset). shutdown flushes
// and stops it; with tracing off it does nothing.
func Init(ctx context.Context, cfg Config) (shutdown func(context.Context) error, err error) {
	otel.SetTextMapPropagator(propagator)
	if !cfg.Enabled {
		return func(context.Context) error { return nil }, nil
	}
	exp := cfg.Exporter
	if exp == nil {
		if exp, err = otlptracehttp.New(ctx); err != nil {
			return nil, fmt.Errorf("obs: OTLP exporter: %w", err)
		}
	}
	// Later options win: OTEL_SERVICE_NAME and OTEL_RESOURCE_ATTRIBUTES
	// override cfg.ServiceName.
	res, err := resource.New(ctx, resource.WithTelemetrySDK(),
		resource.WithAttributes(attribute.String("service.name", cfg.ServiceName)), resource.WithFromEnv())
	if err != nil {
		return nil, fmt.Errorf("obs: resource: %w", err)
	}
	tp := sdktrace.NewTracerProvider(sdktrace.WithBatcher(exp), sdktrace.WithResource(res))
	otel.SetTracerProvider(tp)
	return tp.Shutdown, nil
}

// EnabledFromEnv reports whether tracing is configured: an OTLP endpoint is
// set (OTEL_EXPORTER_OTLP_ENDPOINT or OTEL_EXPORTER_OTLP_TRACES_ENDPOINT)
// and OTEL_SDK_DISABLED is not true. The Helm charts set the endpoint only
// with tracing.enabled.
func EnabledFromEnv() bool {
	if strings.EqualFold(strings.TrimSpace(os.Getenv("OTEL_SDK_DISABLED")), "true") {
		return false
	}
	return os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT") != "" || os.Getenv("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT") != ""
}

// Tracer is tp's tracer for ReliefGrid spans; nil uses the global provider
// (a no-op unless Init enabled tracing).
func Tracer(tp trace.TracerProvider) trace.Tracer {
	if tp == nil {
		tp = otel.GetTracerProvider()
	}
	return tp.Tracer(scope)
}

// Option configures a helper.
type Option func(*options)

type options struct {
	tp    trace.TracerProvider
	attrs []attribute.KeyValue
}

func apply(opts []Option) options {
	var o options
	for _, f := range opts {
		f(&o)
	}
	return o
}

// WithTracerProvider uses tp instead of the global provider; nil keeps the
// global one.
func WithTracerProvider(tp trace.TracerProvider) Option { return func(o *options) { o.tp = tp } }

// WithAttributes adds attributes to the span started.
func WithAttributes(kv ...attribute.KeyValue) Option {
	return func(o *options) { o.attrs = append(o.attrs, kv...) }
}

// Traceparent is the W3C traceparent of ctx's span context: the current
// span's, or a remote parent's passed through unchanged (as with tracing
// off). With neither, it is derived from seed (an operationId) and
// unsampled, so a retried operation writes the same outbox row; this is the
// derivation internal/api and internal/workflow used before S4.4.
func Traceparent(ctx context.Context, seed string) string {
	if sc := trace.SpanContextFromContext(ctx); sc.IsValid() {
		return fmt.Sprintf("00-%s-%s-%s", sc.TraceID(), sc.SpanID(), sc.TraceFlags())
	}
	d := sha256.Sum256([]byte(seed))
	return fmt.Sprintf("00-%x-%x-00", d[:16], d[16:24])
}

// InjectHTTP writes ctx's trace context into h.
func InjectHTTP(ctx context.Context, h http.Header) {
	propagator.Inject(ctx, propagation.HeaderCarrier(h))
}

// Handler wraps h in a server span that continues the caller's trace
// (traceparent). The span is named by the ServeMux pattern that served the
// request, never by its raw path; h's context carries the span. A 5xx is
// an error.
func Handler(h http.Handler, opts ...Option) http.Handler {
	o := apply(opts)
	tr := Tracer(o.tp)
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx := propagator.Extract(r.Context(), propagation.HeaderCarrier(r.Header))
		attrs := append([]attribute.KeyValue{attribute.String("http.request.method", r.Method)}, o.attrs...)
		ctx, span := tr.Start(ctx, r.Method, trace.WithSpanKind(trace.SpanKindServer), trace.WithAttributes(attrs...))
		defer span.End()
		sw := &statusWriter{ResponseWriter: w}
		r = r.WithContext(ctx)
		h.ServeHTTP(sw, r) // a ServeMux sets r.Pattern
		if r.Pattern != "" {
			name := r.Pattern
			if !strings.Contains(name, " ") {
				name = r.Method + " " + name
			}
			span.SetName(name)
			span.SetAttributes(attribute.String("http.route", r.Pattern))
		}
		status := sw.status
		if status == 0 {
			status = http.StatusOK
		}
		span.SetAttributes(attribute.Int("http.response.status_code", status))
		if status >= 500 {
			span.SetStatus(codes.Error, http.StatusText(status))
		}
	})
}

// statusWriter records the status code. Unwrap keeps http.ResponseController
// (Flush, Hijack, deadlines) working through it.
type statusWriter struct {
	http.ResponseWriter
	status int
}

func (w *statusWriter) WriteHeader(code int) {
	if w.status == 0 {
		w.status = code
	}
	w.ResponseWriter.WriteHeader(code)
}

func (w *statusWriter) Write(b []byte) (int, error) {
	if w.status == 0 {
		w.status = http.StatusOK
	}
	return w.ResponseWriter.Write(b)
}

func (w *statusWriter) Flush() { _ = http.NewResponseController(w.ResponseWriter).Flush() }

func (w *statusWriter) Unwrap() http.ResponseWriter { return w.ResponseWriter }

// Transport wraps base (nil: http.DefaultTransport) so each request is a
// client span whose context the request carries as traceparent. The span
// ends when the response headers arrive; a 4xx/5xx or transport failure is
// an error.
func Transport(base http.RoundTripper, opts ...Option) http.RoundTripper {
	if base == nil {
		base = http.DefaultTransport
	}
	o := apply(opts)
	return &transport{base: base, tr: Tracer(o.tp), attrs: o.attrs}
}

type transport struct {
	base  http.RoundTripper
	tr    trace.Tracer
	attrs []attribute.KeyValue
}

func (t *transport) RoundTrip(req *http.Request) (*http.Response, error) {
	attrs := append([]attribute.KeyValue{attribute.String("http.request.method", req.Method),
		attribute.String("server.address", req.URL.Host)}, t.attrs...)
	ctx, span := t.tr.Start(req.Context(), req.Method, trace.WithSpanKind(trace.SpanKindClient), trace.WithAttributes(attrs...))
	defer span.End()
	req = req.Clone(ctx)
	InjectHTTP(ctx, req.Header)
	resp, err := t.base.RoundTrip(req)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return nil, err
	}
	span.SetAttributes(attribute.Int("http.response.status_code", resp.StatusCode))
	if resp.StatusCode >= 400 {
		span.SetStatus(codes.Error, http.StatusText(resp.StatusCode))
	}
	return resp, nil
}

// amqpCarrier reads and writes string values of AMQP headers.
type amqpCarrier amqp.Table

func (c amqpCarrier) Get(key string) string {
	s, _ := c[key].(string)
	return s
}

func (c amqpCarrier) Set(key, value string) { c[key] = value }

func (c amqpCarrier) Keys() []string {
	keys := make([]string, 0, len(c))
	for k := range c {
		keys = append(keys, k)
	}
	return keys
}

// InjectAMQP writes ctx's trace context into headers, which must not be nil.
func InjectAMQP(ctx context.Context, headers amqp.Table) {
	propagator.Inject(ctx, amqpCarrier(headers))
}

// ExtractAMQP returns ctx with the remote trace context in headers; a
// missing, malformed or non-string traceparent adds nothing.
func ExtractAMQP(ctx context.Context, headers amqp.Table) context.Context {
	return propagator.Extract(ctx, amqpCarrier(headers))
}

func parseTraceparent(tp string) trace.SpanContext {
	return trace.SpanContextFromContext(propagator.Extract(context.Background(), propagation.MapCarrier{"traceparent": tp}))
}

// StartPublish starts the producer span for publishing an outbox row whose
// stored traceparent is stored. The first publish continues the stored
// trace; a retry (separated from it by a failure and a wait) is a span of
// its own, under ctx, linked to it (plan §25). Inject the returned context
// into the message (InjectAMQP).
func StartPublish(ctx context.Context, stored string, retry bool, opts ...Option) (context.Context, trace.Span) {
	return startMessaging(ctx, "publish", trace.SpanKindProducer, parseTraceparent(stored), retry, opts)
}

// StartConsume starts the consumer span for a delivery. A first delivery
// continues the publisher's trace (its traceparent header); a redelivery
// is a span of its own, under ctx, linked to it.
func StartConsume(ctx context.Context, headers amqp.Table, redelivered bool, opts ...Option) (context.Context, trace.Span) {
	sc := trace.SpanContextFromContext(ExtractAMQP(context.Background(), headers))
	return startMessaging(ctx, "consume", trace.SpanKindConsumer, sc, redelivered, opts)
}

func startMessaging(ctx context.Context, name string, kind trace.SpanKind, from trace.SpanContext, link bool, opts []Option) (context.Context, trace.Span) {
	o := apply(opts)
	start := []trace.SpanStartOption{trace.WithSpanKind(kind), trace.WithAttributes(o.attrs...)}
	if from.IsValid() {
		if link {
			start = append(start, trace.WithLinks(trace.Link{SpanContext: from}))
		} else {
			ctx = trace.ContextWithRemoteSpanContext(ctx, from)
		}
	}
	return Tracer(o.tp).Start(ctx, name, start...)
}
