// Package api implements the public REST contract over store.Store. Production
// uses coordclient; tests use the same FSM through memstore.
package api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/ParthMalhotra07/ReliefGrid/internal/api/gen"
	"github.com/ParthMalhotra07/ReliefGrid/internal/coordclient"
	"github.com/ParthMalhotra07/ReliefGrid/internal/store"
	"github.com/ParthMalhotra07/ReliefGrid/internal/workflow"
)

// Config describes one trusted dispatcher namespace. CallerNamespace is set by
// the operator, never taken from an untrusted request header. Multi-user public
// deployment needs authenticated principal-to-namespace routing first.
type Config struct {
	Store             store.Store
	Now               func() time.Time
	CallerNamespace   string
	Budget            time.Duration
	CallTimeout       time.Duration
	HeartbeatInterval time.Duration
	StreamToken       string // optional protection of the SSE projection
	Events            *EventHub
	StreamContext     context.Context // ends streams during API shutdown
}

type Server struct {
	cfg     Config
	mux     *http.ServeMux
	matcher *workflow.Matcher
}

func New(cfg Config) (*Server, error) {
	if cfg.Store == nil || cfg.Store.Shards() < 1 || strings.TrimSpace(cfg.CallerNamespace) == "" {
		return nil, fmt.Errorf("api: a store and trusted caller namespace are required")
	}
	if cfg.Now == nil {
		cfg.Now = time.Now
	}
	if cfg.Budget == 0 {
		cfg.Budget = 5 * time.Second
	}
	if cfg.CallTimeout == 0 {
		cfg.CallTimeout = 4 * time.Second
	}
	if cfg.HeartbeatInterval == 0 {
		cfg.HeartbeatInterval = 15 * time.Second
	}
	if cfg.Budget <= 0 || cfg.CallTimeout <= 0 || cfg.CallTimeout > cfg.Budget || cfg.HeartbeatInterval <= 0 {
		return nil, fmt.Errorf("api: invalid request, call or heartbeat budget")
	}
	if cfg.Events == nil {
		cfg.Events = NewEventHub()
	}
	if cfg.StreamContext == nil {
		cfg.StreamContext = context.Background()
	}
	m, err := workflow.New(workflow.Config{
		Store: cfg.Store, Now: cfg.Now, ScanLimit: store.MaxScanLimit,
		CallTimeout: cfg.CallTimeout, WorkTimeout: cfg.Budget,
	})
	if err != nil {
		return nil, fmt.Errorf("api matcher: %w", err)
	}
	s := &Server{cfg: cfg, mux: http.NewServeMux(), matcher: m}
	s.mux.HandleFunc("POST /api/v1/requests", s.endpoint(s.createRequest))
	s.mux.HandleFunc("GET /api/v1/requests", s.endpoint(s.listRequests))
	s.mux.HandleFunc("GET /api/v1/requests/{requestId}", s.endpoint(s.getRequest))
	s.mux.HandleFunc("PUT /api/v1/requests/{requestId}/status", s.endpoint(s.updateRequest))
	s.mux.HandleFunc("POST /api/v1/resources", s.endpoint(s.createResource))
	s.mux.HandleFunc("GET /api/v1/resources", s.endpoint(s.listResources))
	s.mux.HandleFunc("PUT /api/v1/resources/{resourceId}/status", s.endpoint(s.updateResource))
	s.mux.HandleFunc("POST /api/v1/match/{requestId}", s.endpoint(s.matchRequest))
	s.mux.HandleFunc("GET /api/v1/events", s.endpoint(s.events))
	s.mux.HandleFunc("/", s.endpoint(func(http.ResponseWriter, *http.Request) error {
		return issue(404, "NOT_FOUND", "route not found")
	}))
	return s, nil
}

type correlationKey struct{}

func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	ctx := context.WithValue(r.Context(), correlationKey{}, uuid.New())
	// SSE is intentionally long lived. Its individual writes have deadlines.
	if r.URL.Path != "/api/v1/events" {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, s.cfg.Budget)
		defer cancel()
		if err := http.NewResponseController(w).SetWriteDeadline(time.Now().Add(s.cfg.Budget)); err != nil && !errors.Is(err, http.ErrNotSupported) {
			s.endpoint(func(http.ResponseWriter, *http.Request) error {
				return fmt.Errorf("public response deadline: %w", err)
			})(w, r.WithContext(ctx))
			return
		}
	}
	inner := r.WithContext(ctx)
	s.mux.ServeHTTP(w, inner)
	// Report the matched route to a wrapping obs.Handler, which names its
	// span by r.Pattern (never the raw path); the mux set it on our copy.
	r.Pattern = inner.Pattern
}

type problem struct {
	status    int
	code      string
	message   string
	retryable bool
	unknown   bool
	details   []gen.ErrorDetail
}

func (p *problem) Error() string { return p.code + ": " + p.message }

func issue(status int, code, message string) *problem {
	return &problem{status: status, code: code, message: message}
}

func invalid(field, message string) *problem {
	p := issue(400, "INVALID", "request validation failed")
	p.details = []gen.ErrorDetail{{Field: field, Message: message}}
	return p
}

func unavailable(code string, unknown bool) *problem {
	p := issue(503, code, "storage is temporarily unavailable")
	p.retryable, p.unknown = true, unknown
	return p
}

func remoteError(err error, mutation bool) error {
	if !mutation && errors.Is(err, store.ErrNotFound) {
		return issue(404, "NOT_FOUND", "entity not found")
	}
	var ce *coordclient.Error
	if errors.As(err, &ce) {
		code := string(ce.Code)
		if mutation && ce.OutcomeUnknown {
			return unavailable("OUTCOME_UNKNOWN", true)
		}
		switch code {
		case "NO_LEADER", "NOT_LEADER", "RING_VERSION_MISMATCH", "STORAGE_UNAVAILABLE":
			return unavailable(code, false)
		}
		if mutation {
			// coordclient distinguishes a refused/not-sent call from a lost
			// answer after sending. Do not turn its known refusal into an
			// unknown write merely because no committed Result accompanies it.
			switch code {
			case "INVALID":
				return issue(400, code, "storage rejected the command")
			case "NOT_FOUND":
				return issue(404, code, "entity not found")
			case "DOMAIN_CONFLICT", "VERSION_CONFLICT", "OPERATION_ID_REUSED":
				return issue(409, code, "storage refused the command")
			}
			p := issue(503, "STORAGE_UNAVAILABLE", "storage did not accept the command")
			p.retryable = ce.Retryable
			return p
		}
	}
	if errors.Is(err, store.ErrRingVersionMismatch) {
		return unavailable("RING_VERSION_MISMATCH", false)
	}
	if mutation {
		return unavailable("OUTCOME_UNKNOWN", true)
	}
	return unavailable("STORAGE_UNAVAILABLE", false)
}

func committed(result store.Result) error {
	switch result.Outcome {
	case store.OutcomeOK:
		return nil
	case store.OutcomeNotFound:
		return issue(404, "NOT_FOUND", "entity not found")
	case store.OutcomeVersionConflict:
		return issue(409, "VERSION_CONFLICT", "entity changed; reread before retrying")
	case store.OutcomeOperationIDReuse:
		return issue(409, "OPERATION_ID_REUSED", "operation ID has a different payload")
	case store.OutcomeInvalid:
		return issue(400, "INVALID", "invalid domain command")
	default:
		return issue(409, "DOMAIN_CONFLICT", "operation is not allowed in the current state")
	}
}

func invoke[T any](ctx context.Context, timeout time.Duration, fn func(context.Context) (T, error)) (T, error) {
	callCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	return fn(callCtx)
}

func (s *Server) read(ctx context.Context, key string) (store.Record, error) {
	rec, err := invoke(ctx, s.cfg.CallTimeout, func(ctx context.Context) (store.Record, error) {
		return s.cfg.Store.ReadRecord(ctx, key)
	})
	if err != nil {
		return store.Record{}, remoteError(err, false)
	}
	if rec.Deleted {
		return store.Record{}, issue(404, "NOT_FOUND", "entity not found")
	}
	return rec, nil
}

func (s *Server) endpoint(fn func(http.ResponseWriter, *http.Request) error) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if err := fn(w, r); err != nil {
			var p *problem
			if !errors.As(err, &p) {
				slog.ErrorContext(r.Context(), "public API failed", "error", err)
				p = unavailable("STORAGE_UNAVAILABLE", false)
			}
			if p.retryable {
				w.Header().Set("Retry-After", "1")
			}
			body := gen.ErrorEnvelope{
				Code: gen.ErrorEnvelopeCode(p.code), Message: p.message,
				Retryable: p.retryable, OutcomeUnknown: p.unknown,
				CorrelationId: r.Context().Value(correlationKey{}).(uuid.UUID),
			}
			if len(p.details) > 0 {
				body.Details = &p.details
			}
			writeJSON(w, p.status, body)
		}
	}
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	body, err := json.Marshal(value)
	if err != nil {
		slog.Error("encode public response", "error", err)
		http.Error(w, "response encoding failed", 500)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if _, err := w.Write(append(body, '\n')); err != nil {
		slog.Debug("client response interrupted", "error", err)
	}
}
