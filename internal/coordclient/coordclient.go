// Package coordclient is store.Store over HTTP to the coordinator
// (contracts/internal.yaml). The API and matcher use it in place of
// internal/fsm/memstore with no other change; both pass internal/store/storetest.
//
// Error mapping follows contracts/README.md:
//
//   - A committed outcome is a Result with a nil error, whatever the HTTP
//     status: 200, or a 400/404/409/412 error body carrying "result".
//   - A read's 404 NOT_FOUND is store.ErrNotFound.
//   - 503 OUTCOME_UNKNOWN, and any failure after a mutation may have been
//     sent (lost connection, deadline, 5xx without a known code, a 200 that
//     cannot be decoded), is an *Error that matches store.ErrUnknownOutcome.
//     It is never a rejection: retry the same command or read the operation.
//   - NOT_LEADER, NO_LEADER and STORAGE_UNAVAILABLE (never proposed) are a
//     retryable *Error that does not match store.ErrUnknownOutcome.
//   - RING_VERSION_MISMATCH refreshes the ring from GET /internal/ring and,
//     if the coordinator's version changed, retries once with the same
//     operationId and payload; otherwise it is a retryable *Error.
//
// Reads (ReadRecord, ReadClosure, Scan, PendingOutbox, Operation) change
// nothing, so a failed read is a retryable *Error and never
// store.ErrUnknownOutcome, whatever the server said.
//
// WithReadConsistency makes every read explicit (#72): point reads send the
// mode, scans and outbox pages that name none use it, and an answer that
// was served weaker than asked, or does not say how it was served, is a
// retryable *Error rather than a result. Without it, point reads send no
// mode and are not checked, and a strong scan or outbox page refuses only
// an answer served stale.
//
// Mutations and scans carry the client's ringVersion. Every call has a
// deadline: the caller's, capped at the client's timeout. The cap covers the
// whole call, a ring refresh and its retry included.
package coordclient

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"maps"
	"net/http"
	"net/http/httptrace"
	"net/url"
	"os"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"

	"github.com/ParthMalhotra07/ReliefGrid/internal/auth"
	"github.com/ParthMalhotra07/ReliefGrid/internal/internalapi"
	"github.com/ParthMalhotra07/ReliefGrid/internal/obs"
	"github.com/ParthMalhotra07/ReliefGrid/internal/store"
)

// DefaultTimeout caps one call: the plan's 5 s HTTP budget (§14).
const DefaultTimeout = 5 * time.Second

// maxOutboxLimit is the contract's bound on one outbox page.
const maxOutboxLimit = 100

// maxBody bounds an answer the client reads.
const maxBody = 16 << 20

// Client is a store.Store backed by the coordinator. It is safe for
// concurrent use.
type Client struct {
	internalAuth     *auth.SenderConfig
	internalAuthFile string
	unknown          map[string]bool // unresolved operations; never evict an unknown outcome
	base             string
	http             *http.Client
	timeout          time.Duration
	// readMode is WithReadConsistency's mode; "" means none was set.
	readMode store.Consistency
	// tracerProvider records client spans (S4.4); nil uses the global one.
	tracerProvider trace.TracerProvider

	mu   sync.Mutex
	ring internalapi.Ring
}

var _ store.Store = (*Client)(nil)

type Option func(*Client)

// WithInternalAuth validates and wraps the final raw HTTP client before the
// initial ring request, regardless of option order. Never pass an already
// authenticated client; app and node clients must each wrap the raw transport.
func WithInternalAuth(cfg auth.SenderConfig) Option {
	return func(c *Client) { c.internalAuth = &cfg }
}

// WithInternalAuthFile loads a sender credential once in New, before any I/O.
// An empty path leaves authentication unchanged for callers of disabled clusters.
// When set, this file supplies the credential instead of WithInternalAuth.
func WithInternalAuthFile(path string) Option {
	return func(c *Client) { c.internalAuthFile = path }
}

// WithHTTPClient sets the HTTP client (default: a fresh http.Client).
func WithHTTPClient(c *http.Client) Option { return func(cl *Client) { cl.http = c } }

// WithTracerProvider records client spans with tp instead of the global
// provider (S4.4).
func WithTracerProvider(tp trace.TracerProvider) Option {
	return func(cl *Client) { cl.tracerProvider = tp }
}

// span starts the client span of one store call (S4.4), named by method,
// never by key; the coordinator receives its context on every request the
// call makes (a ring-mismatch retry included). end records err; a read's
// NOT_FOUND is an answer, not an error.
func (c *Client) span(ctx context.Context, op string) (context.Context, func(error)) {
	ctx, span := obs.Tracer(c.tracerProvider).Start(ctx, "coordclient."+op, trace.WithSpanKind(trace.SpanKindClient))
	return ctx, func(err error) {
		if err != nil && !errors.Is(err, store.ErrNotFound) { // committed rejections are results, not errors
			span.RecordError(err)
			span.SetStatus(codes.Error, err.Error())
		}
		span.End()
	}
}

// WithTimeout caps every call (default DefaultTimeout).
func WithTimeout(d time.Duration) Option { return func(cl *Client) { cl.timeout = d } }

// WithReadConsistency sets the mode of every read (#72): strong,
// linearizable or stale. New fails on any other mode.
func WithReadConsistency(m store.Consistency) Option { return func(cl *Client) { cl.readMode = m } }

// New connects to the coordinator at baseURL (e.g.
// http://coordinator.reliefgrid.svc.cluster.local:8080) and loads its ring.
func New(ctx context.Context, baseURL string, opts ...Option) (*Client, error) {
	c := &Client{base: strings.TrimRight(baseURL, "/"), http: &http.Client{}, timeout: DefaultTimeout}
	for _, o := range opts {
		o(c)
	}
	if c.internalAuthFile != "" {
		token, err := os.ReadFile(c.internalAuthFile)
		if err != nil {
			return nil, fmt.Errorf("coordclient: read sender token: %w", err)
		}
		c.internalAuth = &auth.SenderConfig{Token: string(token)}
	}
	if c.internalAuth != nil {
		client, err := auth.NewClient(c.http, *c.internalAuth)
		if err != nil {
			return nil, fmt.Errorf("coordclient: %w", err)
		}
		c.http = client
	}
	if c.readMode != "" {
		if _, err := consistency(c.readMode); err != nil {
			return nil, err
		}
	}
	ctx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()
	if _, err := c.refreshRing(ctx); err != nil {
		return nil, err
	}
	return c, nil
}

// refreshRing loads GET /internal/ring and returns its version.
func (c *Client) refreshRing(ctx context.Context) (uint64, error) {
	a, err := c.send(ctx, http.MethodGet, "/internal/ring", nil, nil, nil)
	if err != nil {
		return 0, fmt.Errorf("coordclient: load ring: %w", err)
	}
	var r internalapi.Ring
	if a.status != http.StatusOK || json.Unmarshal(a.body, &r) != nil || r.RingVersion == 0 || r.Shards <= 0 || r.Checksum == "" || r.Hash != internalapi.Xxhash64 {
		return 0, fmt.Errorf("coordclient: load ring: %d %q is not a ring", a.status, a.body)
	}
	c.mu.Lock()
	c.ring = r
	c.mu.Unlock()
	return r.RingVersion, nil
}

func (c *Client) ringVersion() uint64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.ring.RingVersion
}

// Ring returns the ring the client loaded from GET /internal/ring (its last
// refresh). ring.New over it verifies the checksum and maps keys to shards.
func (c *Client) Ring() internalapi.Ring {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.ring
}

// Shards returns the ring's shard count.
func (c *Client) Shards() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.ring.Shards
}

// Error is a failure that is not a committed outcome.
type Error struct {
	Status         int // HTTP status; 0 if no response arrived
	Code           internalapi.ErrorCode
	Message        string
	Retryable      bool
	OutcomeUnknown bool
	// read is set for a read; only a read's NOT_FOUND is store.ErrNotFound.
	read bool
}

func (e *Error) Error() string {
	return fmt.Sprintf("coordclient: %d %s: %s", e.Status, e.Code, e.Message)
}

// Is matches store.ErrUnknownOutcome when the outcome is unknown,
// store.ErrNotFound for a read that found nothing, and
// store.ErrRingVersionMismatch for a ring mismatch that a refresh did not fix
// (for a scan, a cursor from another ring).
func (e *Error) Is(target error) bool {
	switch target {
	case store.ErrUnknownOutcome:
		return e.OutcomeUnknown
	case store.ErrNotFound:
		return e.read && e.Code == internalapi.NOTFOUND
	case store.ErrRingVersionMismatch:
		return e.Code == internalapi.RINGVERSIONMISMATCH
	}
	return false
}

// answer is one HTTP response, buffered.
type answer struct {
	status int
	body   []byte
}

// transportError is a call that got no complete answer. sent reports that
// the request headers were written, so a mutation may have been proposed.
type transportError struct {
	err  error
	sent bool
}

func (e *transportError) Error() string { return e.err.Error() }
func (e *transportError) Unwrap() error { return e.err }

// send makes one request under ctx, which the calling method has already
// capped at the client's timeout.
func (c *Client) send(ctx context.Context, method, path string, query url.Values, header map[string]string, body []byte) (answer, error) {
	var wrote atomic.Bool
	ctx = httptrace.WithClientTrace(ctx, &httptrace.ClientTrace{WroteHeaders: func() { wrote.Store(true) }})
	u := c.base + path
	if len(query) > 0 {
		u += "?" + query.Encode()
	}
	var rd io.Reader = http.NoBody
	if body != nil {
		rd = bytes.NewReader(body)
	}
	req, err := http.NewRequestWithContext(ctx, method, u, rd)
	if err != nil {
		return answer{}, err
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	for k, v := range header {
		req.Header.Set(k, v)
	}
	obs.InjectHTTP(ctx, req.Header)
	resp, err := c.http.Do(req)
	if err != nil {
		return answer{}, &transportError{err: err, sent: wrote.Load()}
	}
	defer func() { _ = resp.Body.Close() }()
	b, err := io.ReadAll(io.LimitReader(resp.Body, maxBody))
	if err != nil {
		return answer{}, &transportError{err: fmt.Errorf("read answer: %w", err), sent: true}
	}
	return answer{status: resp.StatusCode, body: b}, nil
}

// mutation is one store command as a contract route.
type mutation struct {
	op           string // store method, the span's name
	method, path string
	query        url.Values        // without ringVersion
	header       map[string]string // conditional headers
	body         any               // the command; nil for none (DELETE)
	drop         []string          // command fields carried in the path, query or headers
}

// mutate sends m and maps the answer (contracts/README.md). On
// RING_VERSION_MISMATCH it refreshes the ring and, if the version changed,
// retries once with the same command and the new ringVersion. The client's
// timeout caps all of it.
func (c *Client) mutate(ctx context.Context, m mutation) (res store.Result, err error) {
	// Keep uncertainty across retries on this client. Durable callers must also
	// retain their intent across process/client replacement (invariant 5).
	operationID := m.query.Get("operationId")
	if m.body != nil {
		raw, _ := json.Marshal(m.body)
		var meta struct {
			OperationID string `json:"operationId"`
		}
		_ = json.Unmarshal(raw, &meta)
		operationID = meta.OperationID
	}
	identity := m.path + "\x00" + operationID
	defer func() {
		if operationID == "" {
			return
		}
		c.mu.Lock()
		defer c.mu.Unlock()
		if err == nil {
			delete(c.unknown, identity)
			return
		}
		if c.unknown[identity] && !errors.Is(err, store.ErrUnknownOutcome) {
			err = &Error{Code: internalapi.OUTCOMEUNKNOWN, Message: "earlier attempt unresolved; retry the same operationId: " + err.Error(), Retryable: true, OutcomeUnknown: true}
		}
		if errors.Is(err, store.ErrUnknownOutcome) {
			if c.unknown == nil {
				c.unknown = make(map[string]bool)
			}
			c.unknown[identity] = true
		}
	}()
	ctx, end := c.span(ctx, m.op)
	defer func() { end(err) }()
	ctx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()
	for retried := false; ; retried = true {
		rv := c.ringVersion()
		res, err := c.mutateOnce(ctx, m, rv)
		var ce *Error
		if retried || !errors.As(err, &ce) || ce.Code != internalapi.RINGVERSIONMISMATCH {
			return res, err
		}
		if now, rerr := c.refreshRing(ctx); rerr != nil || now == rv {
			return res, err
		}
	}
}

func (c *Client) mutateOnce(ctx context.Context, m mutation, rv uint64) (store.Result, error) {
	query := url.Values{}
	for k, v := range m.query {
		query[k] = v
	}
	var body []byte
	if m.body == nil {
		query.Set("ringVersion", strconv.FormatUint(rv, 10))
	} else {
		var err error
		if body, err = commandBody(m.body, rv, m.drop); err != nil {
			return store.Result{}, &Error{Code: internalapi.INVALID, Message: err.Error()}
		}
	}
	a, err := c.send(ctx, m.method, m.path, query, m.header, body)
	if err != nil {
		var te *transportError
		if errors.As(err, &te) && te.sent {
			return store.Result{}, &Error{Code: internalapi.OUTCOMEUNKNOWN, Message: "no answer after sending: " + err.Error(), Retryable: true, OutcomeUnknown: true}
		}
		return store.Result{}, &Error{Message: "not sent: " + err.Error(), Retryable: true}
	}
	if a.status == http.StatusOK {
		var resp internalapi.MutationResponse
		if err := json.Unmarshal(a.body, &resp); err != nil || resp.Result.Outcome == "" {
			return store.Result{}, &Error{Status: a.status, Code: internalapi.OUTCOMEUNKNOWN, Message: fmt.Sprintf("undecodable answer %q", a.body),
				Retryable: true, OutcomeUnknown: true}
		}
		return resp.Result, nil
	}
	var e internalapi.Error
	decoded := json.Unmarshal(a.body, &e) == nil
	out := &Error{Status: a.status, Code: e.Code, Message: e.Message}
	switch {
	case a.status == http.StatusUnauthorized:
		if !decoded || e.Code != internalapi.UNAUTHORIZED || e.Result != nil || e.OutcomeUnknown {
			out.Code = internalapi.OUTCOMEUNKNOWN
			out.Retryable = true
			out.OutcomeUnknown = true
		}
	case a.status >= 400 && a.status < 500 && decoded && e.Result != nil:
		return *e.Result, nil // a committed rejection
	case a.status == http.StatusConflict && e.Code == internalapi.RINGVERSIONMISMATCH:
		out.Retryable = true
	case a.status >= 400 && a.status < 500:
		// Refused before proposing: malformed, unauthorised or a bad route.
	case a.status == http.StatusServiceUnavailable && (e.Code == internalapi.NOTLEADER || e.Code == internalapi.NOLEADER || e.Code == internalapi.STORAGEUNAVAILABLE):
		out.Retryable = true
	default: // OUTCOME_UNKNOWN, any other 5xx, or a status the contract does not have
		out.Retryable, out.OutcomeUnknown = true, true
		if out.Code == "" {
			out.Code = internalapi.OUTCOMEUNKNOWN
		}
	}
	return store.Result{}, out
}

// commandBody is cmd's JSON without the fields in drop, plus ringVersion.
func commandBody(cmd any, rv uint64, drop []string) ([]byte, error) {
	raw, err := json.Marshal(cmd)
	if err != nil {
		return nil, err
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(raw, &fields); err != nil {
		return nil, err
	}
	for _, k := range drop {
		delete(fields, k)
	}
	fields["ringVersion"] = json.RawMessage(strconv.FormatUint(rv, 10))
	return json.Marshal(fields)
}

// read sends a GET and decodes a 200 into out. Every failure is an *Error
// that is never ErrUnknownOutcome; a 404 NOT_FOUND matches ErrNotFound.
// The client's timeout caps the call, refresh and retry included.
func (c *Client) read(ctx context.Context, op, path string, query url.Values, out any) (err error) {
	ctx, end := c.span(ctx, op)
	defer func() { end(err) }()
	ctx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()
	for retried := false; ; retried = true {
		rv := c.ringVersion()
		if query.Has("ringVersion") {
			query.Set("ringVersion", strconv.FormatUint(rv, 10))
		}
		err := c.readOnce(ctx, path, query, out)
		var ce *Error
		if retried || !errors.As(err, &ce) || ce.Code != internalapi.RINGVERSIONMISMATCH {
			return err
		}
		if now, rerr := c.refreshRing(ctx); rerr != nil || now == rv {
			return err
		}
	}
}

func (c *Client) readOnce(ctx context.Context, path string, query url.Values, out any) error {
	a, err := c.send(ctx, http.MethodGet, path, query, nil, nil)
	if err != nil {
		return &Error{Message: err.Error(), Retryable: true, read: true}
	}
	if a.status == http.StatusOK {
		if err := json.Unmarshal(a.body, out); err != nil {
			return &Error{Status: a.status, Message: fmt.Sprintf("undecodable answer %q", a.body), Retryable: true, read: true}
		}
		return nil
	}
	var e internalapi.Error
	_ = json.Unmarshal(a.body, &e)
	out2 := &Error{Status: a.status, Code: e.Code, Message: e.Message, read: true}
	switch {
	case a.status == http.StatusNotFound && e.Code == internalapi.NOTFOUND:
	case a.status == http.StatusConflict && e.Code == internalapi.RINGVERSIONMISMATCH:
		out2.Retryable = true
	case a.status >= 500:
		out2.Retryable = true // a read changed nothing: never unknown
		if out2.Code == internalapi.OUTCOMEUNKNOWN || out2.Code == "" {
			out2.Code = internalapi.NOLEADER
		}
	}
	if out2.Message == "" {
		out2.Message = strings.TrimSpace(string(a.body))
	}
	return out2
}

func invalid(format string, args ...any) error {
	return &Error{Code: internalapi.INVALID, Message: fmt.Sprintf(format, args...)}
}

// entityID returns key's ID after prefix, or an error if key has another
// prefix: the route is chosen by the key's kind.
func entityID(key, prefix string) (string, error) {
	id, ok := strings.CutPrefix(key, prefix)
	if !ok || id == "" {
		return "", invalid("key %q is not a %s key", key, prefix)
	}
	return id, nil
}

// consistency is the wire form of a read mode: the zero value is sent as
// an explicit strong, so the call never depends on a server default.
func consistency(m store.Consistency) (string, error) {
	switch m {
	case "", store.ConsistencyStrong:
		return string(store.ConsistencyStrong), nil
	case store.ConsistencyLinearizable, store.ConsistencyStale:
		return string(m), nil
	}
	return "", invalid("consistency %q must be strong, linearizable or stale", m)
}

// queryMode is the mode a scan or outbox page is read in: its own, else the
// client's, else strong.
func (c *Client) queryMode(m store.Consistency) (string, error) {
	if m == "" {
		m = c.readMode
	}
	return consistency(m)
}

// servedAs refuses an answer served weaker than asked: a stale miss is only
// a hint, so it must never stand in for an authoritative result. strong and
// linearizable both satisfy either. With a client read mode set, an answer
// must also say how it was served; without one, only a stale answer is
// refused (the behaviour before #72).
func (c *Client) servedAs(asked, served string) error {
	if asked == string(store.ConsistencyStale) {
		return nil
	}
	ok := served != string(store.ConsistencyStale)
	if c.readMode != "" {
		ok = served == string(store.ConsistencyStrong) || served == string(store.ConsistencyLinearizable)
	}
	if ok {
		return nil
	}
	return &Error{Status: http.StatusOK, Message: fmt.Sprintf("asked for a %s read, served %q", asked, served), Retryable: true, read: true}
}

// pointRead is a read of one key, closure or operation. With a client read
// mode it sends the mode and checks the answer's read info against it.
func (c *Client) pointRead(ctx context.Context, op, path string, query url.Values, resp any, info *internalapi.ReadInfo) error {
	if c.readMode != "" {
		if query == nil {
			query = url.Values{}
		}
		query.Set("consistency", string(c.readMode))
	}
	if err := c.read(ctx, op, path, query, resp); err != nil {
		return err
	}
	if c.readMode == "" {
		return nil
	}
	return c.servedAs(string(c.readMode), string(info.Consistency))
}

func (c *Client) CreateEntity(ctx context.Context, cmd store.CreateEntity) (store.Result, error) {
	return c.mutate(ctx, mutation{op: "CreateEntity", method: http.MethodPut, path: "/internal/data/" + url.PathEscape(cmd.Key),
		header: map[string]string{"If-None-Match": "*"}, body: cmd, drop: []string{"key"}})
}

func (c *Client) CompareAndSet(ctx context.Context, cmd store.CompareAndSet) (store.Result, error) {
	ifMatch := map[string]string{"If-Match": `"` + strconv.FormatUint(cmd.ExpectedVersion, 10) + `"`}
	path := "/internal/data/" + url.PathEscape(cmd.Key)
	if cmd.Delete {
		return c.mutate(ctx, mutation{op: "CompareAndSet", method: http.MethodDelete, path: path, header: ifMatch,
			query: url.Values{"operationId": {cmd.OperationID}, "at": {strconv.FormatInt(cmd.At, 10)}}})
	}
	return c.mutate(ctx, mutation{op: "CompareAndSet", method: http.MethodPut, path: path, header: ifMatch, body: cmd,
		drop: []string{"key", "expectedVersion", "delete"}})
}

func (c *Client) TransitionRequest(ctx context.Context, cmd store.TransitionRequest) (store.Result, error) {
	id, err := entityID(cmd.Key, store.RequestKeyPrefix)
	if err != nil {
		return store.Result{}, err
	}
	return c.mutate(ctx, mutation{op: "TransitionRequest", method: http.MethodPost, path: "/internal/requests/" + url.PathEscape(id) + "/transition", body: cmd, drop: []string{"key"}})
}

func (c *Client) AgeRequest(ctx context.Context, cmd store.AgeRequest) (store.Result, error) {
	id, err := entityID(cmd.Key, store.RequestKeyPrefix)
	if err != nil {
		return store.Result{}, err
	}
	return c.mutate(ctx, mutation{op: "AgeRequest", method: http.MethodPost, path: "/internal/requests/" + url.PathEscape(id) + "/age", body: cmd, drop: []string{"key"}})
}

func (c *Client) RetryWorkflow(ctx context.Context, cmd store.RetryWorkflow) (store.Result, error) {
	id, err := entityID(cmd.Key, store.RequestKeyPrefix)
	if err != nil {
		return store.Result{}, err
	}
	return c.mutate(ctx, mutation{op: "RetryWorkflow", method: http.MethodPost, path: "/internal/requests/" + url.PathEscape(id) + "/retry", body: cmd, drop: []string{"key"}})
}

func (c *Client) TransitionResource(ctx context.Context, cmd store.TransitionResource) (store.Result, error) {
	id, err := entityID(cmd.Key, store.ResourceKeyPrefix)
	if err != nil {
		return store.Result{}, err
	}
	return c.mutate(ctx, mutation{op: "TransitionResource", method: http.MethodPost, path: "/internal/resources/" + url.PathEscape(id) + "/transition", body: cmd, drop: []string{"key"}})
}

func (c *Client) TryClaim(ctx context.Context, cmd store.TryClaim) (store.Result, error) {
	id, err := entityID(cmd.ResourceKey, store.ResourceKeyPrefix)
	if err != nil {
		return store.Result{}, err
	}
	return c.mutate(ctx, mutation{op: "TryClaim", method: http.MethodPost, path: "/internal/resources/" + url.PathEscape(id) + "/claim", body: cmd, drop: []string{"resourceKey"}})
}

func (c *Client) CloseAttempt(ctx context.Context, cmd store.CloseAttempt) (store.Result, error) {
	id, err := entityID(cmd.ResourceKey, store.ResourceKeyPrefix)
	if err != nil {
		return store.Result{}, err
	}
	return c.mutate(ctx, mutation{op: "CloseAttempt", method: http.MethodPost, path: "/internal/resources/" + url.PathEscape(id) + "/close-attempt", body: cmd, drop: []string{"resourceKey"}})
}

func (c *Client) MarkOutboxPublished(ctx context.Context, cmd store.MarkOutboxPublished) (store.Result, error) {
	return c.mutate(ctx, mutation{op: "MarkOutboxPublished", method: http.MethodPost, path: "/internal/outbox/published",
		query: url.Values{"shard": {strconv.Itoa(cmd.Shard)}}, body: cmd, drop: []string{"shard"}})
}

func (c *Client) ReadRecord(ctx context.Context, key string) (store.Record, error) {
	rec, _, err := c.ReadRecordInfo(ctx, key)
	return rec, err
}

// ReadRecordInfo is ReadRecord plus how the read was served: mode, replica
// and applied position.
func (c *Client) ReadRecordInfo(ctx context.Context, key string) (store.Record, internalapi.ReadInfo, error) {
	var resp internalapi.RecordResponse
	if err := c.pointRead(ctx, "ReadRecordInfo", "/internal/data/"+url.PathEscape(key), nil, &resp, &resp.Read); err != nil {
		return store.Record{}, internalapi.ReadInfo{}, err
	}
	return resp.Record, resp.Read, nil
}

func (c *Client) ReadClosure(ctx context.Context, resourceKey, attemptID string) (store.Closure, error) {
	id, err := entityID(resourceKey, store.ResourceKeyPrefix)
	if err != nil {
		return store.Closure{}, err
	}
	var resp internalapi.ClosureResponse
	if err := c.pointRead(ctx, "ReadClosure", "/internal/resources/"+url.PathEscape(id)+"/closures/"+url.PathEscape(attemptID), nil, &resp, &resp.Read); err != nil {
		return store.Closure{}, err
	}
	return resp.Closure, nil
}

// Scan is one bounded page from every shard q.Cursor still lists. A shard
// the coordinator could not read is in FailedShards with Partial set; an
// answer that is missing a shard without saying so is made partial here too
// (CONTRIBUTING.md, Invariants, 7), and that shard is kept in NextCursor at
// the position it had.
func (c *Client) Scan(ctx context.Context, q store.ScanQuery) (store.ScanResult, error) {
	if q.Limit < 1 || q.Limit > store.MaxScanLimit {
		return store.ScanResult{}, invalid("scan limit %d must be 1 to %d records per shard", q.Limit, store.MaxScanLimit)
	}
	mode, err := c.queryMode(q.Consistency)
	if err != nil {
		return store.ScanResult{}, err
	}
	expected := map[int]string{} // shards to be read -> the position they are read after
	for s := range c.Shards() {
		expected[s] = ""
	}
	query := url.Values{"kind": {string(q.Kind)}, "limit": {strconv.Itoa(q.Limit)}, "ringVersion": {""}, "consistency": {mode}}
	if q.Cursor != "" {
		cur, err := store.DecodeScanCursor(q.Cursor)
		if err != nil {
			return store.ScanResult{}, invalid("scan cursor: %v", err)
		}
		expected = cur.After
		query.Set("cursor", q.Cursor)
	}
	if q.Type != "" {
		query.Set("type", string(q.Type))
	}
	if len(q.Statuses) > 0 {
		query["statuses"] = q.Statuses
	}
	if len(q.GeohashPrefixes) > 0 {
		query["geohashPrefixes"] = q.GeohashPrefixes
	}
	var resp internalapi.ScanResponse
	if err := c.read(ctx, "Scan", "/internal/scan", query, &resp); err != nil {
		return store.ScanResult{}, err
	}
	if err := c.servedAs(mode, string(resp.Consistency)); err != nil {
		return store.ScanResult{}, err
	}
	out := store.ScanResult{Records: resp.Records, Partial: resp.Partial, Positions: resp.Positions}
	if resp.FailedShards != nil {
		out.FailedShards = *resp.FailedShards
	}
	if resp.NextCursor != nil {
		out.NextCursor = *resp.NextCursor
	}
	answered := map[int]bool{}
	for _, p := range resp.Positions {
		answered[p.Shard] = true
	}
	var missing []int
	for _, s := range slices.Sorted(maps.Keys(expected)) {
		if !answered[s] && !slices.Contains(out.FailedShards, s) {
			out.Partial = true
			out.FailedShards = append(out.FailedShards, s)
			missing = append(missing, s)
		}
	}
	if len(missing) > 0 {
		next := store.ScanCursor{RingVersion: resp.RingVersion, Query: store.ScanQueryHash(q), After: map[int]string{}}
		if out.NextCursor != "" {
			cur, err := store.DecodeScanCursor(out.NextCursor)
			if err != nil {
				return store.ScanResult{}, &Error{Status: http.StatusOK, Message: "coordinator sent a malformed nextCursor: " + err.Error(), Retryable: true, read: true}
			}
			next = cur
		}
		for _, s := range missing {
			next.After[s] = expected[s]
		}
		out.NextCursor = store.EncodeScanCursor(next)
	}
	return out, nil
}

// PendingOutbox long-polls inside the client's timeout: the server's wait,
// the reads around it and any ring refresh share the one budget.
func (c *Client) PendingOutbox(ctx context.Context, q store.OutboxQuery) ([]store.OutboxEvent, error) {
	if q.Shard < 0 || q.Shard >= c.Shards() {
		return nil, invalid("no shard %d", q.Shard)
	}
	if q.Limit < 1 || q.Limit > maxOutboxLimit {
		return nil, invalid("outbox limit %d must be 1 to %d", q.Limit, maxOutboxLimit)
	}
	if q.Wait < 0 || q.Wait > store.MaxOutboxWait || q.Wait%time.Second != 0 {
		return nil, invalid("outbox wait %s must be whole seconds, 0 to %s", q.Wait, store.MaxOutboxWait)
	}
	mode, err := c.queryMode(q.Consistency)
	if err != nil {
		return nil, err
	}
	query := url.Values{"shard": {strconv.Itoa(q.Shard)}, "limit": {strconv.Itoa(q.Limit)}, "consistency": {mode}}
	if q.Wait > 0 {
		query.Set("wait", strconv.Itoa(int(q.Wait/time.Second)))
	}
	var resp internalapi.OutboxPage
	if err := c.read(ctx, "PendingOutbox", "/internal/outbox", query, &resp); err != nil {
		return nil, err
	}
	if err := c.servedAs(mode, string(resp.Read.Consistency)); err != nil {
		return nil, err
	}
	return resp.Events, nil
}

func (c *Client) Operation(ctx context.Context, key, operationID string) (store.Result, error) {
	var resp internalapi.OperationResponse
	if err := c.pointRead(ctx, "Operation", "/internal/operations/"+url.PathEscape(operationID), url.Values{"key": {key}}, &resp, &resp.Read); err != nil {
		return store.Result{}, err
	}
	return resp.Result, nil
}
