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
	"net/http"
	"net/http/httptrace"
	"net/url"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ParthMalhotra07/ReliefGrid/internal/internalapi"
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
	base    string
	http    *http.Client
	timeout time.Duration

	mu   sync.Mutex
	ring internalapi.Ring
}

var _ store.Store = (*Client)(nil)

type Option func(*Client)

// WithHTTPClient sets the HTTP client (default: a fresh http.Client).
func WithHTTPClient(c *http.Client) Option { return func(cl *Client) { cl.http = c } }

// WithTimeout caps every call (default DefaultTimeout).
func WithTimeout(d time.Duration) Option { return func(cl *Client) { cl.timeout = d } }

// New connects to the coordinator at baseURL (e.g.
// http://coordinator.reliefgrid.svc.cluster.local:8080) and loads its ring.
func New(ctx context.Context, baseURL string, opts ...Option) (*Client, error) {
	c := &Client{base: strings.TrimRight(baseURL, "/"), http: &http.Client{}, timeout: DefaultTimeout}
	for _, o := range opts {
		o(c)
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

// Is matches store.ErrUnknownOutcome when the outcome is unknown, and
// store.ErrNotFound for a read that found nothing.
func (e *Error) Is(target error) bool {
	switch target {
	case store.ErrUnknownOutcome:
		return e.OutcomeUnknown
	case store.ErrNotFound:
		return e.read && e.Code == internalapi.NOTFOUND
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
func (c *Client) mutate(ctx context.Context, m mutation) (store.Result, error) {
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
func (c *Client) read(ctx context.Context, path string, query url.Values, out any) error {
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

func (c *Client) CreateEntity(ctx context.Context, cmd store.CreateEntity) (store.Result, error) {
	return c.mutate(ctx, mutation{method: http.MethodPut, path: "/internal/data/" + url.PathEscape(cmd.Key),
		header: map[string]string{"If-None-Match": "*"}, body: cmd, drop: []string{"key"}})
}

func (c *Client) CompareAndSet(ctx context.Context, cmd store.CompareAndSet) (store.Result, error) {
	ifMatch := map[string]string{"If-Match": `"` + strconv.FormatUint(cmd.ExpectedVersion, 10) + `"`}
	path := "/internal/data/" + url.PathEscape(cmd.Key)
	if cmd.Delete {
		return c.mutate(ctx, mutation{method: http.MethodDelete, path: path, header: ifMatch,
			query: url.Values{"operationId": {cmd.OperationID}, "at": {strconv.FormatInt(cmd.At, 10)}}})
	}
	return c.mutate(ctx, mutation{method: http.MethodPut, path: path, header: ifMatch, body: cmd,
		drop: []string{"key", "expectedVersion", "delete"}})
}

func (c *Client) TransitionRequest(ctx context.Context, cmd store.TransitionRequest) (store.Result, error) {
	id, err := entityID(cmd.Key, store.RequestKeyPrefix)
	if err != nil {
		return store.Result{}, err
	}
	return c.mutate(ctx, mutation{method: http.MethodPost, path: "/internal/requests/" + url.PathEscape(id) + "/transition", body: cmd, drop: []string{"key"}})
}

func (c *Client) RetryWorkflow(ctx context.Context, cmd store.RetryWorkflow) (store.Result, error) {
	id, err := entityID(cmd.Key, store.RequestKeyPrefix)
	if err != nil {
		return store.Result{}, err
	}
	return c.mutate(ctx, mutation{method: http.MethodPost, path: "/internal/requests/" + url.PathEscape(id) + "/retry", body: cmd, drop: []string{"key"}})
}

func (c *Client) TransitionResource(ctx context.Context, cmd store.TransitionResource) (store.Result, error) {
	id, err := entityID(cmd.Key, store.ResourceKeyPrefix)
	if err != nil {
		return store.Result{}, err
	}
	return c.mutate(ctx, mutation{method: http.MethodPost, path: "/internal/resources/" + url.PathEscape(id) + "/transition", body: cmd, drop: []string{"key"}})
}

func (c *Client) TryClaim(ctx context.Context, cmd store.TryClaim) (store.Result, error) {
	id, err := entityID(cmd.ResourceKey, store.ResourceKeyPrefix)
	if err != nil {
		return store.Result{}, err
	}
	return c.mutate(ctx, mutation{method: http.MethodPost, path: "/internal/resources/" + url.PathEscape(id) + "/claim", body: cmd, drop: []string{"resourceKey"}})
}

func (c *Client) CloseAttempt(ctx context.Context, cmd store.CloseAttempt) (store.Result, error) {
	id, err := entityID(cmd.ResourceKey, store.ResourceKeyPrefix)
	if err != nil {
		return store.Result{}, err
	}
	return c.mutate(ctx, mutation{method: http.MethodPost, path: "/internal/resources/" + url.PathEscape(id) + "/close-attempt", body: cmd, drop: []string{"resourceKey"}})
}

func (c *Client) MarkOutboxPublished(ctx context.Context, cmd store.MarkOutboxPublished) (store.Result, error) {
	return c.mutate(ctx, mutation{method: http.MethodPost, path: "/internal/outbox/published",
		query: url.Values{"shard": {strconv.Itoa(cmd.Shard)}}, body: cmd, drop: []string{"shard"}})
}

func (c *Client) ReadRecord(ctx context.Context, key string) (store.Record, error) {
	var resp internalapi.RecordResponse
	if err := c.read(ctx, "/internal/data/"+url.PathEscape(key), nil, &resp); err != nil {
		return store.Record{}, err
	}
	return resp.Record, nil
}

func (c *Client) ReadClosure(ctx context.Context, resourceKey, attemptID string) (store.Closure, error) {
	id, err := entityID(resourceKey, store.ResourceKeyPrefix)
	if err != nil {
		return store.Closure{}, err
	}
	var resp internalapi.ClosureResponse
	if err := c.read(ctx, "/internal/resources/"+url.PathEscape(id)+"/closures/"+url.PathEscape(attemptID), nil, &resp); err != nil {
		return store.Closure{}, err
	}
	return resp.Closure, nil
}

// Scan is one bounded page per shard. A shard the coordinator could not
// read is in FailedShards with Partial set; an answer that is missing a
// shard without saying so is made partial here too (CONTRIBUTING.md,
// Invariants, 7).
func (c *Client) Scan(ctx context.Context, q store.ScanQuery) (store.ScanResult, error) {
	if q.Limit < 1 || q.Limit > store.MaxScanLimit {
		return store.ScanResult{}, invalid("scan limit %d must be 1 to %d records per shard", q.Limit, store.MaxScanLimit)
	}
	query := url.Values{"kind": {string(q.Kind)}, "limit": {strconv.Itoa(q.Limit)}, "ringVersion": {""}}
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
	if err := c.read(ctx, "/internal/scan", query, &resp); err != nil {
		return store.ScanResult{}, err
	}
	out := store.ScanResult{Records: resp.Records, Partial: resp.Partial, Positions: resp.Positions}
	if resp.FailedShards != nil {
		out.FailedShards = *resp.FailedShards
	}
	answered := map[int]bool{}
	for _, p := range resp.Positions {
		answered[p.Shard] = true
	}
	for s := range c.Shards() {
		if !answered[s] && !slices.Contains(out.FailedShards, s) {
			out.Partial = true
			out.FailedShards = append(out.FailedShards, s)
		}
	}
	return out, nil
}

func (c *Client) PendingOutbox(ctx context.Context, shard, limit int) ([]store.OutboxEvent, error) {
	if shard < 0 || shard >= c.Shards() {
		return nil, invalid("no shard %d", shard)
	}
	if limit < 1 || limit > maxOutboxLimit {
		return nil, invalid("outbox limit %d must be 1 to %d", limit, maxOutboxLimit)
	}
	var resp internalapi.OutboxPage
	if err := c.read(ctx, "/internal/outbox", url.Values{"shard": {strconv.Itoa(shard)}, "limit": {strconv.Itoa(limit)}}, &resp); err != nil {
		return nil, err
	}
	return resp.Events, nil
}

func (c *Client) Operation(ctx context.Context, key, operationID string) (store.Result, error) {
	var resp internalapi.OperationResponse
	if err := c.read(ctx, "/internal/operations/"+url.PathEscape(operationID), url.Values{"key": {key}}, &resp); err != nil {
		return store.Result{}, err
	}
	return resp.Result, nil
}
