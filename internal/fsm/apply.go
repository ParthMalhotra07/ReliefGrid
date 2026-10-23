package fsm

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/ParthMalhotra07/ReliefGrid/internal/store"
)

// txn is one Apply's SQLite transaction. Handlers check every precondition
// before their first write, so a rejection never leaves a partial change;
// the operation result is written in the same transaction either way.
type txn struct {
	ctx   context.Context
	tx    *sql.Tx
	index uint64
	at    int64
}

func (t *txn) apply(entry []byte) (store.Result, error) {
	cmd, err := decode(entry)
	if err != nil {
		return store.Result{Outcome: store.OutcomeInvalid, Reason: "malformed entry: " + err.Error(), AppliedIndex: t.index}, nil
	}
	id := cmd.meta.OperationID
	if id == "" {
		return store.Result{Outcome: store.OutcomeInvalid, Reason: "missing operationId", AppliedIndex: t.index}, nil
	}

	stored, hash, found, err := readOperation(t.ctx, t.tx, id)
	if err != nil {
		return store.Result{}, err
	}
	if found {
		if hash != cmd.hash {
			return store.Result{Outcome: store.OutcomeOperationIDReuse, OperationID: id, Key: stored.Key,
				Reason: "operationId already used with a different payload", AppliedIndex: t.index}, nil
		}
		stored.Replayed = true
		return stored, nil
	}

	t.at = cmd.meta.At
	var res store.Result
	if t.at <= 0 {
		res = invalid("", "missing command timestamp")
	} else if res, err = t.dispatch(cmd.body); err != nil {
		return store.Result{}, err
	}
	res.OperationID = id
	res.AppliedIndex = t.index
	body, err := json.Marshal(res)
	if err != nil {
		return store.Result{}, fmt.Errorf("encode result: %w", err)
	}
	if _, err := t.tx.ExecContext(t.ctx,
		`INSERT INTO operation_results (operation_id, payload_hash, key, result, applied_index) VALUES (?, ?, ?, ?, ?)`,
		id, cmd.hash, res.Key, string(body), t.index); err != nil {
		return store.Result{}, fmt.Errorf("store result: %w", err)
	}
	return res, nil
}

func (t *txn) dispatch(body any) (store.Result, error) {
	switch c := body.(type) {
	case *store.CreateEntity:
		return t.createEntity(c)
	case *store.CompareAndSet:
		return t.compareAndSet(c)
	case *store.TransitionRequest:
		return t.transitionRequest(c)
	case *store.RetryWorkflow:
		return t.retryWorkflow(c)
	case *store.TransitionResource:
		return t.transitionResource(c)
	case *store.TryClaim:
		return t.tryClaim(c)
	case *store.CloseAttempt:
		return t.closeAttempt(c)
	case *store.MarkOutboxPublished:
		return t.markOutboxPublished(c)
	}
	return store.Result{}, fmt.Errorf("no handler for %T", body)
}

func result(o store.Outcome, key string, version uint64) store.Result {
	return store.Result{Outcome: o, Key: key, Version: version}
}

func reject(o store.Outcome, key string, version uint64, format string, args ...any) store.Result {
	return store.Result{Outcome: o, Key: key, Version: version, Reason: fmt.Sprintf(format, args...)}
}

func invalid(key, format string, args ...any) store.Result {
	return reject(store.OutcomeInvalid, key, 0, format, args...)
}

func isDomainKey(key string) bool {
	return strings.HasPrefix(key, store.RequestKeyPrefix) || strings.HasPrefix(key, store.ResourceKeyPrefix)
}

func validType(t store.ResourceType) bool {
	switch t {
	case store.TypeWater, store.TypeFood, store.TypeMedicine, store.TypeTransportation, store.TypeShelter:
		return true
	}
	return false
}

func validRequestStatus(s store.RequestStatus) bool {
	switch s {
	case store.RequestPending, store.RequestWaiting, store.RequestMatching, store.RequestMatched,
		store.RequestInProgress, store.RequestRetrying, store.RequestCancelling, store.RequestFulfilling,
		store.RequestCancelled, store.RequestFulfilled:
		return true
	}
	return false
}

// validDisposition reports whether d is a status a closed attempt may
// release its resource to.
func validDisposition(d store.ResourceStatus) bool {
	return d == store.ResourceAvailable || d == store.ResourceConsumed || d == store.ResourceOffline
}

func validAttempt(a *store.Attempt) bool {
	return a != nil && a.AttemptID != "" && a.ResourceID != "" && a.Fingerprint != ""
}

// --- persistence helpers ---

func (t *txn) record(key string) (store.Record, bool, error) {
	return readRecord(t.ctx, t.tx, key)
}

func (t *txn) closure(resourceKey, attemptID string) (store.Closure, bool, error) {
	return readClosure(t.ctx, t.tx, resourceKey, attemptID)
}

func (t *txn) putRecord(rec store.Record) error {
	var typ, status, geohash string
	switch {
	case rec.Request != nil:
		typ, status, geohash = string(rec.Request.Type), string(rec.Request.Status), rec.Request.Geohash
	case rec.Resource != nil:
		typ, status, geohash = string(rec.Resource.Type), string(rec.Resource.Status), rec.Resource.Geohash
	}
	body, err := json.Marshal(rec)
	if err != nil {
		return fmt.Errorf("encode %q: %w", rec.Key, err)
	}
	deleted := 0
	if rec.Deleted {
		deleted = 1
	}
	_, err = t.tx.ExecContext(t.ctx,
		`INSERT OR REPLACE INTO records (key, kind, version, deleted, type, status, geohash, body, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		rec.Key, string(rec.Kind), rec.Version, deleted, typ, status, geohash, string(body), rec.UpdatedAt)
	if err != nil {
		return fmt.Errorf("write %q: %w", rec.Key, err)
	}
	return nil
}

func (t *txn) putClosure(c store.Closure) error {
	_, err := t.tx.ExecContext(t.ctx,
		`INSERT INTO attempt_closures (resource_key, attempt_id, request_id, fingerprint, reason, disposition, closed_at) VALUES (?, ?, ?, ?, ?, ?, ?)`,
		c.ResourceKey, c.AttemptID, c.RequestID, c.Fingerprint, c.Reason, string(c.Disposition), c.ClosedAt)
	if err != nil {
		return fmt.Errorf("write closure %s/%s: %w", c.ResourceKey, c.AttemptID, err)
	}
	return nil
}

// checkOutbox returns a rejection reason if events cannot be inserted.
func (t *txn) checkOutbox(events []store.OutboxEvent) (string, error) {
	seen := make(map[string]bool, len(events))
	for _, ev := range events {
		if ev.EventID == "" || ev.EventType == "" {
			return "outbox event needs eventId and eventType", nil
		}
		if seen[ev.EventID] {
			return fmt.Sprintf("duplicate outbox eventId %q", ev.EventID), nil
		}
		seen[ev.EventID] = true
		var n int
		if err := t.tx.QueryRowContext(t.ctx, `SELECT count(*) FROM outbox WHERE event_id = ?`, ev.EventID).Scan(&n); err != nil {
			return "", fmt.Errorf("check outbox %q: %w", ev.EventID, err)
		}
		if n > 0 {
			return fmt.Sprintf("outbox eventId %q already exists", ev.EventID), nil
		}
	}
	return "", nil
}

func (t *txn) putOutbox(events []store.OutboxEvent, key string, version uint64) error {
	for _, ev := range events {
		ev.EntityKey, ev.EntityVersion, ev.CreatedAt, ev.Published = key, version, t.at, false
		body, err := json.Marshal(ev)
		if err != nil {
			return fmt.Errorf("encode event %q: %w", ev.EventID, err)
		}
		if _, err := t.tx.ExecContext(t.ctx,
			`INSERT INTO outbox (event_id, entity_key, entity_version, event, published, created_at) VALUES (?, ?, ?, ?, 0, ?)`,
			ev.EventID, key, version, string(body), t.at); err != nil {
			return fmt.Errorf("write event %q: %w", ev.EventID, err)
		}
	}
	return nil
}

// commit writes rec at its next version with its outbox events.
func (t *txn) commit(rec store.Record, events []store.OutboxEvent) (store.Result, error) {
	rec.UpdatedAt = t.at
	if err := t.putRecord(rec); err != nil {
		return store.Result{}, err
	}
	if err := t.putOutbox(events, rec.Key, rec.Version); err != nil {
		return store.Result{}, err
	}
	return result(store.OutcomeOK, rec.Key, rec.Version), nil
}

// --- commands ---

func (t *txn) createEntity(c *store.CreateEntity) (store.Result, error) {
	rec := store.Record{Key: c.Key, Version: 1}
	switch {
	case c.Key == "":
		return invalid("", "missing key"), nil
	case strings.HasPrefix(c.Key, store.RequestKeyPrefix):
		r := c.Request
		if r == nil || c.Resource != nil || c.Value != nil {
			return invalid(c.Key, "a req: key takes exactly a request"), nil
		}
		if store.RequestKey(r.ID) != c.Key || !validType(r.Type) {
			return invalid(c.Key, "request id must match the key and type must be valid"), nil
		}
		if (r.Status != "" && r.Status != store.RequestPending) || r.CurrentAttempt != nil || r.AssignedResourceID != "" {
			return reject(store.OutcomeDomainConflict, c.Key, 0, "a request is created PENDING with no attempt"), nil
		}
		req := *r
		req.Status, req.CreatedAt, req.UpdatedAt = store.RequestPending, t.at, t.at
		rec.Kind, rec.Request = store.KindRequest, &req
	case strings.HasPrefix(c.Key, store.ResourceKeyPrefix):
		r := c.Resource
		if r == nil || c.Request != nil || c.Value != nil {
			return invalid(c.Key, "a res: key takes exactly a resource"), nil
		}
		if store.ResourceKey(r.ID) != c.Key || !validType(r.Type) || r.AvailableFrom >= r.AvailableUntil {
			return invalid(c.Key, "resource id must match the key, type must be valid and from < until"), nil
		}
		status := r.Status
		if status == "" {
			status = store.ResourceAvailable
		}
		if !validDisposition(status) || r.ActiveClaim != nil {
			return reject(store.OutcomeDomainConflict, c.Key, 0, "a resource is created AVAILABLE, OFFLINE or CONSUMED with no claim"), nil
		}
		res := *r
		res.Status, res.CreatedAt, res.UpdatedAt = status, t.at, t.at
		rec.Kind, rec.Resource = store.KindResource, &res
	default:
		if c.Value == nil || c.Request != nil || c.Resource != nil {
			return invalid(c.Key, "a generic key takes exactly a value"), nil
		}
		rec.Kind, rec.Value = store.KindKV, c.Value
	}

	if cur, ok, err := t.record(c.Key); err != nil {
		return store.Result{}, err
	} else if ok {
		return reject(store.OutcomeExists, c.Key, cur.Version, "key exists"), nil
	}
	if reason, err := t.checkOutbox(c.Outbox); err != nil || reason != "" {
		return invalid(c.Key, "%s", reason), err
	}
	return t.commit(rec, c.Outbox)
}

func (t *txn) compareAndSet(c *store.CompareAndSet) (store.Result, error) {
	if c.Key == "" || (c.Value == nil) == !c.Delete {
		return invalid(c.Key, "need a key and exactly one of value or delete"), nil
	}
	if isDomainKey(c.Key) {
		return reject(store.OutcomeDomainConflict, c.Key, 0, "domain records change only through domain commands"), nil
	}
	cur, ok, err := t.record(c.Key)
	if err != nil {
		return store.Result{}, err
	}
	if cur.Version != c.ExpectedVersion {
		return reject(store.OutcomeVersionConflict, c.Key, cur.Version, "expected version %d, have %d", c.ExpectedVersion, cur.Version), nil
	}
	if !ok && c.Delete {
		return reject(store.OutcomeNotFound, c.Key, 0, "nothing to delete"), nil
	}
	rec := store.Record{Key: c.Key, Kind: store.KindKV, Version: cur.Version + 1, Value: c.Value, Deleted: c.Delete}
	return t.commit(rec, nil)
}

// requestTransition reports whether from -> to is a legal request move
// (v3.1 §8, §17-§19).
func requestTransition(from, to store.RequestStatus) bool {
	switch to {
	case store.RequestMatching:
		return from == store.RequestPending || from == store.RequestWaiting || from == store.RequestMatching
	case store.RequestWaiting:
		return from == store.RequestPending || from == store.RequestMatching || from == store.RequestRetrying
	case store.RequestMatched, store.RequestRetrying:
		return from == store.RequestMatching
	case store.RequestInProgress:
		return from == store.RequestMatched
	case store.RequestCancelling:
		return from == store.RequestMatching || from == store.RequestRetrying ||
			from == store.RequestMatched || from == store.RequestInProgress
	case store.RequestCancelled:
		return from == store.RequestPending || from == store.RequestWaiting || from == store.RequestCancelling
	case store.RequestFulfilling:
		return from == store.RequestMatched || from == store.RequestInProgress
	case store.RequestFulfilled:
		return from == store.RequestFulfilling
	}
	return false
}

func (t *txn) loadRequest(key string) (store.Record, *store.Result, error) {
	rec, ok, err := t.record(key)
	if err != nil {
		return store.Record{}, nil, err
	}
	if !ok || rec.Deleted || rec.Request == nil {
		r := reject(store.OutcomeNotFound, key, rec.Version, "no request")
		return store.Record{}, &r, nil
	}
	return rec, nil, nil
}

func (t *txn) transitionRequest(c *store.TransitionRequest) (store.Result, error) {
	if !validRequestStatus(c.To) || !validRequestStatus(c.ExpectedStatus) {
		return invalid(c.Key, "unknown status"), nil
	}
	rec, miss, err := t.loadRequest(c.Key)
	if err != nil || miss != nil {
		return deref(miss), err
	}
	req := *rec.Request
	curAttempt := ""
	if req.CurrentAttempt != nil {
		curAttempt = req.CurrentAttempt.AttemptID
	}
	if rec.Version != c.ExpectedVersion || req.Status != c.ExpectedStatus || curAttempt != c.ExpectedAttemptID {
		return reject(store.OutcomeVersionConflict, c.Key, rec.Version,
			"expected v%d %s attempt %q, have v%d %s attempt %q",
			c.ExpectedVersion, c.ExpectedStatus, c.ExpectedAttemptID, rec.Version, req.Status, curAttempt), nil
	}
	if !requestTransition(req.Status, c.To) {
		return reject(store.OutcomeDomainConflict, c.Key, rec.Version, "illegal transition %s -> %s", req.Status, c.To), nil
	}

	switch c.To {
	case store.RequestMatching:
		if !validAttempt(c.Attempt) {
			return invalid(c.Key, "MATCHING needs a complete attempt"), nil
		}
		if c.Attempt.AttemptID == curAttempt {
			return reject(store.OutcomeDomainConflict, c.Key, rec.Version, "an attempt ID is never reused"), nil
		}
		if req.Status != store.RequestMatching {
			req.WorkflowGeneration++
		}
		a := *c.Attempt
		req.CurrentAttempt, req.NextAttemptAt = &a, 0
	case store.RequestWaiting:
		req.CurrentAttempt, req.NextAttemptAt = nil, c.NextAttemptAt
		req.RetryCount++
	case store.RequestMatched:
		req.AssignedResourceID = req.CurrentAttempt.ResourceID
	case store.RequestCancelling, store.RequestFulfilling:
		if !validDisposition(c.Disposition) {
			return invalid(c.Key, "%s needs a disposition of AVAILABLE, CONSUMED or OFFLINE", c.To), nil
		}
		req.Disposition = c.Disposition
	}
	if reason, err := t.checkOutbox(c.Outbox); err != nil || reason != "" {
		return invalid(c.Key, "%s", reason), err
	}
	req.Status = c.To
	if c.LastError != "" {
		req.LastError = c.LastError
	}
	req.UpdatedAt = t.at
	rec.Request, rec.Version = &req, rec.Version+1
	return t.commit(rec, c.Outbox)
}

func (t *txn) retryWorkflow(c *store.RetryWorkflow) (store.Result, error) {
	rec, miss, err := t.loadRequest(c.Key)
	if err != nil || miss != nil {
		return deref(miss), err
	}
	if rec.Version != c.ExpectedVersion {
		return reject(store.OutcomeVersionConflict, c.Key, rec.Version, "expected version %d, have %d", c.ExpectedVersion, rec.Version), nil
	}
	req := *rec.Request
	if req.Status == store.RequestCancelled || req.Status == store.RequestFulfilled {
		return reject(store.OutcomeDomainConflict, c.Key, rec.Version, "request is terminal"), nil
	}
	if reason, err := t.checkOutbox(c.Outbox); err != nil || reason != "" {
		return invalid(c.Key, "%s", reason), err
	}
	req.NextAttemptAt = c.NextAttemptAt
	req.RetryCount++
	if c.LastError != "" {
		req.LastError = c.LastError
	}
	req.UpdatedAt = t.at
	rec.Request, rec.Version = &req, rec.Version+1
	return t.commit(rec, c.Outbox)
}

func (t *txn) loadResource(key string) (store.Record, *store.Result, error) {
	rec, ok, err := t.record(key)
	if err != nil {
		return store.Record{}, nil, err
	}
	if !ok || rec.Deleted || rec.Resource == nil {
		r := reject(store.OutcomeNotFound, key, rec.Version, "no resource")
		return store.Record{}, &r, nil
	}
	return rec, nil, nil
}

func (t *txn) transitionResource(c *store.TransitionResource) (store.Result, error) {
	switch c.Status {
	case "", store.ResourceAvailable, store.ResourceOffline, store.ResourceAssigned, store.ResourceConsumed:
	default:
		return invalid(c.Key, "unknown status %q", c.Status), nil
	}
	if c.Status == "" && c.Availability == nil {
		return invalid(c.Key, "nothing to update"), nil
	}
	if c.Availability != nil && c.Availability.From >= c.Availability.Until {
		return invalid(c.Key, "availability needs from < until"), nil
	}
	rec, miss, err := t.loadResource(c.Key)
	if err != nil || miss != nil {
		return deref(miss), err
	}
	if rec.Version != c.ExpectedVersion {
		return reject(store.OutcomeVersionConflict, c.Key, rec.Version, "expected version %d, have %d", c.ExpectedVersion, rec.Version), nil
	}
	res := *rec.Resource
	switch {
	case c.Status == store.ResourceAssigned || c.Status == store.ResourceConsumed:
		return reject(store.OutcomeDomainConflict, c.Key, rec.Version, "%s is set only by claims and closures", c.Status), nil
	case c.Status != "" && res.Status == store.ResourceAssigned:
		return reject(store.OutcomeDomainConflict, c.Key, rec.Version, "an assigned resource cannot change status; close its attempt first"), nil
	}
	if reason, err := t.checkOutbox(c.Outbox); err != nil || reason != "" {
		return invalid(c.Key, "%s", reason), err
	}
	if c.Status != "" {
		res.Status = c.Status
	}
	if c.Availability != nil {
		res.AvailableFrom, res.AvailableUntil = c.Availability.From, c.Availability.Until
	}
	res.UpdatedAt = t.at
	rec.Resource, rec.Version = &res, rec.Version+1
	return t.commit(rec, c.Outbox)
}

func (t *txn) validClaimInput(resourceKey, requestID string, a *store.Attempt) bool {
	return requestID != "" && validAttempt(a) && store.ResourceKey(a.ResourceID) == resourceKey
}

func (t *txn) tryClaim(c *store.TryClaim) (store.Result, error) {
	if !t.validClaimInput(c.ResourceKey, c.RequestID, &c.Attempt) || !validType(c.Type) {
		return invalid(c.ResourceKey, "TryClaim needs a request, a complete attempt for this resource and a valid type"), nil
	}
	rec, miss, err := t.loadResource(c.ResourceKey)
	if err != nil || miss != nil {
		return deref(miss), err
	}
	res := *rec.Resource
	a := c.Attempt

	// 1. A closure for this attempt is permanent.
	if cl, ok, err := t.closure(c.ResourceKey, a.AttemptID); err != nil {
		return store.Result{}, err
	} else if ok {
		if cl.RequestID != c.RequestID || cl.Fingerprint != a.Fingerprint {
			return reject(store.OutcomeDomainConflict, c.ResourceKey, rec.Version, "attempt %s is bound to other inputs", a.AttemptID), nil
		}
		return reject(store.ClaimClosed, c.ResourceKey, rec.Version, "attempt closed: %s", cl.Reason), nil
	}
	if held := res.ActiveClaim; held != nil {
		// 2. Same attempt already holds it: success, no change.
		if held.AttemptID == a.AttemptID {
			if held.RequestID != c.RequestID || held.Fingerprint != a.Fingerprint {
				return reject(store.OutcomeDomainConflict, c.ResourceKey, rec.Version, "attempt %s is bound to other inputs", a.AttemptID), nil
			}
			return result(store.ClaimSuccess, c.ResourceKey, rec.Version), nil
		}
		// 4. Another attempt holds it: close this one.
		return t.rejectClaim(c, rec.Version, store.ClaimConflictClosed, "held by attempt "+held.AttemptID)
	}
	// 5. Ineligible by discrete rules.
	switch {
	case res.Status != store.ResourceAvailable:
		return t.rejectClaim(c, rec.Version, store.ClaimRejectedClosed, "resource is "+string(res.Status))
	case res.Type != c.Type:
		return t.rejectClaim(c, rec.Version, store.ClaimRejectedClosed, "type "+string(res.Type)+" does not serve "+string(c.Type))
	case t.at < res.AvailableFrom || t.at >= res.AvailableUntil:
		return t.rejectClaim(c, rec.Version, store.ClaimRejectedClosed, "outside availability window")
	}
	// 3. Available and eligible: claim it.
	res.Status = store.ResourceAssigned
	res.ActiveClaim = &store.Claim{RequestID: c.RequestID, AttemptID: a.AttemptID, Fingerprint: a.Fingerprint}
	res.UpdatedAt = t.at
	rec.Resource, rec.Version = &res, rec.Version+1
	committed, err := t.commit(rec, nil)
	committed.Outcome = store.ClaimSuccess
	return committed, err
}

// rejectClaim records the rejected attempt's closure; the resource itself is unchanged.
func (t *txn) rejectClaim(c *store.TryClaim, version uint64, o store.Outcome, reason string) (store.Result, error) {
	err := t.putClosure(store.Closure{ResourceKey: c.ResourceKey, AttemptID: c.Attempt.AttemptID, RequestID: c.RequestID,
		Fingerprint: c.Attempt.Fingerprint, Reason: reason, ClosedAt: t.at})
	return reject(o, c.ResourceKey, version, "%s", reason), err
}

func (t *txn) closeAttempt(c *store.CloseAttempt) (store.Result, error) {
	if !t.validClaimInput(c.ResourceKey, c.RequestID, &c.Attempt) || !validDisposition(c.Disposition) {
		return invalid(c.ResourceKey, "CloseAttempt needs a request, a complete attempt for this resource and a disposition of AVAILABLE, CONSUMED or OFFLINE"), nil
	}
	rec, miss, err := t.loadResource(c.ResourceKey)
	if err != nil || miss != nil {
		return deref(miss), err
	}
	res := *rec.Resource
	a := c.Attempt

	cl, closed, err := t.closure(c.ResourceKey, a.AttemptID)
	if err != nil {
		return store.Result{}, err
	}
	if closed && (cl.RequestID != c.RequestID || cl.Fingerprint != a.Fingerprint) {
		return reject(store.OutcomeDomainConflict, c.ResourceKey, rec.Version, "attempt %s is bound to other inputs", a.AttemptID), nil
	}
	held := res.ActiveClaim
	holds := held != nil && held.AttemptID == a.AttemptID
	if holds && (held.RequestID != c.RequestID || held.Fingerprint != a.Fingerprint) {
		return reject(store.OutcomeDomainConflict, c.ResourceKey, rec.Version, "attempt %s is bound to other inputs", a.AttemptID), nil
	}
	if !closed {
		cl = store.Closure{ResourceKey: c.ResourceKey, AttemptID: a.AttemptID, RequestID: c.RequestID,
			Fingerprint: a.Fingerprint, Reason: c.Reason, Disposition: c.Disposition, ClosedAt: t.at}
		if err := t.putClosure(cl); err != nil {
			return store.Result{}, err
		}
	}
	if !holds {
		// Fence only: never release somebody else's claim.
		return result(store.OutcomeOK, c.ResourceKey, rec.Version), nil
	}
	// A closure and a live claim for one attempt cannot coexist, so cl is
	// the one just written and carries this command's disposition.
	res.Status, res.ActiveClaim, res.UpdatedAt = cl.Disposition, nil, t.at
	rec.Resource, rec.Version = &res, rec.Version+1
	return t.commit(rec, nil)
}

func (t *txn) markOutboxPublished(c *store.MarkOutboxPublished) (store.Result, error) {
	if len(c.EventIDs) == 0 || len(c.EventIDs) > 100 {
		return invalid("", "mark 1 to 100 events"), nil
	}
	args := make([]any, len(c.EventIDs))
	for i, id := range c.EventIDs {
		args[i] = id
	}
	if _, err := t.tx.ExecContext(t.ctx,
		`UPDATE outbox SET published = 1 WHERE published = 0 AND event_id IN (`+placeholders(len(args))+`)`, args...); err != nil {
		return store.Result{}, fmt.Errorf("mark published: %w", err)
	}
	return result(store.OutcomeOK, "", 0), nil
}

func deref(r *store.Result) store.Result {
	if r == nil {
		return store.Result{}
	}
	return *r
}
