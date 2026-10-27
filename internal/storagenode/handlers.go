package storagenode

import (
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/hashicorp/raft"

	"github.com/ParthMalhotra07/ReliefGrid/internal/internalapi"
	"github.com/ParthMalhotra07/ReliefGrid/internal/raftstore"
	"github.com/ParthMalhotra07/ReliefGrid/internal/store"
)

// Mutations decode the body strictly (unknown fields are 400), check
// ringVersion (409 RING_VERSION_MISMATCH, not proposed) and key ownership
// (400 INVALID, not proposed), build the store command with the key taken
// from the path, Apply it under the request timeout and write the result
// with writeResult or the failure with writeFailure.
//
// Reads parse ?consistency (strong default, linearizable served as strong
// until S4.1, stale opt-in), check key ownership (a foreign key is 400,
// never a 404 "absent") and run through node.Read.

const (
	maxBodyBytes  = 1 << 20
	maxScanLimit  = store.MaxScanLimit // records per shard per scan
	maxOutboxWait = 1                  // seconds a pending-outbox read may long-poll
	// outboxRereadMargin is kept, beyond the first read's duration, for a
	// long-poll's second read.
	outboxRereadMargin = 100 * time.Millisecond
)

func (s *server) putData(w http.ResponseWriter, r *http.Request) {
	key := r.PathValue("key")
	inm, im := r.Header.Get("If-None-Match"), r.Header.Get("If-Match")
	if (inm == "") == (im == "") {
		writeError(w, http.StatusBadRequest, internalapi.INVALID, "exactly one of If-None-Match: * or If-Match is required")
		return
	}
	if inm != "" && inm != "*" {
		writeError(w, http.StatusBadRequest, internalapi.INVALID, "If-None-Match must be *")
		return
	}
	var expected uint64
	if im != "" {
		v, err := internalapi.ParseETag(im)
		if err != nil {
			writeError(w, http.StatusBadRequest, internalapi.INVALID, "If-Match: "+err.Error())
			return
		}
		expected = v
	}
	var body internalapi.PutDataBody
	if !s.decode(w, r, &body) || !s.checkRing(w, body.RingVersion) || !s.ownsKey(w, key) {
		return
	}
	m := store.Meta{OperationID: body.OperationId, At: body.At}
	if inm == "*" {
		s.apply(w, r, store.CreateEntity{Meta: m, Key: key, Request: body.Request, Resource: body.Resource,
			Value: deref(body.Value), Outbox: deref(body.Outbox)}, true)
		return
	}
	if body.Request != nil || body.Resource != nil || body.Outbox != nil {
		writeError(w, http.StatusBadRequest, internalapi.INVALID, "If-Match writes a generic value only; domain records change through their routes")
		return
	}
	s.apply(w, r, store.CompareAndSet{Meta: m, Key: key, ExpectedVersion: expected, Value: deref(body.Value)}, true)
}

func (s *server) deleteData(w http.ResponseWriter, r *http.Request) {
	key := r.PathValue("key")
	expected, err := internalapi.ParseETag(r.Header.Get("If-Match"))
	if err != nil {
		writeError(w, http.StatusBadRequest, internalapi.INVALID, "If-Match is required: "+err.Error())
		return
	}
	q := r.URL.Query()
	opID := q.Get("operationId")
	at, aerr := strconv.ParseInt(q.Get("at"), 10, 64)
	rv, rerr := strconv.ParseUint(q.Get("ringVersion"), 10, 64)
	if opID == "" || aerr != nil || rerr != nil {
		writeError(w, http.StatusBadRequest, internalapi.INVALID, "operationId, at and ringVersion query parameters are required")
		return
	}
	if !s.checkRing(w, rv) || !s.ownsKey(w, key) {
		return
	}
	s.apply(w, r, store.CompareAndSet{Meta: store.Meta{OperationID: opID, At: at}, Key: key, ExpectedVersion: expected, Delete: true}, true)
}

func (s *server) transitionRequest(w http.ResponseWriter, r *http.Request) {
	key := store.RequestKey(r.PathValue("id"))
	var b internalapi.TransitionRequestBody
	if !s.decode(w, r, &b) || !s.checkRing(w, b.RingVersion) || !s.ownsKey(w, key) {
		return
	}
	s.apply(w, r, store.TransitionRequest{
		Meta: store.Meta{OperationID: b.OperationId, At: b.At}, Key: key,
		ExpectedVersion: b.ExpectedVersion, ExpectedStatus: b.ExpectedStatus, ExpectedAttemptID: b.ExpectedAttemptId,
		To: b.To, Attempt: b.Attempt, Disposition: deref(b.Disposition), NextAttemptAt: deref(b.NextAttemptAt),
		LastError: deref(b.LastError), Outbox: deref(b.Outbox),
	}, false)
}

func (s *server) retryWorkflow(w http.ResponseWriter, r *http.Request) {
	key := store.RequestKey(r.PathValue("id"))
	var b internalapi.RetryWorkflowBody
	if !s.decode(w, r, &b) || !s.checkRing(w, b.RingVersion) || !s.ownsKey(w, key) {
		return
	}
	s.apply(w, r, store.RetryWorkflow{
		Meta: store.Meta{OperationID: b.OperationId, At: b.At}, Key: key, ExpectedVersion: b.ExpectedVersion,
		NextAttemptAt: b.NextAttemptAt, LastError: deref(b.LastError), Outbox: deref(b.Outbox),
	}, false)
}

func (s *server) transitionResource(w http.ResponseWriter, r *http.Request) {
	key := store.ResourceKey(r.PathValue("id"))
	var b internalapi.TransitionResourceBody
	if !s.decode(w, r, &b) || !s.checkRing(w, b.RingVersion) || !s.ownsKey(w, key) {
		return
	}
	s.apply(w, r, store.TransitionResource{
		Meta: store.Meta{OperationID: b.OperationId, At: b.At}, Key: key, ExpectedVersion: b.ExpectedVersion,
		Status: deref(b.Status), Availability: b.Availability, Outbox: deref(b.Outbox),
	}, false)
}

func (s *server) tryClaim(w http.ResponseWriter, r *http.Request) {
	key := store.ResourceKey(r.PathValue("id"))
	var b internalapi.TryClaimBody
	if !s.decode(w, r, &b) || !s.checkRing(w, b.RingVersion) || !s.ownsKey(w, key) {
		return
	}
	s.apply(w, r, store.TryClaim{
		Meta: store.Meta{OperationID: b.OperationId, At: b.At}, ResourceKey: key,
		RequestID: b.RequestId, Attempt: b.Attempt, Type: b.Type,
	}, false)
}

func (s *server) closeAttempt(w http.ResponseWriter, r *http.Request) {
	key := store.ResourceKey(r.PathValue("id"))
	var b internalapi.CloseAttemptBody
	if !s.decode(w, r, &b) || !s.checkRing(w, b.RingVersion) || !s.ownsKey(w, key) {
		return
	}
	s.apply(w, r, store.CloseAttempt{
		Meta: store.Meta{OperationID: b.OperationId, At: b.At}, ResourceKey: key, RequestID: b.RequestId,
		Attempt: b.Attempt, Disposition: b.Disposition, Reason: b.Reason,
	}, false)
}

func (s *server) markOutboxPublished(w http.ResponseWriter, r *http.Request) {
	if !s.ownsShard(w, r.URL.Query().Get("shard")) {
		return
	}
	var b internalapi.MarkPublishedBody
	if !s.decode(w, r, &b) || !s.checkRing(w, b.RingVersion) {
		return
	}
	if len(b.EventIds) == 0 || len(b.EventIds) > 100 {
		writeError(w, http.StatusBadRequest, internalapi.INVALID, fmt.Sprintf("eventIds must hold 1 to 100 events, have %d", len(b.EventIds)))
		return
	}
	s.apply(w, r, store.MarkOutboxPublished{Meta: store.Meta{OperationID: b.OperationId, At: b.At}, Shard: s.shard, EventIDs: b.EventIds}, false)
}

// apply proposes cmd; genericCAS marks PUT/DELETE /internal/data (ETag, and
// 412 for VERSION_CONFLICT).
func (s *server) apply(w http.ResponseWriter, r *http.Request, cmd any, genericCAS bool) {
	ctx, cancel := context.WithTimeout(r.Context(), s.timeout)
	defer cancel()
	res, err := s.node.Apply(ctx, cmd)
	if err != nil {
		s.writeFailure(w, err)
		return
	}
	s.writeResult(w, res, genericCAS)
}

func (s *server) getData(w http.ResponseWriter, r *http.Request) {
	key := r.PathValue("key")
	mode, ok := s.readMode(w, r)
	if !ok || !s.ownsKey(w, key) {
		return
	}
	var rec store.Record
	info, ok := s.read(r.Context(), w, mode, func(ctx context.Context, rd raftstore.Reader) (err error) {
		rec, err = rd.ReadRecord(ctx, key)
		return err
	})
	if !ok {
		return
	}
	w.Header().Set("ETag", internalapi.FormatETag(rec.Version))
	writeJSON(w, http.StatusOK, internalapi.RecordResponse{Record: rec, RingVersion: s.ring.Version(), Read: info})
}

func (s *server) getClosure(w http.ResponseWriter, r *http.Request) {
	key, attemptID := store.ResourceKey(r.PathValue("id")), r.PathValue("attemptId")
	mode, ok := s.readMode(w, r)
	if !ok || !s.ownsKey(w, key) {
		return
	}
	var c store.Closure
	info, ok := s.read(r.Context(), w, mode, func(ctx context.Context, rd raftstore.Reader) (err error) {
		c, err = rd.ReadClosure(ctx, key, attemptID)
		return err
	})
	if ok {
		writeJSON(w, http.StatusOK, internalapi.ClosureResponse{Closure: c, RingVersion: s.ring.Version(), Read: info})
	}
}

func (s *server) getOperation(w http.ResponseWriter, r *http.Request) {
	opID, key := r.PathValue("operationId"), r.URL.Query().Get("key")
	mode, ok := s.readMode(w, r)
	if !ok {
		return
	}
	if key == "" {
		writeError(w, http.StatusBadRequest, internalapi.INVALID, "the key query parameter (owning entity key) is required")
		return
	}
	if !s.ownsKey(w, key) {
		return
	}
	var res store.Result
	info, ok := s.read(r.Context(), w, mode, func(ctx context.Context, rd raftstore.Reader) (err error) {
		res, err = rd.Operation(ctx, opID)
		return err
	})
	if ok {
		res.Shard = s.shard
		writeJSON(w, http.StatusOK, internalapi.OperationResponse{Result: res, RingVersion: s.ring.Version(), Read: info})
	}
}

func (s *server) pendingOutbox(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	if !s.ownsShard(w, q.Get("shard")) {
		return
	}
	limit, err := strconv.Atoi(q.Get("limit"))
	if err != nil || limit < 1 || limit > 100 {
		writeError(w, http.StatusBadRequest, internalapi.INVALID, "limit must be 1 to 100")
		return
	}
	wait := 0
	if v := q.Get("wait"); v != "" {
		if wait, err = strconv.Atoi(v); err != nil || wait < 0 || wait > maxOutboxWait {
			writeError(w, http.StatusBadRequest, internalapi.INVALID, fmt.Sprintf("wait must be 0 to %d seconds", maxOutboxWait))
			return
		}
	}
	mode, ok := s.readMode(w, r)
	if !ok {
		return
	}
	var events []store.OutboxEvent
	page := func(ctx context.Context, rd raftstore.Reader) (err error) {
		events, err = rd.PendingOutbox(ctx, limit)
		return err
	}
	// The whole long-poll, both reads and the wait, fits the storage budget
	// and the request's own deadline, whichever is sooner (#27), so the
	// coordinator's and the caller's budgets hold however slow reads are.
	ctx, cancel := context.WithTimeout(r.Context(), s.timeout)
	defer cancel()
	start := time.Now()
	info, ok := s.read(ctx, w, mode, page)
	if !ok {
		return
	}
	// Long-poll (§14): wait for a commit outside the read, then read a
	// fenced page once more. An empty page after the wait is a real
	// "nothing pending". The wait leaves room for the second read and is
	// skipped when there is none: the first page is already a fenced read.
	if len(events) == 0 && wait > 0 {
		if until, ok := waitUntil(ctx, time.Since(start), time.Duration(wait)*time.Second); ok {
			wctx, wcancel := context.WithDeadline(ctx, until)
			s.node.WaitApplyAfter(wctx, info.AppliedIndex)
			wcancel()
			if info, ok = s.read(ctx, w, mode, page); !ok {
				return
			}
		}
	}
	writeJSON(w, http.StatusOK, internalapi.OutboxPage{Shard: s.shard, Events: nonNil(events), RingVersion: s.ring.Version(), Read: info})
}

// scan reads this shard only, keys after the after parameter; the
// coordinator merges shards (S1.4) and splits cursors into after (S1.6). A
// cursor reaching a node is refused rather than read from the first key.
func (s *server) scan(w http.ResponseWriter, r *http.Request) {
	mode, ok := s.readMode(w, r)
	if !ok {
		return
	}
	q := r.URL.Query()
	sq := store.ScanQuery{Kind: store.Kind(q.Get("kind")), Type: store.ResourceType(q.Get("type")),
		Statuses: q["statuses"], GeohashPrefixes: q["geohashPrefixes"]}
	switch sq.Kind {
	case store.KindRequest, store.KindResource, store.KindKV:
	default:
		writeError(w, http.StatusBadRequest, internalapi.INVALID, "kind must be request, resource or kv")
		return
	}
	n, err := strconv.Atoi(q.Get("limit"))
	if err != nil || n < 1 || n > maxScanLimit {
		writeError(w, http.StatusBadRequest, internalapi.INVALID, fmt.Sprintf("limit is required, 1 to %d records per shard", maxScanLimit))
		return
	}
	sq.Limit = n
	if q.Has("cursor") {
		writeError(w, http.StatusBadRequest, internalapi.INVALID, "a storage node takes after, not cursor; send cursors to the coordinator")
		return
	}
	after := q.Get("after")
	if v := q.Get("ringVersion"); v != "" {
		rv, err := strconv.ParseUint(v, 10, 64)
		if err != nil {
			writeError(w, http.StatusBadRequest, internalapi.INVALID, "ringVersion must be an unsigned integer")
			return
		}
		if !s.checkRing(w, rv) {
			return
		}
	}
	var recs []store.Record
	info, ok := s.read(r.Context(), w, mode, func(ctx context.Context, rd raftstore.Reader) (err error) {
		recs, err = rd.Scan(ctx, sq, after)
		return err
	})
	if ok {
		writeJSON(w, http.StatusOK, internalapi.ScanResponse{
			Records: nonNil(recs), Positions: []store.ShardPosition{{Shard: s.shard, AppliedIndex: info.AppliedIndex, HasMore: len(recs) == sq.Limit}},
			RingVersion: s.ring.Version(), Consistency: internalapi.ScanResponseConsistency(info.Consistency),
		})
	}
}

// waitUntil is when a long-poll's wait must end: after wait, but early
// enough to leave the second read as long as the first took plus
// outboxRereadMargin before ctx's deadline. ok=false means no wait fits.
func waitUntil(ctx context.Context, firstRead, wait time.Duration) (until time.Time, ok bool) {
	now := time.Now()
	until = now.Add(wait)
	if deadline, has := ctx.Deadline(); has {
		if last := deadline.Add(-firstRead - outboxRereadMargin); last.Before(until) {
			until = last
		}
	}
	return until, until.After(now)
}

// read runs fn through the node under ctx and the storage timeout and
// writes any failure; ok reports whether the caller should write the
// response.
//
// A stale read is served from this replica's own FSM, so it is refused
// (503, retryable) until the FSM covers the leader's commit index: a
// replica catching up would answer with old versions or a false "absent".
func (s *server) read(ctx context.Context, w http.ResponseWriter, mode raftstore.ReadMode, fn func(context.Context, raftstore.Reader) error) (internalapi.ReadInfo, bool) {
	if mode == raftstore.ReadStale {
		if st := s.node.Status(); !st.CaughtUp {
			w.Header().Set("Retry-After", retryAfterSeconds)
			writeJSON(w, http.StatusServiceUnavailable, internalapi.Error{Code: internalapi.STORAGEUNAVAILABLE, Message: s.podName + " is " + notCaughtUp(st), Retryable: true})
			return internalapi.ReadInfo{}, false
		}
	}
	ctx, cancel := context.WithTimeout(ctx, s.timeout)
	defer cancel()
	info, err := s.node.Read(ctx, mode, fn)
	if err != nil {
		s.writeReadFailure(w, err)
		return internalapi.ReadInfo{}, false
	}
	return internalapi.ReadInfo{
		Consistency: internalapi.ReadInfoConsistency(info.Consistency), ServedBy: info.ServedBy,
		AppliedIndex: info.AppliedIndex, StalePossible: info.StalePossible,
	}, true
}

// readMode parses ?consistency. linearizable is served as strong until the
// S4.1 read fence lands; the response reports "strong".
func (s *server) readMode(w http.ResponseWriter, r *http.Request) (raftstore.ReadMode, bool) {
	switch c := r.URL.Query().Get("consistency"); c {
	case "", "strong", "linearizable":
		return raftstore.ReadStrong, true
	case "stale":
		return raftstore.ReadStale, true
	default:
		writeError(w, http.StatusBadRequest, internalapi.INVALID, fmt.Sprintf("consistency %q: want strong, linearizable or stale", c))
		return 0, false
	}
}

// decode reads a JSON body strictly: unknown fields and trailing data are 400.
func (s *server) decode(w http.ResponseWriter, r *http.Request, v any) bool {
	dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, maxBodyBytes))
	dec.DisallowUnknownFields()
	err := dec.Decode(v)
	if err == nil {
		if _, terr := dec.Token(); !errors.Is(terr, io.EOF) {
			err = errors.New("trailing data after the JSON body")
		}
	}
	if err != nil {
		writeError(w, http.StatusBadRequest, internalapi.INVALID, "body: "+err.Error())
		return false
	}
	return true
}

// checkRing refuses a caller whose ringVersion differs; nothing is proposed.
func (s *server) checkRing(w http.ResponseWriter, v uint64) bool {
	if have := s.ring.Version(); v != have {
		writeJSON(w, http.StatusConflict, internalapi.Error{
			Code: internalapi.RINGVERSIONMISMATCH, Message: fmt.Sprintf("caller ringVersion %d, node has %d", v, have),
			Retryable: true, RingVersion: &have,
		})
		return false
	}
	return true
}

// ownsKey refuses a key another shard owns: a mutation is never proposed
// and a read never reports such a key as absent.
func (s *server) ownsKey(w http.ResponseWriter, key string) bool {
	owner, err := s.ring.Owner(key)
	if err != nil {
		writeError(w, http.StatusBadRequest, internalapi.INVALID, err.Error())
		return false
	}
	if owner != s.shard {
		writeError(w, http.StatusBadRequest, internalapi.INVALID,
			fmt.Sprintf("key %q is owned by shard %d; this node serves shard %d", key, owner, s.shard))
		return false
	}
	return true
}

func (s *server) ownsShard(w http.ResponseWriter, raw string) bool {
	n, err := strconv.Atoi(raw)
	if err != nil || n != s.shard {
		writeError(w, http.StatusBadRequest, internalapi.INVALID, fmt.Sprintf("shard %q: this node serves shard %d", raw, s.shard))
		return false
	}
	return true
}

func (s *server) status(w http.ResponseWriter, r *http.Request) {
	st := s.node.Status()
	writeJSON(w, http.StatusOK, statusResponse{
		PodName: st.PodName, Incarnation: st.Incarnation, GroupID: st.GroupID, Shard: st.Shard,
		ServerID: string(st.ServerID), Address: string(st.Address), State: st.State, Term: st.Term,
		LeaderID: string(st.LeaderID), LeaderAddr: string(st.LeaderAddr), CommitIndex: st.CommitIndex,
		LastLogIndex: st.LastLogIndex, RaftAppliedIndex: st.RaftAppliedIndex, AppliedIndex: st.AppliedIndex,
		RestoredIndex: st.RestoredIndex, HasState: st.HasState, CaughtUp: st.CaughtUp, RingVersion: s.ring.Version(),
		LeaderCommitIndex: st.LeaderCommitIndex,
	})
}

// bootstrap is operator-only (v3.1 §11): bearer OPERATOR_TOKEN, and disabled
// when no token is configured.
func (s *server) bootstrap(w http.ResponseWriter, r *http.Request) {
	if !s.operatorOnly(w, r) {
		return
	}
	var body bootstrapRequest
	dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, maxBodyBytes))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&body); err != nil {
		writeMgmtError(w, http.StatusBadRequest, "body: "+err.Error())
		return
	}
	if len(body.Servers) == 0 {
		writeMgmtError(w, http.StatusBadRequest, "servers must list the group's voters")
		return
	}
	servers := make([]raft.Server, 0, len(body.Servers))
	for _, sv := range body.Servers {
		if sv.ID == "" || sv.Address == "" {
			writeMgmtError(w, http.StatusBadRequest, "every server needs an id and an address")
			return
		}
		servers = append(servers, raft.Server{Suffrage: raft.Voter, ID: raft.ServerID(sv.ID), Address: raft.ServerAddress(sv.Address)})
	}
	ctx, cancel := context.WithTimeout(r.Context(), s.timeout)
	defer cancel()
	switch err := s.node.Bootstrap(ctx, servers); {
	case errors.Is(err, raftstore.ErrBadBootstrap):
		writeMgmtError(w, http.StatusBadRequest, err.Error())
	case errors.Is(err, raftstore.ErrHasState):
		writeMgmtError(w, http.StatusConflict, "refused: "+s.podName+" already has Raft state; bootstrap only forms a new group")
	case err != nil:
		writeMgmtError(w, http.StatusInternalServerError, "bootstrap: "+err.Error())
	default:
		writeJSON(w, http.StatusOK, map[string]string{"bootstrapped": s.podName})
	}
}

// snapshot is operator-only: it makes Raft snapshot now (S2.1 Verify, and
// before planned maintenance).
func (s *server) snapshot(w http.ResponseWriter, r *http.Request) {
	if !s.operatorOnly(w, r) {
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), snapshotTimeout)
	defer cancel()
	info, err := s.node.Snapshot(ctx)
	if err != nil {
		writeMgmtError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, snapshotResponse{PodName: s.podName, SnapshotID: info.ID, RaftIndex: info.RaftIndex, AppliedIndex: info.AppliedIndex})
}

// snapshotTimeout bounds a forced snapshot: VACUUM INTO and Persist of the
// whole database, longer than an ordinary request.
const snapshotTimeout = 60 * time.Second

// dump is operator-only (it covers every row of the shard): this replica's
// own FSM, read locally in one transaction, as a digest. Replicas are
// compared only at equal appliedIndex.
func (s *server) dump(w http.ResponseWriter, r *http.Request) {
	if !s.operatorOnly(w, r) {
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), s.timeout)
	defer cancel()
	var rows map[string][]string
	_, err := s.node.Read(ctx, raftstore.ReadStale, func(ctx context.Context, rd raftstore.Reader) error {
		var err error
		rows, err = rd.Dump(ctx)
		return err
	})
	if err != nil {
		writeMgmtError(w, http.StatusInternalServerError, "dump: "+err.Error())
		return
	}
	applied, err := dumpAppliedIndex(rows)
	if err != nil {
		writeMgmtError(w, http.StatusInternalServerError, "dump: "+err.Error())
		return
	}
	canonical, err := json.Marshal(rows) // map keys are sorted; rows are in key order
	if err != nil {
		writeMgmtError(w, http.StatusInternalServerError, "dump: "+err.Error())
		return
	}
	sum := sha256.Sum256(canonical)
	counts := make(map[string]int, len(rows))
	for table, lines := range rows {
		counts[table] = len(lines)
	}
	writeJSON(w, http.StatusOK, dumpResponse{PodName: s.podName, AppliedIndex: applied, SHA256: hex.EncodeToString(sum[:]), Rows: counts})
}

// dumpAppliedIndex is the applied_index row of the dump's meta table: the
// index the dump's transaction saw, not one read separately.
func dumpAppliedIndex(rows map[string][]string) (uint64, error) {
	const prefix = "k=applied_index | v="
	for _, line := range rows["meta"] {
		if v, ok := strings.CutPrefix(line, prefix); ok {
			return strconv.ParseUint(v, 10, 64)
		}
	}
	return 0, errors.New("no applied_index in meta")
}

func (s *server) operatorOnly(w http.ResponseWriter, r *http.Request) bool {
	if s.operatorToken == "" {
		writeError(w, http.StatusForbidden, internalapi.FORBIDDEN, "operator endpoints are disabled: no operator token is configured")
		return false
	}
	scheme, token, ok := strings.Cut(r.Header.Get("Authorization"), " ")
	if !ok || !strings.EqualFold(scheme, "Bearer") || subtle.ConstantTimeCompare([]byte(token), []byte(s.operatorToken)) != 1 {
		w.Header().Set("WWW-Authenticate", `Bearer realm="storagenode"`)
		writeError(w, http.StatusUnauthorized, internalapi.UNAUTHORIZED, "operator bearer token required")
		return false
	}
	return true
}

func (s *server) healthz(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

// readyz is data readiness: the FSM has completed every command up to the
// commit index (never raft.AppliedIndex()).
func (s *server) readyz(w http.ResponseWriter, r *http.Request) {
	if st := s.node.Status(); !st.CaughtUp {
		writeMgmtError(w, http.StatusServiceUnavailable, notCaughtUp(st))
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ready"})
}

// notCaughtUp says why a replica is not data-ready.
func notCaughtUp(st raftstore.Status) string {
	if st.LeaderID == "" {
		return fmt.Sprintf("not caught up: no known leader (fsm at %d)", st.AppliedIndex)
	}
	return fmt.Sprintf("not caught up: fsm at %d, leader commit %d, local commit %d", st.AppliedIndex, st.LeaderCommitIndex, st.CommitIndex)
}

// transferLeader is operator-only: the leader hands leadership to another
// voter and answers once it no longer leads. It changes who leads only;
// ownership and membership are untouched.
func (s *server) transferLeader(w http.ResponseWriter, r *http.Request) {
	if !s.operatorOnly(w, r) {
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), transferTimeout)
	defer cancel()
	err := s.node.StepDown(ctx)
	st := s.node.Status()
	switch {
	case errors.Is(err, raftstore.ErrNotLeader):
		writeMgmtError(w, http.StatusConflict, fmt.Sprintf("NOT_LEADER: %s is not the leader of shard %d; the leader is %q", s.podName, s.shard, st.LeaderID))
	case err != nil:
		writeMgmtError(w, http.StatusServiceUnavailable, "transfer-leader: "+err.Error())
	default:
		writeJSON(w, http.StatusOK, transferResponse{PodName: s.podName, State: st.State, Term: st.Term, LeaderID: string(st.LeaderID)})
	}
}

// transferTimeout bounds a leadership transfer (StepDown's own attempts
// take up to about 5 s).
const transferTimeout = 10 * time.Second

func writeMgmtError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]string{"error": msg})
}

func deref[T any](p *T) T {
	var zero T
	if p == nil {
		return zero
	}
	return *p
}

func nonNil[T any](s []T) []T {
	if s == nil {
		return []T{}
	}
	return s
}

func itoa(n int) string { return strconv.Itoa(n) }
