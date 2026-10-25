package main

import (
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"strings"

	"github.com/ParthMalhotra07/ReliefGrid/internal/internalapi"
	"github.com/ParthMalhotra07/ReliefGrid/internal/raftstore"
	"github.com/ParthMalhotra07/ReliefGrid/internal/store"
)

// The mapping follows contracts/README.md:
//
//	OK, SUCCESS, CONFLICT_CLOSED, REJECTED_CLOSED, CLOSED  200 MutationResponse
//	INVALID                         400  (error body carries the Result)
//	NOT_FOUND, read ErrNotFound     404
//	DOMAIN_CONFLICT, OPERATION_ID_REUSED  409
//	VERSION_CONFLICT, EXISTS        412
//	ringVersion mismatch            409 RING_VERSION_MISMATCH, ringVersion set, never proposed
//	raftstore.ErrNotLeader          503 NOT_LEADER with leaderHint, or NO_LEADER (Retry-After) if none is known
//	raftstore.ErrUnavailable        503 NO_LEADER, Retry-After
//	store.ErrUnknownOutcome         503 OUTCOME_UNKNOWN, outcomeUnknown=true, Retry-After
//	any other read error            503 STORAGE_UNAVAILABLE, Retry-After (local storage failed)
//
// Only OUTCOME_UNKNOWN sets outcomeUnknown; every 503 is retryable.

const retryAfterSeconds = "1"

// outcomeStatus is the HTTP status and error code of a committed outcome;
// the code is empty for a 200.
func outcomeStatus(o store.Outcome) (int, internalapi.ErrorCode) {
	switch o {
	case store.OutcomeOK, store.ClaimSuccess, store.ClaimConflictClosed, store.ClaimRejectedClosed, store.ClaimClosed:
		return http.StatusOK, ""
	case store.OutcomeInvalid:
		return http.StatusBadRequest, internalapi.INVALID
	case store.OutcomeNotFound:
		return http.StatusNotFound, internalapi.NOTFOUND
	case store.OutcomeDomainConflict:
		return http.StatusConflict, internalapi.DOMAINCONFLICT
	case store.OutcomeOperationIDReuse:
		return http.StatusConflict, internalapi.OPERATIONIDREUSED
	case store.OutcomeVersionConflict:
		return http.StatusPreconditionFailed, internalapi.VERSIONCONFLICT
	case store.OutcomeExists:
		return http.StatusPreconditionFailed, internalapi.EXISTS
	}
	// An outcome this build does not know is still committed; never report
	// it as success.
	return http.StatusConflict, internalapi.DOMAINCONFLICT
}

// writeResult writes a committed Result: 200 MutationResponse (with the ETag
// when etag is set), or the error body with the Result for a committed
// rejection.
func (s *server) writeResult(w http.ResponseWriter, res store.Result, etag bool) {
	status, code := outcomeStatus(res.Outcome)
	if status == http.StatusOK {
		if etag {
			w.Header().Set("ETag", internalapi.FormatETag(res.Version))
		}
		writeJSON(w, http.StatusOK, internalapi.MutationResponse{Result: res, RingVersion: s.ring.Version()})
		return
	}
	msg := res.Reason
	if msg == "" {
		msg = string(res.Outcome)
	}
	writeJSON(w, status, internalapi.Error{Code: code, Message: msg, Result: &res})
}

// writeFailure writes an Apply error that is not a committed outcome. Any
// error not known to be "never proposed" is an unknown outcome (invariant 5).
func (s *server) writeFailure(w http.ResponseWriter, err error) {
	if s.writeNotProposed(w, err) {
		return
	}
	w.Header().Set("Retry-After", retryAfterSeconds)
	writeJSON(w, http.StatusServiceUnavailable, internalapi.Error{
		Code: internalapi.OUTCOMEUNKNOWN, Message: err.Error(), Retryable: true, OutcomeUnknown: true,
	})
}

// writeReadFailure writes a Read error. A read changes nothing, so it is
// never an unknown outcome; a failed read is never an empty result either.
// An error that is neither not-found nor not-proposed is a local storage
// failure (e.g. SQLite), not a missing leader.
func (s *server) writeReadFailure(w http.ResponseWriter, err error) {
	if errors.Is(err, store.ErrNotFound) {
		writeError(w, http.StatusNotFound, internalapi.NOTFOUND, err.Error())
		return
	}
	if s.writeNotProposed(w, err) {
		return
	}
	slog.Error("read failed", "err", err)
	w.Header().Set("Retry-After", retryAfterSeconds)
	writeJSON(w, http.StatusServiceUnavailable, internalapi.Error{Code: internalapi.STORAGEUNAVAILABLE, Message: "local storage read failed: " + err.Error(), Retryable: true})
}

// writeNotProposed handles not-leader and unavailable, which mean nothing
// was proposed, and reports whether it wrote a response.
func (s *server) writeNotProposed(w http.ResponseWriter, err error) bool {
	var nle *raftstore.NotLeaderError
	switch {
	case errors.As(err, &nle) && nle.LeaderID != "":
		term := s.node.Status().Term
		podName, _, _ := strings.Cut(string(nle.LeaderID), "/")
		writeJSON(w, http.StatusServiceUnavailable, internalapi.Error{
			Code:       internalapi.NOTLEADER,
			Message:    s.podName + " is not the leader of shard " + itoa(s.shard),
			Retryable:  true,
			LeaderHint: &internalapi.LeaderHint{NodeId: podName, Address: s.leaderHTTP(nle.LeaderAddr), Term: &term},
		})
		return true
	case errors.Is(err, raftstore.ErrNotLeader), errors.Is(err, raftstore.ErrUnavailable):
		w.Header().Set("Retry-After", retryAfterSeconds)
		writeJSON(w, http.StatusServiceUnavailable, internalapi.Error{
			Code: internalapi.NOLEADER, Message: "shard " + itoa(s.shard) + " has no leader: " + err.Error(), Retryable: true,
		})
		return true
	}
	return false
}

// writeError writes an error body with no Result (bad input, wrong shard).
func writeError(w http.ResponseWriter, status int, code internalapi.ErrorCode, msg string) {
	writeJSON(w, status, internalapi.Error{Code: code, Message: msg})
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(v); err != nil {
		slog.Error("write response", "err", err)
	}
}
