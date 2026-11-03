package storagenode

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"time"

	"github.com/ParthMalhotra07/ReliefGrid/internal/fsm"
	"github.com/ParthMalhotra07/ReliefGrid/internal/fsm/merkle"
	"github.com/ParthMalhotra07/ReliefGrid/internal/raftstore"
)

// merkleTimeout bounds one digest: a full read of the shard (about 3.5 µs a
// row measured, so well under this for the project's datasets).
const merkleTimeout = 30 * time.Second

// merkleResponse is GET /mgmt/merkle: this replica's content digest at one
// applied index, with the metadata compared beside it.
type merkleResponse struct {
	PodName     string `json:"podName"`
	RingVersion uint64 `json:"ringVersion"`
	fsm.MerkleReport
}

// merkleBucketResponse is GET /mgmt/merkle with table and bucket.
type merkleBucketResponse struct {
	PodName     string `json:"podName"`
	RingVersion uint64 `json:"ringVersion"`
	fsm.MerkleBucketReport
}

// indexMismatchResponse is the 409 when the replica is not at the index
// asked for: its own index, so the caller can choose a new target.
type indexMismatchResponse struct {
	Error        string `json:"error"`
	Code         string `json:"code"`
	AppliedIndex uint64 `json:"appliedIndex"`
}

// merkle serves GET /mgmt/merkle[?at=N][&table=T&bucket=B] (S4.5), operator
// only: a stale read of this replica's own FSM, never proposed and never
// part of Apply. With at, the digest is exactly at command index N or the
// answer is 409 INDEX_MISMATCH; comparisons between replicas always pass
// at. Without it, the digest is at whatever index the read sees.
func (s *server) merkle(w http.ResponseWriter, r *http.Request) {
	if !s.operatorOnly(w, r) {
		return
	}
	q := r.URL.Query()
	at := fsm.AnyIndex
	if q.Has("at") {
		v, err := strconv.ParseUint(q.Get("at"), 10, 64)
		if err != nil || v == fsm.AnyIndex {
			writeMgmtError(w, http.StatusBadRequest, fmt.Sprintf("at %q: want a command index (leave at out for any index)", q.Get("at")))
			return
		}
		at = v
	}
	bucket := -1
	if q.Has("table") != q.Has("bucket") {
		writeMgmtError(w, http.StatusBadRequest, "table and bucket go together")
		return
	}
	if q.Has("bucket") {
		b, err := strconv.Atoi(q.Get("bucket"))
		if err != nil || b < 0 || b >= merkle.Buckets {
			writeMgmtError(w, http.StatusBadRequest, fmt.Sprintf("bucket %q: want 0 to %d", q.Get("bucket"), merkle.Buckets-1))
			return
		}
		bucket = b
	}

	ctx, cancel := context.WithTimeout(r.Context(), merkleTimeout)
	defer cancel()
	var (
		full merkleResponse
		one  merkleBucketResponse
	)
	_, err := s.node.Read(ctx, raftstore.ReadStale, func(ctx context.Context, rd raftstore.Reader) error {
		var err error
		if bucket < 0 {
			full.MerkleReport, err = rd.Merkle(ctx, at)
		} else {
			one.MerkleBucketReport, err = rd.MerkleBucket(ctx, at, q.Get("table"), bucket)
		}
		return err
	})
	var mis *fsm.IndexMismatchError
	switch {
	case errors.As(err, &mis):
		writeJSON(w, http.StatusConflict, indexMismatchResponse{Error: mis.Error(), Code: "INDEX_MISMATCH", AppliedIndex: mis.Got})
	case errors.Is(err, fsm.ErrMerkleArgument):
		writeMgmtError(w, http.StatusBadRequest, err.Error())
	case err != nil:
		writeMgmtError(w, http.StatusInternalServerError, "merkle: "+err.Error())
	case bucket < 0:
		full.PodName, full.RingVersion = s.podName, s.ring.Version()
		writeJSON(w, http.StatusOK, full)
	default:
		one.PodName, one.RingVersion = s.podName, s.ring.Version()
		writeJSON(w, http.StatusOK, one)
	}
}
