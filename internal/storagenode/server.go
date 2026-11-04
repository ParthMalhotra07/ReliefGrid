package storagenode

import (
	"context"
	"net"
	"net/http"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/hashicorp/raft"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	"github.com/ParthMalhotra07/ReliefGrid/internal/internalapi"
	"github.com/ParthMalhotra07/ReliefGrid/internal/obs"
	"github.com/ParthMalhotra07/ReliefGrid/internal/raftstore"
	"github.com/ParthMalhotra07/ReliefGrid/internal/ring"
	"github.com/ParthMalhotra07/ReliefGrid/internal/store"
)

// node is the part of *raftstore.Node the handlers use; tests fake it to
// produce outcomes a real cluster cannot produce on demand.
type node interface {
	Apply(ctx context.Context, cmd any) (store.Result, error)
	Read(ctx context.Context, mode raftstore.ReadMode, fn func(context.Context, raftstore.Reader) error) (raftstore.ReadInfo, error)
	WaitApplyAfter(ctx context.Context, index uint64)
	Status() raftstore.Status
	Term() uint64
	Bootstrap(ctx context.Context, servers []raft.Server) error
	StepDown(ctx context.Context) error
	Snapshot(ctx context.Context) (raftstore.SnapshotInfo, error)
}

var _ node = (*raftstore.Node)(nil)

// server serves one shard replica.
type server struct {
	node    node
	ring    *ring.Ring
	shard   int
	podName string
	// serverID is this replica's Raft ServerID (podName/incarnation), named
	// on every data-route answer.
	serverID string
	timeout  time.Duration
	// tracerProvider records data-route server spans (S4.4); nil uses the
	// global provider.
	tracerProvider trace.TracerProvider
	// leaderHTTP turns the Raft leader's address into the address of its
	// data endpoint for leaderHint when the coordinator has not named it
	// (production: same host, this replica's HTTP port).
	leaderHTTP func(raft.ServerAddress) string
	// leaders is the coordinator's last answer for this shard: the leader
	// and its advertised HTTP address, a hint for leaderHint only.
	leaders atomic.Pointer[internalapi.ShardLeader]
	// noConfigAt: when the coordinator last rejected heartbeat.configuration
	// (it is older than #21), unix ns; 0 when it accepts it. Never a latch:
	// the configuration is offered again every ConfigRetry and on every
	// re-registration.
	noConfigAt atomic.Int64
	// operatorToken guards /mgmt/bootstrap; empty disables it (403).
	operatorToken string
	// seams is the seams build's armed boundaries (empty otherwise).
	seams seamRegistry //nolint:unused // used by the seams build (seams_on.go)
}

// dataRoutes is the storage-node subset of contracts/internal.yaml. Every
// mutation checks ringVersion and that this shard owns the key before it is
// proposed; neither failure is ever proposed. Every answer names this
// replica (internalapi.HeaderServerID, HeaderShard) so the coordinator can
// tell it from another process on the same address.
func (s *server) dataRoutes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("PUT /internal/data/{key}", s.putData)
	mux.HandleFunc("GET /internal/data/{key}", s.getData)
	mux.HandleFunc("DELETE /internal/data/{key}", s.deleteData)
	mux.HandleFunc("GET /internal/scan", s.scan)
	mux.HandleFunc("POST /internal/requests/{id}/transition", s.transitionRequest)
	mux.HandleFunc("POST /internal/requests/{id}/retry", s.retryWorkflow)
	mux.HandleFunc("POST /internal/resources/{id}/transition", s.transitionResource)
	mux.HandleFunc("POST /internal/resources/{id}/claim", s.tryClaim)
	mux.HandleFunc("POST /internal/resources/{id}/close-attempt", s.closeAttempt)
	mux.HandleFunc("GET /internal/resources/{id}/closures/{attemptId}", s.getClosure)
	mux.HandleFunc("GET /internal/outbox", s.pendingOutbox)
	mux.HandleFunc("POST /internal/outbox/published", s.markOutboxPublished)
	mux.HandleFunc("GET /internal/operations/{operationId}", s.getOperation)
	shard := strconv.Itoa(s.shard)
	// Each request is a server span continuing the coordinator's attempt
	// span (S4.4); a proposal under it is raftstore's raft.apply.
	return obs.Handler(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set(internalapi.HeaderServerID, s.serverID)
		w.Header().Set(internalapi.HeaderShard, shard)
		mux.ServeHTTP(w, r)
	}), obs.WithTracerProvider(s.tracerProvider), obs.WithAttributes(attribute.Int("reliefgrid.shard", s.shard)))
}

// mgmtRoutes is the management endpoint (not in the contract; operator and
// kubelet only).
//
//	GET  /mgmt/status     raftstore.Status plus ringVersion
//	POST /mgmt/bootstrap  {"servers":[{"id","address"}]}; operator bearer token
//	                      (401 UNAUTHORIZED without it, 403 FORBIDDEN if none
//	                      is configured); 400 if the servers omit this node or
//	                      give it another address; 409 if Raft state exists
//	POST /mgmt/snapshot   operator token: take a Raft snapshot now; {podName,
//	                      snapshotId, raftIndex, appliedIndex}
//	GET  /mgmt/dump       operator token: the local FSM's rows (a stale read,
//	                      one transaction) as {podName, appliedIndex, sha256,
//	                      rows{table: count}}; compare replicas only at equal
//	                      appliedIndex
//	GET  /healthz         200 while the process serves
//	POST /mgmt/transfer-leader
//	                      operator token: the leader hands leadership to
//	                      another voter (raftstore.StepDown); 200 {podName,
//	                      state, term, leaderId} once it no longer leads, 409
//	                      NOT_LEADER on a follower, 503 if it still leads
//	POST /mgmt/membership operator token: {op: remove|add-nonvoter|add-voter,
//	                      id, address, prevIndex}; on the leader, one change
//	                      decided from prevIndex; 200 {index}; 409 NOT_LEADER
//	                      or CONFIG_CHANGED (nothing changed); 503 outcome
//	                      unknown (re-read the configuration)
//	GET  /readyz          200 only when the FSM covers the leader's commit
//	                      index (Status.CaughtUp)
func (s *server) mgmtRoutes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /mgmt/status", s.status)
	mux.HandleFunc("POST /mgmt/bootstrap", s.bootstrap)
	mux.HandleFunc("POST /mgmt/snapshot", s.snapshot)
	mux.HandleFunc("GET /mgmt/dump", s.dump)
	mux.HandleFunc("POST /mgmt/transfer-leader", s.transferLeader)
	mux.HandleFunc("POST /mgmt/membership", s.membership)
	mux.HandleFunc("GET /healthz", s.healthz)
	mux.HandleFunc("GET /readyz", s.readyz)
	registerSeamRoutes(mux, s) // seams build only (S4.6b); a no-op otherwise
	return mux
}

// statusResponse is the /mgmt/status body: raftstore.Status in JSON.
type statusResponse struct {
	PodName          string `json:"podName"`
	Incarnation      string `json:"incarnation"`
	GroupID          string `json:"groupId"`
	Shard            int    `json:"shard"`
	ServerID         string `json:"serverId"`
	Address          string `json:"address"`
	State            string `json:"state"`
	Term             uint64 `json:"term"`
	LeaderID         string `json:"leaderId"`
	LeaderAddr       string `json:"leaderAddr"`
	CommitIndex      uint64 `json:"commitIndex"`
	LastLogIndex     uint64 `json:"lastLogIndex"`
	RaftAppliedIndex uint64 `json:"raftAppliedIndex"` // diagnostic only
	AppliedIndex     uint64 `json:"appliedIndex"`     // FSM's completed index
	RestoredIndex    uint64 `json:"restoredIndex"`    // last snapshot restored since start; 0 if none
	HasState         bool   `json:"hasState"`
	CaughtUp         bool   `json:"caughtUp"`
	RingVersion      uint64 `json:"ringVersion"`
	// LeaderCommitIndex: the leader's commit index as this replica learned
	// it; caughtUp is judged against it.
	LeaderCommitIndex uint64 `json:"leaderCommitIndex"`
	// Configuration is the group's latest configuration as this replica
	// knows it, and whether it is committed (#21).
	Configuration *configurationJSON `json:"configuration,omitempty"`
	// Admitted is false only while the disk waits for M7 admission (state
	// Unadmitted); AdmissionReason says why.
	Admitted        bool   `json:"admitted"`
	AdmissionReason string `json:"admissionReason,omitempty"`
	// Seams is true only in a binary built with the seams tag (S4.6b), so
	// a test runner can refuse to arm boundaries on anything else.
	Seams bool `json:"seams,omitempty"`
}

// transferResponse is the /mgmt/transfer-leader body: this replica after
// handing leadership over.
type transferResponse struct {
	PodName  string `json:"podName"`
	State    string `json:"state"`
	Term     uint64 `json:"term"`
	LeaderID string `json:"leaderId"`
}

// snapshotResponse is the /mgmt/snapshot body.
type snapshotResponse struct {
	PodName      string `json:"podName"`
	SnapshotID   string `json:"snapshotId"`
	RaftIndex    uint64 `json:"raftIndex"`
	AppliedIndex uint64 `json:"appliedIndex"`
}

// dumpResponse is the /mgmt/dump body: a digest of the rows, not the rows.
type dumpResponse struct {
	PodName      string         `json:"podName"`
	AppliedIndex uint64         `json:"appliedIndex"` // the dump's own meta applied_index
	SHA256       string         `json:"sha256"`       // over the canonical rows of every table
	Rows         map[string]int `json:"rows"`
}

// bootstrapRequest is the /mgmt/bootstrap body.
type bootstrapRequest struct {
	Servers []bootstrapServer `json:"servers"`
}

type bootstrapServer struct {
	ID      string `json:"id"`
	Address string `json:"address"`
}

// HTTPAddrFor replaces the port of a Raft address with the HTTP port: every
// replica of a StatefulSet listens on the same ports.
func HTTPAddrFor(raftAddr raft.ServerAddress, httpPort string) string {
	host, _, err := net.SplitHostPort(string(raftAddr))
	if err != nil {
		return ""
	}
	return net.JoinHostPort(host, httpPort)
}
