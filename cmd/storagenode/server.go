package main

import (
	"context"
	"net"
	"net/http"
	"time"

	"github.com/hashicorp/raft"

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
}

var _ node = (*raftstore.Node)(nil)

// server serves one shard replica.
type server struct {
	node    node
	ring    *ring.Ring
	shard   int
	podName string
	timeout time.Duration
	// leaderHTTP turns the Raft leader's address into the address of its
	// data endpoint for leaderHint (production: same host, HTTP port).
	leaderHTTP func(raft.ServerAddress) string
	// operatorToken guards /mgmt/bootstrap; empty disables it (403).
	operatorToken string
}

// dataRoutes is the storage-node subset of contracts/internal.yaml. Every
// mutation checks ringVersion and that this shard owns the key before it is
// proposed; neither failure is ever proposed.
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
	return mux
}

// mgmtRoutes is the management endpoint (not in the contract; operator and
// kubelet only).
//
//	GET  /mgmt/status     raftstore.Status plus ringVersion
//	POST /mgmt/bootstrap  {"servers":[{"id","address"}]}; operator bearer token
//	                      (401 UNAUTHORIZED without it, 403 FORBIDDEN if none
//	                      is configured); 400 if the servers omit this node or
//	                      give it another address; 409 if Raft state exists
//	GET  /healthz         200 while the process serves
//	GET  /readyz          200 only when the FSM has caught up (Status.CaughtUp)
func (s *server) mgmtRoutes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /mgmt/status", s.status)
	mux.HandleFunc("POST /mgmt/bootstrap", s.bootstrap)
	mux.HandleFunc("GET /healthz", s.healthz)
	mux.HandleFunc("GET /readyz", s.readyz)
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
	HasState         bool   `json:"hasState"`
	CaughtUp         bool   `json:"caughtUp"`
	RingVersion      uint64 `json:"ringVersion"`
}

// bootstrapRequest is the /mgmt/bootstrap body.
type bootstrapRequest struct {
	Servers []bootstrapServer `json:"servers"`
}

type bootstrapServer struct {
	ID      string `json:"id"`
	Address string `json:"address"`
}

// httpAddrFor replaces the port of a Raft address with the HTTP port: every
// replica of a StatefulSet listens on the same ports.
func httpAddrFor(raftAddr raft.ServerAddress, httpPort string) string {
	host, _, err := net.SplitHostPort(string(raftAddr))
	if err != nil {
		return ""
	}
	return net.JoinHostPort(host, httpPort)
}
