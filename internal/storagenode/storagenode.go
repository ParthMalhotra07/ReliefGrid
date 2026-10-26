// Package storagenode serves one shard replica over HTTP: the storage-node
// subset of contracts/internal.yaml, the management endpoint, and
// registration and heartbeats to the coordinator. cmd/storagenode wires it to
// the environment; tests run it in-process.
package storagenode

import (
	"net/http"
	"time"

	"github.com/hashicorp/raft"

	"github.com/ParthMalhotra07/ReliefGrid/internal/raftstore"
	"github.com/ParthMalhotra07/ReliefGrid/internal/ring"
)

// Config is one replica's server configuration.
type Config struct {
	Ring    *ring.Ring
	Shard   int
	PodName string
	// Timeout is the deadline on every Apply and read (plan §14: 3 s storage).
	Timeout time.Duration
	// LeaderHTTP turns the Raft leader's address into the address of its
	// data endpoint for leaderHint.
	LeaderHTTP func(raft.ServerAddress) string
	// OperatorToken guards /mgmt/bootstrap; empty disables it (403).
	OperatorToken string
}

// Server serves one shard replica.
type Server struct{ s *server }

// New serves n with cfg.
func New(n *raftstore.Node, cfg Config) *Server {
	return &Server{s: &server{
		node: n, ring: cfg.Ring, shard: cfg.Shard, podName: cfg.PodName, timeout: cfg.Timeout,
		leaderHTTP: cfg.LeaderHTTP, operatorToken: cfg.OperatorToken,
	}}
}

// DataRoutes is the storage-node subset of contracts/internal.yaml.
func (s *Server) DataRoutes() http.Handler { return s.s.dataRoutes() }

// MgmtRoutes is the management endpoint.
func (s *Server) MgmtRoutes() http.Handler { return s.s.mgmtRoutes() }
