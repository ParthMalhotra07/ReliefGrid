// Package storagenode serves one shard replica over HTTP: the storage-node
// subset of contracts/internal.yaml, the management endpoint, and
// registration and heartbeats to the coordinator. cmd/storagenode wires it to
// the environment; tests run it in-process.
package storagenode

import (
	"log/slog"
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
	if seamsBuild {
		slog.Warn("storagenode: built with the seams tag: /mgmt/test/seams can pause this replica; test images only")
	}
	return &Server{s: &server{
		node: n, ring: cfg.Ring, shard: cfg.Shard, podName: cfg.PodName, serverID: string(n.Identity().ServerID()), timeout: cfg.Timeout,
		leaderHTTP: cfg.LeaderHTTP, operatorToken: cfg.OperatorToken,
	}}
}

// DataRoutes is the storage-node subset of contracts/internal.yaml.
func (s *Server) DataRoutes() http.Handler { return s.s.dataRoutes() }

// MgmtRoutes is the management endpoint.
func (s *Server) MgmtRoutes() http.Handler { return s.s.mgmtRoutes() }

// HeartbeatConfig points a replica at the coordinator.
type HeartbeatConfig struct {
	// CoordinatorURL is the coordinator's base URL, e.g.
	// http://coordinator.reliefgrid.svc.cluster.local:8080.
	CoordinatorURL string
	// Address is this replica's data endpoint (host:port) as the
	// coordinator and peers reach it.
	Address string
	// Interval between heartbeats (default 1 s; the coordinator marks a
	// node SUSPECT after 3 s and DOWN after 5 s without one).
	Interval time.Duration
	// Client sends the calls (default: a client with a 1 s timeout).
	Client *http.Client
	// ConfigRetry is how often a replica whose coordinator rejected
	// heartbeat.configuration (one older than #21) offers it again
	// (default 30 s); a re-registration offers it at once.
	ConfigRetry time.Duration
}
