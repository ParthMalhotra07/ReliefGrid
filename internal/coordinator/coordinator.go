// Package coordinator routes every internal data route to the leader of the
// shard that owns its key (contracts/internal.yaml, plan §09, §14, §15, §40).
//
// It owns the fixed ring loaded from contracts/ring.yaml and keeps a table
// of storage nodes from registration and heartbeats. Node state (UP,
// SUSPECT after SuspectAfter, DOWN after DownAfter without a heartbeat) and
// leader hints are observations for routing only: the coordinator never
// promotes a leader, changes membership or moves a key.
//
// A mutation is forwarded byte for byte, so every retry carries the same
// operationId and payload. Within Budget the coordinator follows NOT_LEADER
// hints, probes the shard's other members when no hint works, and retries an
// unknown outcome with the same bytes. A shard whose members are all DOWN
// fails at once rather than after the budget. If any attempt may have been
// proposed and none returned a committed outcome, the answer is 503
// OUTCOME_UNKNOWN, never NO_LEADER (CONTRIBUTING.md, Invariants, 5).
package coordinator

import (
	"errors"
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/ParthMalhotra07/ReliefGrid/internal/internalapi"
	"github.com/ParthMalhotra07/ReliefGrid/internal/ring"
)

// Defaults from plan §14 and §15; every one is a starting target to measure.
const (
	DefaultSuspectAfter   = 3 * time.Second
	DefaultDownAfter      = 5 * time.Second
	DefaultBudget         = 4 * time.Second // coordinator share of the 5 s HTTP budget
	DefaultAttemptTimeout = 3500 * time.Millisecond
	// HTTPBudget is the caller's budget for one call; the routing budget
	// must leave it room to answer.
	HTTPBudget = 5 * time.Second
)

// ValidateBudgets checks plan §14's ordering: one attempt fits in the
// routing budget, which stays under the caller's HTTP budget.
func ValidateBudgets(budget, attempt time.Duration) error {
	if budget >= HTTPBudget {
		return fmt.Errorf("coordinator: budget %s must be below the %s HTTP budget", budget, HTTPBudget)
	}
	if attempt > budget {
		return fmt.Errorf("coordinator: attempt timeout %s must not exceed the budget %s", attempt, budget)
	}
	return nil
}

// Config configures a Coordinator. Zero durations take the defaults.
type Config struct {
	Ring *ring.Ring
	// SuspectAfter and DownAfter are heartbeat ages that mark a node
	// SUSPECT and DOWN.
	SuspectAfter, DownAfter time.Duration
	// Budget bounds one routed request, all retries included.
	Budget time.Duration
	// AttemptTimeout bounds one forward to a storage node; the storage node
	// bounds its own wait at 3 s. Zero takes DefaultAttemptTimeout, capped
	// at Budget.
	AttemptTimeout time.Duration
	// OperatorToken guards /admin; empty disables it (403).
	OperatorToken string
	// Client forwards to storage nodes (default: a fresh http.Client).
	Client *http.Client
	// Now is the clock for heartbeat ages (default time.Now).
	Now func() time.Time
}

// Coordinator serves contracts/internal.yaml. It is safe for concurrent use.
//
// Its node table is in memory: after a restart it is empty until the
// storage nodes register again (a heartbeat it does not know gets 404, which
// sends the node back to registration).
type Coordinator struct {
	cfg  Config
	ring *ring.Ring

	mu      sync.Mutex
	nodes   map[string]*member // by pod name
	leaders []leader           // by shard
	configs []shardConfig      // by shard: the newest committed Raft configuration reported
}

// New checks cfg and fills its defaults.
func New(cfg Config) (*Coordinator, error) {
	if cfg.Ring == nil {
		return nil, errors.New("coordinator: no ring")
	}
	if cfg.SuspectAfter <= 0 {
		cfg.SuspectAfter = DefaultSuspectAfter
	}
	if cfg.DownAfter <= 0 {
		cfg.DownAfter = DefaultDownAfter
	}
	if cfg.SuspectAfter >= cfg.DownAfter {
		return nil, fmt.Errorf("coordinator: SuspectAfter %s must be below DownAfter %s", cfg.SuspectAfter, cfg.DownAfter)
	}
	if cfg.Budget <= 0 {
		cfg.Budget = DefaultBudget
	}
	if cfg.AttemptTimeout <= 0 {
		cfg.AttemptTimeout = min(DefaultAttemptTimeout, cfg.Budget)
	}
	if err := ValidateBudgets(cfg.Budget, cfg.AttemptTimeout); err != nil {
		return nil, err
	}
	if cfg.Client == nil {
		cfg.Client = &http.Client{}
	}
	if cfg.Now == nil {
		cfg.Now = time.Now
	}
	return &Coordinator{cfg: cfg, ring: cfg.Ring, nodes: map[string]*member{}, leaders: make([]leader, cfg.Ring.Shards()), configs: make([]shardConfig, cfg.Ring.Shards())}, nil
}

// Handler serves every route of contracts/internal.yaml.
func (c *Coordinator) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /internal/nodes/register", c.register)
	mux.HandleFunc("POST /internal/nodes/heartbeat", c.heartbeat)
	mux.HandleFunc("GET /internal/ring", func(w http.ResponseWriter, _ *http.Request) { writeJSON(w, http.StatusOK, c.ringInfo()) })
	mux.HandleFunc("GET /internal/scan", c.scan)

	mux.HandleFunc("PUT /internal/data/{key}", c.byKey(pathKey, true))
	mux.HandleFunc("GET /internal/data/{key}", c.byKey(pathKey, false))
	mux.HandleFunc("DELETE /internal/data/{key}", c.byKey(pathKey, true))
	mux.HandleFunc("POST /internal/requests/{id}/transition", c.byKey(requestKey, true))
	mux.HandleFunc("POST /internal/requests/{id}/retry", c.byKey(requestKey, true))
	mux.HandleFunc("POST /internal/resources/{id}/transition", c.byKey(resourceKey, true))
	mux.HandleFunc("POST /internal/resources/{id}/claim", c.byKey(resourceKey, true))
	mux.HandleFunc("POST /internal/resources/{id}/close-attempt", c.byKey(resourceKey, true))
	mux.HandleFunc("GET /internal/resources/{id}/closures/{attemptId}", c.byKey(resourceKey, false))
	mux.HandleFunc("GET /internal/operations/{operationId}", c.byKey(queryKey, false))
	mux.HandleFunc("GET /internal/outbox", c.byShard(false))
	mux.HandleFunc("POST /internal/outbox/published", c.byShard(true))

	mux.HandleFunc("GET /admin/ring", c.operator(c.adminRing))
	mux.HandleFunc("GET /admin/nodes", c.operator(c.adminNodes))
	mux.HandleFunc("GET /admin/locate/{key}", c.operator(c.adminLocate))
	mux.HandleFunc("GET /admin/distribution", c.operator(c.adminDistribution))
	return mux
}

func (c *Coordinator) ringInfo() internalapi.Ring {
	return internalapi.Ring{RingVersion: c.ring.Version(), Hash: internalapi.Xxhash64, Shards: c.ring.Shards(),
		VnodesPerShard: c.ring.VNodesPerShard(), Checksum: c.ring.Checksum()}
}
