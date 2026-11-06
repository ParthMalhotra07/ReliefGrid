package storagenode

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/ParthMalhotra07/ReliefGrid/internal/internalapi"
	"github.com/ParthMalhotra07/ReliefGrid/internal/raftstore"
)

// StateUnadmitted is /mgmt/status's state while a disk waits for M7
// admission: its Raft transport and data listener are not open.
const StateUnadmitted = "Unadmitted"

// GatedConfig describes a replica waiting for admission.
type GatedConfig struct {
	RingVersion   uint64
	Address       string // the Raft address it will advertise once admitted
	OperatorToken string
}

// Gated is the management endpoint of a replica whose disk is not admitted
// (M7, v3.1 §10): status, liveness and readiness only. Every operator action
// that needs Raft answers 409 UNADMITTED.
type Gated struct {
	id       raftstore.Identity
	hasState bool
	cfg      GatedConfig
	mu       sync.Mutex
	reason   string
}

// NewGated serves the management endpoint for id until it is admitted.
func NewGated(id raftstore.Identity, hasState bool, cfg GatedConfig) *Gated {
	return &Gated{id: id, hasState: hasState, cfg: cfg, reason: "waiting for admission"}
}

// SetReason records why the disk is still waiting; status reports it.
func (g *Gated) SetReason(why string) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.reason = why
}

// MgmtRoutes is the gated management endpoint.
func (g *Gated) MgmtRoutes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /mgmt/status", g.status)
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	})
	mux.HandleFunc("GET /readyz", func(w http.ResponseWriter, _ *http.Request) {
		writeMgmtError(w, http.StatusServiceUnavailable, "not ready: "+g.why())
	})
	refuse := func(w http.ResponseWriter, _ *http.Request) {
		writeMgmtError(w, http.StatusConflict, "UNADMITTED: "+g.id.PodName+" has no open Raft transport: "+g.why())
	}
	for _, route := range []string{"POST /mgmt/bootstrap", "POST /mgmt/membership", "POST /mgmt/snapshot", "POST /mgmt/transfer-leader", "GET /mgmt/dump", "GET /mgmt/merkle"} {
		mux.HandleFunc(route, refuse)
	}
	return mux
}

func (g *Gated) why() string {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.reason
}

func (g *Gated) status(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, statusResponse{
		Seams:   seamsBuild,
		PodName: g.id.PodName, Incarnation: g.id.Incarnation, GroupID: g.id.GroupID, Shard: g.id.Shard,
		ServerID: string(g.id.ServerID()), Address: g.cfg.Address, State: StateUnadmitted, HasState: g.hasState,
		RingVersion: g.cfg.RingVersion, Admitted: false, AdmissionReason: g.why(),
	})
}

// RunHeartbeats registers the waiting disk with the coordinator and reports
// it unavailable every Interval until ctx ends, so the coordinator lists the
// new incarnation (unadmitted, never routed to) instead of the old one.
func (g *Gated) RunHeartbeats(ctx context.Context, cfg HeartbeatConfig) error {
	if cfg.Interval <= 0 {
		cfg.Interval = time.Second
	}
	if cfg.Client == nil {
		cfg.Client = &http.Client{Timeout: time.Second}
	}
	base := strings.TrimRight(cfg.CoordinatorURL, "/")
	reg := internalapi.NodeRegistration{NodeId: g.id.PodName, GroupId: g.id.GroupID, ShardId: g.id.Shard, Incarnation: g.id.Incarnation,
		Address: cfg.Address, ProtocolVersion: protocolVersion, SchemaVersion: schemaVersion}
	hb := internalapi.Heartbeat{NodeId: g.id.PodName, GroupId: g.id.GroupID, ShardId: g.id.Shard, Incarnation: g.id.Incarnation,
		Health: internalapi.HealthUnavailable}
	tick := time.NewTicker(cfg.Interval)
	defer tick.Stop()
	registered := false
	for {
		var err error
		if !registered {
			if err = post(ctx, cfg.Client, base+"/internal/nodes/register", reg, &internalapi.RegisterResponse{}); err == nil {
				registered = true
			}
		}
		if registered {
			if err = post(ctx, cfg.Client, base+"/internal/nodes/heartbeat", hb, &internalapi.HeartbeatResponse{}); errors.Is(err, errNotRegistered) {
				registered = false
			}
		}
		if err != nil && ctx.Err() == nil {
			slog.Warn("coordinator call failed (unadmitted)", "registered", registered, "err", err)
		}
		select {
		case <-ctx.Done():
			return nil
		case <-tick.C:
		}
	}
}
