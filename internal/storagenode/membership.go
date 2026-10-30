package storagenode

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"time"

	"github.com/hashicorp/raft"

	"github.com/ParthMalhotra07/ReliefGrid/internal/raftstore"
)

// membershipNode is the part of *raftstore.Node that storagectl replace
// drives through POST /mgmt/membership.
type membershipNode interface {
	RemoveServer(ctx context.Context, id raft.ServerID, prevIndex uint64) (uint64, error)
	AddNonvoter(ctx context.Context, id raft.ServerID, addr raft.ServerAddress, prevIndex uint64) (uint64, error)
	AddVoter(ctx context.Context, id raft.ServerID, addr raft.ServerAddress, prevIndex uint64) (uint64, error)
}

var _ membershipNode = (*raftstore.Node)(nil)

// configurationJSON is raftstore.Configuration in /mgmt/status (#21).
type configurationJSON struct {
	Index     uint64       `json:"index"`
	Committed bool         `json:"committed"`
	Servers   []serverJSON `json:"servers"`
}

type serverJSON struct {
	ID       string `json:"id"`
	Address  string `json:"address"`
	Suffrage string `json:"suffrage"` // Voter or Nonvoter
}

func toConfigurationJSON(c raftstore.Configuration) *configurationJSON {
	if c.Index == 0 && len(c.Servers) == 0 {
		return nil
	}
	out := &configurationJSON{Index: c.Index, Committed: c.Committed, Servers: []serverJSON{}}
	for _, s := range c.Servers {
		out.Servers = append(out.Servers, serverJSON{ID: string(s.ID), Address: string(s.Address), Suffrage: s.Suffrage})
	}
	return out
}

// membershipRequest is the /mgmt/membership body.
type membershipRequest struct {
	Op        string `json:"op"` // remove, add-nonvoter or add-voter
	ID        string `json:"id"`
	Address   string `json:"address,omitempty"` // for the adds
	PrevIndex uint64 `json:"prevIndex"`
}

// membership is operator-only: one membership change through this node,
// which must lead, decided from the configuration at prevIndex. 409
// NOT_LEADER and 409 CONFIG_CHANGED changed nothing (re-read and retry);
// 503 means the outcome is unknown and the caller re-reads the
// configuration (v3.1 §11 step 2: never assume the change failed).
func (s *server) membership(w http.ResponseWriter, r *http.Request) {
	if !s.operatorOnly(w, r) {
		return
	}
	mn, ok := s.node.(membershipNode)
	if !ok {
		writeMgmtError(w, http.StatusNotImplemented, "this node does not take membership changes")
		return
	}
	var body membershipRequest
	dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, maxBodyBytes))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&body); err != nil {
		writeMgmtError(w, http.StatusBadRequest, "body: "+err.Error())
		return
	}
	id, addr := raft.ServerID(body.ID), raft.ServerAddress(body.Address)
	var change func(context.Context) (uint64, error)
	switch body.Op {
	case "remove":
		change = func(ctx context.Context) (uint64, error) { return mn.RemoveServer(ctx, id, body.PrevIndex) }
	case "add-nonvoter":
		change = func(ctx context.Context) (uint64, error) { return mn.AddNonvoter(ctx, id, addr, body.PrevIndex) }
	case "add-voter":
		change = func(ctx context.Context) (uint64, error) { return mn.AddVoter(ctx, id, addr, body.PrevIndex) }
	default:
		writeMgmtError(w, http.StatusBadRequest, "op must be remove, add-nonvoter or add-voter")
		return
	}
	switch {
	case body.ID == "":
		writeMgmtError(w, http.StatusBadRequest, "id is required")
		return
	case body.Op != "remove" && body.Address == "":
		writeMgmtError(w, http.StatusBadRequest, "address is required to add a server")
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), membershipTimeout)
	defer cancel()
	index, err := change(ctx)
	switch {
	case err == nil:
		writeJSON(w, http.StatusOK, map[string]uint64{"index": index})
	case errors.Is(err, raftstore.ErrNotLeader):
		writeMgmtError(w, http.StatusConflict, "NOT_LEADER: "+s.podName+" is not the leader; the leader is "+string(s.node.Status().LeaderID))
	case errors.Is(err, raftstore.ErrConfigurationChanged):
		writeMgmtError(w, http.StatusConflict, "CONFIG_CHANGED: "+err.Error())
	default:
		writeMgmtError(w, http.StatusServiceUnavailable, "outcome unknown; re-read the configuration: "+err.Error())
	}
}

// membershipTimeout bounds one membership change's commit.
const membershipTimeout = 10 * time.Second
