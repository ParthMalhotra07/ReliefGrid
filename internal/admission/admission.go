// Package admission is the storage pod's side of the M7 replacement gate
// (plan Part 1 M7, v3.1 §10): before a replica opens its Raft transport it
// reads its shard's bootstrap manifest and opens only if this disk is
// admitted. Only storagectl writes the manifest; this package only reads it.
package admission

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"time"

	"github.com/ParthMalhotra07/ReliefGrid/internal/raftstore"
)

// Manifest phases, as storagectl bootstrap writes them.
const (
	PhasePending       = "PENDING"
	PhaseBootstrapping = "BOOTSTRAPPING"
	PhaseBootstrapped  = "BOOTSTRAPPED"
)

// Manifest is the part of a shard's manifest ConfigMap admission reads.
type Manifest struct {
	Phase    string
	PeerIDs  []string          // the group's voters as storagectl last recorded them
	Admitted map[string]string // podName -> incarnation, written by storagectl replace
	Removed  []string          // ServerIDs storagectl replace removed from the group
}

// Parse reads the ConfigMap data. A malformed field is an error, never an
// admission.
func Parse(data map[string]string) (Manifest, error) {
	m := Manifest{Phase: data["phase"]}
	switch m.Phase {
	case PhasePending, PhaseBootstrapping, PhaseBootstrapped:
	default:
		return Manifest{}, fmt.Errorf("admission: manifest phase %q", m.Phase)
	}
	for key, dst := range map[string]any{"peerIds": &m.PeerIDs, "admitted": &m.Admitted, "removed": &m.Removed} {
		if v := data[key]; v != "" {
			if err := json.Unmarshal([]byte(v), dst); err != nil {
				return Manifest{}, fmt.Errorf("admission: manifest %s: %w", key, err)
			}
		}
	}
	return m, nil
}

// Decide is the M7 rule for this disk. An empty disk (no Raft state) opens
// its transport only for a fresh install (PENDING: no group exists yet, and
// the bootstrap Job records this incarnation), as one of the recorded
// voters, or once storagectl replace has admitted this incarnation. A disk
// with Raft state is an ordinary restart unless replace removed its
// ServerID from the group.
func Decide(m Manifest, id raftstore.Identity, hasState bool) (bool, string) {
	sid := string(id.ServerID())
	// Removed first: no stale peerIds or admitted entry reopens a removed ID.
	if slices.Contains(m.Removed, sid) {
		return false, fmt.Sprintf("removed: %s was removed from the group by storagectl replace", sid)
	}
	if hasState {
		return true, "ordinary restart of a disk with Raft state"
	}
	switch {
	case m.Phase == PhasePending:
		return true, "fresh install: the manifest is PENDING"
	case slices.Contains(m.PeerIDs, sid):
		return true, fmt.Sprintf("%s is a recorded voter", sid)
	case m.Admitted[id.PodName] == id.Incarnation:
		return true, fmt.Sprintf("admitted[%s] = %s", id.PodName, id.Incarnation)
	}
	return false, fmt.Sprintf("not admitted: %s is not a recorded voter and admitted[%s] is %q (storagectl replace admits it)",
		sid, id.PodName, m.Admitted[id.PodName])
}

// The API server's refusals of a manifest read: operator errors (RBAC, a
// wrong or missing manifest), not a transient outage.
var (
	ErrForbidden = errors.New("admission: manifest read forbidden (403)")
	ErrNotFound  = errors.New("admission: manifest not found (404)")
)

// Source reads the shard's manifest ConfigMap data.
type Source interface {
	Get(ctx context.Context) (map[string]string, error)
}

// Wait polls src every poll until this disk is admitted or ctx ends. An
// empty disk fails closed: an unreadable or malformed manifest is not an
// admission. A disk with Raft state does not depend on the API server for
// an ordinary restart: if the manifest cannot be read it is admitted, and
// held only when a readable manifest lists it as removed. onWait, if set,
// gets each reason it is still waiting.
func Wait(ctx context.Context, src Source, id raftstore.Identity, hasState bool, poll time.Duration, onWait func(why string)) error {
	t := time.NewTicker(poll)
	defer t.Stop()
	lastLoud := ""
	for {
		ok, why, loud := check(ctx, src, id, hasState)
		if ok {
			return nil
		}
		if loud && why != lastLoud {
			// Waiting will not fix these: an operator has to.
			slog.Error("!!! M7 admission cannot read the manifest; this disk stays closed until it can !!!", "serverId", id.ServerID(), "reason", why)
		}
		if loud {
			lastLoud = why
		}
		if onWait != nil {
			onWait(why)
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("admission: %s: %w", why, ctx.Err())
		case <-t.C:
		}
	}
}

// check reads and decides once; loud marks a reason an operator must fix.
func check(ctx context.Context, src Source, id raftstore.Identity, hasState bool) (ok bool, why string, loud bool) {
	data, err := src.Get(ctx)
	var problem string
	switch {
	case err == nil:
		m, perr := Parse(data)
		if perr == nil {
			ok, why = Decide(m, id, hasState)
			return ok, why, false
		}
		problem, loud = fmt.Sprintf("manifest malformed: %v", perr), true
	case errors.Is(err, ErrForbidden):
		problem, loud = fmt.Sprintf("manifest forbidden (403, check Role storage-admission): %v", err), true
	case errors.Is(err, ErrNotFound):
		problem, loud = fmt.Sprintf("manifest not found (404): %v", err), true
	default:
		problem = fmt.Sprintf("manifest unreachable: %v", err)
	}
	if hasState {
		return true, "ordinary restart; " + problem, false
	}
	return false, problem, loud
}
