// Package admission is the storage pod's side of the M7 replacement gate
// (plan Part 1 M7, v3.1 §10): before a replica opens its Raft transport it
// reads its shard's bootstrap manifest and opens only if this disk is
// admitted. Only storagectl writes the manifest; this package only reads it.
package admission

import (
	"context"
	"encoding/json"
	"fmt"
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
	if hasState {
		if slices.Contains(m.Removed, sid) {
			return false, fmt.Sprintf("removed: %s was removed from the group by storagectl replace", sid)
		}
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
	for {
		ok, why := check(ctx, src, id, hasState)
		if ok {
			return nil
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

func check(ctx context.Context, src Source, id raftstore.Identity, hasState bool) (bool, string) {
	data, err := src.Get(ctx)
	if err == nil {
		var m Manifest
		if m, err = Parse(data); err == nil {
			return Decide(m, id, hasState)
		}
	}
	if hasState {
		return true, fmt.Sprintf("ordinary restart; manifest unreadable (%v)", err)
	}
	return false, fmt.Sprintf("manifest unreadable: %v", err)
}
