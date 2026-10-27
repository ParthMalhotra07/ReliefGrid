package main

import (
	"encoding/json"
	"fmt"
	"slices"
	"strconv"
)

// Bootstrap phases recorded in a shard's manifest ConfigMap (v3.1 §10).
const (
	phasePending       = "PENDING"       // rendered by the chart; no group exists
	phaseBootstrapping = "BOOTSTRAPPING" // peerIds recorded; a bootstrap may have been issued
	phaseBootstrapped  = "BOOTSTRAPPED"  // a quorum-backed fence applied; never bootstrap again
)

// manifest is one shard's bootstrap manifest. It lives in a ConfigMap outside
// the data PVCs, so losing every disk does not erase the record that the
// group was created.
type manifest struct {
	Name            string // ConfigMap name
	ResourceVersion string // for compare-and-swap updates
	ManagedBy       string // its app.kubernetes.io/managed-by label

	GroupID     string
	Shard       int
	RingVersion uint64
	Peers       []peer
	Phase       string
	PeerIDs     []string // podName/incarnation of the group's voters, in Peers order

	// M7 (written only by storagectl replace).
	Admitted map[string]string // podName -> incarnation admitted to open Raft
	Removed  []string          // ServerIDs replace removed from the group
	Replace  *replaceRecord    // the replacement in progress, if any
}

// replaceRecord is a replacement in progress, persisted so that a crashed
// storagectl resumes it rather than starting another (v3.1 §11).
type replaceRecord struct {
	PodName     string `json:"podName"`
	OldID       string `json:"oldId"`
	NewID       string `json:"newId"`
	Phase       string `json:"phase"`
	ConfigIndex uint64 `json:"configIndex"` // the configuration the last recorded phase saw
}

type peer struct {
	PodName string `json:"podName"`
	Address string `json:"address"` // stable Raft FQDN host:port
}

// parseManifest reads the ConfigMap data written by the chart and the Job.
func parseManifest(name, resourceVersion string, data map[string]string) (manifest, error) {
	m := manifest{Name: name, ResourceVersion: resourceVersion, GroupID: data["groupId"], Phase: data["phase"]}
	var err error
	if m.Shard, err = strconv.Atoi(data["shard"]); err != nil {
		return manifest{}, fmt.Errorf("manifest %s: shard %q: %w", name, data["shard"], err)
	}
	if m.RingVersion, err = strconv.ParseUint(data["ringVersion"], 10, 64); err != nil {
		return manifest{}, fmt.Errorf("manifest %s: ringVersion %q: %w", name, data["ringVersion"], err)
	}
	if err := json.Unmarshal([]byte(data["peers"]), &m.Peers); err != nil {
		return manifest{}, fmt.Errorf("manifest %s: peers: %w", name, err)
	}
	if v := data["peerIds"]; v != "" {
		if err := json.Unmarshal([]byte(v), &m.PeerIDs); err != nil {
			return manifest{}, fmt.Errorf("manifest %s: peerIds: %w", name, err)
		}
	}
	for key, dst := range map[string]any{"admitted": &m.Admitted, "removed": &m.Removed, "replace": &m.Replace} {
		if v := data[key]; v != "" {
			if err := json.Unmarshal([]byte(v), dst); err != nil {
				return manifest{}, fmt.Errorf("manifest %s: %s: %w", name, key, err)
			}
		}
	}
	if r := m.Replace; r != nil {
		switch r.Phase {
		case replacePlanned, replaceRemoved, replaceAdmitted, replaceNonvoter, replaceVoter:
		default:
			return manifest{}, fmt.Errorf("manifest %s: replace phase %q", name, r.Phase)
		}
	}
	switch {
	case m.GroupID == "":
		return manifest{}, fmt.Errorf("manifest %s: groupId is empty", name)
	case len(m.Peers) == 0:
		return manifest{}, fmt.Errorf("manifest %s: no peers", name)
	}
	switch m.Phase {
	case phasePending, phaseBootstrapping, phaseBootstrapped:
	default:
		return manifest{}, fmt.Errorf("manifest %s: unknown phase %q", name, m.Phase)
	}
	if m.Phase != phasePending && len(m.PeerIDs) != len(m.Peers) {
		return manifest{}, fmt.Errorf("manifest %s: phase %s needs %d peerIds, has %d", name, m.Phase, len(m.Peers), len(m.PeerIDs))
	}
	return m, nil
}

// data is the ConfigMap data for m.
func (m manifest) data() map[string]string {
	peers, _ := json.Marshal(m.Peers)
	d := map[string]string{
		"groupId":     m.GroupID,
		"shard":       strconv.Itoa(m.Shard),
		"ringVersion": strconv.FormatUint(m.RingVersion, 10),
		"peers":       string(peers),
		"phase":       m.Phase,
	}
	if len(m.PeerIDs) > 0 {
		ids, _ := json.Marshal(m.PeerIDs)
		d["peerIds"] = string(ids)
	}
	if len(m.Admitted) > 0 {
		b, _ := json.Marshal(m.Admitted)
		d["admitted"] = string(b)
	}
	if len(m.Removed) > 0 {
		b, _ := json.Marshal(m.Removed)
		d["removed"] = string(b)
	}
	if m.Replace != nil {
		b, _ := json.Marshal(m.Replace)
		d["replace"] = string(b)
	}
	return d
}

// differs is empty if m is storagectl's record of the group spec describes;
// otherwise it says what differs. Only phase and peerIds may change after
// creation, so anything else means the ConfigMap was planted, left by an
// older chart, or describes another group.
func (m manifest) differs(spec manifest) string {
	switch {
	case m.ManagedBy != managedByStoragectl:
		return fmt.Sprintf("label %s is %q, not %q", labelManagedBy, m.ManagedBy, managedByStoragectl)
	case m.GroupID != spec.GroupID:
		return fmt.Sprintf("groupId is %s, the chart's is %s", m.GroupID, spec.GroupID)
	case m.Shard != spec.Shard:
		return fmt.Sprintf("shard is %d, the chart's is %d", m.Shard, spec.Shard)
	case m.RingVersion != spec.RingVersion:
		return fmt.Sprintf("ringVersion is %d, the chart's is %d", m.RingVersion, spec.RingVersion)
	case !slices.Equal(m.Peers, spec.Peers):
		return fmt.Sprintf("peers are %v, the chart's are %v", m.Peers, spec.Peers)
	}
	return ""
}

func (m manifest) majority() int { return len(m.Peers)/2 + 1 }
