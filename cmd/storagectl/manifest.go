package main

import (
	"encoding/json"
	"fmt"
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

	GroupID     string
	Shard       int
	RingVersion uint64
	Peers       []peer
	Phase       string
	PeerIDs     []string // podName/incarnation of the three original voters
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
	return d
}

func (m manifest) majority() int { return len(m.Peers)/2 + 1 }
