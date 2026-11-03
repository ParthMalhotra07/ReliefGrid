// Package audit compares the Merkle digests of a shard's replicas (S4.5,
// v3.1 §15 and §26) through their management endpoints, at one command
// index they all hold, and narrows a mismatch to table, bucket and key. It
// only reads; it never repairs. A divergent replica is rebuilt through Raft
// (restart it: its FSM is derived from snapshot and log, M3) or replaced
// with storagectl replace, never patched with rows from another replica.
//
// The audit needs writers, relay and matcher admission quiescent. It picks
// the highest applied index the replicas report, waits until all of them
// are exactly there, and asks each for its digest at that index. A replica
// that has moved answers 409 with its new index, and the audit retargets;
// if they will not hold one index it gives up as not quiescent rather than
// compare different times. It never asks for a digest without an exact
// index.
package audit

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"slices"
	"strconv"
	"time"

	"github.com/ParthMalhotra07/ReliefGrid/internal/fsm/merkle"
)

// anyIndex is fsm.AnyIndex: the index a replica never reaches and that the
// audit never asks for (this package does not import fsm, so storagectl
// does not link SQLite).
const anyIndex = math.MaxUint64

// Verdict is the audit's outcome.
type Verdict string

const (
	Equal        Verdict = "equal"         // every replica has the same root at the index
	Divergent    Verdict = "divergent"     // they differ; Rows names where
	Incomplete   Verdict = "incomplete"    // a replica failed or they are not comparable: no verdict
	NotQuiescent Verdict = "not-quiescent" // they would not hold one index: no verdict
)

// ExitCode is the storagectl exit status for v (the same codes as invcheck).
func (v Verdict) ExitCode() int {
	switch v {
	case Equal:
		return 0
	case Divergent:
		return 1
	case NotQuiescent:
		return 3
	default:
		return 2
	}
}

// Config is what to audit and how patiently.
type Config struct {
	Addrs    []string      // management endpoints (host:port) of one shard's replicas
	Token    string        // operator token
	Client   *http.Client  // default http.DefaultClient
	Attempts int           // targets tried before giving up as not quiescent (default 5)
	Settle   time.Duration // how long every replica may take to reach a target (default 30s)
	Poll     time.Duration // how often statuses are read while waiting (default 200ms)
}

// Pod is one replica as the audit saw it.
type Pod struct {
	Pod              string      `json:"pod"`
	Addr             string      `json:"addr"`
	AppliedIndex     uint64      `json:"appliedIndex"`
	RaftAppliedIndex uint64      `json:"raftAppliedIndex"`
	RingVersion      uint64      `json:"ringVersion"`
	SchemaVersion    string      `json:"schemaVersion,omitempty"`
	Tombstones       int         `json:"tombstones"`
	Root             merkle.Hash `json:"root"`
}

// Report is the audit's result, printed by storagectl audit.
type Report struct {
	Verdict      Verdict            `json:"verdict"`
	AppliedIndex uint64             `json:"appliedIndex,omitempty"` // the command index compared
	Pods         []Pod              `json:"pods,omitempty"`
	Tables       []merkle.TableDiff `json:"tables,omitempty"`
	Rows         []merkle.RowDiff   `json:"rows,omitempty"`
	Notes        []string           `json:"notes,omitempty"`
	Error        string             `json:"error,omitempty"`
}

type status struct {
	PodName          string `json:"podName"`
	GroupID          string `json:"groupId"`
	Shard            int    `json:"shard"`
	RingVersion      uint64 `json:"ringVersion"`
	AppliedIndex     uint64 `json:"appliedIndex"`
	RaftAppliedIndex uint64 `json:"raftAppliedIndex"`
}

type digestResponse struct {
	PodName     string `json:"podName"`
	RingVersion uint64 `json:"ringVersion"`
	Summary     struct {
		SchemaVersion string `json:"schemaVersion"`
		AppliedIndex  uint64 `json:"appliedIndex"`
		Tombstones    int    `json:"tombstones"`
	} `json:"summary"`
	Digest merkle.Digest `json:"digest"`
}

type bucketResponse struct {
	AppliedIndex uint64        `json:"appliedIndex"`
	Table        string        `json:"table"`
	Bucket       int           `json:"bucket"`
	Hash         merkle.Hash   `json:"hash"`
	Leaves       []merkle.Leaf `json:"leaves"`
}

// errMoved: a replica is no longer at the target; Index is where it is.
type errMoved struct{ Index uint64 }

func (e *errMoved) Error() string { return fmt.Sprintf("replica moved to applied index %d", e.Index) }

type auditor struct {
	cfg   Config
	names []string // pod name per address, unique
}

// Run audits the replicas at cfg.Addrs. It never returns an error: every
// failure is a Report with a verdict and the reason.
func Run(ctx context.Context, cfg Config) Report {
	if len(cfg.Addrs) < 2 {
		return Report{Verdict: Incomplete, Error: "audit: need at least two replicas"}
	}
	if cfg.Token == "" {
		return Report{Verdict: Incomplete, Error: "audit: an operator token is required"}
	}
	if cfg.Client == nil {
		cfg.Client = http.DefaultClient
	}
	if cfg.Attempts <= 0 {
		cfg.Attempts = 5
	}
	if cfg.Settle <= 0 {
		cfg.Settle = 30 * time.Second
	}
	if cfg.Poll <= 0 {
		cfg.Poll = 200 * time.Millisecond
	}
	a := &auditor{cfg: cfg}
	return a.run(ctx)
}

func (a *auditor) run(ctx context.Context) Report {
	sts, err := a.statuses(ctx)
	if err != nil {
		return Report{Verdict: Incomplete, Error: err.Error()}
	}
	if err := a.comparable(sts); err != nil {
		return Report{Verdict: Incomplete, Error: err.Error()}
	}
	target := maxApplied(sts)
	for range a.cfg.Attempts {
		if target == anyIndex {
			return Report{Verdict: Incomplete, Error: "audit: a replica reports an impossible applied index; not asking for a digest at it"}
		}
		sts, err = a.waitAt(ctx, target)
		var mv *errMoved
		if errors.As(err, &mv) {
			target = mv.Index
			continue
		}
		if err != nil {
			return a.failed(err)
		}
		digests, err := a.digests(ctx, target)
		if errors.As(err, &mv) {
			target = max(mv.Index, target)
			continue
		}
		if err != nil {
			return a.failed(err)
		}
		return a.compare(ctx, target, sts, digests)
	}
	return Report{Verdict: NotQuiescent, AppliedIndex: target,
		Error: fmt.Sprintf("audit: replicas did not hold one applied index in %d attempts; quiesce writers and relay first", a.cfg.Attempts)}
}

// errStuck: the replicas did not all reach the target within Settle.
type errStuck struct{ msg string }

func (e *errStuck) Error() string { return e.msg }

func (a *auditor) failed(err error) Report {
	var st *errStuck
	if errors.As(err, &st) {
		return Report{Verdict: NotQuiescent, Error: err.Error()}
	}
	return Report{Verdict: Incomplete, Error: err.Error()}
}

func (a *auditor) statuses(ctx context.Context) ([]status, error) {
	sts := make([]status, len(a.cfg.Addrs))
	for i, addr := range a.cfg.Addrs {
		if err := a.get(ctx, addr, "/mgmt/status", false, &sts[i]); err != nil {
			return nil, err
		}
	}
	if a.names == nil {
		seen := map[string]bool{}
		for i, s := range sts {
			name := s.PodName
			if name == "" || seen[name] {
				name = a.cfg.Addrs[i]
			}
			seen[name] = true
			a.names = append(a.names, name)
		}
	}
	return sts, nil
}

// comparable checks the replicas are one shard of one group on one ring.
func (a *auditor) comparable(sts []status) error {
	for i, s := range sts[1:] {
		r := sts[0]
		if s.GroupID != r.GroupID || s.Shard != r.Shard {
			return fmt.Errorf("audit: %s is %s shard %d, %s is %s shard %d: not one shard's replicas",
				a.names[i+1], s.GroupID, s.Shard, a.names[0], r.GroupID, r.Shard)
		}
		if s.RingVersion != r.RingVersion {
			return fmt.Errorf("audit: %s has ring version %d, %s has %d: not comparable", a.names[i+1], s.RingVersion, a.names[0], r.RingVersion)
		}
	}
	return nil
}

func maxApplied(sts []status) uint64 {
	m := uint64(0)
	for _, s := range sts {
		m = max(m, s.AppliedIndex)
	}
	return m
}

// waitAt polls until every replica's applied index is exactly target. A
// replica past it is errMoved; Settle passing first is errStuck.
func (a *auditor) waitAt(ctx context.Context, target uint64) ([]status, error) {
	deadline := time.Now().Add(a.cfg.Settle)
	for {
		sts, err := a.statuses(ctx)
		if err != nil {
			return nil, err
		}
		if err := a.comparable(sts); err != nil {
			return nil, err
		}
		if m := maxApplied(sts); m > target {
			return nil, &errMoved{Index: m}
		}
		var behind []string
		for i, s := range sts {
			if s.AppliedIndex != target {
				behind = append(behind, fmt.Sprintf("%s at %d", a.names[i], s.AppliedIndex))
			}
		}
		if behind == nil {
			return sts, nil
		}
		if time.Now().After(deadline) {
			return nil, &errStuck{msg: fmt.Sprintf("audit: not every replica reached applied index %d within %s: %v", target, a.cfg.Settle, behind)}
		}
		select {
		case <-ctx.Done():
			return nil, fmt.Errorf("audit: waiting for applied index %d: %w", target, ctx.Err())
		case <-time.After(a.cfg.Poll):
		}
	}
}

func (a *auditor) digests(ctx context.Context, at uint64) ([]digestResponse, error) {
	out := make([]digestResponse, len(a.cfg.Addrs))
	for i, addr := range a.cfg.Addrs {
		if err := a.get(ctx, addr, "/mgmt/merkle?at="+strconv.FormatUint(at, 10), true, &out[i]); err != nil {
			return nil, err
		}
		if out[i].Summary.AppliedIndex != at {
			return nil, fmt.Errorf("audit: %s answered for applied index %d, asked for %d", a.names[i], out[i].Summary.AppliedIndex, at)
		}
	}
	return out, nil
}

func (a *auditor) compare(ctx context.Context, at uint64, sts []status, ds []digestResponse) Report {
	rep := Report{AppliedIndex: at}
	byPod := map[string]merkle.Digest{}
	for i, d := range ds {
		rep.Pods = append(rep.Pods, Pod{Pod: a.names[i], Addr: a.cfg.Addrs[i], AppliedIndex: d.Summary.AppliedIndex,
			RaftAppliedIndex: sts[i].RaftAppliedIndex, RingVersion: d.RingVersion, SchemaVersion: d.Summary.SchemaVersion,
			Tombstones: d.Summary.Tombstones, Root: d.Digest.Root})
		byPod[a.names[i]] = d.Digest
	}
	slices.SortFunc(rep.Pods, func(x, y Pod) int {
		switch {
		case x.Pod < y.Pod:
			return -1
		case x.Pod > y.Pod:
			return 1
		}
		return 0
	})
	for _, p := range rep.Pods[1:] {
		if p.SchemaVersion != rep.Pods[0].SchemaVersion || p.RingVersion != rep.Pods[0].RingVersion {
			rep.Verdict, rep.Error = Incomplete, fmt.Sprintf("audit: %s has schema %q ring %d, %s has schema %q ring %d: not comparable",
				p.Pod, p.SchemaVersion, p.RingVersion, rep.Pods[0].Pod, rep.Pods[0].SchemaVersion, rep.Pods[0].RingVersion)
			return rep
		}
		if p.RaftAppliedIndex != rep.Pods[0].RaftAppliedIndex {
			rep.Notes = []string{fmt.Sprintf("raft applied indexes differ at command index %d (term no-ops, barriers and configuration entries move them without changing records); the comparison is at the command index", at)}
		}
	}
	tables, err := merkle.DiffBuckets(byPod)
	if err != nil {
		rep.Verdict, rep.Error = Incomplete, err.Error()
		return rep
	}
	if tables == nil {
		rep.Verdict = Equal
		return rep
	}
	rep.Tables = tables
	for _, td := range tables {
		ti := slices.IndexFunc(ds[0].Digest.Tables, func(t merkle.TableDigest) bool { return t.Name == td.Table })
		for _, b := range td.Buckets {
			leaves := map[string][]merkle.Leaf{}
			for i, addr := range a.cfg.Addrs {
				var br bucketResponse
				path := fmt.Sprintf("/mgmt/merkle?at=%d&table=%s&bucket=%d", at, td.Table, b)
				err := a.get(ctx, addr, path, true, &br)
				var mv *errMoved
				if errors.As(err, &mv) {
					rep.Verdict, rep.Error = NotQuiescent, fmt.Sprintf("audit: %s moved to %d while its buckets were read", a.names[i], mv.Index)
					rep.Rows = nil
					return rep
				}
				if err != nil {
					rep.Verdict, rep.Error = Incomplete, err.Error()
					return rep
				}
				if br.Hash != ds[i].Digest.Tables[ti].Buckets[b] || merkle.BucketHash(br.Leaves) != br.Hash {
					rep.Verdict, rep.Error = Incomplete, fmt.Sprintf("audit: %s: bucket %s/%d does not match its digest", a.names[i], td.Table, b)
					return rep
				}
				leaves[a.names[i]] = br.Leaves
			}
			rep.Rows = append(rep.Rows, merkle.DiffRows(td.Table, leaves)...)
		}
	}
	rep.Verdict = Divergent
	return rep
}

// get decodes a 200 answer into out. A 409 INDEX_MISMATCH is errMoved.
func (a *auditor) get(ctx context.Context, addr, path string, auth bool, out any) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://"+addr+path, nil)
	if err != nil {
		return fmt.Errorf("audit: %s: %w", addr, err)
	}
	if auth {
		req.Header.Set("Authorization", "Bearer "+a.cfg.Token)
	}
	resp, err := a.cfg.Client.Do(req)
	if err != nil {
		return fmt.Errorf("audit: GET %s%s: %w", addr, path, err)
	}
	defer func() { _ = resp.Body.Close() }()
	body, err := io.ReadAll(io.LimitReader(resp.Body, 8<<20))
	if err != nil {
		return fmt.Errorf("audit: GET %s%s: %w", addr, path, err)
	}
	if resp.StatusCode == http.StatusConflict {
		var m struct {
			Code         string `json:"code"`
			AppliedIndex uint64 `json:"appliedIndex"`
		}
		if json.Unmarshal(body, &m) == nil && m.Code == "INDEX_MISMATCH" {
			return &errMoved{Index: m.AppliedIndex}
		}
	}
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("audit: GET %s%s: HTTP %d: %s", addr, path, resp.StatusCode, bytes.TrimSpace(body))
	}
	if err := json.Unmarshal(body, out); err != nil {
		return fmt.Errorf("audit: GET %s%s: decode: %w", addr, path, err)
	}
	return nil
}
