// Package judge holds the S4.3 chaos suite's pass criteria (v3.1 §26). The
// harness (chaos/scripts/lib.sh) observes the cluster and runs invcheck
// verify and check; Judge turns those facts into a verdict, so the criteria
// are Go code with tests rather than jq inside a shell script.
//
// Every experiment must have: a verdict from verify and check; no
// acknowledged write lost; no unknown outcome left unresolved after
// resolving by operation ID (the raw count is reported, not judged); no
// stale read; readers whose reads were checked; a clean, quiescent invariant
// check; a bounded settle; availability lined up with the fault; and writes
// acknowledged somewhere during it. Each experiment then adds its own (see
// experiment). A negative control passes only if its injected fault is
// caught.
package judge

import (
	"encoding/json"
	"fmt"
	"maps"
	"slices"
	"sort"
	"strings"

	"github.com/ParthMalhotra07/ReliefGrid/loadtest/ledger"
)

// Negative controls: runs that must be caught.
const (
	NegativeLostWrite   = "lost-write"   // an acknowledged write overwritten before verify
	NegativeStaleReader = "stale-reader" // readers in stale mode on a lagging replica
)

// MinCoverage is the fewest write attempts, reads, and direct reads per
// probe a run must have on the faulted shard after the grace, in
// [from+grace, to]. Below it the fault was not observed while it fully
// held, and the zero counts that would pass it (no ack, no read served
// without a majority) mean nothing.
//
// The binding case in the saved S4.3 runs is lose-quorum's writes: every
// write to the cut-off shard waits out a call timeout, and its 18 s after
// the grace held 16 failed attempts (all of them its errorsObserved). The
// other shard faults had 23 or more acknowledged writes, and 32 or more
// served reads, after the grace; probes read every 50 ms. Three is about a
// fifth of the binding case, yet more than one or two stray samples.
const MinCoverage = 3

// ReplicasPerShard is each shard's Raft group size; its pods are the
// StatefulSet's storage-shard-<shard>-0..2.
const ReplicasPerShard = 3

// Replicas are shard's pods.
func Replicas(shard int) []string {
	pods := make([]string, ReplicasPerShard)
	for i := range pods {
		pods[i] = fmt.Sprintf("storage-shard-%d-%d", shard, i)
	}
	return pods
}

// Input is everything the harness observed in one run.
type Input struct {
	Experiment string  `json:"experiment"`
	Shard      int     `json:"shard"`  // the faulted shard
	Target     string  `json:"target"` // the faulted pod, also its probe's name
	Negative   string  `json:"negative,omitempty"`
	Bounds     Bounds  `json:"bounds"`
	Verify     *Verify `json:"verify"`
	VerifyExit int     `json:"verifyExit"`
	Check      *Check  `json:"check"`
	CheckExit  int     `json:"checkExit"`
	Harness    Harness `json:"harness"`
}

// Bounds are the time limits the run is held to.
type Bounds struct {
	ElectionMaxSeconds float64 `json:"electionMaxSeconds"` // fault start -> a new leader at a higher term
	RecoveryMaxSeconds float64 `json:"recoveryMaxSeconds"` // heal -> the first ack on the faulted shard
}

// Verify is invcheck verify --json.
type Verify struct {
	OK                bool                `json:"ok"`
	Incomplete        string              `json:"incomplete"`
	Ledger            ledger.Stats        `json:"ledger"`
	Resolution        ledger.Resolution   `json:"resolution"`
	Verdict           *ledger.Verdict     `json:"verdict"`
	Availability      *ledger.WindowStats `json:"availability"`
	AvailabilityError string              `json:"availabilityError"`
}

// Check is invcheck check --json.
type Check struct {
	OK         bool   `json:"ok"`
	Verdict    string `json:"verdict"`
	Incomplete string `json:"incomplete"`
	Report     *struct {
		Quiescent  bool              `json:"quiescent"`
		Violations []json.RawMessage `json:"violations"`
	} `json:"report"`
}

// Harness is what lib.sh measured on the cluster.
type Harness struct {
	SettleOK     bool   `json:"settleOk"`
	SettleDetail string `json:"settleDetail"`
	Recovery     *struct {
		Seconds    float64 `json:"seconds"` // -1: no new leader
		Term       uint64  `json:"term"`
		TermBefore uint64  `json:"termBefore"`
		Leader     string  `json:"leader"`
	} `json:"recovery,omitempty"`
	Isolation *struct {
		TargetCommitAdvanced   bool `json:"targetCommitAdvanced"`
		MajorityCommitAdvanced bool `json:"majorityCommitAdvanced"`
		IsolatedStillLed       bool `json:"isolatedStillLed"`
	} `json:"isolation,omitempty"`
	Delay *struct {
		MaxLag   uint64 `json:"maxLag"`   // largest leader commit - delayed follower raftApplied, sampled during
		FinalLag uint64 `json:"finalLag"` // after the settle
		Samples  int    `json:"samples"`
	} `json:"delay,omitempty"`
	Worker *struct {
		Node string `json:"node"`
		Back bool   `json:"back"` // the node and its pods Ready again
	} `json:"worker,omitempty"`
	Coordinator *struct {
		RingBefore          string            `json:"ringBefore"` // version/checksum
		RingAfter           string            `json:"ringAfter"`
		TermsBefore         map[string]uint64 `json:"termsBefore"` // shard -> leader term
		TermsAfter          map[string]uint64 `json:"termsAfter"`
		BootstrapJobsBefore int               `json:"bootstrapJobsBefore"`
		BootstrapJobsAfter  int               `json:"bootstrapJobsAfter"`
		Back                bool              `json:"back"`
	} `json:"coordinator,omitempty"`
}

// Summary is the run in numbers, judged or not.
type Summary struct {
	Acked             int                  `json:"acked"`
	UnknownRaw        int                  `json:"unknownRaw"` // unknown outcomes as the load saw them
	Resolved          int                  `json:"resolved"`
	NotApplied        int                  `json:"notApplied"`
	UnresolvedUnknown int                  `json:"unresolvedUnknown"` // still unknown after resolving by operation ID
	Lost              int                  `json:"lost"`
	StaleReads        int                  `json:"staleReads"`
	ReadsTotal        int                  `json:"readsTotal"`
	ReadsChecked      int                  `json:"readsChecked"`
	ReadsFailed       int                  `json:"readsFailed"`
	ErrorCount        int                  `json:"errorCount"`
	Availability      []ledger.ShardWindow `json:"availability"`
}

// Result is the verdict.
type Result struct {
	Pass      bool     `json:"pass"`
	NoVerdict bool     `json:"noVerdict"`
	Failures  []string `json:"failures"`
	Summary   Summary  `json:"summary"`
}

// Judge applies the criteria to in.
func Judge(in Input) Result {
	r := Result{Failures: []string{}}
	fail := func(format string, args ...any) { r.Failures = append(r.Failures, fmt.Sprintf(format, args...)) }
	v, c := in.Verify, in.Check
	switch {
	case v == nil || (in.VerifyExit != 0 && in.VerifyExit != 1) || v.Incomplete != "" || v.Verdict == nil:
		r.NoVerdict = true
		fail("no verdict: invcheck verify exit %d%s", in.VerifyExit, incomplete(v))
	case c == nil || (in.CheckExit != 0 && in.CheckExit != 1 && in.CheckExit != 3) || c.Incomplete != "":
		r.NoVerdict = true
		fail("no verdict: invcheck check exit %d", in.CheckExit)
	}
	if r.NoVerdict {
		return r
	}
	s := Summary{Acked: v.Ledger.Acked, UnknownRaw: v.Ledger.Unknown, Resolved: v.Resolution.Resolved, NotApplied: v.Resolution.NotApplied,
		UnresolvedUnknown: v.Resolution.StillUnknown, Lost: len(v.Verdict.Lost), StaleReads: len(v.Verdict.StaleReads),
		ReadsTotal: v.Verdict.Reads.Total, ReadsChecked: v.Verdict.Reads.Checked, ReadsFailed: v.Verdict.Reads.Failed,
		ErrorCount: v.Ledger.ErrorCount, Availability: []ledger.ShardWindow{}}
	if v.Availability != nil {
		s.Availability = v.Availability.Shards
	}
	r.Summary = s

	switch in.Negative {
	case NegativeLostWrite:
		if s.Lost == 0 {
			fail("negative control: the overwritten acknowledged write was not caught (0 lost)")
		}
		r.Pass = len(r.Failures) == 0
		return r
	case NegativeStaleReader:
		if s.StaleReads == 0 {
			fail("negative control: stale readers on a lagging replica were not caught (0 stale reads of %d checked)", s.ReadsChecked)
		}
		r.Pass = len(r.Failures) == 0
		return r
	case "":
	default:
		fail("unknown negative control %q", in.Negative)
		return r
	}

	if s.Lost > 0 {
		fail("ledger: %d acknowledged write(s) lost", s.Lost)
	}
	if s.UnresolvedUnknown > 0 {
		fail("ledger: %d unknown outcome(s) still unresolved after resolving by operation ID (%d unknown before)", s.UnresolvedUnknown, s.UnknownRaw)
	}
	if s.StaleReads > 0 {
		fail("reads: %d stale read(s): a read missed a write acknowledged before it began", s.StaleReads)
	}
	if s.ReadsChecked == 0 {
		fail("reads: no read was checked (%d made): the readers did not run against acknowledged writes", s.ReadsTotal)
	}
	switch {
	case in.CheckExit == 3 || c.Report == nil || !c.Report.Quiescent:
		fail("invcheck: a write committed during the check (not quiescent)")
	case in.CheckExit != 0 || len(c.Report.Violations) > 0:
		fail("invcheck: %s (%d violation(s))", c.Verdict, len(c.Report.Violations))
	}
	if !in.Harness.SettleOK {
		fail("settle: %s", in.Harness.SettleDetail)
	}
	if v.Availability == nil {
		fail("availability: not lined up with the fault (%s)", v.AvailabilityError)
		r.Pass = len(r.Failures) == 0
		return r
	}
	during := 0
	for _, w := range s.Availability {
		during += w.AcksDuring
	}
	if during == 0 {
		fail("load: no write was acknowledged during the fault on any shard")
	}
	if !coverage(in, *v.Availability, fail) {
		r.Pass = false
		return r
	}
	experiment(in, s, fail)
	r.Pass = len(r.Failures) == 0
	return r
}

func incomplete(v *Verify) string {
	if v == nil || v.Incomplete == "" {
		return ""
	}
	return ": " + v.Incomplete
}

// coverage fails a run whose interval after the grace, [from+grace, to],
// is empty, or holds fewer than MinCoverage write attempts or reads on the
// faulted shard (every shard for a node or coordinator fault, which touch
// them all). With no interval at all the experiment's criteria are not
// applied: each would be judged on nothing. It reports whether they are.
//
// restart-coordinator counts write attempts across the shards instead, at
// least MinCoverage per shard in total. Every data call goes through the
// coordinator, so while it is down each attempt waits out the client's
// 5 s timeout and a worker retries one operation up to four times: the
// load's few workers move through only one or two keys each, and which
// shards those land on is chance (measured, S4.3: 2, 18 and 21 attempts
// in 30 s). The fault is the shared routing path, not a shard, so any
// shard's attempt exercises it. Reads stay per shard, and so does the
// check that every shard acknowledges writes again after the restart.
func coverage(in Input, a ledger.WindowStats, fail func(string, ...any)) bool {
	if a.ToMs-a.FromMs <= a.GraceMs {
		fail("coverage: no observation window after the grace: the fault lasted %dms, the grace is %dms", a.ToMs-a.FromMs, a.GraceMs)
		return false
	}
	want := []int{in.Shard}
	if in.Experiment == "stop-worker" || in.Experiment == "restart-coordinator" {
		want = []int{0, 1, 2}
	}
	pooled := in.Experiment == "restart-coordinator"
	writes := 0
	for _, id := range want {
		i := slices.IndexFunc(a.Shards, func(w ledger.ShardWindow) bool { return w.Shard == id })
		if i < 0 {
			fail("coverage: no write attempt or read on shard %d", id)
			continue
		}
		writes += a.Shards[i].WriteAttemptsInsideFault
		if n := a.Shards[i].WriteAttemptsInsideFault; !pooled && n < MinCoverage {
			fail("coverage: shard %d had %d write attempt(s) after the grace, want at least %d", id, n, MinCoverage)
		}
		if n := a.Shards[i].ReadsInsideFault; n < MinCoverage {
			fail("coverage: shard %d had %d read(s) after the grace, want at least %d", id, n, MinCoverage)
		}
	}
	if pooled && writes < MinCoverage*len(want) {
		fail("coverage: %d write attempt(s) after the grace across all shards, want at least %d", writes, MinCoverage*len(want))
	}
	return true
}

// experiment adds the criteria particular to in.Experiment (v3.1 §26 table).
func experiment(in Input, s Summary, fail func(string, ...any)) {
	shards := map[int]ledger.ShardWindow{}
	var ids []int
	for _, w := range s.Availability {
		shards[w.Shard] = w
		ids = append(ids, w.Shard)
	}
	sort.Ints(ids)
	faulted, ok := shards[in.Shard]
	h := in.Harness
	needShard := func() bool {
		if !ok {
			fail("availability: no writes or reads on the faulted shard %d", in.Shard)
		}
		return ok
	}
	newLeader := func() {
		switch rec := h.Recovery; {
		case rec == nil || rec.Seconds < 0:
			fail("recovery: the majority elected no new leader")
		case rec.Term <= rec.TermBefore:
			fail("recovery: leader %s at term %d, not above the faulted leader's term %d", rec.Leader, rec.Term, rec.TermBefore)
		case rec.Seconds > in.Bounds.ElectionMaxSeconds:
			fail("recovery: a new leader after %.1fs, over %.0fs", rec.Seconds, in.Bounds.ElectionMaxSeconds)
		}
	}
	// probesRefused: every pod in want was probed at least MinCoverage
	// times after the grace and served none of it; no other pod of the
	// faulted shard was probed (a wrong target is not coverage).
	probesRefused := func(want []string) {
		for _, name := range want {
			_, during := faulted.ProbeReadsDuring[name]
			_, inside := faulted.ProbeReadsInsideFault[name]
			switch n := faulted.ProbeReadsInsideFault[name]; {
			case !during && !inside:
				fail("probe: missing probe for %s", name)
			case n < MinCoverage:
				fail("probe: %s had %d direct read(s) after the grace, want at least %d", name, n, MinCoverage)
			}
			if n := faulted.ProbeServedInsideFault[name]; n > 0 {
				fail("probe: %s served %d linearizable read(s) while cut off from its group", name, n)
			}
		}
		probed := map[string]bool{}
		for _, m := range []map[string]int{faulted.ProbeReadsDuring, faulted.ProbeReadsInsideFault, faulted.ProbeServedInsideFault} {
			for name := range m {
				probed[name] = true
			}
		}
		for _, name := range slices.Sorted(maps.Keys(probed)) {
			if !slices.Contains(want, name) {
				fail("probe: unexpected probe %s on shard %d (want %s)", name, in.Shard, strings.Join(want, ", "))
			}
		}
	}
	allAcking := func(what string) {
		for _, id := range ids {
			if shards[id].AcksDuring == 0 {
				fail("%s: shard %d acknowledged no write during the fault", what, id)
			}
		}
	}

	switch in.Experiment {
	case "isolate-leader", "delay-isolate-leader":
		if iso := h.Isolation; iso == nil {
			fail("isolation: not observed")
		} else {
			if iso.TargetCommitAdvanced {
				fail("isolation: the isolated %s advanced its commit index", in.Target)
			}
			if !iso.MajorityCommitAdvanced {
				fail("isolation: the majority's commit index did not advance")
			}
			if iso.IsolatedStillLed {
				fail("isolation: the isolated %s still led its group", in.Target)
			}
		}
		newLeader()
		if needShard() {
			if faulted.AcksDuring == 0 {
				fail("majority: shard %d acknowledged no write during the fault", in.Shard)
			}
			if slices.Contains(Replicas(in.Shard), in.Target) {
				probesRefused([]string{in.Target})
			} else {
				fail("probe: target %s is not a replica of shard %d", in.Target, in.Shard)
			}
		}
	case "lose-quorum":
		if needShard() {
			if faulted.AcksInsideFault > 0 {
				fail("quorum: shard %d acknowledged %d write(s) with no majority", in.Shard, faulted.AcksInsideFault)
			}
			if faulted.ReadsServedInsideFault > 0 {
				fail("quorum: shard %d served %d linearizable read(s) with no majority", in.Shard, faulted.ReadsServedInsideFault)
			}
			probesRefused(Replicas(in.Shard))
			switch first := faulted.FirstAckAfterHealMs; {
			case first < 0:
				fail("recovery: shard %d acknowledged no write after the heal", in.Shard)
			case float64(first) > in.Bounds.RecoveryMaxSeconds*1000:
				fail("recovery: shard %d's first ack %.1fs after the heal, over %.0fs", in.Shard, float64(first)/1000, in.Bounds.RecoveryMaxSeconds)
			}
		}
		for _, id := range ids {
			if id != in.Shard && shards[id].AcksDuring == 0 {
				fail("quorum: shard %d (not faulted) acknowledged no write during the fault", id)
			}
		}
	case "delay-follower":
		switch d := h.Delay; {
		case d == nil || d.Samples == 0:
			fail("delay: the follower's lag was not sampled")
		case d.MaxLag == 0:
			fail("delay: the delayed follower's lag never rose (%d samples)", d.Samples)
		case d.FinalLag > 0:
			fail("delay: the delayed follower is still %d entries behind after the settle", d.FinalLag)
		}
		if needShard() && faulted.AcksDuring == 0 {
			fail("majority: shard %d acknowledged no write while a follower was delayed", in.Shard)
		}
	case "stop-worker":
		if h.Worker == nil || !h.Worker.Back {
			fail("worker: the stopped node or its pods did not come back")
		}
		if len(ids) < 3 {
			fail("worker: only shards %v had writes or reads; want all three", ids)
		}
		allAcking("worker")
	case "restart-coordinator":
		co := h.Coordinator
		if co == nil {
			fail("coordinator: not observed")
			return
		}
		if !co.Back {
			fail("coordinator: not Ready again after the restart")
		}
		if co.RingBefore == "" || co.RingBefore != co.RingAfter {
			fail("coordinator: ring %q before, %q after", co.RingBefore, co.RingAfter)
		}
		for sh, before := range co.TermsBefore {
			if after, ok := co.TermsAfter[sh]; !ok || after != before {
				fail("coordinator: shard %s's leader term %d before, %d after: the restart caused an election", sh, before, after)
			}
		}
		if co.BootstrapJobsAfter != co.BootstrapJobsBefore {
			fail("coordinator: %d bootstrap Job(s) before, %d after: no bootstrap may follow a restart", co.BootstrapJobsBefore, co.BootstrapJobsAfter)
		}
		for _, id := range ids {
			if w := shards[id]; w.FirstAckAfterHealMs < 0 {
				fail("coordinator: shard %d acknowledged no write after the restart", id)
			}
		}
	case "kill-leader", "kill-follower", "restart-shard", "isolate-replica":
		// S2.3's own assertions stay in lib.sh; the common criteria apply.
	default:
		fail("unknown experiment %q", in.Experiment)
	}
}
