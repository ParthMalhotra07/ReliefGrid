package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"slices"
	"strings"
	"time"
)

// Replacement phases recorded in the manifest's replace field (v3.1 §11).
// Each is recorded only after what it names has committed.
const (
	replacePlanned  = "PLANNED"  // preconditions held; the old voter is to be removed
	replaceRemoved  = "REMOVED"  // RemoveServer(old) committed
	replaceAdmitted = "ADMITTED" // admitted[pod] = new incarnation written (M7)
	replaceNonvoter = "NONVOTER" // the new incarnation is a member (nonvoter)
	replaceVoter    = "VOTER"    // the new incarnation is a voter
)

// Steps after which a crash can be injected (STORAGECTL_CRASH_AT; tests and
// the Verify only): each right after an effect, before the next record.
const (
	stepPlanned          = "planned"
	stepRemoved          = "removed"
	stepRecordedRemoved  = "recorded-removed"
	stepAdmitted         = "admitted"
	stepNonvoter         = "nonvoter"
	stepRecordedNonvoter = "recorded-nonvoter"
	stepVoter            = "voter"
	stepRecordedVoter    = "recorded-voter"
)

var replaceSteps = []string{stepPlanned, stepRemoved, stepRecordedRemoved, stepAdmitted, stepNonvoter, stepRecordedNonvoter, stepVoter, stepRecordedVoter}

// errCrash is the injected crash.
var errCrash = errors.New("storagectl: crash injected (STORAGECTL_CRASH_AT)")

// Membership answers from POST /mgmt/membership.
var (
	errNotLeader     = errors.New("storagectl: not the leader")
	errConfigChanged = errors.New("storagectl: configuration changed")
)

const (
	opRemove      = "remove"
	opAddNonvoter = "add-nonvoter"
	opAddVoter    = "add-voter"
)

// membershipChange is the /mgmt/membership body.
type membershipChange struct {
	Op        string `json:"op"`
	ID        string `json:"id"`
	Address   string `json:"address,omitempty"`
	PrevIndex uint64 `json:"prevIndex"`
}

// memberClient sends membership changes to a node's management endpoint.
type memberClient interface {
	// Membership returns the change's index once it has committed. errNotLeader
	// and errConfigChanged changed nothing; any other error leaves the outcome
	// unknown.
	Membership(ctx context.Context, mgmtAddr string, c membershipChange) (uint64, error)
}

// replacer is storagectl replace (v3.1 §11, M7): replace one voter whose
// disk was lost by the new, empty incarnation now running under its pod
// name. Every decision is made from a fresh read of the manifest and the
// peers, so a run that stops at any point is resumed by the next.
type replacer struct {
	store    manifestStore
	peers    peerClient
	members  memberClient
	mgmtPort string
	poll     time.Duration // between rounds while waiting
	call     time.Duration // per management call
	crashAt  string        // STORAGECTL_CRASH_AT
	log      *slog.Logger
}

func (r *replacer) crash(step string) error {
	if r.crashAt == step {
		r.log.Warn("crashing on purpose (STORAGECTL_CRASH_AT)", "step", step)
		return fmt.Errorf("%w after step %s", errCrash, step)
	}
	return nil
}

// view is one round's reads.
type view struct {
	m      manifest
	idx    int                   // the pod's position in Peers and PeerIDs
	sts    map[string]nodeStatus // reachable peers, by pod
	leader string                // the leader's pod; "" if none
	cfg    *raftConfiguration    // the leader's committed configuration; nil if none
}

func (r *replacer) read(ctx context.Context, spec manifest, pod string) (view, error) {
	m, err := r.store.Get(ctx, spec.Name)
	if err != nil {
		return view{}, err
	}
	if err := refuseForeign(m, spec); err != nil {
		return view{}, err
	}
	v := view{m: m, idx: slices.IndexFunc(m.Peers, func(p peer) bool { return p.PodName == pod }), sts: map[string]nodeStatus{}}
	for _, p := range m.Peers {
		addr, err := mgmtAddr(p.Address, r.mgmtPort)
		if err != nil {
			return view{}, err
		}
		callCtx, cancel := context.WithTimeout(ctx, r.call)
		s, err := r.peers.Status(callCtx, addr)
		cancel()
		if err == nil {
			v.sts[p.PodName] = s
		}
	}
	var term uint64
	for _, p := range m.Peers {
		if s, ok := v.sts[p.PodName]; ok && s.State == "Leader" && s.Term >= term {
			v.leader, term = p.PodName, s.Term
			if s.Configuration != nil && s.Configuration.Committed {
				v.cfg = s.Configuration
			} else {
				v.cfg = nil
			}
		}
	}
	return v, nil
}

func (v view) leaderAddr(port string) string {
	for _, p := range v.m.Peers {
		if p.PodName == v.leader {
			a, _ := mgmtAddr(p.Address, port)
			return a
		}
	}
	return ""
}

// healthyVoters says why the voters other than skip are not all reachable,
// holding Raft state, caught up and following one leader that is among them;
// "" if they are. These are the voters every change needs while the group
// is down to them.
func (v view) healthyVoters(skip string) string {
	if v.cfg == nil {
		return "no leader reports a committed configuration"
	}
	n := 0
	for _, s := range v.cfg.Servers {
		if s.Suffrage != suffrageVoter || s.ID == skip {
			continue
		}
		n++
		pod, _, _ := strings.Cut(s.ID, "/")
		st, ok := v.sts[pod]
		switch {
		case !ok:
			return fmt.Sprintf("voter %s is unreachable", s.ID)
		case st.ServerID != s.ID:
			return fmt.Sprintf("voter %s answers as %s", s.ID, st.ServerID)
		case !st.HasState || !st.CaughtUp:
			return fmt.Sprintf("voter %s is not caught up", s.ID)
		case st.LeaderID != v.sts[v.leader].ServerID:
			return fmt.Sprintf("voter %s follows %q, not %s", s.ID, st.LeaderID, v.sts[v.leader].ServerID)
		}
	}
	if n < 2 {
		return fmt.Sprintf("only %d voters besides %s", n, skip)
	}
	return ""
}

// run replaces pod's old voter in spec's group.
func (r *replacer) run(ctx context.Context, spec manifest, pod string) error {
	lastWait := ""
	wait := func(why string) error {
		if why != lastWait {
			r.log.Info(spec.GroupID+": waiting", "pod", pod, "reason", why)
			lastWait = why
		}
		if err := sleep(ctx, r.poll); err != nil {
			return fmt.Errorf("%s: replace %s: %w (last: %s)", spec.GroupID, pod, err, why)
		}
		return nil
	}
	for {
		v, err := r.read(ctx, spec, pod)
		if err != nil {
			return err
		}
		rec := v.m.Replace
		if rec == nil {
			done, err := r.plan(ctx, v, pod)
			if done || err != nil {
				return err
			}
			continue
		}
		if rec.PodName != pod {
			return fmt.Errorf("%w: %s: a replacement of %s is in progress (phase %s); finish it first", errRefused, v.m.GroupID, rec.PodName, rec.Phase)
		}
		if v.cfg == nil {
			if err := wait("no leader reports a committed configuration"); err != nil {
				return err
			}
			continue
		}
		newCfg, isMember := v.cfg.has(rec.NewID)
		_, oldIsMember := v.cfg.has(rec.OldID)
		var why string
		switch rec.Phase {
		case replacePlanned:
			if !oldIsMember && v.cfg.Index <= rec.ConfigIndex {
				// Not evidence: removal commits a newer configuration than
				// the one PLANNED saw. A view without the old ID at the same
				// index is stale or someone else's.
				why = fmt.Sprintf("%s is absent from a configuration at %d, not newer than PLANNED's %d", rec.OldID, v.cfg.Index, rec.ConfigIndex)
				break
			}
			if !oldIsMember {
				// Removal established from the committed configuration, not
				// from a response that may have been lost (§11 step 2).
				err = r.record(ctx, v, replaceRemoved, func(m *manifest) {
					if !slices.Contains(m.Removed, rec.OldID) {
						m.Removed = append(m.Removed, rec.OldID)
					}
				}, stepRecordedRemoved)
				break
			}
			if why = v.healthyVoters(rec.OldID); why == "" {
				why, err = r.change(ctx, v, membershipChange{Op: opRemove, ID: rec.OldID, PrevIndex: v.cfg.Index}, stepRemoved)
			}
		case replaceRemoved:
			// M7: only now, with the removal committed, may the new
			// incarnation open its transport; re-checked at the write, so a
			// removal that no longer holds never admits.
			if oldIsMember {
				why = rec.OldID + " is a member again; the new incarnation is not admitted while it is"
				break
			}
			_, inc, _ := strings.Cut(rec.NewID, "/")
			err = r.record(ctx, v, replaceAdmitted, func(m *manifest) {
				if m.Admitted == nil {
					m.Admitted = map[string]string{}
				}
				m.Admitted[pod] = inc
			}, stepAdmitted)
		case replaceAdmitted:
			if oldIsMember {
				why = rec.OldID + " is a member again; nothing is added while it is"
				break
			}
			if isMember {
				err = r.record(ctx, v, replaceNonvoter, nil, stepRecordedNonvoter)
				break
			}
			if why = v.healthyVoters(""); why == "" {
				if _, ok := v.sts[pod]; !ok {
					why = pod + " is unreachable"
				} else {
					why, err = r.change(ctx, v, membershipChange{Op: opAddNonvoter, ID: rec.NewID, Address: v.m.Peers[v.idx].Address, PrevIndex: v.cfg.Index}, stepNonvoter)
				}
			}
		case replaceNonvoter:
			if isMember && newCfg.Suffrage == suffrageVoter {
				err = r.record(ctx, v, replaceVoter, nil, stepRecordedVoter)
				break
			}
			st := v.sts[pod]
			switch {
			case oldIsMember:
				why = rec.OldID + " is a member again; nothing is promoted while it is"
			case !isMember:
				why = rec.NewID + " is not in the committed configuration"
			case st.ServerID != rec.NewID || !st.HasState || !st.CaughtUp:
				why = fmt.Sprintf("%s is catching up (state %q, caughtUp %v)", rec.NewID, st.State, st.CaughtUp)
			default:
				if why = v.healthyVoters(rec.NewID); why == "" {
					why, err = r.change(ctx, v, membershipChange{Op: opAddVoter, ID: rec.NewID, Address: v.m.Peers[v.idx].Address, PrevIndex: v.cfg.Index}, stepVoter)
				}
			}
		case replaceVoter:
			var done bool
			if done, why, err = r.finish(ctx, v, pod); done {
				return err
			}
		}
		if err != nil {
			return err
		}
		if why != "" {
			if err := wait(why); err != nil {
				return err
			}
		}
	}
}

// abort clears a replacement stuck in PLANNED, only while nothing can have
// changed: the leader's latest configuration is committed, is still the one
// PLANNED recorded, and still has the old voter. Then nothing was removed and
// the new disk was never admitted, so clearing the record is safe; the disk
// can be replaced again (wiped again first, or as it is). Anything later is
// refused: resume it with storagectl replace instead.
func (r *replacer) abort(ctx context.Context, spec manifest, pod string) error {
	for {
		v, err := r.read(ctx, spec, pod)
		if err != nil {
			return err
		}
		rec := v.m.Replace
		refuse := func(format string, args ...any) error {
			return fmt.Errorf("%w: %s: abort %s: %s; resume it with storagectl replace instead", errRefused, v.m.GroupID, pod, fmt.Sprintf(format, args...))
		}
		switch {
		case rec == nil:
			r.log.Info(v.m.GroupID+": no replacement in progress; nothing to abort", "pod", pod)
			return nil
		case rec.PodName != pod:
			return fmt.Errorf("%w: %s: the replacement in progress is of %s, not %s", errRefused, v.m.GroupID, rec.PodName, pod)
		case rec.Phase != replacePlanned:
			return refuse("phase %s is past PLANNED", rec.Phase)
		case v.cfg == nil:
			return refuse("no leader reports a committed configuration, so a removal may be in flight")
		case v.cfg.Index != rec.ConfigIndex:
			return refuse("the configuration moved from %d to %d since PLANNED (the removal may have committed)", rec.ConfigIndex, v.cfg.Index)
		}
		if _, ok := v.cfg.has(rec.OldID); !ok {
			return refuse("%s is no longer in the configuration", rec.OldID)
		}
		m := v.m
		m.Replace = nil
		if _, err := r.store.Update(ctx, m); errors.Is(err, errConflict) {
			continue
		} else if err != nil {
			return err
		}
		r.log.Info(m.GroupID+": aborted the PLANNED replacement; nothing was removed or admitted", "pod", pod, "oldId", rec.OldID, "newId", rec.NewID)
		return nil
	}
}

// plan checks every precondition and records PLANNED, or finds the
// replacement already done. A refusal changes nothing.
func (r *replacer) plan(ctx context.Context, v view, pod string) (done bool, err error) {
	refuse := func(format string, args ...any) (bool, error) {
		return false, fmt.Errorf("%w: %s: replace %s: %s", errRefused, v.m.GroupID, pod, fmt.Sprintf(format, args...))
	}
	switch {
	case v.m.Phase != phaseBootstrapped:
		return refuse("the group is %s, not BOOTSTRAPPED", v.m.Phase)
	case v.idx < 0:
		return refuse("%s is not a peer of %s", pod, v.m.GroupID)
	}
	oldID := v.m.PeerIDs[v.idx]
	st, ok := v.sts[pod]
	if !ok {
		return refuse("%s is unreachable: the replacement must be running (unadmitted) before replace", pod)
	}
	if st.ServerID == oldID {
		_, inc, _ := strings.Cut(oldID, "/")
		if v.m.Admitted[pod] == inc && v.cfg != nil {
			if got, all := v.cfg.voters(); all && slices.Equal(got, slices.Sorted(slices.Values(v.m.PeerIDs))) {
				r.log.Info(v.m.GroupID+": already replaced; verified", "pod", pod, "voters", got)
				return true, nil
			}
		}
		return refuse("%s is still running as %s, a member: stop it and wipe its disk first", pod, oldID)
	}
	if st.HasState {
		return refuse("%s's disk (%s) is not empty", pod, st.ServerID)
	}
	if v.cfg == nil {
		return refuse("not two healthy voters: no leader reports a committed configuration")
	}
	if _, ok := v.cfg.has(oldID); !ok {
		return refuse("%s is not in the committed configuration", oldID)
	}
	if why := v.healthyVoters(oldID); why != "" {
		return refuse("not two healthy voters besides %s: %s", oldID, why)
	}
	m := v.m
	m.Replace = &replaceRecord{PodName: pod, OldID: oldID, NewID: st.ServerID, Phase: replacePlanned, ConfigIndex: v.cfg.Index}
	if _, err := r.store.Update(ctx, m); errors.Is(err, errConflict) {
		return false, nil
	} else if err != nil {
		return false, err
	}
	r.log.Info(m.GroupID+": recorded", "phase", replacePlanned, "pod", pod, "oldId", oldID, "newId", st.ServerID, "configIndex", v.cfg.Index)
	return false, r.crash(stepPlanned)
}

// record moves the replacement to phase (applying edit to the manifest),
// then reaches the crash point after it. A conflicting write is re-read on
// the next round.
func (r *replacer) record(ctx context.Context, v view, phase string, edit func(*manifest), step string) error {
	m := v.m
	rec := *m.Replace
	rec.Phase, rec.ConfigIndex = phase, v.cfg.Index
	m.Replace = &rec
	m.Admitted, m.Removed = maps.Clone(m.Admitted), slices.Clone(m.Removed)
	if edit != nil {
		edit(&m)
	}
	if _, err := r.store.Update(ctx, m); errors.Is(err, errConflict) {
		return nil
	} else if err != nil {
		return err
	}
	r.log.Info(m.GroupID+": recorded", "phase", phase, "pod", rec.PodName, "configIndex", v.cfg.Index)
	return r.crash(step)
}

// change sends one membership change to the leader. It returns a reason to
// wait and re-read when nothing was established: a refused change changed
// nothing, and an unknown outcome is settled by the next read of the
// committed configuration, never by assuming it failed.
func (r *replacer) change(ctx context.Context, v view, c membershipChange, step string) (string, error) {
	callCtx, cancel := context.WithTimeout(ctx, 3*r.call)
	index, err := r.members.Membership(callCtx, v.leaderAddr(r.mgmtPort), c)
	cancel()
	switch {
	case err == nil:
		r.log.Info(v.m.GroupID+": committed", "op", c.Op, "id", c.ID, "index", index)
		return "", r.crash(step)
	case errors.Is(err, errNotLeader), errors.Is(err, errConfigChanged):
		return fmt.Sprintf("%s %s: %v; re-reading", c.Op, c.ID, err), nil
	default:
		return fmt.Sprintf("%s %s: outcome unknown (%v); re-reading the configuration", c.Op, c.ID, err), nil
	}
}

// finish verifies three committed voters with the new incarnation and
// records them as the group's peerIds (#21), ending the replacement. Not
// done yet, it says why, and the caller waits and reads again.
func (r *replacer) finish(ctx context.Context, v view, pod string) (done bool, why string, err error) {
	rec := v.m.Replace
	want := slices.Clone(v.m.PeerIDs)
	want[v.idx] = rec.NewID
	if got, all := v.cfg.voters(); !all || !slices.Equal(got, slices.Sorted(slices.Values(want))) {
		return false, fmt.Sprintf("committed voters %v (all voters: %v), want %v", got, all, want), nil
	}
	m := v.m
	m.PeerIDs, m.Replace = want, nil
	if _, err := r.store.Update(ctx, m); errors.Is(err, errConflict) {
		return false, "the manifest changed; re-reading", nil
	} else if err != nil {
		return true, "", err
	}
	r.log.Info(m.GroupID+": replaced", "pod", pod, "oldId", rec.OldID, "newId", rec.NewID, "voters", want, "configIndex", v.cfg.Index)
	return true, "", nil
}
