package scripts

import (
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// scripts/verify-S4.2-lib.sh holds the shard-state helpers of the replacement check
// (scripts/verify-S4.2.sh). The final gate of 15 November aborted because the check deleted
// the shard leader, counted the still-terminating old pod as restarted, and read the leader
// once, inside the election window. These tests run the helpers against a fake kubectl whose
// answers change on a timeline, so a wait has something to wait for.

// s42Fake is one fake shard: for each pod, timed `storagectl status` answers and timed
// `get pod -o json` answers. Each answer applies from its offset after the run starts until
// the next one; "" means the call fails (pod gone or unreachable). `storagectl
// transfer-leader` is logged and exits transferRC; once it has succeeded, status answers come
// from afterTransfer (offsets from the transfer) for the pods it lists.
type s42Fake struct {
	status        map[string][]s42At
	pod           map[string][]s42At
	afterTransfer map[string][]s42At
	transferRC    int
}

type s42At struct {
	after time.Duration
	body  string
}

const s42Shard = "storage-shard-1-"

func s42Status(state, leader string) string { return s42StatusTerm(state, leader, 5) }

func s42StatusTerm(state, leader string, term int) string {
	id := func(p string) string { return s42Shard + p + "/id-" + p }
	l := ""
	if leader != "" {
		l = id(leader)
	}
	return fmt.Sprintf(`{"state":%q,"leaderId":%q,"term":%d,"caughtUp":true,"configuration":{"committed":true,"servers":[`+
		`{"id":%q,"suffrage":"Voter"},{"id":%q,"suffrage":"Voter"},{"id":%q,"suffrage":"Voter"}]}}`,
		state, l, term, id("0"), id("1"), id("2"))
}

func s42Pod(uid string, ready, terminating bool) string {
	del := "null"
	if terminating {
		del = `"2026-11-15T11:43:14Z"`
	}
	r := "False"
	if ready {
		r = "True"
	}
	return fmt.Sprintf(`{"metadata":{"uid":%q,"deletionTimestamp":%s},"status":{"conditions":[{"type":"Ready","status":%q}]}}`, uid, del, r)
}

// s42Follower is a member that follows leader (a pod suffix), or knows no leader ("").
func s42Follower(leader string) s42At { return s42At{body: s42Status("Follower", leader)} }

func (f s42Fake) run(t *testing.T, snippet string) (string, int, time.Duration) {
	t.Helper()
	dir := t.TempDir()
	bin, fk := filepath.Join(dir, "bin"), filepath.Join(dir, "fake")
	for _, d := range []string{bin, filepath.Join(fk, "status"), filepath.Join(fk, "pod"), filepath.Join(fk, "after")} {
		if err := os.MkdirAll(d, 0o755); err != nil {
			t.Fatal(err)
		}
	}
	write := func(path string, timeline []s42At) {
		t.Helper()
		var b strings.Builder
		for _, a := range timeline {
			fmt.Fprintf(&b, "%d %s\n", a.after.Milliseconds(), a.body)
		}
		if err := os.WriteFile(path, []byte(b.String()), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	for p, tl := range f.status {
		write(filepath.Join(fk, "status", p), tl)
	}
	for p, tl := range f.afterTransfer {
		write(filepath.Join(fk, "after", p), tl)
	}
	if err := os.WriteFile(filepath.Join(fk, "transfer-rc"), []byte(fmt.Sprint(f.transferRC)), 0o644); err != nil {
		t.Fatal(err)
	}
	for p, tl := range f.pod {
		write(filepath.Join(fk, "pod", p), tl)
	}
	// The fake answers `... exec <pod> -c storagenode -- /storagectl status` and
	// `... get pod <pod> -o json` with the pod's answer current at this instant.
	fake := `#!/usr/bin/env bash
verb="" pod="" prev=""
for a in "$@"; do
	case "$prev" in --context|-n|-c|-o) prev=""; continue ;; esac
	prev="$a"
	case "$a" in
	exec) verb=exec ;;
	get) verb=get ;;
	pod|--|/storagectl|status|transfer-leader|json) ;;
	-*) ;;
	*) [ -z "$pod" ] && pod="$a" ;;
	esac
done
t0="$FAKE/t0"
if [ "$verb" = exec ] && [[ " $* " == *" transfer-leader "* ]]; then
	echo "transfer-leader $pod" >> "$FAKE/calls"
	rc="$(cat "$FAKE/transfer-rc")"
	[ "$rc" = 0 ] && date +%s%N > "$FAKE/transferred"
	exit "$rc"
fi
case "$verb" in exec) f="$FAKE/status/$pod" ;; get) f="$FAKE/pod/$pod" ;; *) exit 64 ;; esac
if [ "$verb" = exec ] && [ -f "$FAKE/transferred" ] && [ -f "$FAKE/after/$pod" ]; then
	f="$FAKE/after/$pod" t0="$FAKE/transferred"
fi
[ -f "$f" ] || exit 1
now=$(( ($(date +%s%N) - $(cat "$t0")) / 1000000 ))
body="$(awk -v now="$now" '$1 <= now { sub(/^[0-9]+ /, ""); b = $0 } END { print b }' "$f")"
[ -n "$body" ] || { echo "Error from server (NotFound): $pod" >&2; exit 1; }
printf '%s\n' "$body"
`
	if err := os.WriteFile(filepath.Join(bin, "kubectl"), []byte(fake), 0o755); err != nil {
		t.Fatal(err)
	}
	script := `set -uo pipefail
CTX=kind-reliefgrid NS=reliefgrid SHARD=1
k() { kubectl --context "$CTX" "$@"; }
kn() { k -n "$NS" "$@"; }
. ./verify-S4.2-lib.sh
` + snippet
	cmd := exec.CommandContext(t.Context(), "bash", "-c", script)
	cmd.Env = append(os.Environ(), "PATH="+bin+":"+os.Getenv("PATH"), "FAKE="+fk)
	if err := os.WriteFile(filepath.Join(fk, "t0"), []byte(fmt.Sprint(time.Now().UnixNano())), 0o644); err != nil {
		t.Fatal(err)
	}
	start := time.Now()
	out, err := cmd.CombinedOutput()
	took := time.Since(start)
	code := 0
	var ee *exec.ExitError
	if errors.As(err, &ee) {
		code = ee.ExitCode()
	} else if err != nil {
		t.Fatal(err)
	}
	return strings.TrimSpace(string(out)), code, took
}

// The 15 November failure: the leader (1-2) is deleted and the survivors elect 1-1 only
// after a delay. A bounded wait finds the new leader; a single read (the old control) did not.
func TestS42LeaderDeletedDelayedElection(t *testing.T) {
	f := s42Fake{status: map[string][]s42At{
		s42Shard + "0": {s42Follower(""), {after: 1500 * time.Millisecond, body: s42Status("Follower", "1")}},
		s42Shard + "1": {s42Follower(""), {after: 1500 * time.Millisecond, body: s42Status("Leader", "1")}},
		// The restarted pod, which hears from the new leader once it is elected.
		s42Shard + "2": {s42Follower(""), {after: 1500 * time.Millisecond, body: s42Status("Follower", "1")}},
	}}
	out, code, _ := f.run(t, `echo "once=[$(leader_pod)]"
group_ok && echo "group_ok while leaderless" || echo "group not ok while leaderless"
p="$(dial_peer storage-shard-1-2 10)"; echo "peer rc=$? peer=[$p]"
group_ok && echo "group ok after election" || echo "group not ok after election"`)
	if code != 0 {
		t.Fatalf("exit %d: %s", code, out)
	}
	for _, want := range []string{
		"once=[]", // what the old control saw
		"group not ok while leaderless",
		"peer rc=0 peer=[storage-shard-1-1]",
		"group ok after election",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("missing %q in:\n%s", want, out)
		}
	}
}

// When the target itself leads, the dial source is another member, never the target.
func TestS42DialPeerAvoidsTargetLeader(t *testing.T) {
	f := s42Fake{status: map[string][]s42At{
		s42Shard + "0": {{body: s42Status("Follower", "2")}},
		s42Shard + "1": {{body: s42Status("Follower", "2")}},
		s42Shard + "2": {{body: s42Status("Leader", "2")}},
	}}
	out, _, _ := f.run(t, `p="$(dial_peer storage-shard-1-2 2)"; echo "peer rc=$? peer=[$p]"`)
	if !strings.Contains(out, "peer rc=0 peer=[storage-shard-1-0]") {
		t.Fatalf("want another member as the dial source, got:\n%s", out)
	}
}

// No leader within the bound: fail closed with an empty peer, after (not before) the bound.
func TestS42NoLeaderWithinBoundFailsClosed(t *testing.T) {
	f := s42Fake{status: map[string][]s42At{
		s42Shard + "0": {s42Follower("")},
		s42Shard + "1": {s42Follower("")},
		s42Shard + "2": {{body: ""}}, // unreachable
	}}
	out, _, took := f.run(t, `p="$(dial_peer storage-shard-1-2 2)"; echo "peer rc=$? peer=[$p]"
await_leader 1 >/dev/null; echo "await rc=$?"`)
	for _, want := range []string{"peer rc=1 peer=[]", "await rc=1"} {
		if !strings.Contains(out, want) {
			t.Errorf("missing %q in:\n%s", want, out)
		}
	}
	if took < 3*time.Second {
		t.Errorf("gave up after %v, before the 2 s + 1 s bounds", took)
	}
}

// Members that disagree, or any member naming no leader, are not a healthy group, even when
// the first member sampled names none (the old check took the first non-empty leaderId).
func TestS42GroupOkRequiresSameNamedLeader(t *testing.T) {
	cases := map[string][3]string{
		"first member names no leader": {s42Status("Follower", ""), s42Status("Leader", "1"), s42Status("Follower", "1")},
		"last member names no leader":  {s42Status("Follower", "1"), s42Status("Leader", "1"), s42Status("Follower", "")},
		"members disagree":             {s42Status("Follower", "2"), s42Status("Leader", "1"), s42Status("Follower", "1")},
	}
	for name, st := range cases {
		t.Run(name, func(t *testing.T) {
			f := s42Fake{status: map[string][]s42At{
				s42Shard + "0": {{body: st[0]}}, s42Shard + "1": {{body: st[1]}}, s42Shard + "2": {{body: st[2]}},
			}}
			out, _, _ := f.run(t, `group_ok && echo OK || echo NOT_OK`)
			if out != "NOT_OK" {
				t.Fatalf("group_ok = %q, want NOT_OK", out)
			}
		})
	}
	f := s42Fake{status: map[string][]s42At{
		s42Shard + "0": {{body: s42Status("Follower", "1")}},
		s42Shard + "1": {{body: s42Status("Leader", "1")}},
		s42Shard + "2": {{body: s42Status("Follower", "1")}},
	}}
	if out, _, _ := f.run(t, `group_ok && echo OK || echo NOT_OK`); out != "OK" {
		t.Fatalf("healthy group: group_ok = %q, want OK", out)
	}
}

// After `delete pod --wait=false` the old pod keeps answering (same UID, terminating, even
// Ready) for a moment. It must not count as replaced; the new UID counts only once Ready.
func TestS42OldPodStillTerminatingIsNotReplaced(t *testing.T) {
	f := s42Fake{pod: map[string][]s42At{s42Shard + "2": {
		{body: s42Pod("uid-old", true, false)},
		{after: 300 * time.Millisecond, body: s42Pod("uid-old", true, true)}, // deleted, still terminating
		{after: 1200 * time.Millisecond, body: ""},                           // gone
		{after: 1500 * time.Millisecond, body: s42Pod("uid-new", false, false)},
		{after: 2500 * time.Millisecond, body: s42Pod("uid-new", true, false)},
	}}}
	out, code, took := f.run(t, `u="$(pod_uid storage-shard-1-2)"; echo "uid=[$u]"
sleep 0.5
pod_replaced storage-shard-1-2 "$u" && echo "terminating: replaced" || echo "terminating: not replaced"
pod_replaced storage-shard-1-2 "" && echo "empty old uid: replaced" || echo "empty old uid: not replaced"
sleep 1.3
pod_replaced storage-shard-1-2 "$u" && echo "new not ready: replaced" || echo "new not ready: not replaced"
wait_for 10 pod_replaced storage-shard-1-2 "$u" && echo "waited: replaced" || echo "waited: not replaced"`)
	if code != 0 {
		t.Fatalf("exit %d: %s", code, out)
	}
	for _, want := range []string{
		"uid=[uid-old]",
		"terminating: not replaced",
		"empty old uid: not replaced",
		"new not ready: not replaced",
		"waited: replaced",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("missing %q in:\n%s", want, out)
		}
	}
	if took < 2500*time.Millisecond {
		t.Errorf("replaced after %v, before the new pod was Ready (2.5 s)", took)
	}
}

// Only the old pod ever answers: the bounded wait fails.
func TestS42ReplacementNeverArrivesFails(t *testing.T) {
	f := s42Fake{pod: map[string][]s42At{s42Shard + "2": {{body: s42Pod("uid-old", true, true)}}}}
	out, _, _ := f.run(t, `wait_for 2 pod_replaced storage-shard-1-2 uid-old && echo replaced || echo "not replaced"`)
	if out != "not replaced" {
		t.Fatalf("got %q, want not replaced", out)
	}
}

// The verifier itself uses the helpers at the three places the gate found: the restart waits
// for the new pod before the health check, the control and the wipe pick their dial source
// through the bounded, fail-closed helper, and none reads leader_pod once for a dial source.
func TestS42VerifierUsesTheWaits(t *testing.T) {
	b, err := os.ReadFile("verify-S4.2.sh")
	if err != nil {
		t.Fatal(err)
	}
	src := string(b)
	for _, want := range []string{
		`. scripts/verify-S4.2-lib.sh`,
		`LEADER_WAIT="${LEADER_WAIT:-60}"`,
		`wait_for 180 pod_replaced "$POD" "$uid0"`,
		`peer0="$(dial_peer "$POD" "$LEADER_WAIT")" ||`,
		`peer="$(dial_peer "$POD" "$LEADER_WAIT")" ||`,
	} {
		if !strings.Contains(src, want) {
			t.Errorf("verify-S4.2.sh lacks %q", want)
		}
	}
	for _, bad := range []string{`peer0="$(leader_pod)"`, "\npeer=\"$(leader_pod)\"\nfor i in"} {
		if strings.Contains(src, bad) {
			t.Errorf("verify-S4.2.sh still reads the dial source once: %q", bad)
		}
	}
	restart := src[strings.Index(src, `echo "== 1.`):strings.Index(src, `echo "== 2.`)]
	if strings.Index(restart, "pod_replaced") > strings.Index(restart, "group_ok") {
		t.Errorf("the restart checks group health before waiting for the new pod:\n%s", restart)
	}
}

// The wipe's "no election while the wiped pod waits" check is about the unadmitted pod. If
// the target leads, deleting it forces an election by itself, so step_off first hands
// leadership away (storagectl transfer-leader, S3.1) and returns only once another member
// leads a settled group. The term read after it is the one the check starts from.
func TestS42StepOffTargetIsLeader(t *testing.T) {
	f := s42Fake{
		status: map[string][]s42At{
			s42Shard + "0": {{body: s42Status("Follower", "2")}},
			s42Shard + "1": {{body: s42Status("Follower", "2")}},
			s42Shard + "2": {{body: s42Status("Leader", "2")}},
		},
		// After the transfer: a short leaderless gap, then 1-0 leads term 6.
		afterTransfer: map[string][]s42At{
			s42Shard + "0": {s42Follower(""), {after: 800 * time.Millisecond, body: s42StatusTerm("Leader", "0", 6)}},
			s42Shard + "1": {s42Follower(""), {after: 800 * time.Millisecond, body: s42StatusTerm("Follower", "0", 6)}},
			s42Shard + "2": {s42Follower(""), {after: 800 * time.Millisecond, body: s42StatusTerm("Follower", "0", 6)}},
		},
	}
	out, code, _ := f.run(t, `r="$(step_off storage-shard-1-2 10)"; echo "rc=$? r=[$r]"
echo "calls=[$(tr '\n' ';' < "$FAKE/calls")]"
echo "term=$(field "$(st "$(leader_pod)")" .term)"`)
	if code != 0 {
		t.Fatalf("exit %d: %s", code, out)
	}
	for _, want := range []string{
		"rc=0 r=[storage-shard-1-0 transferred]",
		"calls=[transfer-leader storage-shard-1-2;]", // exactly one transfer, asked of the target
		"term=6", // the term window starts after the transfer's election
	} {
		if !strings.Contains(out, want) {
			t.Errorf("missing %q in:\n%s", want, out)
		}
	}
}

// A follower target needs no transfer: leadership is left alone.
func TestS42StepOffTargetIsFollower(t *testing.T) {
	f := s42Fake{status: map[string][]s42At{
		s42Shard + "0": {{body: s42Status("Follower", "1")}},
		s42Shard + "1": {{body: s42Status("Leader", "1")}},
		s42Shard + "2": {{body: s42Status("Follower", "1")}},
	}}
	out, _, _ := f.run(t, `r="$(step_off storage-shard-1-2 2)"; echo "rc=$? r=[$r]"; [ -e "$FAKE/calls" ] && echo "transfer called" || echo "no transfer"`)
	for _, want := range []string{"rc=0 r=[storage-shard-1-1 unchanged]", "no transfer"} {
		if !strings.Contains(out, want) {
			t.Errorf("missing %q in:\n%s", want, out)
		}
	}
}

// A refused transfer, or a target that still leads after it, fails closed.
func TestS42StepOffFailsClosed(t *testing.T) {
	leading := map[string][]s42At{
		s42Shard + "0": {{body: s42Status("Follower", "2")}},
		s42Shard + "1": {{body: s42Status("Follower", "2")}},
		s42Shard + "2": {{body: s42Status("Leader", "2")}},
	}
	for name, f := range map[string]s42Fake{
		"transfer refused":       {status: leading, transferRC: 1},
		"target still leads":     {status: leading, afterTransfer: leading},
		"no leader to step away": {status: map[string][]s42At{s42Shard + "0": {s42Follower("")}, s42Shard + "1": {s42Follower("")}, s42Shard + "2": {s42Follower("")}}},
	} {
		t.Run(name, func(t *testing.T) {
			out, _, _ := f.run(t, `r="$(step_off storage-shard-1-2 2)"; echo "rc=$? r=[$r]"`)
			if out != "rc=1 r=[]" {
				t.Fatalf("got %q, want rc=1 r=[]", out)
			}
		})
	}
}

// The wipe step steps leadership off the target before the control and before it reads the
// term the "no election" check compares against.
func TestS42WipeStepsOffBeforeTheTermWindow(t *testing.T) {
	b, err := os.ReadFile("verify-S4.2.sh")
	if err != nil {
		t.Fatal(err)
	}
	src := string(b)
	wipe := src[strings.Index(src, `echo "== 3.`):strings.Index(src, `echo "== 4.`)]
	off := strings.Index(wipe, `step_off "$POD" "$LEADER_WAIT"`)
	if off < 0 {
		t.Fatalf("the wipe step does not call step_off:\n%s", wipe)
	}
	for _, later := range []string{`peer0="$(dial_peer`, `term0=`, `kn delete pvc`} {
		if i := strings.Index(wipe, later); i < off {
			t.Errorf("%q comes before step_off", later)
		}
	}
}
