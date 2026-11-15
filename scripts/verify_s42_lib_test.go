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
// the next one; "" means the call fails (pod gone or unreachable).
type s42Fake struct {
	status map[string][]s42At
	pod    map[string][]s42At
}

type s42At struct {
	after time.Duration
	body  string
}

const s42Shard = "storage-shard-1-"

func s42Status(state, leader string) string {
	id := func(p string) string { return s42Shard + p + "/id-" + p }
	l := ""
	if leader != "" {
		l = id(leader)
	}
	return fmt.Sprintf(`{"state":%q,"leaderId":%q,"caughtUp":true,"configuration":{"committed":true,"servers":[`+
		`{"id":%q,"suffrage":"Voter"},{"id":%q,"suffrage":"Voter"},{"id":%q,"suffrage":"Voter"}]}}`,
		state, l, id("0"), id("1"), id("2"))
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
	for _, d := range []string{bin, filepath.Join(fk, "status"), filepath.Join(fk, "pod")} {
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
	pod|--|/storagectl|status|json) ;;
	-*) ;;
	*) [ -z "$pod" ] && pod="$a" ;;
	esac
done
case "$verb" in exec) f="$FAKE/status/$pod" ;; get) f="$FAKE/pod/$pod" ;; *) exit 64 ;; esac
[ -f "$f" ] || exit 1
now=$(( ($(date +%s%N) - $(cat "$FAKE/t0")) / 1000000 ))
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
