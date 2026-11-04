package scripts

import (
	"errors"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// The 2026-11-03 gate dry run (c4fc5fc) failed demos 1 and 7 on timing
// races with the app running: the matcher, or the app's background writes,
// moved state between a snapshot and the check that compared against it.
// These cases simulate those races against the helpers in
// scripts/demos/kube.sh; none needs a cluster.

// runKube sources lib.sh and kube.sh in a fresh bash, runs script, and
// returns its combined output and exit code.
func runKube(t *testing.T, script string, args ...string) (string, int) {
	t.Helper()
	lib, err := filepath.Abs("demos/lib.sh")
	if err != nil {
		t.Fatal(err)
	}
	kube, err := filepath.Abs("demos/kube.sh")
	if err != nil {
		t.Fatal(err)
	}
	root, err := filepath.Abs("..")
	if err != nil {
		t.Fatal(err)
	}
	full := `. "$LIB"; . "$KUBE"; DEMO_TMP="$(mktemp -d)"; ` + script
	cmd := exec.CommandContext(t.Context(), "bash", append([]string{"-c", full, "test"}, args...)...)
	cmd.Env = append(cmd.Environ(), "LIB="+lib, "KUBE="+kube, "REPO_ROOT="+root, "CLUSTER=reliefgrid",
		"EVIDENCE_ROOT="+t.TempDir())
	out, err := cmd.CombinedOutput()
	code := 0
	if err != nil {
		var ee *exec.ExitError
		if !errors.As(err, &ee) {
			t.Fatalf("bash: %v", err)
		}
		code = ee.ExitCode()
	}
	return string(out), code
}

// stale is a replica's stale-read line for replica_reads_agree.
func stale(pod, code, version, status string) string {
	return pod + " " + code + ` {"read":{"servedBy":"` + pod + `","consistency":"stale"},"record":{"key":"req:r1","kind":"request","version":` + version +
		`,"request":{"id":"r1","status":"` + status + `"}}}`
}

// Demo 1: each replica's stale read is compared, not with the strong read's
// version, but with the others, at a version at or past the strong read's.
func TestReplicaReadsAgree(t *testing.T) {
	for _, c := range []struct {
		name, min string
		lines     []string
		want      int
	}{
		// The gate failure: the strong read saw version 1, then the matcher
		// moved the request to version 2 before the replicas were read.
		{"matcher moved it past the strong read", "1", []string{stale("p0", "200", "2", "MATCHED"), stale("p1", "200", "2", "MATCHED"), stale("p2", "200", "2", "MATCHED")}, 0},
		{"all at the strong read's version", "1", []string{stale("p0", "200", "1", "PENDING"), stale("p1", "200", "1", "PENDING"), stale("p2", "200", "1", "PENDING")}, 0},
		// The matcher wrote between two replica reads: retry, not pass.
		{"moved between replica reads", "1", []string{stale("p0", "200", "1", "PENDING"), stale("p1", "200", "2", "MATCHED"), stale("p2", "200", "2", "MATCHED")}, 75},
		{"all behind the strong read", "2", []string{stale("p0", "200", "1", "PENDING"), stale("p1", "200", "1", "PENDING"), stale("p2", "200", "1", "PENDING")}, 75},
		// Strict: a different record at the same version is a failure now.
		{"same version, different record", "1", []string{stale("p0", "200", "2", "MATCHED"), stale("p1", "200", "2", "MATCHED"), stale("p2", "200", "2", "PENDING")}, 1},
		{"mixed versions still reject same-version divergence", "1", []string{stale("p0", "200", "2", "MATCHED"), stale("p1", "200", "2", "PENDING"), stale("p2", "200", "3", "MATCHED")}, 1},
		{"only one replica", "1", []string{stale("p0", "200", "2", "MATCHED")}, 1},
		{"only two replicas", "1", []string{stale("p0", "200", "2", "MATCHED"), stale("p1", "200", "2", "MATCHED")}, 1},
		{"duplicate replica", "1", []string{stale("p0", "200", "2", "MATCHED"), stale("p0", "200", "2", "MATCHED"), stale("p2", "200", "2", "MATCHED")}, 1},
		{"served by another pod", "1", []string{stale("p0", "200", "1", "PENDING"), stale("p1", "200", "1", "PENDING"), strings.Replace(stale("p2", "200", "1", "PENDING"), `"servedBy":"p2"`, `"servedBy":"p1"`, 1)}, 1},
		{"a replica without it", "1", []string{stale("p0", "200", "1", "PENDING"), stale("p1", "200", "1", "PENDING"), `p2 404 {"code":"NOT_FOUND"}`}, 1},
		{"no port-forward", "1", []string{stale("p0", "200", "1", "PENDING"), stale("p1", "200", "1", "PENDING"), "p2 000 "}, 1},
		{"no reads", "1", nil, 1},
		{"no strong-read version", "", []string{stale("p0", "200", "1", "PENDING")}, 1},
	} {
		t.Run(c.name, func(t *testing.T) {
			out, code := runKube(t, `printf '%s' "$2" | replica_reads_agree "$1"`, c.min, strings.Join(c.lines, "\n"))
			if code != c.want {
				t.Errorf("replica_reads_agree = %d, want %d (%s)", code, c.want, out)
			}
		})
	}
}

// retry_on retries only on the given exit code, within a bound, and prints
// only the last attempt's output.
func TestRetryOn(t *testing.T) {
	// attempt fails with the retryable code until attempt n, then exits with final.
	const attempt = `attempt() { echo "out $RETRY_ATTEMPT"; [ "$RETRY_ATTEMPT" -lt "$1" ] && return 75; return "$2"; }; `
	for _, c := range []struct {
		name, script string
		want         int
		wantOut      string
	}{
		{"race resolves", attempt + `retry_on 75 15 attempt 3 0`, 0, "out 3\n"},
		{"other failure is not retried", attempt + `retry_on 75 15 attempt 1 1`, 1, "out 1\n"},
		{"never resolves within the bound", attempt + `retry_on 75 1 attempt 1000 0`, 75, ""},
		// Demo 6: invcheck check exits 3 when a write committed during its walk.
		{"demo 6: not quiescent then quiescent", `q() { echo "check $RETRY_ATTEMPT"; [ "$RETRY_ATTEMPT" -ge 2 ] || return 3; }; retry_on 3 15 q`, 0, "check 2\n"},
		{"demo 6: violations are not retried", `v() { echo "check $RETRY_ATTEMPT"; return 1; }; retry_on 3 15 v`, 1, "check 1\n"},
		{"retries are recorded", attempt + `retry_on 75 15 attempt 3 0 >/dev/null; grep -c . "$DEMO_TMP/retried"`, 0, "2\n"},
	} {
		t.Run(c.name, func(t *testing.T) {
			out, code := runKube(t, c.script)
			if code != c.want {
				t.Errorf("exit %d, want %d (%s)", code, c.want, out)
			}
			if c.wantOut != "" && !strings.HasSuffix(out, c.wantOut) {
				t.Errorf("output %q, want it to end with %q", out, c.wantOut)
			}
		})
	}
}

// Demo 7: the recovered replica's stale read is retried only while it
// answers 503 "not caught up" (background writes moved the leader commit
// after the catch-up check passed).
func TestStaleGetRetriesOnlyNotCaughtUp(t *testing.T) {
	// A fake curl: answers 503 not caught up for the first $FAKE_503 calls,
	// then $FAKE_FINAL with $FAKE_BODY. It writes the body to curl's -o file.
	const fake = `n=0; curl() { local o="" a; n=$((n + 1)); echo x >>"$DEMO_TMP/calls"
		while [ $# -gt 0 ]; do [ "$1" = -o ] && { o="$2"; shift; }; shift; done
		if [ "$(grep -c . "$DEMO_TMP/calls")" -le "$FAKE_503" ]; then
			echo '{"code":"STORAGE_UNAVAILABLE","message":"storage-shard-1-0 is not caught up: fsm at 13813, leader commit 13864, local commit 13863","outcomeUnknown":false,"retryable":true}' >"$o"; printf 503
		else echo "$FAKE_BODY" >"$o"; printf '%s' "$FAKE_FINAL"; fi; }; `
	const read = `code="$(retry_on 75 "$BOUND" stale_get 1234 demo7-k "$DEMO_TMP/stale.json")"; rc=$?; ` +
		`echo "code=$code rc=$rc calls=$(grep -c . "$DEMO_TMP/calls") body=$(jq -c .code "$DEMO_TMP/stale.json")"`
	ok := `{"read":{"servedBy":"storage-shard-1-0"},"record":{"version":1}}`
	for _, c := range []struct {
		name, n503, final, body, bound, want string
	}{
		{"caught up again after two 503s", "2", "200", ok, "15", `code=200 rc=0 calls=3 body=null`},
		{"answers at once", "0", "200", ok, "15", `code=200 rc=0 calls=1 body=null`},
		{"never caught up within the bound", "100000", "200", ok, "1", `code=503 rc=75 calls=`},
		{"another 503 fails at once", "0", "503", `{"code":"STORAGE_UNAVAILABLE","message":"no leader for shard 1","retryable":true}`, "15", `code=503 rc=0 calls=1 body="STORAGE_UNAVAILABLE"`},
		{"a 404 fails at once", "0", "404", `{"code":"NOT_FOUND","message":"no such key"}`, "15", `code=404 rc=0 calls=1 body="NOT_FOUND"`},
	} {
		t.Run(c.name, func(t *testing.T) {
			out, code := runKube(t, `FAKE_503=$1 FAKE_FINAL=$2 FAKE_BODY=$3 BOUND=$4; `+fake+read, c.n503, c.final, c.body, c.bound)
			if code != 0 || !strings.Contains(out, c.want) {
				t.Errorf("got %q (exit %d), want it to contain %q", out, code, c.want)
			}
		})
	}
}

// Demos 4 and 7: the three replicas' dumps are taken while the app writes,
// so they may be at different appliedIndexes; they must agree at one.
func TestDumpsAgree(t *testing.T) {
	for _, c := range []struct {
		name  string
		lines []string
		want  int
	}{
		{"identical", []string{"p0 900 aaa records=10", "p1 900 aaa records=10", "p2 900 aaa records=10"}, 0},
		// The app wrote between two dumps.
		{"moved between dumps", []string{"p0 900 aaa records=10", "p1 901 bbb records=11", "p2 901 bbb records=11"}, 75},
		// Strict: different state at one appliedIndex is divergence.
		{"diverged at one index", []string{"p0 900 aaa records=10", "p1 900 aaa records=10", "p2 900 ccc records=10"}, 1},
		{"mixed indexes still reject same-index divergence", []string{"p0 900 aaa records=10", "p1 900 ccc records=10", "p2 901 bbb records=11"}, 1},
		{"unreachable", []string{"p0 900 aaa records=10", "p1 unreachable", "p2 900 aaa records=10"}, 1},
		{"two replicas", []string{"p0 900 aaa records=10", "p1 900 aaa records=10"}, 1},
		{"none", nil, 1},
	} {
		t.Run(c.name, func(t *testing.T) {
			out, code := runKube(t, `printf '%s\n' "$1" | dumps_agree`, strings.Join(c.lines, "\n"))
			if code != c.want {
				t.Errorf("dumps_agree = %d, want %d (%s)", code, c.want, out)
			}
		})
	}
}

func TestAgreedDumps(t *testing.T) {
	// A fake dumps: the replicas disagree on the first $1 calls, then agree.
	const fake = `dumps() { echo x >>"$DEMO_TMP/calls"
		if [ "$(grep -c . "$DEMO_TMP/calls")" -le "$MOVING" ]; then printf 'p0 900 aaa records=10\np1 901 bbb records=11\np2 901 bbb records=11\n'
		else printf 'p0 905 eee records=12\np1 905 eee records=12\np2 905 eee records=12\n'; fi; }; `
	for _, c := range []struct {
		name, moving, bound, want string
	}{
		{"settles", "2", "15", "rc=0 calls=3 first=p0 905 eee records=12"},
		{"never settles within the bound", "100000", "1", "rc=75 calls="},
	} {
		t.Run(c.name, func(t *testing.T) {
			out, code := runKube(t, `MOVING=$1; `+fake+`agreed_dumps 1 "$2"; rc=$?; echo "rc=$rc calls=$(grep -c . "$DEMO_TMP/calls") first=$(head -1 "$DEMO_TMP/dumps-1")"`, c.moving, c.bound)
			if code != 0 || !strings.Contains(out, c.want) {
				t.Errorf("got %q (exit %d), want it to contain %q", out, code, c.want)
			}
		})
	}
	// Every call records its retry count in the evidence's command log (demos 4 and 7).
	for _, c := range []struct{ moving, bound, want string }{
		{"2", "15", "dumps of shard 1: 2 retried at different appliedIndexes"},
		{"0", "15", "dumps of shard 1: 0 retried at different appliedIndexes"},
	} {
		out, _ := runKube(t, `MOVING=$1; `+fake+`agreed_dumps 1 "$2" >/dev/null; cat "$DEMO_TMP/commands"`, c.moving, c.bound)
		if !strings.Contains(out, c.want) {
			t.Errorf("moving %s: command log %q, want it to contain %q", c.moving, out, c.want)
		}
	}
	// Divergence at one index is not retried.
	out, _ := runKube(t, `dumps() { echo x >>"$DEMO_TMP/calls"; printf 'p0 900 aaa records=10\np1 900 aaa records=10\np2 900 ccc records=10\n'; }; `+
		`agreed_dumps 1 15; echo "rc=$? calls=$(grep -c . "$DEMO_TMP/calls")"`)
	if !strings.Contains(out, "rc=1 calls=1") {
		t.Errorf("diverged dumps: got %q, want rc=1 after one call", out)
	}
}
