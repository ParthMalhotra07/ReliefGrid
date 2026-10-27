package scripts

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

// demoKubectl puts a kubectl on PATH for the cluster demos' own checks: it knows context
// kind-fake, answers "get pods" with <dir>/pods.json (filtered by shard when asked) and
// "get nodes -l <demo cordon label> -o json" with <dir>/handoff.json, and logs every call to
// <dir>/kubectl.log. Anything else (exec, port-forward, cordon, delete) is only logged.
func demoKubectl(t *testing.T, dir string) []string {
	t.Helper()
	bin := filepath.Join(dir, "bin")
	if err := os.MkdirAll(bin, 0o755); err != nil {
		t.Fatal(err)
	}
	src := `#!/usr/bin/env bash
d=` + fmt.Sprintf("%q", dir) + `
echo "$*" >>"$d/kubectl.log"
args=" $* "
case "$args" in
*" config get-contexts "*) echo kind-fake ;;
*" get pods -l reliefgrid.io/shard="*)
	s="${args##*reliefgrid.io/shard=}"; s="${s%% *}"
	jq --arg s "$s" '.items |= map(select(.metadata.labels["reliefgrid.io/shard"] == $s))' "$d/pods.json" ;;
*" get pods "*) cat "$d/pods.json" ;;
*" get nodes -l reliefgrid.io/demo-cordoned=true -o json "*) cat "$d/handoff.json" 2>/dev/null || echo '{"items":[]}' ;;
*) exit 1 ;;
esac
`
	if err := os.WriteFile(filepath.Join(bin, "kubectl"), []byte(src), 0o755); err != nil {
		t.Fatal(err)
	}
	return []string{"PATH=" + bin + ":" + os.Getenv("PATH"), "KUBE_CONTEXT=kind-fake", "PF_REPO_ROOT=" + t.TempDir()}
}

// storagePods is a "get pods" answer: three replicas of each shard, Ready unless listed in down
// (pod names), which are Pending and unscheduled.
func storagePods(down ...string) string {
	var items []string
	for s := range 3 {
		for i := range 3 {
			name := fmt.Sprintf("storage-shard-%d-%d", s, i)
			ready, phase := "True", "Running"
			for _, d := range down {
				if d == name {
					ready, phase = "False", "Pending"
				}
			}
			items = append(items, fmt.Sprintf(`{"metadata":{"name":%q,"labels":{"app.kubernetes.io/component":"storagenode","reliefgrid.io/shard":"%d"}},`+
				`"status":{"phase":%q,"conditions":[{"type":"Ready","status":%q}]}}`, name, s, phase, ready))
		}
	}
	return `{"items":[` + strings.Join(items, ",") + `]}`
}

func writeFile(t *testing.T, path, content string) {
	t.Helper()
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
}

// changes lists the kubectl calls that would change the cluster.
func changes(t *testing.T, dir string) []string {
	t.Helper()
	b, _ := os.ReadFile(filepath.Join(dir, "kubectl.log"))
	var out []string
	for _, line := range strings.Split(string(b), "\n") {
		if regexp.MustCompile(`\b(cordon|uncordon|delete|annotate|label)\b`).MatchString(line) {
			out = append(out, line)
		}
	}
	return out
}

// Demo 6 refuses to start, changing nothing, if any shard is already missing a replica: it
// would stop a second one and lose the shard's majority. A pending demo-6 hand-off is the
// same case.
func TestDemo6RefusesWithAReplicaMissing(t *testing.T) {
	script, _ := filepath.Abs("demos/demo-6.sh")
	for _, tc := range []struct {
		name, pods, handoff, want string
	}{
		{"a replica down", storagePods("storage-shard-2-1"), "", "shard 2 is missing a replica"},
		{"a hand-off pending", storagePods("storage-shard-1-1"),
			`{"items":[{"metadata":{"name":"reliefgrid-worker3","annotations":{"reliefgrid.io/demo6-handoff":"shard=1 pod=storage-shard-1-1"}}}]}`,
			"shard 1 is missing a replica"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir, root := t.TempDir(), t.TempDir()
			writeFile(t, filepath.Join(dir, "pods.json"), tc.pods)
			if tc.handoff != "" {
				writeFile(t, filepath.Join(dir, "handoff.json"), tc.handoff)
			}
			out, code := runScript(t, "", append(demoKubectl(t, dir), "EVIDENCE_ROOT="+root, "DEMO_RUN_ID=r"), script)
			ev := readFile(t, filepath.Join(root, "r", "demo-6.txt"))
			if code != exitFail || !strings.Contains(ev, "FAIL  refuse: "+tc.want) {
				t.Fatalf("exit %d, want a refusal naming %q\n%s", code, tc.want, out)
			}
			if c := changes(t, dir); len(c) != 0 {
				t.Fatalf("a refused demo 6 changed the cluster: %v", c)
			}
		})
	}
}

// Demo 7 without a hand-off makes its own stop only from a fully healthy shard: with a
// replica already down and no hand-off naming it, it refuses and stops nothing.
func TestDemo7RefusesWithoutHandOffOnAnUnhealthyShard(t *testing.T) {
	script, _ := filepath.Abs("demos/demo-7.sh")
	dir, root := t.TempDir(), t.TempDir()
	writeFile(t, filepath.Join(dir, "pods.json"), storagePods("storage-shard-1-2"))
	out, code := runScript(t, "", append(demoKubectl(t, dir), "EVIDENCE_ROOT="+root, "DEMO_RUN_ID=r"), script)
	ev := readFile(t, filepath.Join(root, "r", "demo-7.txt"))
	if code != exitFail || !strings.Contains(ev, "FAIL  refuse: shard 1 is not fully healthy") {
		t.Fatalf("exit %d, want a refusal\n%s", code, out)
	}
	if c := changes(t, dir); len(c) != 0 {
		t.Fatalf("a refused demo 7 changed the cluster: %v", c)
	}
}

// Demo 7 finds demo 6's hand-off in the cluster (the annotation on the worker demo 6
// cordoned), whatever run directory or make call it comes from. With two hand-offs it
// cannot tell which to continue, and refuses.
func TestDemo7RefusesTwoHandOffs(t *testing.T) {
	script, _ := filepath.Abs("demos/demo-7.sh")
	dir, root := t.TempDir(), t.TempDir()
	writeFile(t, filepath.Join(dir, "pods.json"), storagePods("storage-shard-1-1", "storage-shard-2-0"))
	writeFile(t, filepath.Join(dir, "handoff.json"), `{"items":[`+
		`{"metadata":{"name":"w3","annotations":{"reliefgrid.io/demo6-handoff":"shard=1 pod=storage-shard-1-1 node=w3 serverId=a uid=u1 term=4"}}},`+
		`{"metadata":{"name":"w2","annotations":{"reliefgrid.io/demo6-handoff":"shard=2 pod=storage-shard-2-0 node=w2 serverId=b uid=u2 term=5"}}}]}`)
	out, code := runScript(t, "", append(demoKubectl(t, dir), "EVIDENCE_ROOT="+root, "DEMO_RUN_ID=r"), script)
	ev := readFile(t, filepath.Join(root, "r", "demo-7.txt"))
	if code != exitFail || !strings.Contains(ev, "FAIL  refuse: 2 demo-6 hand-offs") {
		t.Fatalf("exit %d, want a refusal\n%s", code, out)
	}
	if c := changes(t, dir); len(c) != 0 {
		t.Fatalf("a refused demo 7 changed the cluster: %v", c)
	}
}

// One make invocation shares one DEMO_RUN_ID across its demos (make demo-6 demo-7), so their
// evidence lands in one run directory; a DEMO_RUN_ID given by the caller wins.
func TestMakeDemosShareARunID(t *testing.T) {
	out, err := exec.CommandContext(t.Context(), "make", "-C", "..", "-n", "demo-6", "demo-7").CombinedOutput()
	ids := regexp.MustCompile(`DEMO_RUN_ID='([^']+)'`).FindAllStringSubmatch(string(out), -1)
	if err != nil || len(ids) != 2 || ids[0][1] != ids[1][1] || !regexp.MustCompile(`^\d{8}T\d{6}Z-`).MatchString(ids[0][1]) {
		t.Fatalf("make -n demo-6 demo-7: %v, run ids %v\n%s", err, ids, out)
	}
	cmd := exec.CommandContext(t.Context(), "make", "-C", "..", "-n", "demo-6", "demo-7")
	cmd.Env = append(os.Environ(), "DEMO_RUN_ID=mine")
	out, err = cmd.CombinedOutput()
	if err != nil || strings.Count(string(out), "DEMO_RUN_ID='mine'") != 2 {
		t.Fatalf("make -n with DEMO_RUN_ID=mine: %v\n%s", err, out)
	}
}
