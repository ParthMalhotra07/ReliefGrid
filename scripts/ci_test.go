package scripts

import (
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"testing"

	"go.yaml.in/yaml/v3"
)

// CI (S4.6): .github/workflows/ci.yml keeps the fast check job (now with a
// contract-drift step) and adds a changes job, an images job (builds the
// storage, coordinator and tools images) and a smoke job (PRs to main: a kind
// cluster in the runner, the storage chart, three groups with leaders, then
// demo 4, with the evidence uploaded). To limit cost, images and smoke run
// only for ready (non-draft) PRs whose diff touches code, CI or test tooling (scripts/ci-changes.sh).
// They are skipped at the job level rather than by a workflow paths filter, so
// check always runs and a skipped job still reports.
// scripts/ci-smoke.sh is the smoke job's body; its tests below run it with
// fake kind, helm, kubectl and make on PATH and never touch a cluster.

type workflow struct {
	On   map[string]any `yaml:"on"`
	Jobs map[string]job `yaml:"jobs"`
}

type job struct {
	Name     string            `yaml:"name"`
	If       string            `yaml:"if"`
	Needs    any               `yaml:"needs"` // a string or a list
	Timeout  int               `yaml:"timeout-minutes"`
	Strategy map[string]any    `yaml:"strategy"`
	Outputs  map[string]string `yaml:"outputs"`
	Steps    []step            `yaml:"steps"`
}

type step struct {
	Name string            `yaml:"name"`
	If   string            `yaml:"if"`
	Uses string            `yaml:"uses"`
	Run  string            `yaml:"run"`
	With map[string]any    `yaml:"with"`
	Env  map[string]string `yaml:"env"`
}

func (j job) needs() []string {
	switch v := j.Needs.(type) {
	case string:
		return []string{v}
	case []any:
		var out []string
		for _, s := range v {
			out = append(out, s.(string))
		}
		return out
	}
	return nil
}

// text is everything a job runs or configures, for substring checks.
func (j job) text() string {
	b, _ := yaml.Marshal(j)
	return string(b)
}

func loadCI(t *testing.T) workflow {
	t.Helper()
	b, err := os.ReadFile("../.github/workflows/ci.yml")
	if err != nil {
		t.Fatal(err)
	}
	var w workflow
	if err := yaml.Unmarshal(b, &w); err != nil {
		t.Fatalf("ci.yml: %v", err)
	}
	return w
}

func mustJob(t *testing.T, w workflow, name string) job {
	t.Helper()
	j, ok := w.Jobs[name]
	if !ok {
		t.Fatalf("ci.yml has no job %q (jobs: %v)", name, slices.Sorted(func(yield func(string) bool) {
			for k := range w.Jobs {
				if !yield(k) {
					return
				}
			}
		}))
	}
	return j
}

var pinnedAction = regexp.MustCompile(`^[\w.-]+/[\w./-]+@[0-9a-f]{40}$`)

// Every action is pinned to a full commit SHA, as the existing jobs are.
func TestCIActionsPinned(t *testing.T) {
	for name, j := range loadCI(t).Jobs {
		for _, s := range j.Steps {
			if s.Uses != "" && !pinnedAction.MatchString(s.Uses) {
				t.Errorf("job %s: %q is not pinned to a commit SHA", name, s.Uses)
			}
		}
	}
}

// check stays the fast gate: no dependencies, no images, no cluster.
func TestCICheckStaysFast(t *testing.T) {
	j := mustJob(t, loadCI(t), "check")
	if len(j.needs()) > 0 {
		t.Errorf("check needs %v; it must start at once", j.needs())
	}
	if j.Timeout == 0 || j.Timeout > 20 {
		t.Errorf("check timeout-minutes = %d, want 1..20", j.Timeout)
	}
	for _, slow := range []string{"docker build", "kind ", "helm ", "ci-smoke"} {
		if strings.Contains(j.text(), slow) {
			t.Errorf("check runs %q; that belongs in images or smoke", slow)
		}
	}
	if !slices.ContainsFunc(j.Steps, func(s step) bool { return strings.Contains(s.Run, "make gen-check") }) {
		t.Error("check has no contract-drift step (make gen-check)")
	}
	// S4.6b: the seams and kind builds (vet, lint, race tests and the
	// default-binary symbol check) cost about 2.5 minutes; they run in the
	// seams job, only when the seam packages change.
	out := makeDryRun(t, "check")
	for _, tagged := range []string{"-tags seams", "--build-tags seams", "-tags kind", "--build-tags kind"} {
		if strings.Contains(out, tagged) {
			t.Errorf("make check runs %q; that belongs in make check-seams (the seams job)", tagged)
		}
	}
}

func makeDryRun(t *testing.T, target string) string {
	t.Helper()
	out, err := exec.CommandContext(t.Context(), "make", "-C", "..", "-n", target).CombinedOutput()
	if err != nil {
		t.Fatalf("make -n %s: %v\n%s", target, err, out)
	}
	return string(out)
}

// seams (S4.6b) vets, lints and race-tests the seams and kind builds and runs
// the default-binary symbol check, for ready PRs whose diff touches the seam
// packages (scripts/ci-changes.sh seams=true).
func TestCISeamsJob(t *testing.T) {
	w := loadCI(t)
	j := mustJob(t, w, "seams")
	for _, cond := range []string{
		"github.event_name == 'pull_request'",
		"!github.event.pull_request.draft",
		"needs.changes.outputs.seams == 'true'",
	} {
		if !strings.Contains(j.If, cond) {
			t.Errorf("seams if: %q does not contain %q", j.If, cond)
		}
	}
	if !slices.Contains(j.needs(), "changes") {
		t.Errorf("seams needs %v; it must need changes", j.needs())
	}
	if j.Timeout == 0 || j.Timeout > 15 {
		t.Errorf("seams timeout-minutes = %d, want 1..15", j.Timeout)
	}
	if !slices.ContainsFunc(j.Steps, func(s step) bool { return strings.Contains(s.Run, "make check-seams") }) {
		t.Error("seams does not run make check-seams")
	}
	if got := mustJob(t, w, "changes").Outputs["seams"]; got != "${{ steps.diff.outputs.seams }}" {
		t.Error("changes does not output seams from scripts/ci-changes.sh")
	}
	out := makeDryRun(t, "check-seams")
	for _, want := range []string{
		"go vet -tags seams", "go vet -tags kind ./tests/seams/...",
		"golangci-lint run --build-tags seams", "golangci-lint run --build-tags kind ./tests/seams/...",
		"go test -race -tags seams", "./internal/raftstore/...", "./internal/storagenode/...", "./cmd/storagenode/...",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("make -n check-seams has no %q:\n%s", want, out)
		}
	}
	// The symbol check builds storagenode twice; it is seams-tagged so that
	// go test ./... in check skips it.
	b, err := os.ReadFile("../cmd/storagenode/seams_build_test.go")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(string(b), "//go:build seams\n") {
		t.Error("cmd/storagenode/seams_build_test.go is not //go:build seams; check's go test ./... would build storagenode twice")
	}
}

// Go cache: setup-go's own cache is keyed on go.sum alone and saved once, so
// its test results froze at whatever main looked like then, and every PR
// touching a package below the frozen state re-ran it uncached. Instead each
// job that sets up Go restores the newest cache main saved for this go.sum
// (key go-<os>-<go.sum hash>-<sha>, restore-keys the prefix), and only check,
// on a push to main, saves one, so PRs never use up the cache quota.
const (
	goCacheKey     = "go-${{ runner.os }}-${{ hashFiles('go.sum') }}-${{ github.sha }}"
	goCachePrefix  = "go-${{ runner.os }}-${{ hashFiles('go.sum') }}-"
	goCacheRestore = "actions/cache/restore@"
	goCacheSave    = "actions/cache/save@"
)

func TestCIGoCache(t *testing.T) {
	w := loadCI(t)
	var savers []string
	for name, j := range w.Jobs {
		setup := slices.IndexFunc(j.Steps, func(s step) bool { return strings.HasPrefix(s.Uses, "actions/setup-go@") })
		if setup < 0 {
			continue
		}
		if c, ok := j.Steps[setup].With["cache"]; !ok || c != false {
			t.Errorf("job %s: setup-go has cache: %v; want false (the go.sum-only key freezes the cache)", name, c)
		}
		restore := slices.IndexFunc(j.Steps, func(s step) bool { return strings.HasPrefix(s.Uses, goCacheRestore) })
		if restore < 0 {
			t.Errorf("job %s sets up Go but does not restore the Go cache (%s)", name, goCacheRestore)
			continue
		}
		r := j.Steps[restore]
		if r.With["key"] != goCacheKey || r.With["restore-keys"] != goCachePrefix {
			t.Errorf("job %s restore: key %q restore-keys %q; want %q and %q", name, r.With["key"], r.With["restore-keys"], goCacheKey, goCachePrefix)
		}
		if r.If != "" {
			t.Errorf("job %s restore has if: %q; every run restores", name, r.If)
		}
		path, _ := r.With["path"].(string)
		for _, want := range []string{"gocache", "gomodcache"} {
			if !strings.Contains(path, want) {
				t.Errorf("job %s restore path %q does not include the %s output", name, path, want)
			}
		}
		for i, s := range j.Steps {
			if strings.HasPrefix(s.Uses, "actions/cache@") {
				t.Errorf("job %s step %d uses actions/cache (restore and save); PRs would save", name, i)
			}
			if !strings.HasPrefix(s.Uses, goCacheSave) {
				continue
			}
			savers = append(savers, name)
			if s.With["key"] != goCacheKey || s.With["path"] != r.With["path"] {
				t.Errorf("job %s save: key %q path %q; want the restore's key and path", name, s.With["key"], s.With["path"])
			}
			if s.If != "github.event_name == 'push' && github.ref == 'refs/heads/main'" {
				t.Errorf("job %s save if: %q; only pushes to main save", name, s.If)
			}
			if i < restore {
				t.Errorf("job %s saves before it restores", name)
			}
		}
	}
	if !slices.Equal(savers, []string{"check"}) {
		t.Errorf("jobs saving the Go cache: %v; want only check (one writer per key)", savers)
	}
}

// gen-check regenerates from contracts/ and fails on any difference.
func TestGenCheckTarget(t *testing.T) {
	out, err := exec.CommandContext(t.Context(), "make", "-C", "..", "-n", "gen-check").CombinedOutput()
	if err != nil {
		t.Fatalf("make -n gen-check: %v\n%s", err, out)
	}
	gen := strings.Index(string(out), "oapi-codegen")
	diff := strings.Index(string(out), "git diff --exit-code")
	if gen < 0 || diff < gen {
		t.Fatalf("make -n gen-check does not regenerate and then git diff --exit-code:\n%s", out)
	}
}

// gated: the conditions images and smoke share. A PR, not a push to main;
// not a draft; and a diff that touches code.
var gated = []string{
	"github.event_name == 'pull_request'",
	"!github.event.pull_request.draft",
	"needs.changes.outputs.code == 'true'",
}

func checkGated(t *testing.T, name string, j job) {
	t.Helper()
	for _, cond := range gated {
		if !strings.Contains(j.If, cond) {
			t.Errorf("%s if: %q does not contain %q", name, j.If, cond)
		}
	}
	if !slices.Contains(j.needs(), "changes") {
		t.Errorf("%s needs %v; it must need changes", name, j.needs())
	}
}

// A draft that becomes ready must trigger the gated jobs.
func TestCIPullRequestTypes(t *testing.T) {
	pr, ok := loadCI(t).On["pull_request"].(map[string]any)
	if !ok {
		t.Fatal("ci.yml on.pull_request has no types list")
	}
	var types []string
	for _, v := range pr["types"].([]any) {
		types = append(types, v.(string))
	}
	for _, want := range []string{"opened", "synchronize", "reopened", "ready_for_review"} {
		if !slices.Contains(types, want) {
			t.Errorf("on.pull_request.types %v lacks %s", types, want)
		}
	}
	if _, ok := pr["paths"]; ok {
		t.Error("on.pull_request has a paths filter; it would skip check too (filter per job via changes)")
	}
}

// changes decides from the PR's diff whether images and smoke run.
func TestCIChangesJob(t *testing.T) {
	j := mustJob(t, loadCI(t), "changes")
	if !strings.Contains(j.If, "github.event_name == 'pull_request'") {
		t.Errorf("changes if: %q; it only makes sense on a PR", j.If)
	}
	if !slices.ContainsFunc(j.Steps, func(s step) bool { return strings.Contains(s.Run, "scripts/ci-changes.sh") }) {
		t.Error("changes does not run scripts/ci-changes.sh")
	}
}

// images builds all three images and hands them to smoke as artifacts, so
// smoke tests what was built.
func TestCIImagesJob(t *testing.T) {
	j := mustJob(t, loadCI(t), "images")
	checkGated(t, "images", j)
	// Check names are what branch protection lists: "images (storagenode)", not
	// every matrix value.
	if j.Name != "images (${{ matrix.name }})" {
		t.Errorf("images name: %q, want %q", j.Name, "images (${{ matrix.name }})")
	}
	if j.Timeout == 0 || j.Timeout > 20 {
		t.Errorf("images timeout-minutes = %d, want 1..20", j.Timeout)
	}
	text := j.text()
	for _, df := range []string{"cmd/storagenode/Dockerfile", "cmd/coordinator/Dockerfile", "cmd/invcheck/Dockerfile"} {
		if !strings.Contains(text, df) {
			t.Errorf("images does not build %s", df)
		}
		if _, err := os.Stat("../" + df); err != nil {
			t.Errorf("%s: %v", df, err)
		}
	}
	for _, tag := range []string{"reliefgrid/storagenode:dev", "reliefgrid/coordinator:dev", "reliefgrid/tools:dev"} {
		if !strings.Contains(text, tag) {
			t.Errorf("images does not tag %s (the chart and demos use these names)", tag)
		}
	}
	if !slices.ContainsFunc(j.Steps, func(s step) bool { return strings.HasPrefix(s.Uses, "actions/upload-artifact@") }) {
		t.Error("images does not upload the image archives for smoke")
	}
}

// smoke runs on PRs to main only, after images, bounded in time, through
// make ci-smoke, and uploads its evidence whatever the outcome.
func TestCISmokeJob(t *testing.T) {
	j := mustJob(t, loadCI(t), "smoke")
	if !slices.Contains(j.needs(), "images") {
		t.Errorf("smoke needs %v; it must need images", j.needs())
	}
	checkGated(t, "smoke", j)
	if !strings.Contains(j.If, "github.base_ref == 'main'") {
		t.Errorf("smoke if: %q does not limit it to PRs to main", j.If)
	}
	if !slices.ContainsFunc(j.Steps, func(s step) bool { return strings.Contains(s.Run, "scripts/ci-tools.sh") }) {
		t.Error("smoke does not install the pinned tools with scripts/ci-tools.sh")
	}
	if j.Timeout == 0 || j.Timeout > 45 {
		t.Errorf("smoke timeout-minutes = %d, want 1..45", j.Timeout)
	}
	if !slices.ContainsFunc(j.Steps, func(s step) bool { return strings.Contains(s.Run, "make ci-smoke") }) {
		t.Error("smoke does not run make ci-smoke")
	}
	if !slices.ContainsFunc(j.Steps, func(s step) bool {
		return strings.Contains(s.Run, "TestStorageChartNoDuplicateKeys") && s.Env["REQUIRE_HELM"] != ""
	}) {
		t.Error("smoke does not run TestStorageChartNoDuplicateKeys with REQUIRE_HELM set (check has no helm, so it skips there)")
	}
	if !slices.ContainsFunc(j.Steps, func(s step) bool { return strings.HasPrefix(s.Uses, "actions/download-artifact@") }) {
		t.Error("smoke does not download the images job's archives")
	}
	up := slices.IndexFunc(j.Steps, func(s step) bool { return strings.HasPrefix(s.Uses, "actions/upload-artifact@") })
	if up < 0 {
		t.Fatal("smoke does not upload its evidence")
	}
	if s := j.Steps[up]; !strings.Contains(s.If, "always()") {
		t.Errorf("smoke's evidence upload has if: %q; it must run on failure too (always())", s.If)
	}
}

// --- scripts/ci-smoke.sh ------------------------------------------------------------------

// fakeTools puts make, kind, helm and kubectl on PATH. Each appends its
// argv to calls; FAKE_FAIL=<tool> makes that tool exit 1, FAKE_DEMO_FAIL
// makes only make demo-4 fail (with FAIL evidence), and FAKE_NO_LEADER makes
// every /storagectl status a follower without a leader.
func fakeTools(t *testing.T) (bin, calls string) {
	t.Helper()
	bin = t.TempDir()
	calls = filepath.Join(t.TempDir(), "calls")
	fake := func(name, body string) {
		src := "#!/usr/bin/env bash\necho \"" + name + " $*\" >>\"$FAKE_CALLS\"\n" +
			"[ \"${FAKE_FAIL:-}\" = " + name + " ] && exit 1\n" + body + "\nexit 0\n"
		if err := os.WriteFile(filepath.Join(bin, name), []byte(src), 0o755); err != nil {
			t.Fatal(err)
		}
	}
	fake("make", `case "$*" in *demo-4*)
	mkdir -p "$EVIDENCE_ROOT/$DEMO_RUN_ID"
	if [ -n "${FAKE_DEMO_FAIL:-}" ]; then echo "result:     FAIL (1 of 9 checks failed)" >"$EVIDENCE_ROOT/$DEMO_RUN_ID/demo-4.txt"; exit 1; fi
	echo "result:     PASS (9/9 checks)" >"$EVIDENCE_ROOT/$DEMO_RUN_ID/demo-4.txt";;
esac`)
	fake("kind", "")
	fake("helm", "")
	// kubectl ... exec storage-shard-<s>-<i> ... /storagectl status: replica 0 leads.
	fake("kubectl", `if [[ "$*" == *"/storagectl status"* ]]; then
	pod="$(grep -o 'storage-shard-[0-9]-[0-9]' <<<"$*" | head -1)"; s="${pod:14:1}"
	if [ -n "${FAKE_NO_LEADER:-}" ]; then echo '{"state":"Follower","leaderId":"","term":1}'; exit 0; fi
	state=Follower; [ "${pod: -1}" = 0 ] && state=Leader
	echo "{\"state\":\"$state\",\"leaderId\":\"storage-shard-$s-0/inc\",\"term\":2}"
else echo "kubectl output for: $*"; fi`)
	return bin, calls
}

type smokeRun struct {
	out, calls, evidence string
	code                 int
}

func runSmoke(t *testing.T, env ...string) smokeRun {
	t.Helper()
	script, err := filepath.Abs("ci-smoke.sh")
	if err != nil {
		t.Fatal(err)
	}
	bin, calls := fakeTools(t)
	images := t.TempDir()
	for _, n := range []string{"storagenode", "coordinator", "tools"} {
		if err := os.WriteFile(filepath.Join(images, n+".tar"), nil, 0o644); err != nil {
			t.Fatal(err)
		}
	}
	evidence := t.TempDir()
	out, code := runScript(t, "", append([]string{
		"PATH=" + bin + ":" + os.Getenv("PATH"), "FAKE_CALLS=" + calls,
		"IMAGES_DIR=" + images, "EVIDENCE_DIR=" + evidence, "SMOKE_RUN_ID=ci-1-1", "LEADER_TIMEOUT=2",
	}, env...), script)
	b, _ := os.ReadFile(calls)
	return smokeRun{out: out, calls: string(b), evidence: evidence, code: code}
}

// inOrder fails unless calls has a line containing each want, in order.
func inOrder(t *testing.T, calls string, wants ...string) {
	t.Helper()
	lines := strings.Split(calls, "\n")
	at := -1
	for _, w := range wants {
		i := slices.IndexFunc(lines[at+1:], func(l string) bool { return strings.Contains(l, w) })
		if i < 0 {
			t.Fatalf("no call containing %q after line %d; calls:\n%s", w, at, calls)
		}
		at += 1 + i
	}
}

func TestCISmokeHappyPath(t *testing.T) {
	r := runSmoke(t)
	if r.code != 0 {
		t.Fatalf("exit %d, want 0\n%s", r.code, r.out)
	}
	inOrder(t, r.calls,
		"make -s cluster-up",
		"kind load image-archive", // the prebuilt images, never a rebuild
		"helm --kube-context kind-reliefgrid install storage deploy/helm/storage -n reliefgrid --create-namespace --wait --wait-for-jobs",
		"storage-shard-0-0 -c storagenode -- /storagectl status",
		"make -s demo-4",
	)
	for _, n := range []string{"storagenode", "coordinator", "tools"} {
		if !regexp.MustCompile(`(?m)^kind load image-archive --name reliefgrid \S+/` + n + `\.tar$`).MatchString(r.calls) {
			t.Errorf("%s.tar was not kind-loaded into reliefgrid; calls:\n%s", n, r.calls)
		}
	}
	if strings.Contains(r.calls, "-image") || strings.Contains(r.calls, "docker build") {
		t.Errorf("ci-smoke rebuilt images instead of loading the images job's; calls:\n%s", r.calls)
	}
	if got, _ := os.ReadFile(filepath.Join(r.evidence, "ci-1-1", "demo-4.txt")); !strings.Contains(string(got), "PASS") {
		t.Errorf("demo-4 evidence not under EVIDENCE_DIR/SMOKE_RUN_ID: %q", got)
	}
	for _, f := range []string{"leaders.txt", "diagnostics/pods.txt"} {
		if _, err := os.Stat(filepath.Join(r.evidence, "ci-1-1", f)); err != nil {
			t.Errorf("evidence %s: %v", f, err)
		}
	}
}

// A failed install stops before the demo, still writes diagnostics, and
// fails the job.
func TestCISmokeInstallFails(t *testing.T) {
	r := runSmoke(t, "FAKE_FAIL=helm")
	if r.code == 0 {
		t.Fatalf("exit 0 after a failed helm install\n%s", r.out)
	}
	if strings.Contains(r.calls, "demo-4") {
		t.Errorf("demo-4 ran after a failed install; calls:\n%s", r.calls)
	}
	if _, err := os.Stat(filepath.Join(r.evidence, "ci-1-1", "diagnostics", "pods.txt")); err != nil {
		t.Errorf("no diagnostics after a failed install: %v", err)
	}
}

// Groups that never elect a leader fail within LEADER_TIMEOUT, before the
// demo, naming the shard.
func TestCISmokeNoLeaders(t *testing.T) {
	r := runSmoke(t, "FAKE_NO_LEADER=1")
	if r.code == 0 {
		t.Fatalf("exit 0 without leaders\n%s", r.out)
	}
	if strings.Contains(r.calls, "demo-4") {
		t.Errorf("demo-4 ran without leaders; calls:\n%s", r.calls)
	}
	if !strings.Contains(r.out, "shard 0") {
		t.Errorf("output does not name the leaderless shard:\n%s", r.out)
	}
}

// A failing demo fails the job, and its evidence is still there to upload.
func TestCISmokeDemoFails(t *testing.T) {
	r := runSmoke(t, "FAKE_DEMO_FAIL=1")
	if r.code == 0 {
		t.Fatalf("exit 0 after demo-4 failed\n%s", r.out)
	}
	if got, _ := os.ReadFile(filepath.Join(r.evidence, "ci-1-1", "demo-4.txt")); !strings.Contains(string(got), "FAIL") {
		t.Errorf("demo-4's FAIL evidence is missing: %q", got)
	}
	if _, err := os.Stat(filepath.Join(r.evidence, "ci-1-1", "diagnostics", "pods.txt")); err != nil {
		t.Errorf("no diagnostics after a failure: %v", err)
	}
}

// make ci-smoke is the documented entry point.
func TestCISmokeMakeTarget(t *testing.T) {
	out, err := exec.CommandContext(t.Context(), "make", "-C", "..", "-n", "ci-smoke").CombinedOutput()
	if err != nil || !strings.Contains(string(out), "scripts/ci-smoke.sh") {
		t.Fatalf("make -n ci-smoke: %v\n%s", err, out)
	}
}

// --- scripts/ci-changes.sh ----------------------------------------------------------------

// ciChanges commits files on top of an empty base in a new repo and returns
// ci-changes.sh's output lines (code=..., seams=...).
func ciChanges(t *testing.T, files []string) []string {
	t.Helper()
	script, err := filepath.Abs("ci-changes.sh")
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	git := func(args ...string) string {
		t.Helper()
		cmd := exec.CommandContext(t.Context(), "git", append([]string{"-C", dir, "-c", "user.name=t", "-c", "user.email=t@example.com"}, args...)...)
		out, err := cmd.CombinedOutput()
		if err != nil {
			t.Fatalf("git %v: %v\n%s", args, err, out)
		}
		return strings.TrimSpace(string(out))
	}
	git("init", "-q")
	git("commit", "-q", "--allow-empty", "-m", "base")
	base := git("rev-parse", "HEAD")
	for _, f := range files {
		p := filepath.Join(dir, f)
		if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(p, []byte("x\n"), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	git("add", "-A")
	git("commit", "-q", "-m", "change")
	cmd := exec.CommandContext(t.Context(), "bash", script, base, git("rev-parse", "HEAD"))
	cmd.Dir = dir
	out, err := cmd.Output()
	if err != nil {
		t.Fatalf("ci-changes.sh: %v", err)
	}
	return strings.Split(strings.TrimSpace(string(out)), "\n")
}

// ci-changes.sh <base> <head> prints code=true when the diff touches what
// images and smoke test, code=false otherwise.
func TestCIChanges(t *testing.T) {
	for _, tc := range []struct {
		files []string
		want  string
	}{
		{[]string{"docs/notes.md"}, "code=false"},
		{[]string{"README.md"}, "code=false"},
		{[]string{"README.md", "ui/app.js"}, "code=true"}, // the UI is built into an image
		{[]string{"ui/src/index.html"}, "code=true"},
		{[]string{".dockerignore"}, "code=true"},       // changes every image's build context
		{[]string{"uinotes.md"}, "code=false"},         // the directory only
		{[]string{"docs/.dockerignore"}, "code=false"}, // only the root .dockerignore
		{[]string{".github/workflows/ci.yml"}, "code=true"},
		{[]string{"chaos/scripts/lib.sh"}, "code=true"},
		{[]string{"loadtest/ledger/ledger.go"}, "code=true"},
		{[]string{"chaosnotes.md", "loadtesting.md"}, "code=false"}, // the directories only
		{[]string{"cmd/x/main.go"}, "code=true"},
		{[]string{"internal/ring/ring.go"}, "code=true"},
		{[]string{"deploy/helm/storage/values.yaml"}, "code=true"},
		{[]string{"contracts/internal.yaml"}, "code=true"},
		{[]string{"scripts/demos/demo-4.sh"}, "code=true"},
		{[]string{"Makefile"}, "code=true"},
		{[]string{"tools.mk"}, "code=true"},
		{[]string{"go.mod"}, "code=true"},
		{[]string{"go.sum"}, "code=true"},
		{[]string{"docs/a.md", "internal/x.go"}, "code=true"},
		{[]string{"docs/Makefile"}, "code=false"}, // only the root Makefile
		{[]string{"internalnotes.md"}, "code=false"},
	} {
		t.Run(strings.Join(tc.files, ","), func(t *testing.T) {
			lines := ciChanges(t, tc.files)
			if !slices.Contains(lines, tc.want) {
				t.Errorf("got %q, want a line %q", lines, tc.want)
			}
		})
	}
}

// ci-changes.sh also prints seams=true when the diff touches the seam
// packages, their tests or how they are built and run (S4.6b), so the seams
// job runs; seams=false otherwise.
func TestCIChangesSeams(t *testing.T) {
	for _, tc := range []struct {
		files []string
		want  string
	}{
		{[]string{"internal/raftstore/node.go"}, "seams=true"},
		{[]string{"internal/storagenode/server.go"}, "seams=true"},
		{[]string{"cmd/storagenode/main.go"}, "seams=true"},
		{[]string{"internal/testseam/testseam.go"}, "seams=true"},
		{[]string{"internal/fsm/memstore/memstore.go"}, "seams=true"},
		{[]string{"internal/fsm/codec.go"}, "seams=true"}, // memstore's and raftstore's codec
		{[]string{"tests/seams/kind_test.go"}, "seams=true"},
		{[]string{"go.mod"}, "seams=true"},
		{[]string{"go.sum"}, "seams=true"},
		{[]string{"Makefile"}, "seams=true"},
		{[]string{"tools.mk"}, "seams=true"}, // the golangci-lint pin
		{[]string{".github/workflows/ci.yml"}, "seams=true"},
		{[]string{"scripts/ci-changes.sh"}, "seams=true"},
		{[]string{"docs/a.md", "internal/raftstore/x.go"}, "seams=true"},
		{[]string{"internal/ring/ring.go"}, "seams=false"},
		{[]string{"cmd/coordinator/main.go"}, "seams=false"},
		{[]string{"cmd/storagenodex/main.go"}, "seams=false"}, // the directory only
		{[]string{"internal/fsmx/a.go"}, "seams=false"},
		{[]string{"scripts/demos/demo-4.sh"}, "seams=false"},
		{[]string{"docs/operations/test-seams.md"}, "seams=false"},
		{[]string{"tests/workflow/a_test.go"}, "seams=false"},
	} {
		t.Run(strings.Join(tc.files, ","), func(t *testing.T) {
			lines := ciChanges(t, tc.files)
			if !slices.Contains(lines, tc.want) {
				t.Errorf("got %q, want a line %q", lines, tc.want)
			}
		})
	}
}

// A base it cannot diff against is not "no changes": it fails.
func TestCIChangesUnknownBase(t *testing.T) {
	script, err := filepath.Abs("ci-changes.sh")
	if err != nil {
		t.Fatal(err)
	}
	cmd := exec.CommandContext(t.Context(), "bash", script, "0000000000000000000000000000000000000000", "HEAD")
	if out, err := cmd.CombinedOutput(); err == nil {
		t.Fatalf("exit 0 for an unknown base: %s", out)
	}
}

// --- scripts/ci-tools.sh ------------------------------------------------------------------

// The dry run prints, per tool, the URL it would download and the sha256 it
// would require; both come from the tools.mk pins, never from the server.
func TestCIToolsDryRun(t *testing.T) {
	pin := func(name string) string {
		t.Helper()
		out, err := exec.CommandContext(t.Context(), "make", "-C", "..", "-s", "print-"+name).Output()
		if err != nil || strings.TrimSpace(string(out)) == "" {
			t.Fatalf("make print-%s: %v %q", name, err, out)
		}
		return strings.TrimSpace(string(out))
	}
	kind, kubectl, helm := pin("KIND_VERSION"), pin("KUBECTL_VERSION"), pin("HELM_VERSION")
	want := map[string][2]string{
		"kind":    {"https://github.com/kubernetes-sigs/kind/releases/download/" + kind + "/kind-linux-amd64", pin("KIND_SHA256")},
		"kubectl": {"https://dl.k8s.io/release/" + kubectl + "/bin/linux/amd64/kubectl", pin("KUBECTL_SHA256")},
		"helm":    {"https://get.helm.sh/helm-" + helm + "-linux-amd64.tar.gz", pin("HELM_SHA256")},
	}
	for name, w := range want {
		if !regexp.MustCompile(`^[0-9a-f]{64}$`).MatchString(w[1]) {
			t.Errorf("%s sha256 pin %q is not 64 hex digits", name, w[1])
		}
	}
	script, err := filepath.Abs("ci-tools.sh")
	if err != nil {
		t.Fatal(err)
	}
	cmd := exec.CommandContext(t.Context(), "bash", script, t.TempDir())
	cmd.Env = append(os.Environ(), "CI_TOOLS_DRY_RUN=1")
	out, err := cmd.Output()
	if err != nil {
		t.Fatalf("ci-tools.sh dry run: %v\n%s", err, out)
	}
	got := map[string][2]string{}
	for _, line := range strings.Split(strings.TrimSpace(string(out)), "\n") {
		f := strings.Fields(line) // <tool> <url> <sha256>
		if len(f) != 3 {
			t.Fatalf("dry-run line %q: want <tool> <url> <sha256>", line)
		}
		got[f[0]] = [2]string{f[1], f[2]}
	}
	for name, w := range want {
		if got[name] != w {
			t.Errorf("%s: dry run %v, want %v", name, got[name], w)
		}
	}
	if len(got) != len(want) {
		t.Errorf("dry run names %d tools, want %d:\n%s", len(got), len(want), out)
	}
}
