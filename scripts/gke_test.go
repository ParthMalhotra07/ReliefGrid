package scripts

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"testing"
	"time"

	"go.yaml.in/yaml/v3"
)

// S5.1 (GKE). These tests never reach Google Cloud: the scripts run with fake
// terraform, gcloud, helm, kubectl, kind and docker on PATH that log their argv,
// and the Terraform is checked as text plus, when terraform is installed,
// `terraform fmt -check` and `terraform validate` (no credentials, no backend).
//
// Fakes' interface (what the scripts may rely on):
//   terraform output -json        prints $FAKE_TF_OUTPUT (the outputs of infra/gcp)
//   gcloud <group> <res> list ... prints $FAKE_LEFTOVERS/<group>-<res> if it exists
//   gcloud artifacts docker images describe <ref>
//                                 exits 1 if <ref> contains /$FAKE_MISSING_IMAGE@
//   helm template ...             a Traefik Middleware with rateLimit and buffering
//                                 (maxRequestBodyBytes), or nothing with $FAKE_NO_MIDDLEWARE
//   kubectl ... get <kinds> ... -o json
//                                 {"items":[]}, or one stuck PVC / PV ($FAKE_STUCK_PVC) or one
//                                 LoadBalancer Service ($FAKE_STUCK_LB) when <kinds> names them

const (
	gkeProject = "rg-s51-test"
	gkeZone    = "asia-south1-a"
	gkeContext = "gke_" + gkeProject + "_" + gkeZone + "_reliefgrid"
	gkeReg     = "asia-south1-docker.pkg.dev/" + gkeProject + "/reliefgrid"
	gkeIP      = "203.0.113.10"
)

var gkeImages = []string{"storagenode", "coordinator", "tools", "api", "matcher", "ui"}

func gkeDigest(name string) string {
	d := strings.Repeat(string("abcdef0123456789"[len(name)%16]), 64)
	return "sha256:" + d
}

func gkeTFOutput() string {
	out := map[string]map[string]string{
		"project_id":   {"value": gkeProject},
		"region":       {"value": "asia-south1"},
		"zone":         {"value": gkeZone},
		"cluster_name": {"value": "reliefgrid"},
		"kube_context": {"value": gkeContext},
		"registry":     {"value": gkeReg},
		"ingress_ip":   {"value": gkeIP},
	}
	b, _ := json.Marshal(out)
	return string(b)
}

// gkeFakes puts the fake tools on PATH and returns the env for a run.
func gkeFakes(t *testing.T) (env []string, calls string) {
	t.Helper()
	bin := t.TempDir()
	calls = filepath.Join(t.TempDir(), "calls")
	leftovers := t.TempDir()
	tfOut := filepath.Join(t.TempDir(), "tf-output.json")
	if err := os.WriteFile(tfOut, []byte(gkeTFOutput()), 0o644); err != nil {
		t.Fatal(err)
	}
	fake := func(name, body string) {
		src := "#!/usr/bin/env bash\necho \"" + name + " $*\" >>\"$FAKE_CALLS\"\n" + body + "\nexit 0\n"
		if err := os.WriteFile(filepath.Join(bin, name), []byte(src), 0o755); err != nil {
			t.Fatal(err)
		}
	}
	fake("terraform", `[[ " $* " == *" output "* ]] && cat "$FAKE_TF_OUTPUT"`)
	fake("gcloud", `words=(); for a in "$@"; do [[ "$a" == -* ]] || words+=("$a"); done
if [[ " $* " == *" list "* ]]; then f="$FAKE_LEFTOVERS/${words[0]}-${words[1]}"; [ -f "$f" ] && cat "$f"; exit 0; fi
if [[ "$*" == *"images describe"* ]] && [ -n "${FAKE_MISSING_IMAGE:-}" ] && [[ "$*" == *"/$FAKE_MISSING_IMAGE@"* ]]; then echo "NOT_FOUND" >&2; exit 1; fi`)
	// helm template (the app render gke-deploy.sh checks for D8's middleware) prints a
	// Traefik Middleware with rate and size limits, unless FAKE_NO_MIDDLEWARE is set.
	fake("helm", `if [[ " $* " == *" template "* ]] && [ -z "${FAKE_NO_MIDDLEWARE:-}" ]; then
	printf 'apiVersion: traefik.io/v1alpha1\nkind: Middleware\nspec:\n  rateLimit:\n    average: 20\n---\napiVersion: traefik.io/v1alpha1\nkind: Middleware\nspec:\n  buffering:\n    maxRequestBodyBytes: 65536\n'
fi`)
	fake("kubectl", `if [[ " $* " == *" get "* && "$*" == *"json"* ]]; then
	if [ -n "${FAKE_STUCK_PVC:-}" ] && [[ "$*" =~ (pvc|persistentvolume) ]]; then echo '{"items":[{"kind":"PersistentVolumeClaim","metadata":{"name":"data-storage-shard-0-0","namespace":"reliefgrid"}}]}'; exit 0; fi
	if [ -n "${FAKE_STUCK_LB:-}" ] && [[ "$*" =~ (svc|service) ]]; then echo '{"items":[{"kind":"Service","metadata":{"name":"reliefgrid-traefik","namespace":"reliefgrid"},"spec":{"type":"LoadBalancer"}}]}'; exit 0; fi
	echo '{"items":[]}'
fi`)
	fake("kind", "")
	fake("docker", "")
	fake("make", "")
	fake("go", "")
	env = []string{
		"PATH=" + bin + ":" + os.Getenv("PATH"),
		"FAKE_CALLS=" + calls,
		"FAKE_TF_OUTPUT=" + tfOut,
		"FAKE_LEFTOVERS=" + leftovers,
		"KUBE_CONTEXT=", "CHAOS_CONTEXT=",
	}
	return env, calls
}

func leftoversDir(env []string) string {
	for _, e := range env {
		if v, ok := strings.CutPrefix(e, "FAKE_LEFTOVERS="); ok {
			return v
		}
	}
	return ""
}

func readCalls(t *testing.T, calls string) string {
	t.Helper()
	b, err := os.ReadFile(calls)
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		t.Fatal(err)
	}
	return string(b)
}

// noStateChange fails if any call could create, change or delete a cloud resource
// outside the step the script is allowed to take (allow: e.g. "terraform destroy").
func noStateChange(t *testing.T, calls string, allow ...string) {
	t.Helper()
	for l := range strings.SplitSeq(calls, "\n") {
		if slices.ContainsFunc(allow, func(a string) bool { return strings.HasPrefix(l, a) }) {
			continue
		}
		if strings.HasPrefix(l, "terraform ") && regexp.MustCompile(`\b(apply|destroy|import|taint|state)\b`).MatchString(l) {
			t.Errorf("forbidden terraform call: %s", l)
		}
		if strings.HasPrefix(l, "gcloud ") && regexp.MustCompile(`\b(create|delete|update|add-iam-policy-binding|set-iam-policy|copy|deploy|resize)\b`).MatchString(l) {
			t.Errorf("forbidden gcloud call: %s", l)
		}
	}
}

func gkeScript(t *testing.T, name string) string {
	t.Helper()
	p, err := filepath.Abs(name)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(p); err != nil {
		t.Fatalf("%s is missing: %v", name, err)
	}
	return p
}

func writeTemp(t *testing.T, name, content string) string {
	t.Helper()
	p := filepath.Join(t.TempDir(), name)
	if err := os.WriteFile(p, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
	return p
}

func digestsFile(t *testing.T, edit func(m map[string]string)) string {
	t.Helper()
	m := map[string]string{}
	for _, n := range gkeImages {
		m[n] = gkeDigest(n)
	}
	if edit != nil {
		edit(m)
	}
	b, _ := json.Marshal(map[string]any{"commit": strings.Repeat("c", 40), "images": m})
	return writeTemp(t, "image-digests.json", string(b))
}

// --- scripts/gke-deploy.sh ----------------------------------------------------------------

const (
	operatorSecret   = "operator-token-not-for-argv-4f1c"
	dispatcherSecret = "dispatcher-token-not-for-argv-9a2e"
)

type deployRun struct {
	out, calls, evidence string
	code                 int
}

func runDeploy(t *testing.T, env ...string) deployRun {
	t.Helper()
	fenv, calls := gkeFakes(t)
	evidence := t.TempDir()
	base := append(fenv,
		"IMAGE_DIGESTS="+digestsFile(t, nil),
		"OPERATOR_TOKEN_FILE="+writeTemp(t, "operator-token", operatorSecret),
		"DISPATCHER_TOKEN_FILE="+writeTemp(t, "dispatcher-token", dispatcherSecret),
		"ALLOWED_CIDRS=198.51.100.7/32,192.0.2.0/28",
		"APP_VALUES="+writeTemp(t, "values-app-gke.yaml", "# Parth's GKE app values (A's chart)\n"),
		"EVIDENCE_DIR="+evidence,
		"GKE_DEPLOY_APP=true",
	)
	out, code := runScript(t, "", append(base, env...), gkeScript(t, "gke-deploy.sh"))
	return deployRun{out: out, calls: readCalls(t, calls), evidence: evidence, code: code}
}

func TestGKEDeployHappyPath(t *testing.T) {
	r := runDeploy(t)
	if r.code != 0 {
		t.Fatalf("gke-deploy exited %d:\n%s\ncalls:\n%s", r.code, r.out, r.calls)
	}
	inOrder(t, r.calls,
		"terraform -chdir=",
		"gcloud container clusters get-credentials reliefgrid",
		"gcloud artifacts docker images describe",
		"helm upgrade --install storage",
		"helm upgrade --install reliefgrid",
	)
	noStateChange(t, r.calls)
	for l := range strings.SplitSeq(r.calls, "\n") {
		// helm repo add and dependency build (scripts/app-chart-deps.sh) touch no cluster.
		clusterFree := strings.HasPrefix(l, "helm repo ") || strings.HasPrefix(l, "helm dependency ") || strings.HasPrefix(l, "helm template ")
		if strings.HasPrefix(l, "helm ") && !clusterFree && !strings.Contains(l, "--kube-context "+gkeContext) {
			t.Errorf("helm call without --kube-context %s: %s", gkeContext, l)
		}
		if strings.HasPrefix(l, "kubectl ") && !strings.Contains(l, "--context "+gkeContext) {
			t.Errorf("kubectl call without --context %s: %s", gkeContext, l)
		}
		if strings.HasPrefix(l, "kind ") || strings.HasPrefix(l, "docker ") {
			t.Errorf("gke-deploy called a kind-only tool: %s", l)
		}
		if strings.HasPrefix(l, "gcloud container clusters get-credentials") &&
			(!strings.Contains(l, gkeZone) || !strings.Contains(l, gkeProject)) {
			t.Errorf("get-credentials not scoped to the Terraform zone and project: %s", l)
		}
	}
	// Images by digest from the registry Terraform made, every one checked first.
	for _, n := range gkeImages {
		if !strings.Contains(r.calls, "images describe "+gkeReg+"/"+n+"@"+gkeDigest(n)) {
			t.Errorf("did not check %s@%s exists in Artifact Registry", n, gkeDigest(n))
		}
	}
	storage := callLine(t, r.calls, "helm upgrade --install storage")
	for _, want := range []string{
		"deploy/helm/storage", "values-gke.yaml",
		gkeReg + "/storagenode", gkeDigest("storagenode"),
		gkeReg + "/coordinator", gkeDigest("coordinator"),
		"--wait", "--wait-for-jobs", "--set-file operatorToken=",
	} {
		if !strings.Contains(storage, want) {
			t.Errorf("storage install lacks %q:\n%s", want, storage)
		}
	}
	app := callLine(t, r.calls, "helm upgrade --install reliefgrid")
	for _, want := range []string{
		"deploy/helm/reliefgrid", "values-app-gke.yaml", "storage.enabled=false",
		gkeDigest("api"), gkeDigest("matcher"), gkeDigest("ui"),
		gkeIP, "198.51.100.7/32", "192.0.2.0/28", "--set-file",
	} {
		if !strings.Contains(app, want) {
			t.Errorf("app install lacks %q:\n%s", want, app)
		}
	}
	// Secrets go by file, never on a command line or into the evidence.
	for _, secret := range []string{operatorSecret, dispatcherSecret} {
		if strings.Contains(r.calls, secret) || strings.Contains(r.out, secret) {
			t.Errorf("a token's contents reached argv or stdout")
		}
	}
	ev := evidenceText(t, r.evidence)
	if !strings.Contains(ev, gkeDigest("storagenode")) || !strings.Contains(ev, gkeIP) {
		t.Errorf("evidence lacks the digests and the ingress IP:\n%s", ev)
	}
	if strings.Contains(ev, operatorSecret) || strings.Contains(ev, dispatcherSecret) {
		t.Error("evidence contains a token")
	}
}

func callLine(t *testing.T, calls, prefix string) string {
	t.Helper()
	for l := range strings.SplitSeq(calls, "\n") {
		if strings.HasPrefix(l, prefix) {
			return l
		}
	}
	t.Fatalf("no call starting %q; calls:\n%s", prefix, calls)
	return ""
}

func evidenceText(t *testing.T, dir string) string {
	t.Helper()
	var sb strings.Builder
	_ = filepath.WalkDir(dir, func(p string, d os.DirEntry, err error) error {
		if err == nil && !d.IsDir() {
			b, _ := os.ReadFile(p)
			sb.Write(b)
		}
		return nil
	})
	if sb.Len() == 0 {
		t.Fatalf("no evidence written under %s", dir)
	}
	return sb.String()
}

// Storage alone (the leader-kill subset needs no public URL): no app, so no
// dispatcher token or allowlist is needed and no LoadBalancer is created.
func TestGKEDeployStorageOnly(t *testing.T) {
	r := runDeploy(t, "GKE_DEPLOY_APP=false", "DISPATCHER_TOKEN_FILE=", "ALLOWED_CIDRS=", "APP_VALUES=/nonexistent")
	if r.code != 0 {
		t.Fatalf("exit %d:\n%s", r.code, r.out)
	}
	_ = callLine(t, r.calls, "helm upgrade --install storage")
	if strings.Contains(r.calls, "helm upgrade --install reliefgrid") {
		t.Error("GKE_DEPLOY_APP=false still installed the app")
	}
}

// Every refusal happens before anything is installed.
func TestGKEDeployRefusals(t *testing.T) {
	inRepo, err := filepath.Abs("../deploy/helm/storage/values.yaml")
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name string
		env  []string
		want string
	}{
		{"no digests file", []string{"IMAGE_DIGESTS=/nonexistent/image-digests.json"}, "image-digests"},
		{"a digest missing", []string{"IMAGE_DIGESTS=" + digestsFile(t, func(m map[string]string) { delete(m, "ui") })}, "ui"},
		{"a malformed digest", []string{"IMAGE_DIGESTS=" + digestsFile(t, func(m map[string]string) { m["api"] = "latest" })}, "api"},
		{"image not in the registry", []string{"FAKE_MISSING_IMAGE=matcher"}, "matcher"},
		{"no allowlist", []string{"ALLOWED_CIDRS="}, "ALLOWED_CIDRS"},
		{"open allowlist", []string{"ALLOWED_CIDRS=198.51.100.7/32,0.0.0.0/0"}, "0.0.0.0/0"},
		{"open IPv6 allowlist", []string{"ALLOWED_CIDRS=::/0"}, "::/0"},
		{"no dispatcher token", []string{"DISPATCHER_TOKEN_FILE="}, "DISPATCHER_TOKEN_FILE"},
		{"no operator token", []string{"OPERATOR_TOKEN_FILE="}, "OPERATOR_TOKEN_FILE"},
		{"token file in the repo", []string{"OPERATOR_TOKEN_FILE=" + inRepo}, "outside the repository"},
		{"no app values", []string{"APP_VALUES=/nonexistent/values-app-gke.yaml"}, "values-app-gke"},
		{"another context", []string{"KUBE_CONTEXT=kind-reliefgrid"}, gkeContext},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := runDeploy(t, tc.env...)
			if r.code == 0 {
				t.Fatalf("exit 0, want a refusal:\n%s", r.out)
			}
			if !strings.Contains(r.out, tc.want) {
				t.Errorf("output does not name %q:\n%s", tc.want, r.out)
			}
			if strings.Contains(r.calls, "helm ") {
				t.Errorf("helm was called before the refusal:\n%s", r.calls)
			}
			noStateChange(t, r.calls)
		})
	}
}

// D8: the app goes on a public IP only if its chart renders the Traefik rate and
// size limits; helm silently ignores unknown --set keys, so a chart without them
// would otherwise deploy unprotected.
func TestGKEDeployRequiresMiddleware(t *testing.T) {
	r := runDeploy(t, "FAKE_NO_MIDDLEWARE=1")
	if r.code == 0 {
		t.Fatalf("exit 0 without the D8 middleware:\n%s", r.out)
	}
	if !strings.Contains(r.out, "rateLimit") {
		t.Errorf("refusal does not name the missing rateLimit:\n%s", r.out)
	}
	if strings.Contains(r.calls, "helm upgrade --install reliefgrid") {
		t.Error("the app was installed without the middleware")
	}
}

// --- scripts/gke-teardown.sh --------------------------------------------------------------

type teardownRun struct {
	out, calls string
	code       int
}

func runTeardown(t *testing.T, stdin string, leftovers map[string]string, env ...string) teardownRun {
	t.Helper()
	fenv, calls := gkeFakes(t)
	dir := leftoversDir(fenv)
	for k, v := range leftovers {
		if err := os.WriteFile(filepath.Join(dir, k), []byte(v), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	base := append(fenv,
		"GKE_TFVARS="+writeTemp(t, "s51.tfvars", "project_id = \""+gkeProject+"\"\n"),
		"STATE_BUCKET=rg-s51-tfstate",
		"WAIT_SECONDS=2", "POLL_SECONDS=0.2",
		"EVIDENCE_DIR="+t.TempDir(),
	)
	out, code := runScript(t, stdin, append(base, env...), gkeScript(t, "gke-teardown.sh"))
	return teardownRun{out: out, calls: readCalls(t, calls), code: code}
}

// The read-only leftover listings, each scoped to the project.
var leftoverLists = []string{
	"gcloud container clusters list", "gcloud compute instances list", "gcloud compute disks list",
	"gcloud compute forwarding-rules list", "gcloud compute target-pools list",
	"gcloud compute backend-services list", "gcloud compute health-checks list",
	"gcloud compute addresses list", "gcloud compute routers list",
	"gcloud compute firewall-rules list", "gcloud compute networks list",
	"gcloud artifacts repositories list", "gcloud storage buckets list",
}

func TestGKETeardownOrder(t *testing.T) {
	r := runTeardown(t, gkeProject+"\n", nil)
	if r.code != 0 {
		t.Fatalf("exit %d:\n%s\ncalls:\n%s", r.code, r.out, r.calls)
	}
	inOrder(t, r.calls,
		"terraform -chdir=", // outputs: the target
		"gcloud container clusters get-credentials reliefgrid",
		"helm --kube-context "+gkeContext+" uninstall reliefgrid",
		"helm --kube-context "+gkeContext+" uninstall storage",
		"kubectl --context "+gkeContext, // waits for PVCs and LoadBalancers to be gone
		"terraform -chdir=",
		"gcloud container clusters list",
	)
	d := ""
	for l := range strings.SplitSeq(r.calls, "\n") {
		if strings.HasPrefix(l, "terraform ") && strings.Contains(l, " destroy") {
			d = l
		}
	}
	if d == "" || !strings.Contains(d, "-var-file=") {
		t.Errorf("no terraform destroy with the var file: %q", d)
	}
	// Uninstall, then wait, then destroy: destroy comes after every helm/kubectl call.
	lines := strings.Split(r.calls, "\n")
	di := slices.IndexFunc(lines, func(l string) bool { return strings.Contains(l, " destroy") })
	for i, l := range lines {
		if (strings.HasPrefix(l, "helm ") || strings.HasPrefix(l, "kubectl ")) && i > di {
			t.Errorf("%q runs after terraform destroy", l)
		}
	}
	for _, list := range leftoverLists {
		l := callLine(t, r.calls, list)
		if !strings.Contains(l, gkeProject) {
			t.Errorf("%q is not scoped to project %s", l, gkeProject)
		}
	}
	for l := range strings.SplitSeq(r.calls, "\n") {
		if strings.HasPrefix(l, "gcloud ") && !strings.Contains(l, " list") && !strings.Contains(l, "get-credentials") {
			t.Errorf("teardown's only gcloud calls are get-credentials and listings: %s", l)
		}
	}
	noStateChange(t, r.calls, "terraform -chdir=")
	if !strings.Contains(r.out, "nothing left") {
		t.Errorf("no final 'nothing left' line:\n%s", r.out)
	}
}

func TestGKETeardownRefusals(t *testing.T) {
	for _, tc := range []struct {
		name, stdin string
		env         []string
		want        string
	}{
		{"wrong project typed", "some-other-project\n", nil, "not confirmed"},
		{"nothing typed", "", nil, "not confirmed"},
		{"no var file", gkeProject + "\n", []string{"GKE_TFVARS=/nonexistent.tfvars"}, "GKE_TFVARS"},
		{"another context", gkeProject + "\n", []string{"KUBE_CONTEXT=kind-reliefgrid"}, gkeContext},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := runTeardown(t, tc.stdin, nil, tc.env...)
			if r.code == 0 {
				t.Fatalf("exit 0, want a refusal:\n%s", r.out)
			}
			if !strings.Contains(r.out, tc.want) {
				t.Errorf("output does not name %q:\n%s", tc.want, r.out)
			}
			if strings.Contains(r.calls, "helm ") || strings.Contains(r.calls, " destroy") {
				t.Errorf("uninstalled or destroyed before confirmation:\n%s", r.calls)
			}
		})
	}
}

// A PVC or LoadBalancer that does not go away keeps its disk or forwarding rule
// billing; destroying the cluster would orphan it. So teardown stops instead.
func TestGKETeardownStopsBeforeDestroyWhenStuck(t *testing.T) {
	for _, tc := range []struct{ name, env, want string }{
		{"pvc", "FAKE_STUCK_PVC=1", "data-storage-shard-0-0"},
		{"loadbalancer", "FAKE_STUCK_LB=1", "reliefgrid-traefik"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := runTeardown(t, gkeProject+"\n", nil, tc.env)
			if r.code == 0 {
				t.Fatalf("exit 0 with a stuck %s:\n%s", tc.name, r.out)
			}
			if !strings.Contains(r.out, tc.want) {
				t.Errorf("output does not name %s:\n%s", tc.want, r.out)
			}
			if strings.Contains(r.calls, " destroy") {
				t.Error("terraform destroy ran with a stuck resource")
			}
		})
	}
}

func TestGKETeardownLeftoverCheck(t *testing.T) {
	for _, tc := range []struct {
		name      string
		leftovers map[string]string
		ok        bool
		want      string
	}{
		{"orphaned disk", map[string]string{"compute-disks": "pvc-0f3a\n"}, false, "pvc-0f3a"},
		{"orphaned forwarding rule", map[string]string{"compute-forwarding-rules": "a1b2c3\n"}, false, "a1b2c3"},
		{"cluster still there", map[string]string{"container-clusters": "reliefgrid\n"}, false, "reliefgrid"},
		{"another bucket", map[string]string{"storage-buckets": "gs://rg-s51-tfstate\ngs://stray\n"}, false, "stray"},
		{"only the state bucket", map[string]string{"storage-buckets": "gs://rg-s51-tfstate\n"}, true, "nothing left"},
		{"default network only", map[string]string{"compute-networks": "default\n", "compute-firewall-rules": "default-allow-ssh\ndefault-allow-icmp\n"}, true, "nothing left"},
		{"our network left", map[string]string{"compute-networks": "default\nreliefgrid\n"}, false, "reliefgrid"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := runTeardown(t, gkeProject+"\n", tc.leftovers)
			if (r.code == 0) != tc.ok {
				t.Fatalf("exit %d, want ok=%v:\n%s", r.code, tc.ok, r.out)
			}
			if !strings.Contains(r.out, tc.want) {
				t.Errorf("output does not name %q:\n%s", tc.want, r.out)
			}
		})
	}
}

// --- kind-only guard ----------------------------------------------------------------------

// Scripts that run kind, docker exec/stop or conntrack against kind nodes, or that
// delete and recreate the kind cluster, refuse a non-kind context before calling
// any tool. Scripts the GKE leader-kill subset uses are not refused.
func TestKindOnlyGuard(t *testing.T) {
	type c struct{ script, envVar string }
	kindOnly := []c{
		{"../chaos/scripts/stop-worker.sh", "CHAOS_CONTEXT"},
		{"../chaos/scripts/isolate-replica.sh", "CHAOS_CONTEXT"},
		{"../chaos/scripts/suite.sh", "CHAOS_CONTEXT"},
		{"demos/reset.sh", "KUBE_CONTEXT"},
		{"demos/gate.sh", "KUBE_CONTEXT"},
		{"app-deploy.sh", "KUBE_CONTEXT"},
		{"verify-storage-k8s.sh", "KUBE_CONTEXT"},
	}
	// Until a script has its guard it runs on with the fakes, so keep it away from the
	// worktree and from anything real: fake make and go, a temp working directory and
	// temp evidence directories; and check afterwards that the worktree is unchanged.
	before := worktreeStatus(t)
	defer func() {
		if after := worktreeStatus(t); after != before {
			t.Errorf("a kind-only script changed the worktree:\nbefore:\n%s\nafter:\n%s", before, after)
		}
	}()
	for _, tc := range kindOnly {
		t.Run(filepath.Base(tc.script), func(t *testing.T) {
			env, calls := gkeFakes(t)
			tmp := t.TempDir()
			env = append(env, tc.envVar+"="+gkeContext, "RESET_CONFIRM=reliefgrid",
				"EVIDENCE_ROOT="+tmp, "SUITE_OUT="+tmp+"/suite", "A15_EVIDENCE_DIR="+tmp+"/a15", "GATE_ROUNDS=1")
			out, code := runIn(t, tmp, env, gkeScript(t, tc.script))
			if code == 0 {
				t.Fatalf("exit 0 on context %s:\n%s", gkeContext, out)
			}
			if !strings.Contains(out, "kind-only") || !strings.Contains(out, gkeContext) {
				t.Errorf("no kind-only refusal naming %s:\n%s", gkeContext, out)
			}
			for l := range strings.SplitSeq(readCalls(t, calls), "\n") {
				if l != "" {
					t.Errorf("called %q before refusing", l)
				}
			}
		})
	}
	for _, tc := range []c{
		{"../chaos/scripts/kill-leader.sh", "CHAOS_CONTEXT"},
		{"../chaos/scripts/kill-follower.sh", "CHAOS_CONTEXT"},
		{"../chaos/scripts/stop-worker.sh", "CHAOS_CONTEXT"}, // kind context: allowed
	} {
		ctx := gkeContext
		if strings.Contains(tc.script, "stop-worker") {
			ctx = "kind-reliefgrid"
		}
		t.Run(filepath.Base(tc.script)+" on "+ctx, func(t *testing.T) {
			env, _ := gkeFakes(t)
			out, _ := runScript(t, "", append(env, tc.envVar+"="+ctx), gkeScript(t, tc.script), "--help")
			if strings.Contains(out, "kind-only") {
				t.Errorf("refused on %s:\n%s", ctx, out)
			}
		})
	}
}

// runIn is runScript with the working directory set.
func runIn(t *testing.T, dir string, env []string, script string, args ...string) (string, int) {
	t.Helper()
	cmd := exec.CommandContext(t.Context(), "bash", append([]string{script}, args...)...)
	cmd.Dir = dir
	cmd.Env = append(os.Environ(), env...)
	cmd.Stdin = strings.NewReader("")
	out, err := cmd.CombinedOutput()
	var ee *exec.ExitError
	if errors.As(err, &ee) {
		return string(out), ee.ExitCode()
	}
	if err != nil {
		t.Fatalf("run %s: %v\n%s", script, err, out)
	}
	return string(out), 0
}

func worktreeStatus(t *testing.T) string {
	t.Helper()
	out, err := exec.CommandContext(t.Context(), "git", "status", "--porcelain", "--untracked-files=all").Output()
	if err != nil {
		t.Skipf("git status: %v", err)
	}
	return string(out)
}

// --- scripts/image-digests.sh (CI's publish job) -----------------------------------------

func TestImageDigests(t *testing.T) {
	script := gkeScript(t, "image-digests.sh")
	write := func(m map[string]string) string {
		dir := t.TempDir()
		for k, v := range m {
			if err := os.WriteFile(filepath.Join(dir, k), []byte(v+"\n"), 0o644); err != nil {
				t.Fatal(err)
			}
		}
		return dir
	}
	all := map[string]string{}
	for _, n := range gkeImages {
		all[n] = gkeDigest(n)
	}
	commit := strings.Repeat("d", 40)
	out, code := runScript(t, "", nil, script, write(all), commit)
	if code != 0 {
		t.Fatalf("exit %d:\n%s", code, out)
	}
	var got struct {
		Commit string            `json:"commit"`
		Images map[string]string `json:"images"`
	}
	if err := json.Unmarshal([]byte(out), &got); err != nil {
		t.Fatalf("not JSON: %v\n%s", err, out)
	}
	if got.Commit != commit || len(got.Images) != len(gkeImages) {
		t.Errorf("got %+v", got)
	}
	for _, n := range gkeImages {
		if got.Images[n] != gkeDigest(n) {
			t.Errorf("%s: %q, want %q", n, got.Images[n], gkeDigest(n))
		}
	}
	missing := maps(all, func(m map[string]string) { delete(m, "tools") })
	if out, code := runScript(t, "", nil, script, write(missing), commit); code == 0 || !strings.Contains(out, "tools") {
		t.Errorf("a missing digest: exit %d, output %q", code, out)
	}
	bad := maps(all, func(m map[string]string) { m["ui"] = "sha256:xyz" })
	if out, code := runScript(t, "", nil, script, write(bad), commit); code == 0 || !strings.Contains(out, "ui") {
		t.Errorf("a malformed digest: exit %d, output %q", code, out)
	}
}

func maps(m map[string]string, edit func(map[string]string)) map[string]string {
	c := map[string]string{}
	for k, v := range m {
		c[k] = v
	}
	edit(c)
	return c
}

// --- deploy/helm/storage/values-gke.yaml --------------------------------------------------

func helmTemplate(t *testing.T, args ...string) ([]map[string]any, error) {
	t.Helper()
	helm, err := exec.LookPath("helm")
	if err != nil {
		if os.Getenv("REQUIRE_HELM") != "" {
			t.Fatalf("REQUIRE_HELM is set but helm is not on PATH: %v", err)
		}
		t.Skip("helm not on PATH")
	}
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, helm, append([]string{"template", "storage", "../deploy/helm/storage", "--namespace", "reliefgrid"}, args...)...)
	out, err := cmd.Output()
	if err != nil {
		var ee *exec.ExitError
		if errors.As(err, &ee) {
			return nil, errors.New(string(ee.Stderr))
		}
		return nil, err
	}
	var docs []map[string]any
	for doc := range strings.SplitSeq(string(out), "\n---\n") {
		var m map[string]any
		if err := yaml.Unmarshal([]byte(doc), &m); err != nil {
			t.Fatalf("render: %v", err)
		}
		if m != nil {
			docs = append(docs, m)
		}
	}
	return docs, nil
}

func gkeDigestArgs() []string {
	return []string{
		"-f", "../deploy/helm/storage/values-gke.yaml",
		"--set", "image.repository=" + gkeReg + "/storagenode",
		"--set", "image.digest=" + gkeDigest("storagenode"),
		"--set", "coordinator.image.repository=" + gkeReg + "/coordinator",
		"--set", "coordinator.image.digest=" + gkeDigest("coordinator"),
	}
}

func kindsNamed(docs []map[string]any, kind string) []map[string]any {
	var out []map[string]any
	for _, d := range docs {
		if d["kind"] == kind {
			out = append(out, d)
		}
	}
	return out
}

func TestStorageValuesGKE(t *testing.T) {
	docs, err := helmTemplate(t, gkeDigestArgs()...)
	if err != nil {
		t.Fatalf("helm template with values-gke.yaml: %v", err)
	}
	scs := kindsNamed(docs, "StorageClass")
	if len(scs) != 1 {
		t.Fatalf("%d StorageClasses rendered, want 1", len(scs))
	}
	sc := scs[0]
	for _, c := range []struct {
		got  any
		want any
		what string
	}{
		{dig(sc, "metadata", "name"), "reliefgrid-pd", "name"},
		{dig(sc, "provisioner"), "pd.csi.storage.gke.io", "provisioner"},
		{dig(sc, "parameters", "type"), "pd-balanced", "type"},
		{dig(sc, "volumeBindingMode"), "WaitForFirstConsumer", "volumeBindingMode"},
		{dig(sc, "reclaimPolicy"), "Delete", "reclaimPolicy (Retain keeps disks billing after uninstall)"},
		{dig(sc, "allowVolumeExpansion"), true, "allowVolumeExpansion"},
	} {
		if c.got != c.want {
			t.Errorf("StorageClass %s: %v, want %v", c.what, c.got, c.want)
		}
	}
	sts := kindsNamed(docs, "StatefulSet")
	if len(sts) != 3 {
		t.Fatalf("%d StatefulSets, want 3", len(sts))
	}
	for _, s := range sts {
		name := dig(s, "metadata", "name")
		vct := dig(s, "spec", "volumeClaimTemplates", 0)
		if got := dig(vct, "spec", "storageClassName"); got != "reliefgrid-pd" {
			t.Errorf("%s PVC class %v", name, got)
		}
		if got := dig(vct, "spec", "resources", "requests", "storage"); got != "10Gi" {
			t.Errorf("%s PVC size %v, want 10Gi (the PD minimum)", name, got)
		}
		pod := dig(s, "spec", "template", "spec")
		vols, _ := dig(pod, "volumes").([]any)
		for _, v := range vols {
			if ed, ok := dig(v, "emptyDir").(map[string]any); ok && ed["sizeLimit"] == nil {
				t.Errorf("%s emptyDir %v has no sizeLimit", name, dig(v, "name"))
			}
		}
		c := dig(pod, "containers", 0)
		if dig(c, "resources", "requests", "ephemeral-storage") == nil || dig(c, "resources", "limits", "ephemeral-storage") == nil {
			t.Errorf("%s has no ephemeral-storage request and limit", name)
		}
	}
	// Every image of ours by digest, pulled from the registry.
	for _, d := range docs {
		walkContainers(d, func(c map[string]any) {
			img, _ := c["image"].(string)
			if strings.HasPrefix(img, gkeReg) && !strings.Contains(img, "@sha256:") {
				t.Errorf("%s: %s not by digest", dig(d, "metadata", "name"), img)
			}
			if strings.HasPrefix(img, "reliefgrid/") {
				t.Errorf("%s: kind image %s on GKE", dig(d, "metadata", "name"), img)
			}
			if c["imagePullPolicy"] == "Never" {
				t.Errorf("%s: imagePullPolicy Never", dig(d, "metadata", "name"))
			}
		})
	}
	for _, s := range kindsNamed(docs, "Service") {
		if ty := dig(s, "spec", "type"); ty == "LoadBalancer" || ty == "NodePort" {
			t.Errorf("storage Service %v is %v; storage is never exposed", dig(s, "metadata", "name"), ty)
		}
	}
}

// gke-deploy.sh passes the operator token with --set-file, so a token file's
// trailing newline must not become part of the Bearer token.
func TestStorageOperatorTokenTrimmed(t *testing.T) {
	f := writeTemp(t, "operator-token", "tok3n\n")
	docs, err := helmTemplate(t, "--set-file", "operatorToken="+f)
	if err != nil {
		t.Fatal(err)
	}
	for _, s := range kindsNamed(docs, "Secret") {
		if dig(s, "metadata", "name") == "storage-operator-token" {
			if got := dig(s, "data", "token"); got != "dG9rM24=" { // base64("tok3n")
				t.Errorf("token %v, want base64 of the trimmed file", got)
			}
			return
		}
	}
	t.Fatal("no storage-operator-token Secret")
}

// values-gke.yaml without digests does not render: no GKE deploy by tag.
func TestStorageValuesGKERequiresDigests(t *testing.T) {
	_, err := helmTemplate(t, "-f", "../deploy/helm/storage/values-gke.yaml")
	if err == nil || !strings.Contains(err.Error(), "digest") {
		t.Errorf("rendered without digests (err %v)", err)
	}
}

// kind keeps its behaviour: no StorageClass, class standard, tag-referenced kind images.
func TestStorageValuesKindUnchanged(t *testing.T) {
	docs, err := helmTemplate(t)
	if err != nil {
		t.Fatal(err)
	}
	if n := len(kindsNamed(docs, "StorageClass")); n != 0 {
		t.Errorf("kind render has %d StorageClasses", n)
	}
	for _, s := range kindsNamed(docs, "StatefulSet") {
		if got := dig(s, "spec", "volumeClaimTemplates", 0, "spec", "storageClassName"); got != "standard" {
			t.Errorf("kind PVC class %v", got)
		}
	}
}

func walkContainers(v any, f func(map[string]any)) {
	switch x := v.(type) {
	case map[string]any:
		for k, child := range x {
			if k == "containers" || k == "initContainers" {
				for _, c := range child.([]any) {
					f(c.(map[string]any))
				}
				continue
			}
			walkContainers(child, f)
		}
	case []any:
		for _, child := range x {
			walkContainers(child, f)
		}
	}
}

// --- infra/gcp ----------------------------------------------------------------------------

const tfDir = "../infra/gcp"

// tfText is every .tf file of infra/gcp with whole-line comments removed.
func tfText(t *testing.T) string {
	t.Helper()
	files, _ := filepath.Glob(filepath.Join(tfDir, "*.tf"))
	if len(files) == 0 {
		t.Fatalf("no .tf files in %s", tfDir)
	}
	var sb strings.Builder
	for _, f := range files {
		b, err := os.ReadFile(f)
		if err != nil {
			t.Fatal(err)
		}
		for l := range strings.SplitSeq(string(b), "\n") {
			if tl := strings.TrimSpace(l); strings.HasPrefix(tl, "#") || strings.HasPrefix(tl, "//") {
				continue
			}
			sb.WriteString(l + "\n")
		}
	}
	return sb.String()
}

// tfBlocks returns the bodies of every `<kind> "<type>" ["<name>"] {` block of that type.
func tfBlocks(text, kind, typ string) map[string]string {
	head := `(?m)^` + kind + ` "` + regexp.QuoteMeta(typ) + `"(?: "([\w-]+)")? \{`
	if typ == "" { // terraform { ... }
		head = `(?m)^` + kind + `()? \{`
	}
	re := regexp.MustCompile(head)
	out := map[string]string{}
	for _, m := range re.FindAllStringSubmatchIndex(text, -1) {
		name := typ
		if m[2] >= 0 {
			name = text[m[2]:m[3]]
		}
		depth, i := 0, m[1]-1
		for ; i < len(text); i++ {
			if text[i] == '{' {
				depth++
			} else if text[i] == '}' {
				depth--
				if depth == 0 {
					break
				}
			}
		}
		out[name] = text[m[1]:i]
	}
	return out
}

func mustMatch(t *testing.T, what, body string, patterns ...string) {
	t.Helper()
	for _, p := range patterns {
		if !regexp.MustCompile(p).MatchString(body) {
			t.Errorf("%s: no match for %s", what, p)
		}
	}
}

func oneBlock(t *testing.T, text, kind, typ string) string {
	t.Helper()
	b := tfBlocks(text, kind, typ)
	if len(b) != 1 {
		t.Fatalf("%d %s %q blocks, want 1", len(b), kind, typ)
	}
	for _, v := range b {
		return v
	}
	return ""
}

func TestGCPTerraformInputs(t *testing.T) {
	text := tfText(t)
	// budget_amount and budget_currency too: the alert is in the billing account's currency
	// (likely INR), so both are set once D3 is settled; a default 25 could mean 25 rupees.
	for _, name := range []string{"project_id", "billing_account", "admin_cidrs", "alert_emails", "budget_amount", "budget_currency"} {
		b := oneBlock(t, text, "variable", name)
		if regexp.MustCompile(`(?m)^\s*default\s*=`).MatchString(b) {
			t.Errorf("variable %s has a default; it must be given explicitly (D3)", name)
		}
	}
	for name, want := range map[string]string{
		"region": `"asia-south1"`, "zone": `"asia-south1-a"`,
		"data_machine_type": `"e2-standard-2"`, "infra_machine_type": `"e2-standard-4"`,
	} {
		mustMatch(t, "variable "+name, oneBlock(t, text, "variable", name), `(?m)^\s*default\s*=\s*`+regexp.QuoteMeta(want)+`\s*$`)
	}
	mustMatch(t, "terraform", oneBlock(t, text, "terraform", ""), `backend "gcs"`, `required_version`, `hashicorp/google`)
	if _, err := os.Stat(filepath.Join(tfDir, ".terraform.lock.hcl")); err != nil {
		t.Errorf("provider lock file not committed: %v", err)
	}
}

func TestGCPTerraformCluster(t *testing.T) {
	text := tfText(t)
	c := oneBlock(t, text, "resource", "google_container_cluster")
	mustMatch(t, "cluster", c,
		`location\s*=\s*var\.zone`, // zonal
		`datapath_provider\s*=\s*"ADVANCED_DATAPATH"`,
		`initial_node_count\s*=\s*0`,      // no default pool, so no node with the default Compute SA
		`deletion_protection\s*=\s*false`, // or terraform destroy cannot remove it
		`workload_pool\s*=`,
		`master_authorized_networks_config`,
		`maintenance_exclusion`,
		`enable_components\s*=\s*\["SYSTEM_COMPONENTS"\]`,
		`dns_cache_config\s*\{\s*enabled\s*=\s*false`, // NodeLocal DNSCache breaks the coordinator's DNS egress policy (#40)
	)
	for _, bad := range []string{`enable_autopilot\s*=\s*true`, `CLOUD_DNS`, `enable_private_nodes\s*=\s*true`} {
		if regexp.MustCompile(bad).MatchString(c) {
			t.Errorf("cluster sets %s", bad)
		}
	}
	pools := tfBlocks(text, "resource", "google_container_node_pool")
	data, infra := pools["data"], pools["infra"]
	if data == "" || infra == "" {
		t.Fatalf("node pools %v, want data and infra", slices.Sorted(func(y func(string) bool) {
			for k := range pools {
				if !y(k) {
					return
				}
			}
		}))
	}
	common := []string{
		`disk_size_gb\s*=\s*30`, `disk_type\s*=\s*"pd-balanced"`,
		`service_account\s*=\s*google_service_account\.nodes\.email`,
		`mode\s*=\s*"GKE_METADATA"`,
		`auto_repair\s*=\s*true`,
	}
	mustMatch(t, "data pool", data, append(common, `node_count\s*=\s*3`, `machine_type\s*=\s*var\.data_machine_type`, `role\s*=\s*"data"`)...)
	mustMatch(t, "infra pool", infra, append(common, `node_count\s*=\s*1`, `machine_type\s*=\s*var\.infra_machine_type`, `role\s*=\s*"infra"`,
		`taint\s*\{[^}]*key\s*=\s*"role"[^}]*value\s*=\s*"infra"[^}]*effect\s*=\s*"NO_SCHEDULE"`)...)
	for name, p := range pools {
		if regexp.MustCompile(`(spot|preemptible)\s*=\s*true`).MatchString(p) {
			t.Errorf("pool %s uses Spot/preemptible VMs; a preemption is a fault we did not inject", name)
		}
	}
}

func TestGCPTerraformNetwork(t *testing.T) {
	text := tfText(t)
	mustMatch(t, "network", oneBlock(t, text, "resource", "google_compute_network"), `auto_create_subnetworks\s*=\s*false`)
	fws := tfBlocks(text, "resource", "google_compute_firewall")
	denies := 0
	for name, fw := range fws {
		open := regexp.MustCompile(`"0\.0\.0\.0/0"`).MatchString(fw)
		if open && regexp.MustCompile(`(?m)^\s*allow\s*\{`).MatchString(fw) {
			t.Errorf("firewall %s allows 0.0.0.0/0", name)
		}
		if open && regexp.MustCompile(`(?m)^\s*deny\s*\{`).MatchString(fw) {
			denies++
		}
	}
	if denies == 0 {
		t.Error("no explicit deny of ingress from 0.0.0.0/0 to the (public) nodes")
	}
	mustMatch(t, "ingress address", oneBlock(t, text, "resource", "google_compute_address"), `region\s*=\s*var\.region`)
}

func TestGCPTerraformIdentity(t *testing.T) {
	text := tfText(t)
	for _, bad := range []string{`google_service_account_key`, `"roles/editor"`, `"roles/owner"`, `credentials_json`} {
		if strings.Contains(text, bad) {
			t.Errorf("infra/gcp uses %s", bad)
		}
	}
	mustMatch(t, "registry", oneBlock(t, text, "resource", "google_artifact_registry_repository"), `format\s*=\s*"DOCKER"`, `location\s*=\s*var\.region`)
	ar := tfBlocks(text, "resource", "google_artifact_registry_repository_iam_member")
	var reader, writer bool
	for _, b := range ar {
		reader = reader || (strings.Contains(b, `"roles/artifactregistry.reader"`) && strings.Contains(b, "google_service_account.nodes"))
		writer = writer || (strings.Contains(b, `"roles/artifactregistry.writer"`) && strings.Contains(b, "google_service_account.ci"))
	}
	if !reader || !writer {
		t.Errorf("registry IAM: nodes reader %v, CI writer %v (both repo-scoped)", reader, writer)
	}
	for name, b := range tfBlocks(text, "resource", "google_project_iam_member") {
		if strings.Contains(b, "google_service_account.ci") {
			t.Errorf("project-level role %s for the CI identity; it gets the registry only", name)
		}
	}
	p := oneBlock(t, text, "resource", "google_iam_workload_identity_pool_provider")
	mustMatch(t, "WIF provider", p,
		`issuer_uri\s*=\s*"https://token\.actions\.githubusercontent\.com"`,
		`attribute_condition\s*=.*assertion\.repository\s*==`,
		`attribute_condition\s*=.*assertion\.ref\s*==\s*'refs/heads/main'`,
	)
	bind := tfBlocks(text, "resource", "google_service_account_iam_member")
	ok := false
	for _, b := range bind {
		ok = ok || (strings.Contains(b, `"roles/iam.workloadIdentityUser"`) && strings.Contains(b, "principalSet://") && strings.Contains(b, "google_service_account.ci"))
	}
	if !ok {
		t.Error("no workloadIdentityUser binding of the GitHub principal set to the CI service account")
	}
}

func TestGCPTerraformBudget(t *testing.T) {
	b := oneBlock(t, tfText(t), "resource", "google_billing_budget")
	mustMatch(t, "budget", b,
		`billing_account\s*=\s*var\.billing_account`,
		`var\.budget_amount`,
		`threshold_percent\s*=\s*0\.5`, `threshold_percent\s*=\s*0\.9`, `threshold_percent\s*=\s*1(\.0)?\b`,
		`projects\s*=`,
		`monitoring_notification_channels`,
	)
}

func TestGCPTerraformOutputs(t *testing.T) {
	text := tfText(t)
	for _, o := range []string{"project_id", "region", "zone", "cluster_name", "kube_context", "registry", "ingress_ip", "wif_provider", "ci_service_account"} {
		_ = oneBlock(t, text, "output", o)
	}
	mustMatch(t, "kube_context", oneBlock(t, text, "output", "kube_context"), `"gke_\$\{var\.project_id\}_\$\{var\.zone\}_\$\{`)
}

// terraform fmt and validate: no backend, no credentials, but terraform init asks the
// registry for the pinned provider on every run. So it runs only with REQUIRE_TERRAFORM
// set (CI's terraform job, make gcp-validate), keeping make check off the network.
func TestGCPTerraformValidate(t *testing.T) {
	if os.Getenv("REQUIRE_TERRAFORM") == "" {
		t.Skip("set REQUIRE_TERRAFORM=1 (make gcp-validate) to run terraform fmt and validate")
	}
	tf, err := exec.LookPath("terraform")
	if err != nil {
		t.Fatalf("REQUIRE_TERRAFORM is set but terraform is not on PATH: %v", err)
	}
	cache, err := os.UserCacheDir()
	if err != nil {
		t.Fatal(err)
	}
	plugins := filepath.Join(cache, "reliefgrid", "terraform-plugins")
	if err := os.MkdirAll(plugins, 0o755); err != nil {
		t.Fatal(err)
	}
	env := append(os.Environ(), "TF_DATA_DIR="+t.TempDir(), "TF_PLUGIN_CACHE_DIR="+plugins, "TF_IN_AUTOMATION=1", "CHECKPOINT_DISABLE=1")
	run := func(args ...string) {
		t.Helper()
		ctx, cancel := context.WithTimeout(t.Context(), 3*time.Minute)
		defer cancel()
		cmd := exec.CommandContext(ctx, tf, append([]string{"-chdir=" + tfDir}, args...)...)
		cmd.Env = env
		if out, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("terraform %s: %v\n%s", strings.Join(args, " "), err, out)
		}
	}
	run("fmt", "-check", "-diff", "-recursive")
	run("init", "-backend=false", "-input=false", "-lockfile=readonly")
	run("validate", "-no-color")
}

// make gke-deploy, gke-teardown and gcp-validate run the scripts and the offline checks.
func TestGKEMakeTargets(t *testing.T) {
	for target, want := range map[string]string{
		"gke-deploy":   "scripts/gke-deploy.sh",
		"gke-teardown": "scripts/gke-teardown.sh",
		"gcp-validate": "TestGCPTerraform",
	} {
		if out := makeDryRun(t, target); !strings.Contains(out, want) {
			t.Errorf("make -n %s does not run %s:\n%s", target, want, out)
		}
	}
}
