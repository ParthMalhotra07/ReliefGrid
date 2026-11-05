package scripts

import (
	"os"
	"slices"
	"strings"
	"testing"

	"go.yaml.in/yaml/v3"
)

// S5.1: CI publishes the six images to GHCR on main (never on a PR), by digest,
// and records the digests in image-digests.json. When a GKE session is up (the
// repository variable GCP_WORKLOAD_IDENTITY_PROVIDER is set), it also copies each
// image by digest into the session's Artifact Registry, through Workload Identity
// Federation: no service-account key. CI does not deploy (D7).

type publishJob struct {
	If          string            `yaml:"if"`
	Needs       any               `yaml:"needs"`
	Timeout     int               `yaml:"timeout-minutes"`
	Permissions map[string]string `yaml:"permissions"`
	Strategy    struct {
		Matrix struct {
			Include []map[string]string `yaml:"include"`
		} `yaml:"matrix"`
	} `yaml:"strategy"`
	Steps []step `yaml:"steps"`
}

func loadPublish(t *testing.T) (map[string]any, map[string]publishJob) {
	t.Helper()
	b, err := os.ReadFile("../.github/workflows/ci.yml")
	if err != nil {
		t.Fatal(err)
	}
	var w struct {
		On          map[string]any        `yaml:"on"`
		Permissions map[string]string     `yaml:"permissions"`
		Jobs        map[string]publishJob `yaml:"jobs"`
	}
	if err := yaml.Unmarshal(b, &w); err != nil {
		t.Fatal(err)
	}
	if w.Permissions["contents"] != "read" || len(w.Permissions) != 1 {
		t.Errorf("workflow default permissions %v, want only contents: read", w.Permissions)
	}
	return w.On, w.Jobs
}

func onlyOnMain(t *testing.T, name string, j publishJob) {
	t.Helper()
	if strings.Contains(j.If, "pull_request") || !strings.Contains(j.If, "github.ref == 'refs/heads/main'") {
		t.Errorf("%s if: %q; it must run only for main and never for a PR", name, j.If)
	}
}

func TestCIPublishJob(t *testing.T) {
	on, jobs := loadPublish(t)
	if _, ok := on["workflow_dispatch"]; !ok {
		t.Error("no workflow_dispatch: a GKE session re-runs publish on main after terraform apply")
	}
	j, ok := jobs["publish"]
	if !ok {
		t.Fatal("ci.yml has no publish job")
	}
	onlyOnMain(t, "publish", j)
	if !slices.Contains(jobNeeds(j.Needs), "check") {
		t.Errorf("publish needs %v; publish only what passed check", j.Needs)
	}
	if j.Timeout == 0 || j.Timeout > 30 {
		t.Errorf("publish timeout-minutes = %d, want 1..30", j.Timeout)
	}
	for k, want := range map[string]string{"contents": "read", "packages": "write", "id-token": "write"} {
		if j.Permissions[k] != want {
			t.Errorf("publish permissions %s = %q, want %q", k, j.Permissions[k], want)
		}
	}
	got := map[string]string{}
	for _, m := range j.Strategy.Matrix.Include {
		got[m["name"]] = m["dockerfile"]
	}
	for _, n := range gkeImages {
		df, ok := got[n]
		if !ok {
			t.Errorf("publish matrix lacks %s", n)
			continue
		}
		if _, err := os.Stat("../" + df); err != nil {
			t.Errorf("%s dockerfile %s: %v", n, df, err)
		}
	}
	text := func(s step) string { b, _ := yaml.Marshal(s); return string(b) }
	build := slices.IndexFunc(j.Steps, func(s step) bool { return strings.HasPrefix(s.Uses, "docker/build-push-action@") })
	if build < 0 {
		t.Fatal("publish has no docker/build-push-action step")
	}
	bs := j.Steps[build]
	if bs.With["push"] != true || bs.With["provenance"] != false {
		t.Errorf("build step with push=%v provenance=%v, want true/false (one digest per image)", bs.With["push"], bs.With["provenance"])
	}
	if tags, _ := bs.With["tags"].(string); !strings.Contains(tags, "ghcr.io/") || !strings.Contains(tags, "${{ github.sha }}") {
		t.Errorf("tags %q: want ghcr.io/...:${{ github.sha }}", tags)
	}
	if !slices.ContainsFunc(j.Steps, func(s step) bool {
		return strings.HasPrefix(s.Uses, "docker/login-action@") && strings.Contains(text(s), "ghcr.io")
	}) {
		t.Error("publish does not log in to ghcr.io")
	}
	if !slices.ContainsFunc(j.Steps, func(s step) bool {
		return strings.Contains(s.Run, "outputs.digest") || strings.Contains(text(s), "outputs.digest")
	}) {
		t.Error("publish does not record build-push-action's digest output")
	}
	if !slices.ContainsFunc(j.Steps, func(s step) bool {
		return strings.HasPrefix(s.Uses, "actions/upload-artifact@") && strings.Contains(text(s), "digest-")
	}) {
		t.Error("publish does not upload its digest-<name> artifact")
	}
	// The copy into Artifact Registry: keyless, by digest, only while a session is up.
	auth := slices.IndexFunc(j.Steps, func(s step) bool { return strings.HasPrefix(s.Uses, "google-github-actions/auth@") })
	if auth < 0 {
		t.Fatal("publish has no google-github-actions/auth step (WIF)")
	}
	as := j.Steps[auth]
	if _, ok := as.With["workload_identity_provider"]; !ok || as.With["credentials_json"] != nil {
		t.Errorf("auth with %v: want workload_identity_provider and no credentials_json", as.With)
	}
	if !strings.Contains(as.If, "vars.GCP_WORKLOAD_IDENTITY_PROVIDER") {
		t.Errorf("auth if: %q; skip it when no GKE session is up", as.If)
	}
	if !slices.ContainsFunc(j.Steps[auth:], func(s step) bool {
		return strings.Contains(s.Run, "crane copy") && strings.Contains(s.Run, "@") && strings.Contains(s.If, "vars.GCP_WORKLOAD_IDENTITY_PROVIDER")
	}) {
		t.Error("no crane copy by digest into Artifact Registry after auth")
	}

	d, ok := jobs["digests"]
	if !ok {
		t.Fatal("ci.yml has no digests job (merges the digests into image-digests.json)")
	}
	onlyOnMain(t, "digests", d)
	if !slices.Contains(jobNeeds(d.Needs), "publish") {
		t.Errorf("digests needs %v; it must need publish", d.Needs)
	}
	if !slices.ContainsFunc(d.Steps, func(s step) bool { return strings.Contains(s.Run, "scripts/image-digests.sh") }) {
		t.Error("digests does not run scripts/image-digests.sh")
	}
	if !slices.ContainsFunc(d.Steps, func(s step) bool {
		return strings.HasPrefix(s.Uses, "actions/upload-artifact@") && strings.Contains(text(s), "image-digests")
	}) {
		t.Error("digests does not upload image-digests.json")
	}
	// The PR-only jobs stay PR-only.
	if strings.Contains(jobs["images"].If, "push") {
		t.Error("the PR images job now runs on push")
	}
}

func jobNeeds(v any) []string {
	switch n := v.(type) {
	case string:
		return []string{n}
	case []any:
		var out []string
		for _, s := range n {
			out = append(out, s.(string))
		}
		return out
	}
	return nil
}
