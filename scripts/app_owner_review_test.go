package scripts

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

func TestAppDemoResetUsesOneStorageOwnerAndAppDependencyBuild(t *testing.T) {
	root, bin := t.TempDir(), t.TempDir()
	for _, name := range []string{"demos/reset.sh", "demos/common.sh", "port-forwards.sh"} {
		body, err := os.ReadFile(name)
		if err != nil {
			t.Fatal(err)
		}
		dest := filepath.Join(root, "scripts", name)
		if err := os.MkdirAll(filepath.Dir(dest), 0755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(dest, body, 0644); err != nil {
			t.Fatal(err)
		}
	}
	chart := filepath.Join(root, "deploy/helm/reliefgrid")
	if err := os.MkdirAll(chart, 0755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(chart, "Chart.yaml"), []byte("name: test\n"), 0644); err != nil {
		t.Fatal(err)
	}
	if out, err := exec.CommandContext(t.Context(), "git", "init", root).CombinedOutput(); err != nil {
		t.Fatalf("git init: %s %v", out, err)
	}
	fake := `#!/usr/bin/env bash
tool="$(basename "$0")"
echo "$tool $* app_storage=${APP_WITH_STORAGE:-unset}" >> "$APP_REVIEW_CALLS"
case "$tool $*" in
 "helm "*"deploy/helm/reliefgrid"*) echo "missing chart dependencies" >&2;exit 17 ;;
 "kubectl "*"get nodes -o json"*) echo '{"items":[]}' ;;
 "kubectl "*"get secret"*) printf dG9rZW4= ;;
 "curl "*) printf '{"nodes":['; for i in $(seq 9);do [ "$i" = 1 ] || printf ',';printf '{"state":"UP"}';done;printf ']}' ;;
esac
`
	for _, tool := range []string{"make", "helm", "kubectl", "curl"} {
		if err := os.WriteFile(filepath.Join(bin, tool), []byte(fake), 0755); err != nil {
			t.Fatal(err)
		}
	}
	calls := filepath.Join(root, "calls")
	cmd := exec.CommandContext(t.Context(), "bash", filepath.Join(root, "scripts/demos/reset.sh"))
	cmd.Env = append(os.Environ(), "PATH="+bin+":"+os.Getenv("PATH"), "RESET_CONFIRM=owner-test", "CLUSTER=owner-test", "APP_REVIEW_CALLS="+calls)
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("reset from unpackaged checkout: %v\n%s", err, out)
	}
	data, err := os.ReadFile(calls)
	if err != nil {
		t.Fatal(err)
	}
	text := string(data)
	storage := strings.Index(text, "helm --kube-context kind-owner-test install storage deploy/helm/storage")
	app := strings.Index(text, "make -s app-deploy app_storage=false")
	if storage < 0 || app <= storage || strings.Count(text, "install storage deploy/helm/storage") != 1 || strings.Contains(text, "install reliefgrid deploy/helm/reliefgrid") {
		t.Fatalf("reset must install storage once, then delegate app/dependencies: %s", text)
	}
}

func TestAppDeployStorageOwnershipModes(t *testing.T) {
	for _, mode := range []string{"standalone", "embedded", "already-owned", "missing-service"} {
		t.Run(mode, func(t *testing.T) {
			root, bin := t.TempDir(), t.TempDir()
			if err := os.MkdirAll(filepath.Join(root, "scripts"), 0755); err != nil {
				t.Fatal(err)
			}
			src, err := os.ReadFile("app-deploy.sh")
			if err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(root, "scripts/app-deploy.sh"), src, 0644); err != nil {
				t.Fatal(err)
			}
			helper, err := os.ReadFile("app_evidence.py")
			if err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(root, "scripts/app_evidence.py"), helper, 0644); err != nil {
				t.Fatal(err)
			}
			for _, name := range []string{"app-chart-deps.sh", "placement-check.sh"} {
				if err := os.WriteFile(filepath.Join(root, "scripts", name), []byte("#!/usr/bin/env bash\necho dependency-or-placement >> \"$APP_REVIEW_CALLS\"\n"), 0755); err != nil {
					t.Fatal(err)
				}
			}
			fake := `#!/usr/bin/env bash
tool="$(basename "$0")"
args=" $* "
echo "$tool $*" >> "$APP_REVIEW_CALLS"
case "$tool $*" in
 "kubectl "*"get nodes -o json"*)
 python3 - <<'PY'
import json
print(json.dumps({'items':[{'metadata':{'labels':{'role':role}},'status':{'conditions':[{'type':'Ready','status':'True'}]}} for role in ['data','data','data','infra']]}))
PY
 ;;
 "kubectl "*"get service storage-coordinator"*) [ "$APP_REVIEW_MODE" != missing-service ] || exit 17 ;;
 "kubectl "*"get pvc "*) printf fixture-pvc ;;
 "kubectl "*" exec "*)
 state=Follower
 [[ "$args" == *"-0 -c storagenode"* ]] && state=Leader
 printf '{"serverId":"fixture","state":"%s","leaderId":"leader","appliedIndex":1}\n' "$state" ;;
 "helm "*" list "*) if [ "$APP_REVIEW_MODE" = already-owned ];then echo '[{"name":"reliefgrid"}]';else echo '[]';fi ;;
 "helm "*" get values "*) echo '{"storage":{"enabled":true}}' ;;
 "helm "*" upgrade "*) [[ "$args" != *" + "* ]] || exit 17 ;;
 "docker image inspect"*) echo sha256:fixture ;;
esac
`
			for _, name := range []string{"kubectl", "helm", "docker", "kind", "make"} {
				if err := os.WriteFile(filepath.Join(bin, name), []byte(fake), 0755); err != nil {
					t.Fatal(err)
				}
			}
			calls := filepath.Join(root, "calls")
			cmd := exec.CommandContext(t.Context(), "bash", filepath.Join(root, "scripts/app-deploy.sh"))
			withStorage := "false"
			if mode == "embedded" {
				withStorage = "true"
			}
			cmd.Env = append(os.Environ(), "PATH="+bin+":"+os.Getenv("PATH"), "APP_REVIEW_CALLS="+calls, "APP_REVIEW_MODE="+mode, "APP_WITH_STORAGE="+withStorage, "APP_IMAGE_TAG=owner-test", "XDG_CACHE_HOME="+t.TempDir())
			out, err := cmd.CombinedOutput()
			wantSuccess := mode == "standalone" || mode == "embedded"
			if (err == nil) != wantSuccess {
				t.Fatalf("mode=%s err=%v\n%s", mode, err, out)
			}
			data, readErr := os.ReadFile(calls)
			if readErr != nil {
				t.Fatal(readErr)
			}
			text := string(data)
			if !wantSuccess {
				if strings.Contains(text, "upgrade --install") || strings.Contains(text, "docker ") || strings.Contains(text, "app-images") {
					t.Fatalf("unsafe deployment proceeded: %s", text)
				}
				return
			}
			if !strings.Contains(text, "--set storage.enabled="+withStorage) {
				t.Fatalf("missing explicit storage ownership: %s", text)
			}
			storageBuild := strings.Contains(text, "-f cmd/storagenode/Dockerfile")
			if storageBuild != (mode == "embedded") {
				t.Fatalf("wrong storage image ownership: %s", text)
			}
			dependency := strings.Index(text, "dependency-or-placement")
			install := strings.Index(text, "upgrade --install")
			if dependency < 0 || dependency >= install {
				t.Fatalf("install ran before dependency preparation: %s", text)
			}
		})
	}
}
