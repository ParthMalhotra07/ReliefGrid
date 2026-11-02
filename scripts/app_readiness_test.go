package scripts

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestAppPreflightWaitsForOneHealthySnapshot(t *testing.T) {
	for _, fixture := range []struct {
		name, preflight, budget, want string
		code                          int
	}{
		{"recovers", `calls=$((calls+1)); if [ "$calls" -lt 3 ];then PREFLIGHT_WHY='not all nine storage pods are Ready';return 1;fi;COORD_PORT=1234;return 0`, "5", "calls=3 coord=1234", 0},
		{"persistent-unready", `calls=$((calls+1));PREFLIGHT_WHY='not all nine storage pods are Ready';return 1`, "0", "calls=1 coord=", 1},
		{"other-failure", `calls=$((calls+1));PREFLIGHT_WHY='shard 1 has no single agreed leader';return 1`, "5", "calls=1 coord=", 1},
	} {
		t.Run(fixture.name, func(t *testing.T) {
			root := t.TempDir()
			source, err := os.ReadFile("demos/app.sh")
			if err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(root, "app.sh"), source, 0644); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(root, "kube.sh"), nil, 0644); err != nil {
				t.Fatal(err)
			}
			body := "set -u\n. \"" + filepath.Join(root, "app.sh") + "\"\ncalls=0;COORD_PORT=''\npreflight(){ " + fixture.preflight + "; }\nawait_app_preflight " + fixture.budget + "\nresult=$?\necho \"calls=$calls coord=$COORD_PORT\"\nexit $result\n"
			path := filepath.Join(root, "run.sh")
			if err := os.WriteFile(path, []byte(body), 0755); err != nil {
				t.Fatal(err)
			}
			out, code := runScript(t, "", nil, path)
			if code != fixture.code || !strings.Contains(out, fixture.want) {
				t.Fatalf("exit=%d want=%d output=%s", code, fixture.code, out)
			}
		})
	}
}
