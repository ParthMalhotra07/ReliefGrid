package scripts

import (
	"os/exec"
	"strings"
	"testing"
)

// loadtest/run.sh confines the kind nodes to KIND_CPUS and sets the power
// profile for a run; its EXIT trap, restore(), must put both back whatever
// ended the run. This runs restore() alone, under set -e with an ERR trap,
// with fake docker and powerprofilesctl, after the sampler has already
// exited (kill fails): the case that once left the laptop in performance mode.
func TestLoadRunRestoreAlwaysCompletes(t *testing.T) {
	script := `set -euo pipefail
eval "$(sed -n '/^restore() {/,/^}/p' ../loadtest/run.sh)"
log() { echo "log: $*"; }
exec 3>&1
docker() { echo "docker $*" >&3; return 1; }   # fd 3: restore() discards stdout; a failing docker must not stop it
powerprofilesctl() { if [ "$1" = get ]; then echo performance; else echo "powerprofilesctl $*"; fi; }
nproc() { echo 8; }
NODES=(n1 n2); declare -A CPUSET_BEFORE=([n1]="" [n2]="2-3"); POWER_BEFORE=balanced
sleep 0 & sampler_pid=$!; wait "$sampler_pid"   # already exited
trap 'echo "ERR trap fired"' ERR
restore
echo "restore returned"
`
	out, err := exec.CommandContext(t.Context(), "bash", "-c", script).CombinedOutput()
	got := string(out)
	if err != nil {
		t.Fatalf("restore() ended the shell: %v\n%s", err, got)
	}
	for _, want := range []string{
		"docker update --cpuset-cpus 0-7 n1", // empty cpuset = every CPU, spelled out
		"docker update --cpuset-cpus 2-3 n2",
		"powerprofilesctl set balanced",
		"restore returned",
	} {
		if !strings.Contains(got, want) {
			t.Errorf("missing %q in:\n%s", want, got)
		}
	}
	if strings.Contains(got, "ERR trap fired") {
		t.Errorf("restore() tripped the ERR trap:\n%s", got)
	}
}
