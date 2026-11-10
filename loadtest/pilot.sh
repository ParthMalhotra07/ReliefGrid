#!/usr/bin/env bash
# Authorized pilot only. See loadtest/PILOT.md.
set -euo pipefail
: "${AUTHORIZED_CLUSTER:?separately authorized fresh cluster name}"
: "${OUT:?absolute new evidence directory outside this repository}"
: "${K6:?absolute checksum-verified k6 binary}"
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"
export CLUSTER="$AUTHORIZED_CLUSTER"
# This branch's Makefile cluster-up uses a fixed-name kind configuration.
# A custom cluster name needs a separately reviewed creation path.
test "$CLUSTER" = reliefgrid
[[ "$CLUSTER" =~ ^[a-z0-9][a-z0-9-]*$ ]]
[[ "$OUT" = /* && "$OUT" != "$ROOT" && "$OUT" != "$ROOT/"* ]]
[[ "$K6" = /* ]]
test -z "$(git status --porcelain --untracked-files=no)"
grep -Eq '^K6_VERSION *:= *v2.3.0$' tools.mk
command -v systemd-run >/dev/null || { echo "PREFLIGHT_FAILED: systemd-run unavailable" >&2; exit 7; }
systemctl --user show-environment >/dev/null
test -f /sys/fs/cgroup/cgroup.controllers
mkdir "$OUT"
# shellcheck source=loadtest/pilot-supervision.sh
source "$ROOT/loadtest/pilot-supervision.sh"
trap 'rc=$?; if ((rc)); then echo "STOPPED ($rc): preserve cluster/evidence. OUT=$OUT" >&2; fi' EXIT

host_snapshot() {
  local power online
  power="$(powerprofilesctl get)" || return 1
  online="$(cat /sys/devices/system/cpu/online)" || return 1
  docker inspect "${owned_nodes[@]}" |
    jq --arg power "$power" --arg online "$online" \
      '{power:$power,online:$online,nodes:([.[] | {Id,cpus:.HostConfig.CpusetCpus}] | sort_by(.Id))}'
}

pilot_capture_restoration() {
  if [ ! -f "$OUT/host-before.json" ] || ! declare -p owned_nodes >/dev/null 2>&1; then
    echo "RESTORATION_UNVERIFIED: no complete before snapshot; preserve cluster/evidence in $OUT" >&2
    return 8
  fi
  if ! host_snapshot > "$OUT/host-after.json"; then
    echo "RESTORATION_UNVERIFIED: after snapshot failed; preserve cluster/evidence in $OUT" >&2
    return 8
  fi
  if ! python3 - "$OUT/host-before.json" "$OUT/host-after.json" <<'PYHOST'
import json, sys
a, b = [json.load(open(p)) for p in sys.argv[1:]]
def cpus(spec):
    out = set()
    for part in spec.split(","):
        lo, _, hi = part.partition("-")
        out.update(range(int(lo), int(hi or lo) + 1))
    return out
assert a["power"] == b["power"], "power profile not restored"
assert cpus(a["online"]) == cpus(b["online"]), "online CPUs changed"
assert [n["Id"] for n in a["nodes"]] == [n["Id"] for n in b["nodes"]]
for x, y in zip(a["nodes"], b["nodes"]):
    assert cpus(x["cpus"] or a["online"]) == cpus(y["cpus"] or b["online"]), x["Id"]
PYHOST
  then
    echo "RESTORATION_UNVERIFIED: CPU/power comparison failed; preserve cluster/evidence in $OUT" >&2
    return 8
  fi
  echo "HOST_RESTORATION_CONFIRMED: $OUT/host-after.json" >&2
}

# Inspection alone is insufficient: use the exact stage launch/stop options.
# A failed or unconfirmed probe stops here, before any cluster creation.
supervise preflight /bin/true
test "$service_rc" = 0 || { echo "PREFLIGHT_FAILED: transient launch/reap" >&2; exit 7; }

# Includes tracked, untracked and ignored entries, modes and symlink targets.
# Snapshot/verification data stays outside A1.5; no snapshot file is left there.
a15_digest() {
  tar --sort=name --mtime=@0 --owner=0 --group=0 --numeric-owner \
    -cf - docs/evidence/A1.5 | sha256sum | cut -d' ' -f1
}
a15_before="$(a15_digest)"
printf '%s\n' "$a15_before" > "$OUT/a15-before.sha256"

clusters="$(kind get clusters)"
if grep -Fxq "$CLUSTER" <<<"$clusters"; then
  echo "Refusing pre-existing authorized target" >&2; exit 1
fi
# No ownership is armed for a partial/failed creation.
make cluster-up CLUSTER="$CLUSTER"
nodes="$(kind get nodes --name "$CLUSTER")"
test -n "$nodes"
mapfile -t owned_nodes <<<"$nodes"
docker inspect "${owned_nodes[@]}" | jq -S '[.[] | {Id,Name}] | sort_by(.Id)' > "$OUT/owned-nodes.json"
owned=true

owned_now() {
  local current
  current="$(kind get nodes --name "$CLUSTER")" || return 1
  test -n "$current" || return 1
  mapfile -t current_nodes <<<"$current"
  docker inspect "${current_nodes[@]}" | jq -S '[.[] | {Id,Name}] | sort_by(.Id)' > "$OUT/current-nodes.json"
  cmp "$OUT/owned-nodes.json" "$OUT/current-nodes.json"
}

supervise deploy bash loadtest/cluster.sh
test "$service_rc" = 0

host_snapshot > "$OUT/host-before.json"
supervise smoke bash loadtest/run.sh smoke
# Capture even when service_rc is nonzero, before testing acceptance or deletion.
pilot_capture_restoration
test "$service_rc" = 0
jq -e '.runnerExit == 0 and .k6Exit == 0 and .reportExit == 0 and
       .teeExit == 0 and .safetyFailure == false' "$OUT/smoke.runner-status.json"
python3 -I scripts/load-report-check.py "$OUT/smoke"
jq -e '.ok == true and .verdict == "clean" and
       (has("incomplete") | not) and .report.quiescent == true' "$OUT/smoke/raw/invcheck.json"
test "$(a15_digest)" = "$a15_before"
printf 'SMOKE_ACCEPTED\n' | tee "$OUT/acceptance.txt"

# Recheck identity immediately before deletion; no name-only teardown.
test "$owned" = true
owned_now
make cluster-down CLUSTER="$CLUSTER"
a15_after="$(a15_digest)"
printf '%s\n' "$a15_after" > "$OUT/a15-after.sha256"
test "$a15_after" = "$a15_before"
printf 'A1.5 contents verified; CPU and power restoration verified before teardown\n'

if [ "${PILOT_CALIBRATE:-0}" = 1 ]; then
test "$(cat "$OUT/acceptance.txt")" = SMOKE_ACCEPTED
OUT="$OUT/calibration"
mkdir "$OUT"
clusters="$(kind get clusters)"
if grep -Fxq "$CLUSTER" <<<"$clusters"; then exit 1; fi
make cluster-up CLUSTER="$CLUSTER"
nodes="$(kind get nodes --name "$CLUSTER")"
test -n "$nodes"
mapfile -t owned_nodes <<<"$nodes"
docker inspect "${owned_nodes[@]}" | jq -S '[.[] | {Id,Name}] | sort_by(.Id)' > "$OUT/owned-nodes.json"
owned=true
supervise deploy bash loadtest/cluster.sh
test "$service_rc" = 0
host_snapshot > "$OUT/host-before.json"
supervise step /usr/bin/env START_RPS=10 STEP_RPS=10 STEPS=8 RAMP_S=10 HOLD_S=110   bash loadtest/run.sh step
pilot_capture_restoration
jq -e '.safetyFailure == false and .teeExit == 0 and
       (.k6Exit == 0 or .k6Exit == 99)' "$OUT/step.runner-status.json"
jq -e '.ok == true and .verdict == "clean" and
       (has("incomplete") | not) and .report.quiescent == true' "$OUT/step/raw/invcheck.json"
if python3 -I scripts/load-report-check.py "$OUT/step" --require-saturation; then
  check_rc=0
else
  check_rc=$?
fi
if (( check_rc == 3 )); then
  test "$service_rc" = 3
  jq -e '.runnerExit == 3 and .reportExit == 3' "$OUT/step.runner-status.json"
  echo "CALIBRATION_INCONCLUSIVE: preserve diagnostics; revise rates separately"
  exit 3
fi
test "$check_rc" = 0
test "$service_rc" = 0
jq -e '.runnerExit == 0 and .reportExit == 0' "$OUT/step.runner-status.json"
test "$(a15_digest)" = "$a15_before"
test "$owned" = true
owned_now
make cluster-down CLUSTER="$CLUSTER"
test "$(a15_digest)" = "$a15_before"
echo "CALIBRATION_ACCEPTED"

fi
