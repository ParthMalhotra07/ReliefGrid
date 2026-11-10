#!/usr/bin/env bash
# One S5.2 load run on kind, from dataset to checked report.
#
#   loadtest/run.sh smoke
#   loadtest/run.sh step       START_RPS=10 STEP_RPS=10 STEPS=8 RAMP_S=10 HOLD_S=110
#   loadtest/run.sh baseline   RATE=<req/s> DURATION_S=600 WARMUP_SECONDS=120
#   loadtest/run.sh soak       RATE=<req/s> DURATION_S=1800 WARMUP_SECONDS=120
# Common: SEED=52 K6=k6 K6_CPUS=6-7 KIND_CPUS=0-5 MAX_VUS=400. The dataset is
# sized to the offered operations (OPS overrides).
#
# 1. Records the facts a figure depends on: host (CPU, RAM, kernel, power
#    profile, swap, load, clock speed), versions (k6, kind, kubectl, helm,
#    Go, node image, every deployed image's digest) and configuration (Raft
#    profile, read fence, tracing, autoscaling, replicas).
# 2. Generates the seeded dataset (loadtest/cmd/gendata).
# 3. Scrapes /metrics of every storage node and the coordinator (through
#    kubectl port-forward), and samples every pod's cgroup on its kind node
#    every SAMPLE_SECONDS while k6 runs.
# 4. Runs the scenario with k6 pinned to K6_CPUS (taskset).
# 5. Scrapes again, lets matching settle (SETTLE_SECONDS), reads back every
#    request the run created (loadreport statuses), and runs invcheck check
#    as a Job until the outbox has drained.
# 6. Builds docs/evidence/S5.2/<runId>/summary.json and SHA256SUMS
#    (loadreport build; raw files over 5 MiB go to SPILL_DIR) and checks it
#    with scripts/load-report-check.py.
# Isolation (decided 2026-11-06): for the run, the kind node containers are
# confined to KIND_CPUS (docker update --cpuset-cpus, default 0-5), k6 runs
# on K6_CPUS (default 6-7) and the power profile is performance; all three
# are recorded in the report. One EXIT trap restores the nodes' cpusets and
# the previous power profile, whether the run passes, fails or is
# interrupted. Deploy with loadtest/cluster.sh (Guaranteed pods).
# Kind only: it refuses any other context. It changes nothing on the cluster
# but one invcheck Job, deleted afterwards.
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"
# shellcheck source=scripts/port-forwards.sh
source scripts/port-forwards.sh

SCENARIO="${1:?usage: loadtest/run.sh <scenario: smoke>}"
SCRIPT="loadtest/k6/$SCENARIO.js"
[ -f "$SCRIPT" ] || { echo "run: no scenario $SCRIPT" >&2; exit 2; }
CLUSTER="${CLUSTER:-reliefgrid}"
CTX="kind-$CLUSTER"
NS="${NAMESPACE:-reliefgrid}"
RELEASE="${APP_RELEASE:-reliefgrid}"
API_URL="${API_URL:-http://127.0.0.1:8080}"
SEED="${SEED:-52}"
K6="${K6:-k6}"
K6_CPUS="${K6_CPUS:-6-7}"
KIND_CPUS="${KIND_CPUS:-0-5}"
POWER_PROFILE="${POWER_PROFILE:-performance}"
# Scenario parameters: what k6 is given (-e), the executor and length the
# report states, and how many operations the dataset must hold.
MAX_VUS="${MAX_VUS:-400}"
K6_ENV=(-e "MAX_VUS=$MAX_VUS")
STEPS_PLAN="[]" # [{targetRps, fromS, toS}] relative to the k6 start
case "$SCENARIO" in
smoke)
	EXECUTOR=constant-arrival-rate DURATION_S=60 MAX_VUS=1 OFFERED=120 WARMUP_SECONDS="${WARMUP_SECONDS:-0}" ;;
step)
	START_RPS="${START_RPS:-10}" STEP_RPS="${STEP_RPS:-10}" STEPS="${STEPS:-8}" RAMP_S="${RAMP_S:-10}" HOLD_S="${HOLD_S:-110}"
	EXECUTOR=ramping-arrival-rate DURATION_S=$((STEPS * (RAMP_S + HOLD_S))) WARMUP_SECONDS=0
	K6_ENV+=(-e "START_RPS=$START_RPS" -e "STEP_RPS=$STEP_RPS" -e "STEPS=$STEPS" -e "RAMP_S=$RAMP_S" -e "HOLD_S=$HOLD_S")
	OFFERED=0
	STEPS_PLAN="$(python3 -c 'import json, sys
a, d, n, r, h = map(int, sys.argv[1:])
print(json.dumps([{"targetRps": a + i * d, "fromS": i * (r + h) + r, "toS": (i + 1) * (r + h)} for i in range(n)]))' \
		"$START_RPS" "$STEP_RPS" "$STEPS" "$RAMP_S" "$HOLD_S")"
	for ((i = 0; i < STEPS; i++)); do
		t=$((START_RPS + i * STEP_RPS)) p=$((START_RPS + (i > 0 ? i - 1 : 0) * STEP_RPS))
		OFFERED=$((OFFERED + (t + p) * RAMP_S / 2 + t * HOLD_S))
	done ;;
baseline | soak)
	RATE="${RATE:?RATE (req/s) is required for $SCENARIO}"
	DURATION_S="${DURATION_S:-$([ "$SCENARIO" = baseline ] && echo 600 || echo 1800)}"
	EXECUTOR=constant-arrival-rate WARMUP_SECONDS="${WARMUP_SECONDS:-120}" OFFERED=$((RATE * DURATION_S))
	K6_ENV+=(-e "RATE=$RATE" -e "DURATION_S=$DURATION_S") ;;
*) echo "run: no parameters for scenario $SCENARIO" >&2; exit 2 ;;
esac
OPS="${OPS:-$((OFFERED * 105 / 100 + 50))}"
SAMPLE_SECONDS="${SAMPLE_SECONDS:-5}"
SETTLE_SECONDS="${SETTLE_SECONDS:-20}"
SPILL_DIR="${SPILL_DIR:-$HOME/.cache/reliefgrid/S5.2}"
TOOLS_IMAGE="${TOOLS_IMAGE:-reliefgrid/tools:dev}"
# RUN_DIR is an explicit, unique output path; never infer it from timestamps in logs.
RUN_ID="S5.2-$SCENARIO-$(date -u +%Y%m%dT%H%M%SZ)"
DIR="${RUN_DIR:-$ROOT/docs/evidence/S5.2/$RUN_ID}"
[[ "$DIR" = /* ]] || DIR="$ROOT/$DIR"
# Reject every retained sidecar, including dangling symlinks, before mutation.
for sidecar in "$DIR.runner.log" "$DIR.runner-status.json" "$DIR.report-check.log"; do
  if [ -e "$sidecar" ] || [ -L "$sidecar" ]; then
    echo "run: output sidecar already exists: $sidecar" >&2
    exit 2
  fi
done
mkdir -p "$(dirname "$DIR")"
mkdir "$DIR" || { echo "run: output directory already exists: $DIR" >&2; exit 2; }
# mkdir is the per-run ownership gate; O_EXCL/noclobber reservations additionally
# cover another invocation reserving an adjacent path after the checks above.
# Keep the descriptors: later writes never reopen a retained sidecar by name.
set -C
if ! exec {run_log_fd}>"$DIR.runner.log"; then exit 2; fi
if ! exec {status_fd}>"$DIR.runner-status.json"; then exit 2; fi
if ! exec {report_fd}>"$DIR.report-check.log"; then exit 2; fi
set +C
mkdir -p "$DIR/raw/metrics-start" "$DIR/raw/metrics-end"
printf 'RUN_DIR=%s\n' "$DIR"
exec 3>&1 4>&2
exec > >(tee -a /dev/fd/"$run_log_fd" >&3) 2>&1
log_pid=$!
k6_exit=null
report_exit=null
safety_failure=false
finish() {
	local runner_exit=$?
	trap - EXIT
	set +e
	if declare -F restore >/dev/null; then restore; fi
	if [ "$safety_failure" = true ]; then runner_exit=4; fi
	exec >&3 2>&4
	local tee_exit=0
	wait "$log_pid" || tee_exit=$?
	if [ "$tee_exit" != 0 ] && [ "$runner_exit" = 0 ]; then runner_exit=5; fi
	printf '{"runnerExit":%s,"k6Exit":%s,"reportExit":%s,"teeExit":%s,"safetyFailure":%s}\n' \
	 "$runner_exit" "$k6_exit" "$report_exit" "$tee_exit" "$safety_failure" >&"$status_fd"
	exit "$runner_exit"
}
trap finish EXIT
pin() { sed -n "s/^$1 *:= *//p" tools.mk; }
K6_VERSION="${K6_VERSION:-$(pin K6_VERSION)}"
[ -n "$K6_VERSION" ] || { echo "run: K6_VERSION is not pinned in tools.mk (#125); set K6_VERSION" >&2; exit 2; }

kc() { kubectl --request-timeout=15s --context "$CTX" "$@"; }
kn() { kc -n "$NS" "$@"; }
log() { printf '%s run: %s\n' "$(date +%H:%M:%S)" "$*" >&2; }
die() { log "FAILED: $*"; exit 1; }
trap 'log "FAILED: line $LINENO: $BASH_COMMAND (exit $?)"' ERR

# --- preflight -------------------------------------------------------------------------------
[ "$(kubectl config get-contexts -o name | grep -cx "$CTX")" = 1 ] || die "no kube context $CTX"
kc get nodes >/dev/null || die "cluster $CLUSTER does not answer"
got="$("$K6" version | awk '{print $2; exit}')"
[ "$got" = "$K6_VERSION" ] || die "k6 is $got, the pin is $K6_VERSION"
command -v taskset >/dev/null || die "taskset is not installed"
curl -sf -o /dev/null "$API_URL/api/v1/requests?limit=1" || die "the API does not answer at $API_URL"

# --- isolation, restored on any exit ---------------------------------------------------------
python3 -c 'import sys
def s(x): return {c for p in x.split(",") for lo, _, hi in [p.partition("-")] for c in range(int(lo), int(hi or lo) + 1)}
sys.exit(1 if s(sys.argv[1]) & s(sys.argv[2]) else 0)' "$KIND_CPUS" "$K6_CPUS" || die "K6_CPUS $K6_CPUS overlaps KIND_CPUS $KIND_CPUS"
mapfile -t NODES < <(kind get nodes --name "$CLUSTER")
[ "${#NODES[@]}" -gt 0 ] || die "no kind nodes for $CLUSTER"
CPU_ONLINE="$(cat /sys/devices/system/cpu/online)"
declare -A CPUSET_BEFORE
for n in "${NODES[@]}"; do
	CPUSET_BEFORE[$n]="$(docker inspect -f '{{.HostConfig.CpusetCpus}}' "$n")"
done
POWER_BEFORE="$(powerprofilesctl get 2>/dev/null || echo unknown)"
sampler_pid=""
restore() {
	# Runs on every exit. No command here may end it early: errexit and the
	# ERR trap are off, and each step reports its own failure.
	set +e
	trap - ERR
	local n
	if [ -n "$sampler_pid" ]; then kill "$sampler_pid" 2>/dev/null; wait "$sampler_pid" 2>/dev/null; fi
	if [ -n "${pf:-}" ]; then kill "$pf" 2>/dev/null; fi
	for n in "${NODES[@]}"; do
		# An empty cpuset means every CPU; docker update needs it spelled out.
		docker update --cpuset-cpus "${CPUSET_BEFORE[$n]:-${CPU_ONLINE:-0-$(($(nproc) - 1))}}" "$n" >/dev/null 2>&1 ||
			{ safety_failure=true; log "WARNING: could not restore the cpuset of $n"; }
		[ "$(docker inspect -f '{{.HostConfig.CpusetCpus}}' "$n")" = "${CPUSET_BEFORE[$n]:-${CPU_ONLINE:-0-$(($(nproc) - 1))}}" ] || safety_failure=true
	done
	if [ "$POWER_BEFORE" != unknown ] && [ "$(powerprofilesctl get 2>/dev/null)" != "$POWER_BEFORE" ]; then
		powerprofilesctl set "$POWER_BEFORE" || { safety_failure=true; log "WARNING: could not restore the power profile $POWER_BEFORE"; }
	fi
	if [ "$POWER_BEFORE" != unknown ]; then
		[ "$(powerprofilesctl get 2>/dev/null)" = "$POWER_BEFORE" ] || safety_failure=true
	fi
	log "restored: node cpusets, power profile $(powerprofilesctl get 2>/dev/null || echo unknown)"
}

trap 'exit 130' INT
trap 'exit 143' TERM HUP
for n in "${NODES[@]}"; do
	docker update --cpuset-cpus "$KIND_CPUS" "$n" >/dev/null || die "docker update --cpuset-cpus $KIND_CPUS $n"
	[ "$(docker inspect -f '{{.HostConfig.CpusetCpus}}' "$n")" = "$KIND_CPUS" ] || die "$n did not take cpuset $KIND_CPUS"
done
if [ "$POWER_BEFORE" != unknown ]; then
	powerprofilesctl set "$POWER_PROFILE" || die "powerprofilesctl set $POWER_PROFILE"
	[ "$(powerprofilesctl get)" = "$POWER_PROFILE" ] || die "power profile is $(powerprofilesctl get), not $POWER_PROFILE"
fi
log "isolation: kind nodes on CPUs $KIND_CPUS, k6 on $K6_CPUS, power profile $(powerprofilesctl get 2>/dev/null || echo unknown)"

log "run $RUN_ID in $DIR"

# --- facts: host, versions, configuration ----------------------------------------------------
mhz() { awk -F: '/^cpu MHz/ { s += $2; n++ } END { printf "%.0f", s / n }' /proc/cpuinfo; }
meminfo() { awk -v k="$1" '$1 == k":" { print $2 }' /proc/meminfo; }
host_json="$(jq -n \
	--arg cpu "$(awk -F': ' '/^model name/ { print $2; exit }' /proc/cpuinfo)" \
	--argjson cpus "$(nproc)" \
	--argjson mem "$(awk -v kb="$(meminfo MemTotal)" 'BEGIN { printf "%.1f", kb / 1048576 }')" \
	--arg kernel "$(uname -r)" \
	--arg power "$(powerprofilesctl get 2>/dev/null || echo unknown)" \
	--arg k6cpus "$K6_CPUS" --arg kindcpus "$KIND_CPUS" --arg restored "$POWER_BEFORE" \
	--arg degraded "$(powerprofilesctl list 2>/dev/null | awk -v p="$POWER_PROFILE" '$0 ~ "^[* ] *" p ":" { f = 1; next } f && /Degraded:/ { sub(/^ *Degraded: */, ""); print; exit } /^[* ] *[a-z-]+:$/ { f = 0 }')" \
	--argjson swap "$(( ($(meminfo SwapTotal) - $(meminfo SwapFree)) / 1024 ))" \
	--argjson load "$(cut -d' ' -f1 /proc/loadavg)" \
	--argjson mhz "$(mhz)" \
	'{cpuModel: $cpu, logicalCpus: $cpus, memTotalGiB: $mem, kernel: $kernel, powerProfile: $power, k6Cpus: $k6cpus,
	  kindCpus: $kindcpus, powerProfileRestored: $restored, powerProfileDegraded: $degraded,
	  swapUsedMiB: $swap, loadAvgStart: $load, cpuMHzStart: $mhz}')"

images_json="$(kn get pods -o json | jq '[.items[] | select(.status.phase == "Running") |
	{key: (.metadata.labels["app.kubernetes.io/component"] // .metadata.labels["app.kubernetes.io/name"] // .metadata.name),
	 value: (.status.containerStatuses[0].imageID | capture("(?<d>sha256:[0-9a-f]{64})").d)}] | unique_by(.key) | from_entries')"
versions_json="$(jq -n \
	--arg k6 "$K6_VERSION" --arg kind "$(kind version | awk '{print $2}')" --arg node "$(pin KIND_NODE_IMAGE)" \
	--arg kubectl "$(kubectl version --client -o json | jq -r .clientVersion.gitVersion)" \
	--arg helm "$(helm version --short | cut -d+ -f1)" --arg go "$(go env GOVERSION | sed 's/^go//')" \
	--argjson images "$images_json" \
	'{k6: $k6, kind: $kind, kindNodeImage: $node, kubectl: $kubectl, helm: $helm, go: $go, images: $images}')"

envof() { kn get "$1" -o jsonpath="{range .spec.template.spec.containers[0].env[*]}{.name}={.value}{\"\\n\"}{end}" | sed -n "s/^$2=//p"; }
config_json="$(jq -n \
	--arg profile "$(envof sts/storage-shard-0 PROFILE)" \
	--arg fence "$(envof sts/storage-shard-0 STRONG_READ_FENCE)" \
	--arg tracing "$([ -n "$(envof "deploy/$RELEASE-api" OTEL_EXPORTER_OTLP_ENDPOINT)" ] && echo on || echo off)" \
	--arg autoscaling "$([ -n "$(kn get hpa -o name 2>/dev/null)" ] && echo on || echo off)" \
	--argjson api "$(kn get "deploy/$RELEASE-api" -o jsonpath='{.spec.replicas}')" \
	--argjson matcher "$(kn get "deploy/$RELEASE-matcher" -o jsonpath='{.spec.replicas}')" \
	--arg qos "$(kn get pods --field-selector=status.phase=Running -o json | jq -r '[.items[].status.qosClass] | unique | join(",")')" \
	--argjson limits "$(kn get pods --field-selector=status.phase=Running -o json | jq '[.items[] | {key: .metadata.name,
		value: ([.spec.containers[].resources.limits.cpu // empty | if endswith("m") then rtrimstr("m") | tonumber else tonumber * 1000 end] | add // 0)}] | from_entries')" \
	'{raftProfile: ($profile | if . == "" then "demo" else . end), readFence: ($fence | if . == "" then "fence" else . end),
	  tracing: $tracing, autoscaling: $autoscaling, replicas: {api: $api, matcher: $matcher}, podQoS: $qos,
	  podCpuLimitsMillicores: $limits}')"
git_json="$(jq -n --arg sha "$(git rev-parse HEAD)" --argjson dirty "$([ -z "$(git status --porcelain --untracked-files=no)" ] && echo false || echo true)" \
	'{sha: $sha, dirty: $dirty}')"

# --- dataset ---------------------------------------------------------------------------------
go run ./loadtest/cmd/gendata -seed "$SEED" -run "$RUN_ID" -ops "$OPS" -out "$DIR/dataset.json" > "$DIR/dataset-info.json"
log "dataset: $(jq -c '{seed, ops, requests, resources}' "$DIR/dataset-info.json")"

# --- scrapes and cgroup samples --------------------------------------------------------------
targets() { kn get pods -l app.kubernetes.io/name=storage -o jsonpath='{range .items[?(@.status.phase=="Running")]}{.metadata.name}{"\n"}{end}' | grep -v bootstrap; }
scrape() { # scrape <dir>
	local pod port # pf is global: restore() kills a scrape cut short
	for pod in $(targets); do
		port="$(python3 -c 'import socket; s=socket.socket(); s.bind(("127.0.0.1", 0)); print(s.getsockname()[1])')"
		# kubectl itself in the background, so $! is the process to kill (#50).
		kubectl --context "$CTX" -n "$NS" port-forward "pod/$pod" "$port:9090" >/dev/null 2>&1 &
		pf=$!
		for _ in $(seq 50); do curl -sf -o "$1/$pod.prom" "http://127.0.0.1:$port/metrics" && break; sleep 0.1; done
		kill "$pf" 2>/dev/null || true
		wait "$pf" 2>/dev/null || true
		pf=""
		[ -s "$1/$pod.prom" ] || die "could not scrape $pod"
	done
}
# Each pod's cgroup directory on its node (cgroup v2, systemd driver).
cgroups="$(kn get pods -o json | jq -r '.items[] | select(.status.phase == "Running") |
	[.metadata.name, .spec.nodeName, (.status.qosClass | ascii_downcase), (.metadata.uid | gsub("-"; "_"))] | @tsv')"
sample_once() {
	local node now
	now="$(( $(date +%s%N) / 1000000 ))" # ms; uutils date ignores %3N
	for node in $(cut -f2 <<<"$cgroups" | sort -u); do
		awk -v n="$node" -F'\t' '$2 == n' <<<"$cgroups" | while IFS=$'\t' read -r pod _ qos uid; do
			if [ "$qos" = guaranteed ]; then
				printf '%s /sys/fs/cgroup/kubelet.slice/kubelet-kubepods.slice/kubelet-kubepods-pod%s.slice\n' "$pod" "$uid"
			else
				printf '%s /sys/fs/cgroup/kubelet.slice/kubelet-kubepods.slice/kubelet-kubepods-%s.slice/kubelet-kubepods-%s-pod%s.slice\n' "$pod" "$qos" "$qos" "$uid"
			fi
		done | docker exec -i "$node" sh -c 'while read -r pod p; do
			u=$(awk "/^usage_usec/ {print \$2}" "$p/cpu.stat"); n=$(awk "/^nr_periods/ {print \$2}" "$p/cpu.stat")
			t=$(awk "/^nr_throttled/ {print \$2}" "$p/cpu.stat"); m=$(cat "$p/memory.current")
			echo "$pod $u $n $t $m"; done' | awk -v now="$now" '{printf "{\"tMs\":%s,\"pod\":\"%s\",\"usageUsec\":%s,\"nrPeriods\":%s,\"nrThrottled\":%s,\"memBytes\":%s}\n", now, $1, $2, $3, $4, $5}'
	done
}
sampler() { while :; do sample_once >> "$DIR/raw/cgroup.jsonl" || true; sleep "$SAMPLE_SECONDS"; done; }

scrape "$DIR/raw/metrics-start"
sampler &
sampler_pid=$!

# --- k6 ---------------------------------------------------------------------------------------
log "k6 $SCENARIO on CPU $K6_CPUS: ${DURATION_S}s, $OPS ops in the dataset"
set +e
taskset -c "$K6_CPUS" "$K6" run --quiet --no-color --out json="$DIR/raw/k6-points.jsonl.gz" \
	-e API_URL="$API_URL" -e DATASET="$DIR/dataset.json" -e SUMMARY="$DIR/k6-summary.json" "${K6_ENV[@]}" \
	"$SCRIPT" 2>"$DIR/raw/k6.log"
k6_exit=$?
set -e
log "k6 exit $k6_exit$([ "$k6_exit" = 99 ] && echo ': a threshold was crossed; the report says which')"
[ "$k6_exit" = 0 ] || [ "$k6_exit" = 99 ] || die "k6 failed: $(tail -5 "$DIR/raw/k6.log")"
# The test's own start: k6 emits its first vus point at t=0, after the
# dataset has loaded. Every window (warm-up, steps) counts from it.
# (grep -m1 closes the pipe early; zcat's SIGPIPE must not end the run.)
started="$(zcat "$DIR/raw/k6-points.jsonl.gz" | grep -m1 '"metric":"vus","type":"Point"' | jq -r .data.time || true)"
[ -n "$started" ] || die "no vus point in k6's output"
ended="$(date -u -d "$started + $DURATION_S seconds" +%Y-%m-%dT%H:%M:%S.%NZ)"
steps_json="$(python3 -c 'import json, sys
from datetime import datetime, timedelta
t0 = datetime.fromisoformat(sys.argv[1])
print(json.dumps([{"targetRps": s["targetRps"], "from": (t0 + timedelta(seconds=s["fromS"])).isoformat(),
                   "to": (t0 + timedelta(seconds=s["toS"])).isoformat()} for s in json.loads(sys.argv[2])]))' "$started" "$STEPS_PLAN")"
sample_once >> "$DIR/raw/cgroup.jsonl"
kill "$sampler_pid" 2>/dev/null || true
wait "$sampler_pid" 2>/dev/null || true
sampler_pid=""
scrape "$DIR/raw/metrics-end"
host_json="$(jq --argjson mhz "$(mhz)" '. + {cpuMHzEnd: $mhz}' <<<"$host_json")"

# --- settle, read back, invcheck -------------------------------------------------------------
log "settling ${SETTLE_SECONDS}s"
sleep "$SETTLE_SECONDS"
go run ./loadtest/cmd/loadreport statuses -dir "$DIR" -api "$API_URL" | tee "$DIR/raw/statuses-summary.json" >&2

job="s52-invcheck-$(date +%s)"
inv_exit=97
for attempt in 1 2 3 4 5 6; do
	jq -n --arg name "$job-$attempt" --arg image "$TOOLS_IMAGE" '{apiVersion: "batch/v1", kind: "Job",
		metadata: {name: $name}, spec: {backoffLimit: 0, activeDeadlineSeconds: 110, template: {
		metadata: {labels: {"reliefgrid.io/coordinator-client": "true"}},
		spec: {restartPolicy: "Never", automountServiceAccountToken: false,
			nodeSelector: {role: "infra"}, tolerations: [{key: "role", operator: "Equal", value: "infra", effect: "NoSchedule"}],
			containers: [{name: "check", image: $image, imagePullPolicy: "Never",
				args: ["check", "--coordinator", "http://storage-coordinator:8080", "--timeout", "90s", "--max-backlog", "0", "--json"],
				resources: {requests: {cpu: "100m", memory: "128Mi"}, limits: {memory: "512Mi"}}}]}}}}' | kn create -f - >/dev/null
	kn wait --for=condition=complete --timeout=115s "job/$job-$attempt" >/dev/null 2>&1 ||
		kn wait --for=condition=failed --timeout=5s "job/$job-$attempt" >/dev/null 2>&1 || true
	kn logs "job/$job-$attempt" > "$DIR/raw/invcheck.json" 2>/dev/null || true
	inv_exit="$(kn get pods -l "job-name=$job-$attempt" -o jsonpath='{.items[0].status.containerStatuses[0].state.terminated.exitCode}')"
	kn delete job "$job-$attempt" --wait=false >/dev/null 2>&1 || true
	# 3: not quiescent (the outbox has not drained yet): wait and check again
	[ "$inv_exit" = 3 ] || break
	log "invcheck: not quiescent yet (attempt $attempt)"
	sleep 10
done
log "invcheck exit $inv_exit"

# --- report -----------------------------------------------------------------------------------
jq -n --arg run "$RUN_ID" --arg scenario "$SCENARIO" --arg started "$started" --arg ended "$ended" \
	--argjson warmup "$WARMUP_SECONDS" --argjson git "$git_json" --argjson host "$host_json" \
	--argjson versions "$versions_json" --argjson config "$config_json" \
	--arg executor "$EXECUTOR" --argjson duration "$DURATION_S" --argjson maxvus "$MAX_VUS" --argjson steps "$steps_json" \
	--argjson inv "${inv_exit:-97}" \
	'{goal: "S5.2", runId: $run, scenario: $scenario, startedAt: $started, endedAt: $ended, warmupSeconds: $warmup,
	  git: $git, host: $host, versions: $versions, config: $config,
	  load: {executor: $executor, durationSeconds: $duration, maxVUs: $maxvus},
	  steps: $steps, invcheck: {exitCode: $inv}}' > "$DIR/facts.json"
go run ./loadtest/cmd/loadreport build -dir "$DIR" -spill "$SPILL_DIR" >&2
jq -e '.invariants.ledger == "PASS" and .invariants.invcheck == "PASS"' "$DIR/summary.json" >/dev/null || safety_failure=true
leaked="$(repo_port_forwards "$ROOT")"
[ -z "$leaked" ] || die "port-forwards outlived the run: $leaked"
set +e
python3 -I scripts/load-report-check.py "$DIR" >&"$report_fd" 2>&1
report_exit=$?
cat "$DIR.report-check.log"
set -e
exit "$report_exit"
