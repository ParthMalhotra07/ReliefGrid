#!/usr/bin/env bash
# One S5.2 load run on kind, from dataset to checked report.
#
#   loadtest/run.sh smoke            (SEED=52 OPS=200 K6_CPUS=7 K6=k6)
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
# Kind only: it refuses any other context. It changes nothing on the cluster
# but one invcheck Job, deleted afterwards.
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

SCENARIO="${1:?usage: loadtest/run.sh <scenario: smoke>}"
SCRIPT="loadtest/k6/$SCENARIO.js"
[ -f "$SCRIPT" ] || { echo "run: no scenario $SCRIPT" >&2; exit 2; }
CLUSTER="${CLUSTER:-reliefgrid}"
CTX="kind-$CLUSTER"
NS="${NAMESPACE:-reliefgrid}"
RELEASE="${APP_RELEASE:-reliefgrid}"
API_URL="${API_URL:-http://127.0.0.1:8080}"
SEED="${SEED:-52}"
OPS="${OPS:-200}"
K6="${K6:-k6}"
K6_CPUS="${K6_CPUS:-7}"
WARMUP_SECONDS="${WARMUP_SECONDS:-0}"
SAMPLE_SECONDS="${SAMPLE_SECONDS:-5}"
SETTLE_SECONDS="${SETTLE_SECONDS:-20}"
SPILL_DIR="${SPILL_DIR:-$HOME/.cache/reliefgrid/S5.2}"
TOOLS_IMAGE="${TOOLS_IMAGE:-reliefgrid/tools:dev}"
pin() { sed -n "s/^$1 *:= *//p" tools.mk; }
K6_VERSION="${K6_VERSION:-$(pin K6_VERSION)}"
[ -n "$K6_VERSION" ] || { echo "run: K6_VERSION is not pinned in tools.mk (#125); set K6_VERSION" >&2; exit 2; }

kc() { kubectl --request-timeout=15s --context "$CTX" "$@"; }
kn() { kc -n "$NS" "$@"; }
log() { printf '%s run: %s\n' "$(date +%H:%M:%S)" "$*" >&2; }
die() { log "FAILED: $*"; exit 1; }

# --- preflight -------------------------------------------------------------------------------
[ "$(kubectl config get-contexts -o name | grep -cx "$CTX")" = 1 ] || die "no kube context $CTX"
kc get nodes >/dev/null || die "cluster $CLUSTER does not answer"
got="$("$K6" version | awk '{print $2; exit}')"
[ "$got" = "$K6_VERSION" ] || die "k6 is $got, the pin is $K6_VERSION"
command -v taskset >/dev/null || die "taskset is not installed"
curl -sf -o /dev/null "$API_URL/api/v1/requests?limit=1" || die "the API does not answer at $API_URL"

RUN_ID="S5.2-$SCENARIO-$(date -u +%Y%m%dT%H%M%SZ)"
DIR="docs/evidence/S5.2/$RUN_ID"
mkdir -p "$DIR/raw/metrics-start" "$DIR/raw/metrics-end"
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
	--arg k6cpus "$K6_CPUS" \
	--argjson swap "$(( ($(meminfo SwapTotal) - $(meminfo SwapFree)) / 1024 ))" \
	--argjson load "$(cut -d' ' -f1 /proc/loadavg)" \
	--argjson mhz "$(mhz)" \
	'{cpuModel: $cpu, logicalCpus: $cpus, memTotalGiB: $mem, kernel: $kernel, powerProfile: $power, k6Cpus: $k6cpus,
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
	--arg qos "$(kn get pods -o json | jq -r '[.items[].status.qosClass] | unique | join(",")')" \
	'{raftProfile: ($profile | if . == "" then "demo" else . end), readFence: ($fence | if . == "" then "fence" else . end),
	  tracing: $tracing, autoscaling: $autoscaling, replicas: {api: $api, matcher: $matcher}, podQoS: $qos}')"
git_json="$(jq -n --arg sha "$(git rev-parse HEAD)" --argjson dirty "$([ -z "$(git status --porcelain --untracked-files=no)" ] && echo false || echo true)" \
	'{sha: $sha, dirty: $dirty}')"

# --- dataset ---------------------------------------------------------------------------------
go run ./loadtest/cmd/gendata -seed "$SEED" -run "$RUN_ID" -ops "$OPS" -out "$DIR/dataset.json" > "$DIR/dataset-info.json"
log "dataset: $(jq -c '{seed, ops, requests, resources}' "$DIR/dataset-info.json")"

# --- scrapes and cgroup samples --------------------------------------------------------------
targets() { kn get pods -l app.kubernetes.io/name=storage -o jsonpath='{range .items[?(@.status.phase=="Running")]}{.metadata.name}{"\n"}{end}' | grep -v bootstrap; }
scrape() { # scrape <dir>
	local pod port pf
	for pod in $(targets); do
		port="$(python3 -c 'import socket; s=socket.socket(); s.bind(("127.0.0.1", 0)); print(s.getsockname()[1])')"
		kn port-forward "pod/$pod" "$port:9090" >/dev/null 2>&1 &
		pf=$!
		for _ in $(seq 50); do curl -sf -o "$1/$pod.prom" "http://127.0.0.1:$port/metrics" && break; sleep 0.1; done
		kill "$pf" 2>/dev/null || true
		wait "$pf" 2>/dev/null || true
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
			u=$(awk "/^usage_usec/ {print \$2}" "$p/cpu.stat"); t=$(awk "/^nr_throttled/ {print \$2}" "$p/cpu.stat"); m=$(cat "$p/memory.current")
			echo "$pod $u $t $m"; done' | awk -v now="$now" '{printf "{\"tMs\":%s,\"pod\":\"%s\",\"usageUsec\":%s,\"nrThrottled\":%s,\"memBytes\":%s}\n", now, $1, $2, $3, $4}'
	done
}
sampler() { while :; do sample_once >> "$DIR/raw/cgroup.jsonl" || true; sleep "$SAMPLE_SECONDS"; done; }

scrape "$DIR/raw/metrics-start"
sampler &
sampler_pid=$!
trap 'kill "$sampler_pid" 2>/dev/null || true' EXIT

# --- k6 ---------------------------------------------------------------------------------------
started="$(date -u +%Y-%m-%dT%H:%M:%S.%NZ)"
log "k6 $SCENARIO on CPU $K6_CPUS"
set +e
taskset -c "$K6_CPUS" "$K6" run --quiet --no-color --out json="$DIR/raw/k6-points.jsonl.gz" \
	-e API_URL="$API_URL" -e DATASET="$ROOT/$DIR/dataset.json" -e SUMMARY="$ROOT/$DIR/k6-summary.json" "$SCRIPT" 2>"$DIR/raw/k6.log"
k6_exit=$?
set -e
ended="$(date -u +%Y-%m-%dT%H:%M:%S.%NZ)"
log "k6 exit $k6_exit (99: thresholds crossed; the report says which)"
[ "$k6_exit" = 0 ] || [ "$k6_exit" = 99 ] || die "k6 failed: $(tail -5 "$DIR/raw/k6.log")"
sample_once >> "$DIR/raw/cgroup.jsonl"
kill "$sampler_pid" 2>/dev/null || true
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
	--argjson k6opts "$("$K6" inspect -e DATASET="$ROOT/$DIR/dataset.json" "$SCRIPT" | jq '.scenarios | to_entries[0].value')" \
	--argjson inv "${inv_exit:-97}" \
	'{goal: "S5.2", runId: $run, scenario: $scenario, startedAt: $started, endedAt: $ended, warmupSeconds: $warmup,
	  git: $git, host: $host, versions: $versions, config: $config,
	  load: {executor: $k6opts.executor, durationSeconds: (($k6opts.duration // "0s") | rtrimstr("s") | split("m") |
	           if length == 2 then (.[0] | tonumber) * 60 + ((.[1] | if . == "" then "0" else . end) | tonumber) else (.[0] | tonumber) end),
	         maxVUs: ($k6opts.maxVUs // $k6opts.vus // 1)},
	  invcheck: {exitCode: $inv}}' > "$DIR/facts.json"
go run ./loadtest/cmd/loadreport build -dir "$DIR" -spill "$SPILL_DIR" >&2
python3 -I scripts/load-report-check.py "$DIR"
