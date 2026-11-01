#!/usr/bin/env bash
# S4.3 chaos suite (make chaos-suite). Against the running "reliefgrid" kind cluster (never
# recreated): builds and loads the storage, coordinator and tools images, installs or upgrades
# deploy/helm/storage, measures resources, installs and checks Chaos Mesh (chaos/mesh/install.sh),
# measures again, runs every experiment with a fixed seed plus both negative controls, measures
# again, and writes the evidence:
#   docs/evidence/S4.3/suite-<runId>/
#     install-check.json      the Chaos Mesh check (versions, digests, smoke injection)
#     resources.txt           before Chaos Mesh, after its install, after the runs
#     reports/<run>.json      every run's report (chaos/scripts/lib.sh)
#     summary.json, summary.txt
# Exit 0 only if every experiment passed and both controls caught their fault; 1 otherwise; 2
# if the suite could not run.
#   chaos/scripts/suite.sh --summarize DIR   rebuild DIR's summary from its reports only
#
# Environment: SEED_BASE (430), ONLY (comma-separated runs, e.g. lose-quorum,control-lost-write),
# SKIP_INSTALL (1: Chaos Mesh already installed and checked), CHAOS_CONTEXT, CHAOS_NAMESPACE, and
# the pins (through make).
set -uo pipefail

CTX="${CHAOS_CONTEXT:-kind-reliefgrid}"
NS="${CHAOS_NAMESPACE:-reliefgrid}"
KIND_CLUSTER="${CTX#kind-}"
SEED_BASE="${SEED_BASE:-430}"
HERE="$(cd "$(dirname "$0")" && pwd)"
ROOT="$(cd "$HERE/../.." && pwd)"
RUN_ID="${SUITE_RUN_ID:-$(date -u +%Y%m%dT%H%M%SZ)-$(hostname -s)}"
OUT="${SUITE_OUT:-$ROOT/docs/evidence/S4.3/suite-$RUN_ID}"

# The runs, in order: name, script, flags. Controls must fail in the expected way.
RUNS=(
	"kill-leader|kill-leader|"
	"kill-follower|kill-follower|"
	"restart-shard|restart-shard|"
	"isolate-replica|isolate-replica|--role leader"
	"isolate-leader|isolate-leader|"
	"delay-isolate-leader|delay-isolate-leader|"
	"lose-quorum|lose-quorum|"
	"delay-follower|delay-follower|"
	"stop-worker|stop-worker|"
	"restart-coordinator|restart-coordinator|"
	"control-lost-write|kill-follower|--inject lost-write"
	"control-stale-reader|delay-follower|--stale-readers"
)

k() { kubectl --context "$CTX" "$@"; }
log() { printf '%s suite: %s\n' "$(date +%H:%M:%S)" "$*" >&2; }
die() { log "NO VERDICT: $*"; exit 2; }

# resources <label>: memory and CPU of every kind node, the host, and each Chaos Mesh pod
resources() {
	local p n pod uid f m
	echo "## $1 ($(date -u +%Y-%m-%dT%H:%M:%SZ))"
	docker stats --no-stream --format '{{.Name}} cpu {{.CPUPerc}} mem {{.MemUsage}}' $(kind get nodes --name "$KIND_CLUSTER" 2>/dev/null) 2>&1
	free -m | sed -n 1,2p
	for p in $(k -n chaos-mesh get pods -o jsonpath='{range .items[*]}{.metadata.name}:{.spec.nodeName}:{.metadata.uid}{"\n"}{end}' 2>/dev/null); do
		pod="${p%%:*}" n="$(cut -d: -f2 <<<"$p")" uid="$(cut -d: -f3 <<<"$p" | tr - _)"
		f="$(docker exec "$n" sh -c "find /sys/fs/cgroup/kubelet.slice -maxdepth 3 -type d -name '*pod${uid}.slice' 2>/dev/null | head -1")"
		m="$(docker exec "$n" cat "$f/memory.current" 2>/dev/null)"
		echo "chaos-mesh pod $pod on $n: $((${m:-0} / 1048576)) MiB"
	done
	echo
}

# run_one <name> <script> <seed> <flags...>: one report, its log and its exit code
run_one() {
	local name="$1" script="$2" seed="$3"
	shift 3
	log "run $name: $script.sh --seed $seed $*"
	"$HERE/$script.sh" --seed "$seed" --out "$OUT/reports/$name.json" "$@" >"$OUT/reports/$name.log" 2>&1
	echo $? >"$OUT/reports/$name.exit"
}

# row <name>: the run's summary row as JSON (a missing report is a failed row)
row() {
	local name="$1" report="$OUT/reports/$1.json" code row
	code="$(cat "$OUT/reports/$name.exit" 2>/dev/null || echo null)"
	[ -s "$report" ] || { jq -cn --arg n "$name" --argjson c "$code" '{name: $n, exit: $c, pass: false, reason: "no report"}'; return; }
	case "$name" in
	control-lost-write) row='(.control.pass == true) and ([.failures[] | select(test("lost") | not)] | length == 0)' ;;
	control-stale-reader) row='(.control.pass == true) and ([.failures[] | select(test("stale read") | not)] | length == 0)' ;;
	*) row='.pass == true' ;;
	esac
	jq -c --arg n "$name" --argjson c "$code" "{name: \$n, script: .experiment, seed, exit: \$c, pass: ($row),
		control: (.control // null | if . then {negative, caught: .pass} else null end),
		failures, recoverySeconds: .recovery.seconds, settleSeconds: .settle.seconds,
		summary: (.judge.summary // null | if . then del(.availability) else null end),
		faulted: (.target.shard as \$s | [(.availability.shards // [])[] | select(.shard == \$s)] | first // null),
		faultSeconds: (((.fault.healedAt // 0) - (.fault.startedAt // 0)) / 1000)}" "$report"
}

# summarize: summary.json and summary.txt from the reports of every run in RUNS (or ONLY);
# exit 0 only if all passed
summarize() {
	local r name rows="$OUT/rows.jsonl"
	: >"$rows"
	for r in "${RUNS[@]}"; do
		name="${r%%|*}"
		if [ -n "${ONLY:-}" ] && [[ ",$ONLY," != *",$name,"* ]]; then continue; fi
		row "$name" >>"$rows"
	done
	[ -s "$OUT/install-check.json" ] || echo null >"$OUT/install-check.json"
	jq -s --arg run "$RUN_ID" --slurpfile check "$OUT/install-check.json" \
		'{runId: $run, pass: (length > 0 and all(.pass)), chaosMesh: $check[0], runs: .}' "$rows" >"$OUT/summary.json"
	{
		echo "# S4.3 chaos suite $RUN_ID"
		echo "result: $(jq -r 'if .pass then "PASSED" else "FAILED" end' "$OUT/summary.json") ($(jq '[.runs[] | select(.pass)] | length' "$OUT/summary.json")/$(jq '.runs | length' "$OUT/summary.json") runs)"
		echo "chaos mesh: $(jq -c '.chaosMesh | {version, kubernetes, images, smoke}' "$OUT/summary.json")"
		echo
		printf '%-22s %-5s %6s %7s %6s %7s %8s %6s %6s %9s %9s %9s\n' run pass faultS recovS acked unknown unresolv lost stale probeSrvd quorumAck firstAckH
		jq -r '.runs[] | [.name, (if .pass then "PASS" else "FAIL" end), (.faultSeconds // "-"), (.recoverySeconds // "-"),
			(.summary.acked // "-"), (.summary.unknownRaw // "-"), (.summary.unresolvedUnknown // "-"), (.summary.lost // "-"),
			(.summary.staleReads // "-"), ((.faulted.probeServedInsideFault // {}) | add // 0), (.faulted.acksInsideFault // "-"),
			(.faulted.firstAckAfterHealMs // "-")] | @tsv' "$OUT/summary.json" |
			awk -F'\t' '{ printf "%-22s %-5s %6s %7s %6s %7s %8s %6s %6s %9s %9s %9s\n", $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12 }'
		echo
		echo "## failures"
		jq -r '.runs[] | select(.pass | not) | "\(.name): \(.failures // [.reason] | join("; "))"' "$OUT/summary.json"
		echo
		echo "## faulted shard availability (writes and reads around each fault)"
		jq -r '.runs[] | "\(.name): \(.faulted // {} | {acksBefore, acksDuring, acksAfter, acksInsideFault, readsDuring, readsServedDuring, readsServedInsideFault, probeServedInsideFault, firstAckAfterFaultMs, firstAckAfterHealMs, writeLatencyMs, readLatencyMs, errorsDuring} | tojson)"' "$OUT/summary.json"
	} >"$OUT/summary.txt"
	cat "$OUT/summary.txt"
	jq -e .pass "$OUT/summary.json" >/dev/null
}

if [ "${1:-}" = --summarize ]; then
	OUT="${2:?--summarize DIR}"
	RUN_ID="$(basename "$OUT")"
	summarize
	exit $?
fi

for t in kubectl helm jq docker kind; do command -v "$t" >/dev/null || die "$t is not installed"; done
k get nodes >/dev/null 2>&1 || die "kind cluster $KIND_CLUSTER is not running (make cluster-up)"
mkdir -p "$OUT/reports"
cd "$ROOT" || die "cd $ROOT"
make -s storage-image coordinator-image tools-image >"$OUT/build.log" 2>&1 || die "image build failed (see $OUT/build.log)"
helm --kube-context "$CTX" upgrade --install storage deploy/helm/storage -n "$NS" --create-namespace --wait --timeout 6m >"$OUT/helm.log" 2>&1 ||
	die "helm upgrade --install storage (see $OUT/helm.log)"

: >"$OUT/resources.txt"
if [ "${SKIP_INSTALL:-0}" != 1 ]; then
	resources "before Chaos Mesh (storage only)" >>"$OUT/resources.txt"
	"$ROOT/chaos/mesh/install.sh" install >"$OUT/install.log" 2>&1 || die "Chaos Mesh install (see $OUT/install.log)"
fi
"$ROOT/chaos/mesh/install.sh" check >"$OUT/install-check.json" 2>"$OUT/install-check.log" || die "Chaos Mesh check failed: $(cat "$OUT/install-check.json")"
sleep 20
resources "after the Chaos Mesh install (idle)" >>"$OUT/resources.txt"

i=0
for r in "${RUNS[@]}"; do
	IFS='|' read -r name script flags <<<"$r"
	i=$((i + 1))
	if [ -n "${ONLY:-}" ] && [[ ",$ONLY," != *",$name,"* ]]; then continue; fi
	# shellcheck disable=SC2086
	run_one "$name" "$script" $((SEED_BASE + i)) $flags
	log "$name: $(row "$name" | jq -r 'if .pass then "PASS" else "FAIL: \(.failures // [.reason] | join("; "))" end')"
done
resources "after the runs" >>"$OUT/resources.txt"
summarize
