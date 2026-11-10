#!/usr/bin/env bash
# S5.2 Verify: the k6 report states throughput and p50/p95/p99, error rate and
# the saturation point, with hardware and versions recorded (plan Part 1 §4).
#
#   scripts/verify-S5.2.sh [docs/evidence/S5.2/<step runId>]
#
# 1. The S5.2 unit tests pass: the report checker on its fixtures, the
#    storage and coordinator metrics, the seeded dataset generator and the
#    report builder (whose output the checker must accept).
# 2. Every run directory under docs/evidence/S5.2/ passes
#    scripts/load-report-check.py: every field present and consistent, every
#    committed file matching SHA256SUMS, the ledger and invcheck PASS.
# 3. At least one step (saturation) run exists, and it (or the run named on
#    the command line) passes with --require-saturation: the saturation point
#    is the last step inside writes p99 <= 1 s, reads by ID p99 <= 250 ms,
#    listings p99 <= 500 ms, errors <= 1% and no dropped iterations.
# Read-only: it runs no load and needs no cluster. Writes its output to
# docs/evidence/S5.2/verify-<host>.txt.
set -uo pipefail
cd "$(dirname "$0")/.." || exit 1
EVIDENCE_ROOT="docs/evidence/S5.2"
STEP_RUN="${1:-}"
mkdir -p "$EVIDENCE_ROOT"
exec > >(tee "$EVIDENCE_ROOT/verify-$(hostname -s).txt") 2>&1

fails=0
pass() { echo "PASS  $1"; }
fail() { echo "FAIL  $1"; fails=$((fails + 1)); }

echo "== S5.2 Verify $(date -u +%Y-%m-%dT%H:%M:%SZ), git $(git rev-parse --short=12 HEAD)"

# --- 1 ---------------------------------------------------------------------------------------
# gotest <label> <go test args...>: passes only if the tests ran and passed. A
# -run filter that matches nothing ("no tests to run") is a failure.
gotest() {
	local label="$1" out rc
	shift
	out="$(go test -count=1 "$@" 2>&1)"
	rc=$?
	echo "$out"
	if [ "$rc" -eq 0 ] && ! grep -q 'no tests to run\|no test files' <<<"$out"; then pass "$label"; else fail "$label"; fi
}
gotest "report checker tests (scripts/load_report_test.go)" -run 'TestLoadReport|TestLoadSmoke|TestLoadRun|TestLoadCluster|TestLoadSupervisor|TestLoadPilot' ./scripts/
gotest "storage metrics endpoint tests" -run 'TestMetrics' ./internal/storagenode/
gotest "coordinator metrics tests" -run 'TestMetrics' ./internal/coordinator/
gotest "dataset generator tests (loadtest/dataset)" ./loadtest/dataset/
gotest "report builder tests (loadtest/report)" ./loadtest/report/

# --- 2 ---------------------------------------------------------------------------------------
runs=()
for d in "$EVIDENCE_ROOT"/*/; do
	[ -f "$d/summary.json" ] && runs+=("${d%/}")
done
if [ "${#runs[@]}" -eq 0 ]; then
	fail "no run directories with a summary.json under $EVIDENCE_ROOT"
fi
step_runs=()
for d in "${runs[@]}"; do
	if python3 -I scripts/load-report-check.py "$d"; then pass "report $d"; else fail "report $d"; fi
	if python3 -I -c 'import json,sys; sys.exit(json.load(open(sys.argv[1]))["scenario"] != "step")' "$d/summary.json" 2>/dev/null; then
		step_runs+=("$d")
	fi
done

# --- 3 ---------------------------------------------------------------------------------------
if [ -n "$STEP_RUN" ]; then
	step_runs=("${STEP_RUN%/}")
fi
if [ "${#step_runs[@]}" -eq 0 ]; then
	fail "no step (saturation) run under $EVIDENCE_ROOT"
else
	for d in "${step_runs[@]}"; do
		if python3 -I scripts/load-report-check.py "$d" --require-saturation; then
			pass "saturation point in $d: $(python3 -I -c 'import json,sys; s=json.load(open(sys.argv[1]))["saturation"]; print(f"{s[\"rps\"]} req/s; broken at {s[\"brokenAt\"]} by {\",\".join(s[\"brokenBy\"])}")' "$d/summary.json")"
		else
			fail "saturation point in $d"
		fi
	done
fi

echo
if [ "$fails" -ne 0 ]; then
	echo "S5.2 Verify: FAIL ($fails)"
	exit 1
fi
echo "S5.2 Verify: PASS"
