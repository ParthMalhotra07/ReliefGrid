#!/usr/bin/env bash
# Verify check for S4.3 (Chaos Mesh suite): "make chaos-suite report is all green".
# Run from the repo root, against the running "reliefgrid" kind cluster (never recreated):
#   make verify-S4.3                       (passes the Chaos Mesh pins from tools.mk)
#   SEED_BASE=440 make verify-S4.3
# First the checks that need no cluster: every experiment script plans deterministically, the
# pins are well formed, and the tests of the install script, the judge and the harness's
# verdict path pass. Then chaos/scripts/suite.sh: Chaos Mesh installed and checked, the ten
# experiments and both negative controls, resources measured before and after. Writes
# docs/evidence/S4.3/chaos-suite-<host>.txt (code fingerprint, dirty flag, every PASS/FAIL line,
# the suite's summary and resources) beside the suite's own directory. Exit 0 only if all passed.
# STATIC_ONLY=1 runs only the checks that need no cluster and writes no evidence.
set -uo pipefail

EVIDENCE="docs/evidence/S4.3/chaos-suite-$(hostname -s).txt"
RUN_ID="$(date -u +%Y%m%dT%H%M%SZ)-$(hostname -s)"
SUITE_DIR="docs/evidence/S4.3/suite-$RUN_ID"
EXPERIMENTS=(kill-leader kill-follower restart-shard isolate-replica isolate-leader lose-quorum delay-follower
	delay-isolate-leader stop-worker restart-coordinator)

tmp="$(mktemp -d)"
trap 'rm -rf "$tmp"' EXIT
results="$tmp/results"
: >"$results"
passes=0 failures=0
pass() { printf 'PASS  %s\n' "$1" | tee -a "$results"; passes=$((passes + 1)); }
fail() {
	{ printf 'FAIL  %s\n' "$1"; [ -n "${2:-}" ] && sed 's/^/      /' <<<"$2"; } | tee -a "$results"
	failures=$((failures + 1))
}

write_evidence() {
	local code_paths code_tree code_dirty result
	code_paths=(go.mod go.sum Makefile tools.mk cmd/invcheck internal loadtest chaos deploy/helm/storage scripts/verify-S4.3.sh)
	code_tree="$(git ls-tree -r HEAD -- "${code_paths[@]}" | git hash-object --stdin)"
	code_dirty="$(git status --porcelain -- "${code_paths[@]}" | grep -q . && echo yes || echo no)"
	if [ "$failures" -eq 0 ]; then result="PASSED ($passes/$passes checks)"; else result="FAILED ($failures of $((passes + failures)) checks failed)"; fi
	mkdir -p "$(dirname "$EVIDENCE")"
	{
		echo "# S4.3 Chaos Mesh suite"
		echo "result:     $result"
		echo "host:       $(hostname -s)"
		echo "date:       $(date -u +%Y-%m-%dT%H:%M:%SZ)"
		echo "code tree:  ${code_tree:0:12} (git ls-tree -r HEAD -- ${code_paths[*]} | git hash-object --stdin)"
		echo "code dirty: $code_dirty (uncommitted changes under those paths)"
		echo "suite:      $SUITE_DIR"
		echo
		echo "## checks"
		cat "$results"
		echo
		cat "$SUITE_DIR/summary.txt" 2>/dev/null || echo "(no suite summary)"
		echo
		echo "## resources"
		cat "$SUITE_DIR/resources.txt" 2>/dev/null || echo "(not measured)"
	} 2>&1 | sed 's/[[:space:]]*$//' | awk '{ l[NR] = $0 } END { n = NR; while (n > 0 && l[n] == "") n--; for (i = 1; i <= n; i++) print l[i] }' >"$EVIDENCE"
	echo "evidence: $EVIDENCE ($result)"
}

echo "== 1. without a cluster"
for e in "${EXPERIMENTS[@]}"; do
	a="$(chaos/scripts/$e.sh --seed 7 --plan 2>&1)" b="$(chaos/scripts/$e.sh --seed 7 --plan 2>&1)"
	[ "$a" = "$b" ] && jq -e --arg e "$e" '.experiment == $e' <<<"$a" >/dev/null 2>&1 &&
		pass "$e: --seed 7 --plan is one JSON plan, the same both times" || fail "$e: --seed 7 --plan is one JSON plan, the same both times" "$a"
done
for v in CHAOS_MESH_VERSION CHAOS_MESH_CHART_SHA256 CHAOS_MESH_IMAGE_DIGEST CHAOS_DAEMON_IMAGE_DIGEST; do
	[ -n "${!v:-}" ] && pass "pin $v is set (${!v:0:19}...)" || fail "pin $v is set (run through make verify-S4.3)"
done
if go test -count=1 -run 'TestChaosMesh|TestHarnessVerdicts|TestChaosExperimentPlans|TestSuiteSummary' ./scripts/ >"$tmp/go1.log" 2>&1 &&
	go test -count=1 ./chaos/judge/ ./loadtest/ledger/ >"$tmp/go2.log" 2>&1 &&
	go test -count=1 -run 'TestJudgeCommand|TestProbe|TestDirectRead|TestOwner|TestRing|TestConsistency|TestVerifyChecksTheReads' ./cmd/invcheck/ >"$tmp/go3.log" 2>&1; then
	pass "tests: install script, judge, ledger windows, probes and the harness's verdict path"
else
	fail "tests: install script, judge, ledger windows, probes and the harness's verdict path" "$(tail -20 "$tmp"/go*.log)"
fi

if [ "${STATIC_ONLY:-0}" = 1 ]; then
	echo "STATIC_ONLY=1: stopping before the suite (no cluster touched, no evidence written)"
	[ "$failures" -eq 0 ]
	exit
fi

echo "== 2. the suite"
SUITE_RUN_ID="$RUN_ID" SUITE_OUT="$SUITE_DIR" chaos/scripts/suite.sh
rc=$?
case "$rc" in
0) pass "make chaos-suite: every experiment passed and both negative controls caught their fault" ;;
1) fail "make chaos-suite: every experiment passed and both negative controls caught their fault" "$(sed -n '/## failures/,/^$/p' "$SUITE_DIR/summary.txt" 2>/dev/null)" ;;
*) fail "make chaos-suite: reached a verdict" "exit $rc" ;;
esac
if [ -s "$SUITE_DIR/summary.json" ]; then
	jq -e '.chaosMesh.ok == true and .chaosMesh.version == "2.8.7" and (.chaosMesh.images | all(test("@sha256:")))' "$SUITE_DIR/summary.json" >/dev/null &&
		pass "chaos mesh: 2.8.7 running by digest, a smoke NetworkChaos injected and recovered" ||
		fail "chaos mesh: 2.8.7 running by digest, a smoke NetworkChaos injected and recovered" "$(jq -c .chaosMesh "$SUITE_DIR/summary.json")"
	jq -e '[.runs[].name] | contains(["kill-leader","kill-follower","restart-shard","isolate-replica","isolate-leader","delay-isolate-leader",
		"lose-quorum","delay-follower","stop-worker","restart-coordinator","control-lost-write","control-stale-reader"])' "$SUITE_DIR/summary.json" >/dev/null &&
		pass "suite: all ten experiments and both controls ran" || fail "suite: all ten experiments and both controls ran" "$(jq -c '[.runs[].name]' "$SUITE_DIR/summary.json")"
	jq -e '[.runs[] | select(.name | startswith("control") | not) | .summary | (.lost == 0 and .unresolvedUnknown == 0 and .staleReads == 0)] | all' "$SUITE_DIR/summary.json" >/dev/null &&
		pass "suite: every experiment 0 lost, 0 unresolved unknowns, 0 stale reads" ||
		fail "suite: every experiment 0 lost, 0 unresolved unknowns, 0 stale reads" "$(jq -c '[.runs[] | {name, summary}]' "$SUITE_DIR/summary.json")"
	[ "$(grep -c '^## ' "$SUITE_DIR/resources.txt" 2>/dev/null)" -ge 3 ] && pass "resources: measured before Chaos Mesh, after its install and after the runs" ||
		fail "resources: measured before Chaos Mesh, after its install and after the runs"
fi
k() { kubectl --context "${CHAOS_CONTEXT:-kind-reliefgrid}" "$@"; }
left="$(k -n "${CHAOS_NAMESPACE:-reliefgrid}" get networkchaos -o name 2>/dev/null)"
[ -z "$left" ] && pass "no NetworkChaos left behind" || fail "no NetworkChaos left behind" "$left"
down="$(k get nodes --no-headers 2>/dev/null | awk '$2 != "Ready" { print $1 }')"
[ -z "$down" ] && pass "every kind node Ready" || fail "every kind node Ready" "$down"

write_evidence
[ "$failures" -eq 0 ]
