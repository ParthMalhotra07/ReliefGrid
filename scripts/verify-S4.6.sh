#!/usr/bin/env bash
# S4.6 Verify: CI builds the images, runs the kind smoke test and catches contract drift.
#
#   scripts/verify-S4.6.sh <PR> <drift probe PR>
#
# 1. The CI tests (scripts/ci_test.go) pass.
# 2. On <PR> (a code PR to main), check, changes, the three images jobs and smoke ran and
#    passed on its head commit, and the smoke evidence artifact holds demo 4 with result PASS.
# 3. On <drift probe PR> (a contract edited without make gen), check failed at its
#    contract-drift step, on the generated file.
# Prints each job's measured duration. Read-only on GitHub: it opens, closes and merges
# nothing. Needs gh (authenticated), jq and go. Writes its output to
# docs/evidence/S4.6/verify-<host>.txt.
set -uo pipefail

if [ $# -ne 2 ]; then
	echo "usage: $0 <PR> <drift probe PR>" >&2
	exit 2
fi
PR="$1" PROBE="$2"
REPO="${REPO:-ParthMalhotra07/ReliefGrid}"
cd "$(dirname "$0")/.." || exit 1
EVIDENCE="docs/evidence/S4.6/verify-$(hostname -s).txt"
mkdir -p "$(dirname "$EVIDENCE")"
exec > >(tee "$EVIDENCE") 2>&1

fails=0
pass() { echo "PASS  $1"; }
fail() { echo "FAIL  $1"; fails=$((fails + 1)); }
tmp="$(mktemp -d)"
trap 'rm -rf "$tmp"' EXIT

echo "== S4.6 Verify $(date -u +%Y-%m-%dT%H:%M:%SZ), git $(git rev-parse --short=12 HEAD)"

# --- 1 ---------------------------------------------------------------------------------------
if go test -count=1 -run 'TestCI|TestGenCheck' ./scripts/; then pass "CI tests (scripts/ci_test.go)"; else fail "CI tests (scripts/ci_test.go)"; fi

# run_of <PR>: the ci workflow's latest run on the PR's head commit
run_of() {
	local sha
	sha="$(gh pr view "$1" --repo "$REPO" --json headRefOid --jq .headRefOid)" || return 1
	gh run list --repo "$REPO" --workflow ci.yml --commit "$sha" --limit 1 --json databaseId --jq '.[0].databaseId // empty'
}
# durations <run id>: one line per job, "<name> <conclusion> <seconds> s"
durations() {
	gh run view "$1" --repo "$REPO" --json jobs --jq '.jobs[] |
		"\(.name)\t\(.conclusion)\t\(if .startedAt and .completedAt and .conclusion != "skipped" then ((.completedAt | fromdate) - (.startedAt | fromdate) | tostring) + " s" else "-" end)"' | column -t -s $'\t'
}

# --- 2 ---------------------------------------------------------------------------------------
run="$(run_of "$PR")"
if [ -z "$run" ]; then
	fail "PR #$PR: no ci run on its head commit"
else
	echo
	echo "# PR #$PR: run $run (https://github.com/$REPO/actions/runs/$run)"
	gh run view "$run" --repo "$REPO" --json jobs >"$tmp/jobs.json"
	durations "$run"
	for want in check changes "images (storagenode)" "images (coordinator)" "images (tools)" smoke; do
		c="$(jq -r --arg n "$want" '[.jobs[] | select(.name == $n)][0].conclusion // "missing"' "$tmp/jobs.json")"
		[ "$c" = success ] && pass "PR #$PR: $want ran and passed" || fail "PR #$PR: $want ran and passed (got $c)"
	done
	if gh run download "$run" --repo "$REPO" -p 'smoke-evidence-*' -D "$tmp/evidence" >/dev/null 2>&1; then
		d4="$(find "$tmp/evidence" -name demo-4.txt | head -1)"
		if [ -n "$d4" ] && grep -q '^result: *PASS' "$d4"; then
			pass "PR #$PR: smoke evidence has demo-4 $(sed -n 's/^result: *//p' "$d4")"
		else
			fail "PR #$PR: smoke evidence has demo-4 with result PASS"
		fi
		t="$(find "$tmp/evidence" -name timings.txt | head -1)"
		[ -n "$t" ] && { echo; echo "# smoke steps (timings.txt)"; cat "$t"; }
		l="$(find "$tmp/evidence" -name leaders.txt | head -1)"
		[ -n "$l" ] && { echo; echo "# leaders (leaders.txt)"; cat "$l"; }
	else
		fail "PR #$PR: smoke evidence artifact downloads"
	fi
fi

# --- 3 ---------------------------------------------------------------------------------------
run="$(run_of "$PROBE")"
if [ -z "$run" ]; then
	fail "probe PR #$PROBE: no ci run on its head commit"
else
	echo
	echo "# drift probe PR #$PROBE: run $run (https://github.com/$REPO/actions/runs/$run)"
	durations "$run"
	c="$(gh run view "$run" --repo "$REPO" --json jobs --jq '[.jobs[] | select(.name == "check")][0].conclusion // "missing"')"
	[ "$c" = failure ] && pass "probe PR #$PROBE: check failed" || fail "probe PR #$PROBE: check failed (got $c)"
	# The run's log archive, read by ci-drift-log.sh: gh run view --log-failed splits logs by
	# step file names, which GitHub's archive no longer has (one file per job since late 2026).
	if ! gh api "repos/$REPO/actions/runs/$run/logs" >"$tmp/probe-logs.zip" 2>"$tmp/probe-logs.err"; then
		fail "probe PR #$PROBE: download the run's log archive ($(head -1 "$tmp/probe-logs.err"))"
	elif bash scripts/ci-drift-log.sh "$tmp/probe-logs.zip" >"$tmp/probe-drift.txt" 2>&1; then
		pass "probe PR #$PROBE: the contract-drift step (make gen-check) failed on internal/internalapi/types.gen.go"
		echo
		echo "# the drift it reported"
		cat "$tmp/probe-drift.txt"
	else
		fail "probe PR #$PROBE: the contract-drift step (make gen-check) failed on internal/internalapi/types.gen.go"
		cat "$tmp/probe-drift.txt"
	fi
fi

echo
if [ "$fails" = 0 ]; then echo "S4.6 Verify: PASS"; else echo "S4.6 Verify: FAIL ($fails)"; fi
exit $((fails > 0))
