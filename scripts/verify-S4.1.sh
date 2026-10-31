#!/usr/bin/env bash
# verify-S4.1.sh: the S4.1 Verify gate for the read fence (M5).
#
#   scripts/verify-S4.1.sh                 # seeds 1..20
#   SEEDS="7 8 9" scripts/verify-S4.1.sh   # chosen seeds
#
# Runs the v3.1 §14 read-fence suite in internal/raftstore once per seed
# with go test -race -count=1 -shuffle=<seed> (the seed fixes the test
# order; rerun a failing seed with the same command), then the
# linearizable routes in internal/storagenode and cmd/storagenode once.
# Every seed must pass before STRONG_READ_FENCE=fence may stay the default;
# otherwise ship STRONG_READ_FENCE=barrier.
#
# Writes docs/evidence/S4.1/fence-seeds-<UTC time>.txt with the result, the
# code tree hash and whether the tree was dirty, and one line per seed.
# Exits 0 only if every run passed.
set -uo pipefail
cd "$(dirname "$0")/.."

SEEDS="${SEEDS:-$(seq -s ' ' 1 20)}"
SUITE='^(TestFencedRead|TestStaleReadUnaffectedByReadFence|TestOpenRejectsUnknownReadFence|TestStrongReadWaitsForFSMNotRaftAppliedIndex|TestNewLeaderNotReadyUntilOwnTermCommits)'
EVIDENCE="docs/evidence/S4.1/fence-seeds-$(date -u +%Y%m%dT%H%M%SZ).txt"
tmp="$(mktemp -d)"
trap 'rm -rf "$tmp"' EXIT

passes=0 failures=0
: >"$tmp/results"
for seed in $SEEDS; do
	start=$SECONDS
	if go test -race -count=1 -shuffle="$seed" -run "$SUITE" ./internal/raftstore/ >"$tmp/seed-$seed.log" 2>&1; then
		passes=$((passes + 1))
		echo "seed $seed: PASS ($((SECONDS - start)) s)" | tee -a "$tmp/results"
	else
		failures=$((failures + 1))
		echo "seed $seed: FAIL ($((SECONDS - start)) s): $(grep -E -- '--- FAIL|_test.go:[0-9]+' "$tmp/seed-$seed.log" | head -3 | tr '\n' ' ')" | tee -a "$tmp/results"
	fi
done
if go test -race -count=1 -run 'TestReadConsistency|TestOutboxConsistencyAndLongPoll|TestOtherReadRoutes|TestStorageNodesWriteSurvivesLeaderKill|TestLoadConfigReadFence' \
	./internal/storagenode/ ./cmd/storagenode/ >"$tmp/routes.log" 2>&1; then
	passes=$((passes + 1))
	echo "routes and config: PASS" | tee -a "$tmp/results"
else
	failures=$((failures + 1))
	echo "routes and config: FAIL: $(grep -E -- '--- FAIL|_test.go:[0-9]+' "$tmp/routes.log" | head -3 | tr '\n' ' ')" | tee -a "$tmp/results"
fi

code_paths=(go.mod go.sum cmd internal deploy/helm/storage scripts/verify-S4.1.sh)
code_tree="$(git ls-tree -r HEAD -- "${code_paths[@]}" | git hash-object --stdin)"
code_dirty="$(git status --porcelain -- "${code_paths[@]}" | grep -q . && echo yes || echo no)"
if [ "$failures" -eq 0 ]; then result="PASSED ($passes/$passes runs)"; else result="FAILED ($failures of $((passes + failures)) runs failed)"; fi
mkdir -p "$(dirname "$EVIDENCE")"
{
	echo "# S4.1 read fence: §14 suite, seeded runs"
	echo "result:     $result"
	echo "host:       $(hostname -s)"
	echo "date:       $(date -u +%Y-%m-%dT%H:%M:%SZ)"
	echo "go:         $(go env GOVERSION)"
	echo "seeds:      $SEEDS"
	echo "suite:      $SUITE"
	echo "code tree:  ${code_tree:0:12} (git ls-tree -r HEAD -- ${code_paths[*]} | git hash-object --stdin)"
	echo "code dirty: $code_dirty"
	echo
	echo "## runs"
	cat "$tmp/results"
} 2>&1 | sed 's/[[:space:]]*$//' >"$EVIDENCE"
echo "evidence: $EVIDENCE ($result)"
[ "$failures" -eq 0 ]
