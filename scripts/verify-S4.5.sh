#!/usr/bin/env bash
# verify-S4.5.sh: the S4.5 Verify gate for the Merkle audit.
#
#   scripts/verify-S4.5.sh
#
# Go part (no cluster): runs every S4.5 suite with go test -race -count=1:
#   - internal/fsm/merkle: encoding, domain separation, bucket tree, diffs
#   - internal/fsm: equal roots across replicas and a VACUUM INTO image; the
#     negative control (eight single-row corruptions, each pinpointed to
#     table, key, kind and pod); the exact-index guard; one read
#     transaction under a concurrent apply; schema column coverage
#   - internal/raftstore: a follower corrupted outside Raft is repaired by
#     a rebuild from its log; a follower that installed a snapshot has the
#     leader's root
#   - internal/storagenode: /mgmt/merkle, and the audit over three real
#     replicas (equal, then a corrupted follower pinpointed)
#   - internal/storagenode/audit and cmd/storagectl: verdicts and exit codes,
#     never a digest without an exact index or at fsm.AnyIndex
# then BenchmarkMerkle (a full digest of a file database), recording the
# measured cost per row.
#
# Kind part (equal roots on every shard of a deployed cluster after a seeded
# load): not in this script yet; it is added after the 4 Nov gate, when the
# cluster is free. The evidence file says so.
#
# Writes docs/evidence/S4.5/verify-<UTC time>.txt with the result, the code
# tree hash and whether the tree was dirty, one line per suite and the
# benchmark. Exits 0 only if every suite passed.
set -uo pipefail
cd "$(dirname "$0")/.."

EVIDENCE="docs/evidence/S4.5/verify-$(date -u +%Y%m%dT%H%M%SZ).txt"
tmp="$(mktemp -d)"
trap 'rm -rf "$tmp"' EXIT

suites=(
	"merkle package|./internal/fsm/merkle/|."
	"fsm digest and negative control|./internal/fsm/|Merkle"
	"raft repair and snapshot install|./internal/raftstore/|^(TestDivergentFollowerIsRepairedByRebuild|TestSnapshotInstalledFollowerHasLeaderRoot)$"
	"mgmt route and real-replica audit|./internal/storagenode/|Merkle|^TestAuditOverRealReplicas$"
	"audit client|./internal/storagenode/audit/...|."
	"storagectl merkle and audit|./cmd/storagectl/|^(TestMerkleCommand|TestAuditCommand)"
)

passes=0 failures=0
: >"$tmp/results"
for s in "${suites[@]}"; do
	name="${s%%|*}" rest="${s#*|}"
	pkg="${rest%%|*}" run="${rest#*|}"
	start=$SECONDS
	log="$tmp/$(echo "$name" | tr ' ' '-').log"
	if go test -race -count=1 -run "$run" "$pkg" >"$log" 2>&1; then
		passes=$((passes + 1))
		echo "$name: PASS ($((SECONDS - start)) s)" | tee -a "$tmp/results"
	else
		failures=$((failures + 1))
		echo "$name: FAIL ($((SECONDS - start)) s): $(grep -E -- '--- FAIL|_test.go:[0-9]+|panic' "$log" | head -3 | tr '\n' ' ')" | tee -a "$tmp/results"
	fi
done

bench_ok=yes
if ! go test -run '^$' -bench '^BenchmarkMerkle$' -benchtime 10x ./internal/fsm/ >"$tmp/bench.log" 2>&1; then
	bench_ok=no
	failures=$((failures + 1))
	echo "benchmark: FAIL: $(tail -3 "$tmp/bench.log" | tr '\n' ' ')" | tee -a "$tmp/results"
fi
cpu="$(sed -n 's/^cpu: //p' "$tmp/bench.log")"
# "BenchmarkMerkle/entities=20000-8  10  179214418 ns/op  50000 rows" -> rows, ms per digest, µs per row
bench_lines="$(awk '/^BenchmarkMerkle\// {
	for (i = 1; i <= NF; i++) { if ($(i+1) == "ns/op") ns = $i; if ($(i+1) == "rows") rows = $i }
	printf "  %s rows: %.1f ms per full digest, %.2f µs per row\n", rows, ns / 1e6, ns / 1e3 / rows }' "$tmp/bench.log")"
[ "$bench_ok" = yes ] && echo "benchmark:" && echo "$bench_lines"

code_paths=(go.mod go.sum cmd internal scripts/verify-S4.5.sh)
code_tree="$(git ls-tree -r HEAD -- "${code_paths[@]}" | git hash-object --stdin)"
code_dirty="$(git status --porcelain -- "${code_paths[@]}" | grep -q . && echo yes || echo no)"
if [ "$failures" -eq 0 ]; then result="PASSED ($passes/$passes suites, benchmark recorded)"; else result="FAILED ($failures failed)"; fi
mkdir -p "$(dirname "$EVIDENCE")"
{
	echo "# S4.5 Merkle audit: Go part of the Verify"
	echo "result:     $result"
	echo "host:       $(hostname -s)"
	echo "date:       $(date -u +%Y-%m-%dT%H:%M:%SZ)"
	echo "go:         $(go env GOVERSION)"
	echo "cpu:        ${cpu:-unknown}"
	echo "code tree:  ${code_tree:0:12} (git ls-tree -r HEAD -- ${code_paths[*]} | git hash-object --stdin)"
	echo "dirty:      $code_dirty"
	echo "commit:     $(git rev-parse --short HEAD)"
	echo
	echo "## suites (go test -race -count=1)"
	cat "$tmp/results"
	echo
	echo "## BenchmarkMerkle (file database, full digest, -benchtime 10x; measured, not estimated)"
	echo "${bench_lines:-  not recorded}"
	echo
	echo "## kind part"
	echo "pending: equal roots on every shard of a deployed cluster runs after the 4 Nov gate (the cluster is in use)."
} >"$EVIDENCE"
echo "evidence: $EVIDENCE"
echo "S4.5 Go part: $result"
[ "$failures" -eq 0 ]
