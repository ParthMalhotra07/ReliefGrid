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
# Writes docs/evidence/S4.5/verify-<UTC time>.txt with the result, the code
# tree hash and whether the tree was dirty, one line per suite and the
# benchmark. Exits 0 only if every suite passed.
#
#   scripts/verify-S4.5.sh --kind
#
# Kind part: equal roots on every shard of a deployed cluster after a seeded
# load. Needs the local kind cluster with deploy/helm/storage installed and the
# tools image loaded (make demo-reset); it never recreates the cluster.
#   1. a seeded load: invcheck load as a Job (chaos/scripts/lib.sh), prefix
#      s45-seed<SEED> (SEED, default 1), WORKERS writers x KEYS keys x OPS
#      writes each; the key set and every value are a function of the seed.
#      Seeds are one per cluster: a rerun on the same cluster needs a new SEED.
#   2. invcheck verify: nothing acknowledged was lost.
#   3. quiesce: the load has finished; every Deployment except the
#      coordinator (the app's API and matcher, which hosts the relay) is scaled
#      to 0 and its pods are gone; then invcheck check
#      (claim invariants, and no write committed during its walk). On any
#      exit, including a failure, Ctrl-C or SIGTERM, an EXIT trap scales those
#      Deployments back to their replicas in their Helm release.
#   4. storagectl audit per shard, as an operator Job (audit_job) against the
#      three replicas' management endpoints: verdict equal, exit 0, three pods
#      at one appliedIndex with one root, and that index past the load.
# Writes docs/evidence/S4.5/kind-<UTC time>.txt with each shard's audit JSON.
# Exits 0 only if every check passed.
set -uo pipefail
cd "$(dirname "$0")/.."

# audit_job <shard> <storagectl audit args...>: storagectl audit as a Job from the storage
# bootstrap Job's pod spec (storage image, operator label, OPERATOR_TOKEN), the same way
# scripts/storage-replace.sh runs storagectl: the shards' NetworkPolicy admits the management
# port (8081) only from operator pods. Prints the audit's JSON; returns storagectl's exit code.
audit_job() {
	local shard="$1" name="s45-audit-shard-$1" boot i code
	shift
	boot="$(kn get jobs -l app.kubernetes.io/component=storage-bootstrap --sort-by=.metadata.creationTimestamp -o name | tail -1)"
	[ -n "$boot" ] || { echo "no bootstrap Job to take the pod spec from" >&2; return 99; }
	kn delete job "$name" --ignore-not-found --wait >/dev/null 2>&1
	kn get "$boot" -o json | jq --arg name "$name" --argjson args "$(printf '%s\n' "$@" | jq -R . | jq -sc .)" '.spec.template as $t
		| {apiVersion: "batch/v1", kind: "Job",
		   metadata: {name: $name, labels: {"app.kubernetes.io/name": "storage", "app.kubernetes.io/component": "storage-audit"}},
		   spec: {backoffLimit: 0, ttlSecondsAfterFinished: 3600,
		     template: {
		       metadata: {labels: ($t.metadata.labels | del(.["controller-uid"], .["batch.kubernetes.io/controller-uid"], .["job-name"], .["batch.kubernetes.io/job-name"]))},
		       spec: ($t.spec | .containers |= map(
		         .command = (["/storagectl", "audit"] + $args)
		         | .env = [.env[] | select(.name == "OPERATOR_TOKEN")]))}}}' |
		kn apply -f - >/dev/null || { echo "could not create Job $name" >&2; return 99; }
	for ((i = 0; i < 720; i++)); do
		job_done "$name" && break
		sleep 0.5
	done
	job_done "$name" || { echo "Job $name did not finish within 6 minutes" >&2; return 98; }
	kn logs "job/$name" --tail=-1 2>/dev/null | awk '/^\{/ { f = 1 } f { print } f && /^\}/ { exit }'
	kn logs "job/$name" --tail=-1 2>/dev/null | grep '^storagectl:' >&2
	code="$(job_exit "$name")"
	return "${code:-97}"
}

# helm_replicas <release>: "<deployment> <replicas>" for each Deployment in the release's
# manifest (replicas left out of a manifest mean 1, the Kubernetes default).
helm_replicas() {
	helm --kube-context "$CTX" -n "$NS" get manifest "$1" | awk '
		function emit() { if (kind == "Deployment" && name != "") print name, (reps == "" ? 1 : reps) }
		/^---/ { emit(); kind = name = reps = ""; meta = 0; next }
		/^kind:/ { kind = $2 }
		/^metadata:/ { meta = 1; next }
		/^[^ #]/ { meta = 0 }
		meta && /^  name:/ && name == "" { name = $2; gsub(/["\047]/, "", name) }
		/^  replicas:/ { reps = $2 }
		END { emit() }'
}

# restore_app: the EXIT trap of the kind part. Scales every Deployment the quiesce planned to
# stop ($kind_tmp/scaled: "<deployment> <replicas>", written before the first scale-down) back
# to its replicas in its Helm release, so a failed, interrupted or killed run never leaves the
# app at 0. Runs on any exit; INT, TERM and HUP are turned into exits so it runs then too.
restore_app() {
	local d n
	[ -n "${kind_tmp:-}" ] || return 0
	if [ -s "$kind_tmp/scaled" ]; then
		while read -r d n; do
			if kn scale "$d" --replicas "$n" >/dev/null 2>&1; then
				echo "restored $d to $n replicas" >&2
			else
				echo "WARNING: could not restore $d to $n replicas: run kubectl -n $NS scale $d --replicas $n" >&2
			fi
		done <"$kind_tmp/scaled"
	fi
	rm -rf "$kind_tmp"
}

kind_part() {
	local SEED="${SEED:-1}" WORKERS="${WORKERS:-4}" KEYS="${KEYS:-4}" OPS="${OPS:-50}"
	EXPERIMENT=s45 seed="$SEED"
	# shellcheck source=chaos/scripts/lib.sh
	. chaos/scripts/lib.sh
	local evidence="docs/evidence/S4.5/kind-$(date -u +%Y%m%dT%H%M%SZ).txt"
	local passes=0 failures=0 prefix="s45-seed$SEED" load_job="s45-load-seed$SEED" shard out code idx
	kind_tmp="$(mktemp -d)"
	local tmp="$kind_tmp"
	work="$tmp" && : >"$work/notes"
	: >"$tmp/results" && : >"$tmp/audits" && : >"$tmp/scaled"
	run_id="$load_job"
	trap restore_app EXIT
	trap 'exit 130' INT
	trap 'exit 143' TERM HUP
	pass() { echo "PASS  $1" | tee -a "$tmp/results"; passes=$((passes + 1)); }
	fail() { echo "FAIL  $1${2:+: $2}" | tee -a "$tmp/results"; failures=$((failures + 1)); }

	kind get clusters 2>/dev/null | grep -qx "$KIND_CLUSTER" || { echo "kind part: no kind cluster $KIND_CLUSTER (make demo-reset)" >&2; return 2; }
	for shard in 0 1 2; do
		kn get statefulset "storage-shard-$shard" >/dev/null 2>&1 || { echo "kind part: storage-shard-$shard is not deployed (make demo-reset)" >&2; return 2; }
	done

	# The Job helpers poll for minutes. They run in this shell with their output in a file,
	# never inside $(...): bash runs a trap only once the foreground command returns, so a
	# SIGTERM to this process during a subshell would wait out the whole poll before the
	# trap restores the app.
	echo "== 1. seeded load ($prefix: $WORKERS writers x $KEYS keys x $OPS writes)"
	ensure_pvc || { echo "kind part: could not create PVC $PVC" >&2; return 2; }
	local before=()
	for shard in 0 1 2; do before[shard]="$(st "storage-shard-$shard-0" | jq -r '.appliedIndex // 0')"; done
	run_job "$load_job" load --coordinator "$COORD_URL" --ledger "/ledger/$load_job.json" --prefix "$prefix" \
		--workers "$WORKERS" --keys "$KEYS" --ops "$OPS" --timeout 600s >"$tmp/out"
	code=$?
	kn logs "job/$load_job" --tail=-1 >"$tmp/load.log" 2>&1
	[ "$code" -eq 0 ] && pass "seeded load ran to completion (exit 0)" || fail "seeded load" "exit $code: $(kn logs "job/$load_job" --tail=5 2>&1 | tr '\n' ' ')"
	run_job "s45-verify-seed$SEED" verify --coordinator "$COORD_URL" --ledger "/ledger/$load_job.json" --json >"$tmp/out"
	code=$? out="$(cat "$tmp/out")"
	echo "$out" >"$tmp/verify.json"
	[ "$code" -eq 0 ] && pass "invcheck verify: nothing acknowledged was lost ($(jq -c '{acked: .ledger.acked, unknown: .ledger.unknown, lost: (.verdict.lost | length)}' <<<"$out" 2>/dev/null))" ||
		fail "invcheck verify" "exit $code: $(head -c 400 <<<"$out")"

	echo "== 2. quiesce"
	# The plan first: each app Deployment and its replicas in its Helm release (by the
	# meta.helm.sh/release-name annotation). The trap restores that, not the count seen now,
	# which an earlier broken run may have left at 0.
	local d n rel plan
	plan="$(kn get deployments -o json | jq -r '.items[] | select(.metadata.name != "storage-coordinator")
		| [.metadata.name, (.metadata.annotations["meta.helm.sh/release-name"] // "-"), (.spec.replicas // 1)] | @tsv')" ||
		{ echo "kind part: could not list the Deployments to quiesce" >&2; return 2; }
	: >"$tmp/helm"
	for rel in $(cut -f2 <<<"$plan" | sort -u); do
		[ "$rel" = - ] || helm_replicas "$rel" >>"$tmp/helm" || { echo "kind part: could not read Helm release $rel" >&2; return 2; }
	done
	local name want
	while IFS=$'\t' read -r name rel n; do
		[ -n "$name" ] || continue
		want="$(awk -v d="$name" '$1 == d { print $2; exit }' "$tmp/helm")"
		if [ -z "$want" ]; then
			echo "kind part: $name is not in a Helm release; it will be restored to its current $n replicas" >&2
			want="$n"
		fi
		echo "deployment.apps/$name $want" >>"$tmp/scaled"
	done <<<"$plan"
	while read -r d n; do
		kn scale "$d" --replicas 0 >/dev/null || fail "scale $d to 0"
	done <"$tmp/scaled"
	local quiet=yes
	while read -r d n; do
		kn wait --for=delete pod -l "$(kn get "$d" -o json | jq -r '.spec.selector.matchLabels | to_entries | map("\(.key)=\(.value)") | join(",")')" --timeout=120s >/dev/null 2>&1 || quiet=no
	done <"$tmp/scaled"
	[ "$quiet" = yes ] && pass "quiesced: load finished; $(wc -l <"$tmp/scaled") app Deployment(s) at 0 replicas ($(awk '{sub("deployment.apps/", "", $1); printf "%s%s", s, $1; s=", "}' "$tmp/scaled"))" ||
		fail "quiesce: app pods still running"

	run_job "s45-check-seed$SEED" check --coordinator "$COORD_URL" --json >"$tmp/out"
	code=$? out="$(cat "$tmp/out")"
	echo "$out" >"$tmp/check.json"
	[ "$code" -eq 0 ] && pass "invcheck check: claim invariants clean, no write during the walk (outbox pending per shard $(jq -c "[.report.outbox[]?.pending]" <<<"$out" 2>/dev/null))" || fail "invcheck check" "exit $code: $(head -c 400 <<<"$out")"

	echo "== 3. storagectl audit per shard"
	local pods addrs verdict nidx nroot
	for shard in 0 1 2; do
		addrs=()
		for pod in $(shard_pods "$shard"); do addrs+=(--addr "$pod.storage-shard-$shard.$NS.svc.cluster.local:8081"); done
		audit_job "$shard" "${addrs[@]}" >"$tmp/out" 2>"$tmp/audit-$shard.err"
		code=$? out="$(cat "$tmp/out")"
		{ [ "$shard" = 0 ] || echo; echo "### shard $shard (storagectl audit exit $code)"; echo "${out:-(no output)}"; } >>"$tmp/audits"
		verdict="$(jq -r .verdict <<<"$out" 2>/dev/null)"
		idx="$(jq -r .appliedIndex <<<"$out" 2>/dev/null)"
		pods="$(jq -r '.pods | length' <<<"$out" 2>/dev/null)"
		nidx="$(jq -r '[.pods[].appliedIndex] | unique | length' <<<"$out" 2>/dev/null)"
		nroot="$(jq -r '[.pods[].root] | unique | length' <<<"$out" 2>/dev/null)"
		if [ "$code" -eq 0 ] && [ "$verdict" = equal ] && [ "$pods" = 3 ] && [ "$nidx" = 1 ] && [ "$nroot" = 1 ] &&
			[ "$(jq -r '.pods[0].appliedIndex' <<<"$out")" = "$idx" ] && [ "${idx:-0}" -gt "${before[shard]:-0}" ]; then
			pass "shard $shard: equal, 3 replicas at appliedIndex $idx (was ${before[shard]} before the load), root $(jq -r '.pods[0].root' <<<"$out" | cut -c1-16)"
		else
			fail "shard $shard" "exit $code, verdict ${verdict:-none}, pods ${pods:-?}, distinct indexes ${nidx:-?}, distinct roots ${nroot:-?}, index ${idx:-?} (before ${before[shard]}): $(head -c 300 "$tmp/audit-$shard.err")"
		fi
	done

	local result
	if [ "$failures" -eq 0 ]; then result="PASSED ($passes/$passes checks)"; else result="FAILED ($failures of $((passes + failures)) checks failed)"; fi
	local code_paths=(go.mod go.sum cmd internal loadtest deploy/helm/storage chaos/scripts/lib.sh scripts/verify-S4.5.sh)
	mkdir -p "$(dirname "$evidence")"
	{
		echo "# S4.5 Merkle audit: kind part of the Verify"
		echo "result:     $result"
		echo "host:       $(hostname -s)"
		echo "date:       $(date -u +%Y-%m-%dT%H:%M:%SZ)"
		echo "cluster:    $CTX, namespace $NS, $(k get nodes --no-headers 2>/dev/null | wc -l) nodes"
		echo "images:     $(kn get pod storage-shard-0-0 -o jsonpath='{.status.containerStatuses[?(@.name=="storagenode")].imageID}')"
		echo "code tree:  $(git ls-tree -r HEAD -- "${code_paths[@]}" | git hash-object --stdin | cut -c1-12) (git ls-tree -r HEAD -- ${code_paths[*]} | git hash-object --stdin)"
		echo "dirty:      $(git status --porcelain -- "${code_paths[@]}" | grep -q . && echo yes || echo no)"
		echo "commit:     $(git rev-parse --short HEAD)"
		echo "load:       seed $SEED, prefix $prefix, $WORKERS writers x $KEYS keys x $OPS writes"
		echo
		echo "## checks"
		cat "$tmp/results"
		echo
		echo "## invcheck load (log), verify and check (JSON)"
		cat "$tmp/load.log"
		jq -c 'del(.ledger.errors)' "$tmp/verify.json" 2>/dev/null || cat "$tmp/verify.json"
		jq -c . "$tmp/check.json" 2>/dev/null || cat "$tmp/check.json"
		echo
		echo "## storagectl audit, one per shard"
		cat "$tmp/audits"
	} >"$evidence"
	echo "evidence: $evidence"
	echo "S4.5 kind part: $result"
	[ "$failures" -eq 0 ]
}

if [ "${1:-}" = --kind ]; then
	kind_part
	exit $?
fi

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
	echo "separate run: scripts/verify-S4.5.sh --kind writes docs/evidence/S4.5/kind-<time>.txt"
} >"$EVIDENCE"
echo "evidence: $EVIDENCE"
echo "S4.5 Go part: $result"
[ "$failures" -eq 0 ]
