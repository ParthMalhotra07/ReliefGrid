#!/usr/bin/env bash
# CI smoke test (S4.6): the storage stack on a fresh kind cluster in the runner.
#
#   make ci-smoke      (IMAGES_DIR and EVIDENCE_DIR set; CI's smoke job)
#
# 1. make cluster-up: the five-node cluster of deploy/kind/cluster.yaml, the smallest the
#    storage chart schedules on (one replica of a shard per data worker; the coordinator on
#    the infra worker).
# 2. kind load image-archive for every IMAGES_DIR/*.tar: the images the images job built,
#    never a rebuild, so this tests exactly what was built.
# 3. helm install deploy/helm/storage --wait --wait-for-jobs (the bootstrap Job forms the
#    three Raft groups).
# 4. Wait until each shard has exactly one Leader that all three replicas name
#    (LEADER_TIMEOUT seconds, default 180).
# 5. make demo-4: seeded keys, distribution and locate, with its own evidence.
# On every exit, diagnostics (pods, events, descriptions, logs) and per-step timings are
# written next to the demo's evidence, in EVIDENCE_DIR/SMOKE_RUN_ID/. The exit status is 0
# only if every step passed.
#
# Environment: IMAGES_DIR, EVIDENCE_DIR (required); SMOKE_RUN_ID (default ci-local-<UTC>);
# CLUSTER (reliefgrid); NAMESPACE (reliefgrid); INSTALL_TIMEOUT (10m); LEADER_TIMEOUT (180).
set -uo pipefail

: "${IMAGES_DIR:?IMAGES_DIR: directory of image archives (*.tar)}"
: "${EVIDENCE_DIR:?EVIDENCE_DIR: where the evidence goes}"
CLUSTER="${CLUSTER:-reliefgrid}"
NS="${NAMESPACE:-reliefgrid}"
CTX="kind-$CLUSTER"
RUN_ID="${SMOKE_RUN_ID:-ci-local-$(date -u +%Y%m%dT%H%M%SZ)}"
INSTALL_TIMEOUT="${INSTALL_TIMEOUT:-10m}"
LEADER_TIMEOUT="${LEADER_TIMEOUT:-180}"
OUT="$EVIDENCE_DIR/$RUN_ID"

cd "$(dirname "$0")/.." || exit 1
mkdir -p "$OUT/diagnostics" || exit 1
t0="$SECONDS"

kn() { kubectl --context "$CTX" -n "$NS" "$@"; }

# step <description> <command...>: run it, record its duration, stop the smoke test on failure
step() {
	local desc="$1" s="$SECONDS" rc
	shift
	echo "smoke: $desc"
	"$@"
	rc=$?
	printf '%-50s %4d s  exit %d\n' "$desc" "$((SECONDS - s))" "$rc" >>"$OUT/timings.txt"
	if [ "$rc" != 0 ]; then
		echo "smoke: FAILED: $desc (exit $rc, after $((SECONDS - t0)) s)" >&2
		exit 1
	fi
}

load_images() {
	local tar n=0
	for tar in "$IMAGES_DIR"/*.tar; do
		[ -e "$tar" ] || break
		sha256sum "$tar" >>"$OUT/images.txt"
		kind load image-archive --name "$CLUSTER" "$tar" || return 1
		n=$((n + 1))
	done
	[ "$n" -gt 0 ] || { echo "smoke: no image archives in $IMAGES_DIR" >&2; return 1; }
}

# leader_of <shard>: the leader's pod if exactly one replica is Leader and all three name it
leader_of() {
	local i s state lead="" n=0 ids=""
	for i in 0 1 2; do
		s="$(timeout 10 kubectl --context "$CTX" -n "$NS" exec "storage-shard-$1-$i" -c storagenode -- /storagectl status 2>/dev/null)" || return 1
		state="$(jq -r '.state // empty' <<<"$s")" || return 1
		ids+="$(jq -r '.leaderId // empty' <<<"$s")"$'\n'
		[ "$state" = Leader ] && { n=$((n + 1)) lead="storage-shard-$1-$i"; }
	done
	[ "$n" = 1 ] && [ "$(grep -c . <<<"$ids")" = 3 ] && [ "$(grep . <<<"$ids" | sort -u | wc -l)" = 1 ] && echo "$lead"
}

wait_leaders() {
	local shard lead deadline=$((SECONDS + LEADER_TIMEOUT))
	for shard in 0 1 2; do
		until lead="$(leader_of "$shard")"; do
			if [ "$SECONDS" -ge "$deadline" ]; then
				echo "smoke: shard $shard has no single agreed leader after ${LEADER_TIMEOUT} s" >&2
				return 1
			fi
			sleep 2
		done
		echo "shard $shard: leader $lead" | tee -a "$OUT/leaders.txt"
	done
}

demo4() { DEMO_RUN_ID="$RUN_ID" EVIDENCE_ROOT="$EVIDENCE_DIR" make -s demo-4; }

# diagnostics: everything needed to debug a red run from the artifact alone. Best effort.
diagnostics() {
	local d="$OUT/diagnostics" p
	echo "smoke: writing diagnostics to $d"
	kn get pods -o wide >"$d/pods.txt" 2>&1
	kn get events --sort-by=.lastTimestamp >"$d/events.txt" 2>&1
	kn describe pods >"$d/describe-pods.txt" 2>&1
	kn get jobs,statefulsets,deployments,pvc,configmaps -o wide >"$d/resources.txt" 2>&1
	kubectl --context "$CTX" get nodes -o wide >"$d/nodes.txt" 2>&1
	helm --kube-context "$CTX" status storage -n "$NS" >"$d/helm-status.txt" 2>&1
	for p in $(kn get pods -o name 2>/dev/null); do
		p="${p#pod/}"
		kn logs "$p" --all-containers --timestamps >"$d/logs-$p.txt" 2>&1
		kn logs "$p" --all-containers --timestamps --previous >"$d/logs-$p-previous.txt" 2>/dev/null || rm -f "$d/logs-$p-previous.txt"
	done
	printf '%-50s %4d s\n' "total" "$((SECONDS - t0))" >>"$OUT/timings.txt"
}
trap diagnostics EXIT

step "create the kind cluster" make -s cluster-up
step "load the image archives into kind" load_images
step "install deploy/helm/storage" helm --kube-context "$CTX" install storage deploy/helm/storage -n "$NS" --create-namespace \
	--wait --wait-for-jobs --timeout "$INSTALL_TIMEOUT"
step "wait for three groups with leaders" wait_leaders
step "demo 4: seeded keys, distribution, locate" demo4
echo "smoke: PASS in $((SECONDS - t0)) s (evidence: $OUT)"
