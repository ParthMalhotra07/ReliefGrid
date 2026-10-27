#!/usr/bin/env bash
# placement-check: every storage shard has its replicas on distinct data workers.
# Run before any node-loss demo (plan §24): one replica of every shard per data worker,
# so losing a worker costs each shard one replica, never two.
#
#   scripts/placement-check.sh            (make placement-check)
#
# For each shard N in SHARDS, pods storage-shard-N-0..REPLICAS-1 must all exist, be
# scheduled, run on nodes labelled role=data, and run on pairwise different nodes.
# Prints one line per shard; exits 1 naming the shard and the pod or worker at fault.
# PLACEMENT_PODS_JSON / PLACEMENT_NODES_JSON: check these saved `kubectl get pods|nodes
# -o json` files instead of the live cluster (the Verify checks doctored copies this way).
set -uo pipefail

CTX="${KUBE_CONTEXT:-kind-reliefgrid}"
NS="${NAMESPACE:-reliefgrid}"
SHARDS="${SHARDS:-0 1 2}"
REPLICAS="${REPLICAS:-3}"

command -v jq >/dev/null || { echo "placement-check: jq is required" >&2; exit 2; }
if [ -n "${PLACEMENT_PODS_JSON:-}" ]; then
	pods="$(cat "$PLACEMENT_PODS_JSON")" || exit 2
else
	pods="$(kubectl --context "$CTX" -n "$NS" get pods -l app.kubernetes.io/component=storagenode -o json)" || exit 2
fi
if [ -n "${PLACEMENT_NODES_JSON:-}" ]; then
	nodes="$(cat "$PLACEMENT_NODES_JSON")" || exit 2
else
	nodes="$(kubectl --context "$CTX" get nodes -o json)" || exit 2
fi
data_nodes="$(jq -r '.items[] | select(.metadata.labels.role == "data") | .metadata.name' <<<"$nodes")"
[ -n "$data_nodes" ] || { echo "placement-check: no node is labelled role=data" >&2; exit 1; }

bad=0
for s in $SHARDS; do
	problems="" placed="" seen=""
	declare -A on=()
	for i in $(seq 0 $((REPLICAS - 1))); do
		p="storage-shard-$s-$i"
		item="$(jq -c --arg p "$p" '.items[] | select(.metadata.name == $p)' <<<"$pods")"
		if [ -z "$item" ]; then
			problems+="; $p is missing"
			continue
		fi
		node="$(jq -r '.spec.nodeName // empty' <<<"$item")"
		if [ -z "$node" ]; then
			problems+="; $p is not scheduled"
			continue
		fi
		grep -qx -- "$node" <<<"$data_nodes" || problems+="; $p is on $node, which is not a data worker (role=data)"
		if [ -n "${on[$node]:-}" ]; then
			problems+="; ${on[$node]} and $p share worker $node"
		fi
		on[$node]="$p"
		placed+=" $p@$node"
	done
	unset on
	if [ -n "$problems" ]; then
		echo "shard $s: FAIL${problems#;} (placed:${placed:- none})"
		bad=1
	else
		echo "shard $s: OK$placed"
	fi
done
[ "$bad" = 0 ] || { echo "placement-check: FAILED" >&2; exit 1; }
echo "placement-check: every shard has $REPLICAS replicas on distinct data workers"
