#!/usr/bin/env bash
# make storage-replace SHARD=N POD=<pod>: run storagectl replace (v3.1 §11, M7) as Job
# storage-replace-<pod>, with the bootstrap Job's pod spec (its service account, operator
# label, OPERATOR_TOKEN and MANIFESTS). It only creates the Job: watch it with
#   kubectl -n reliefgrid logs -f job/storage-replace-<pod>
# and run it again to resume an interrupted replacement.
# CRASH_AT=<step> sets STORAGECTL_CRASH_AT on this one Job (the S4.2 Verify only); the chart
# has no way to set it.
set -euo pipefail
: "${SHARD:?SHARD is required}" "${POD:?POD is required}"
CTX="${KUBE_CONTEXT:-kind-reliefgrid}"
NS="${NAMESPACE:-reliefgrid}"
CRASH_AT="${CRASH_AT:-}"
kn() { kubectl --context "$CTX" -n "$NS" "$@"; }

job="$(kn get jobs -l app.kubernetes.io/component=storage-bootstrap --sort-by=.metadata.creationTimestamp -o name | tail -1)"
[ -n "$job" ] || { echo "storage-replace: no bootstrap Job to take the pod spec from (helm upgrade first)" >&2; exit 1; }
if [ -n "$CRASH_AT" ]; then
	echo "storage-replace: !!! CRASH_AT=$CRASH_AT: this Job will stop on purpose after that step (Verify only) !!!" >&2
fi
name="storage-replace-$POD"
kn delete job "$name" --ignore-not-found --wait >/dev/null
kn get "$job" -o json | jq --arg name "$name" --arg shard "$SHARD" --arg pod "$POD" --arg crash "$CRASH_AT" '
	.spec.template as $t
	| {apiVersion: "batch/v1", kind: "Job",
	   metadata: {name: $name, labels: {"app.kubernetes.io/name": "storage", "app.kubernetes.io/component": "storage-replace"}},
	   spec: {backoffLimit: 0, ttlSecondsAfterFinished: 3600,
	     template: {
	       metadata: {labels: ($t.metadata.labels | del(.["controller-uid"], .["batch.kubernetes.io/controller-uid"], .["job-name"], .["batch.kubernetes.io/job-name"]))},
	       spec: ($t.spec | .containers |= map(
	         .command = ["/storagectl", "replace", "--shard", $shard, "--pod", $pod]
	         | .env = ([.env[] | select(.name != "HOLD_SECONDS" and .name != "BOOTSTRAP_TIMEOUT_SECONDS")]
	                   + (if $crash != "" then [{name: "STORAGECTL_CRASH_AT", value: $crash}] else [] end))))}}}' |
	kn apply -f - >/dev/null
echo "job/$name created"
