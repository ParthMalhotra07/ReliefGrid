#!/usr/bin/env bash
# Tear down the S5.1 GKE session and prove nothing is left billing (plan §8).
#
#   make gke-teardown     (scripts/gke-teardown.sh)
#
# Some billable resources are made by Kubernetes, not Terraform: the PD disks behind the PVCs
# and the LoadBalancer's forwarding rule, health check and firewall rules. Destroying the
# cluster first would orphan them, still billing. So, in order:
#   1. the target from Terraform's outputs; you type the project ID to confirm
#   2. evidence: Terraform outputs (no secrets), helm releases, PVCs and their volumes
#   3. helm uninstall of the app and storage (and tracing, Chaos Mesh if present); delete the
#      namespaces and the storage PVCs (reclaim Delete: the CSI driver deletes the disks)
#   4. wait until no PVC, no PV and no LoadBalancer Service is left; if they do not go within
#      WAIT_SECONDS, stop here: nothing is destroyed, the cluster stays for a look
#   5. terraform destroy with the session's var file
#   6. read-only listings scoped to the project; any leftover fails the script, except the
#      state bucket (STATE_BUCKET) and the project's own default network and its rules
# Only steps 3 and 5 change anything; every gcloud call is get-credentials or a listing.
#
# Environment: GKE_TFVARS (the session's *.tfvars, required), STATE_BUCKET (the Terraform state
# bucket, allowed to remain), TEARDOWN_CONFIRM (the project ID: skips the question),
# GKE_TF_DIR (infra/gcp), NAMESPACE (reliefgrid), KUBE_CONTEXT (if set, must be Terraform's),
# WAIT_SECONDS (600), POLL_SECONDS (10), EVIDENCE_DIR (docs/evidence/S5.1/teardown-<UTC time>).
set -uo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT" || exit 1

TF_DIR="${GKE_TF_DIR:-infra/gcp}"
NS="${NAMESPACE:-reliefgrid}"
WAIT_SECONDS="${WAIT_SECONDS:-600}"
POLL_SECONDS="${POLL_SECONDS:-10}"
EVIDENCE_DIR="${EVIDENCE_DIR:-docs/evidence/S5.1/teardown-$(date -u +%Y%m%dT%H%M%SZ)}"
STATE_BUCKET="${STATE_BUCKET:-}"

die() {
	echo "gke-teardown: $*" >&2
	exit 1
}

for t in terraform gcloud helm kubectl jq; do command -v "$t" >/dev/null || die "$t is not installed"; done
[ -n "${GKE_TFVARS:-}" ] && [ -f "$GKE_TFVARS" ] || die "GKE_TFVARS: the session's var file '${GKE_TFVARS:-}' is missing (terraform destroy needs it)"
TFVARS="$(realpath "$GKE_TFVARS")"

# --- 1. the target ----------------------------------------------------------------------------
tf="$(terraform -chdir="$TF_DIR" output -json)" || die "terraform output failed in $TF_DIR"
out() { jq -r --arg k "$1" '.[$k].value // ""' <<<"$tf"; }
PROJECT="$(out project_id)" REGION="$(out region)" ZONE="$(out zone)" CLUSTER="$(out cluster_name)" CTX="$(out kube_context)"
for v in PROJECT REGION ZONE CLUSTER CTX; do
	[ -n "${!v}" ] || die "Terraform output for $v is empty: nothing applied in this state?"
done
if [ -n "${KUBE_CONTEXT:-}" ] && [ "$KUBE_CONTEXT" != "$CTX" ]; then
	die "KUBE_CONTEXT is $KUBE_CONTEXT, but Terraform's cluster is $CTX; refusing"
fi
echo "gke-teardown: this uninstalls everything in cluster $CLUSTER ($ZONE), then destroys every"
echo "gke-teardown: Terraform resource of project $PROJECT (var file $TFVARS)."
if [ "${TEARDOWN_CONFIRM:-}" != "$PROJECT" ]; then
	read -r -p "Type the project ID to confirm: " TEARDOWN_CONFIRM || true
	[ "${TEARDOWN_CONFIRM:-}" = "$PROJECT" ] || die "not confirmed; nothing was changed"
fi

# --- 2. evidence ------------------------------------------------------------------------------
mkdir -p "$EVIDENCE_DIR"
jq 'with_entries(.value |= .value)' <<<"$tf" >"$EVIDENCE_DIR/terraform-outputs.json"
gcloud container clusters get-credentials "$CLUSTER" --zone "$ZONE" --project "$PROJECT" >/dev/null ||
	die "get-credentials for $CLUSTER failed: if the cluster is already gone, check the listings by hand before destroying"
k() { kubectl --context "$CTX" --request-timeout=20s "$@"; }
helm --kube-context "$CTX" list -A >"$EVIDENCE_DIR/helm-releases.txt" 2>&1
k get pvc -A -o wide >"$EVIDENCE_DIR/pvcs.txt" 2>&1
k get pv -o wide >"$EVIDENCE_DIR/pvs.txt" 2>&1

# --- 3. uninstall -----------------------------------------------------------------------------
echo "gke-teardown: uninstalling the releases"
for rel in reliefgrid storage tracing; do
	helm --kube-context "$CTX" uninstall "$rel" -n "$NS" --ignore-not-found --wait --timeout 5m
done
helm --kube-context "$CTX" uninstall chaos-mesh -n chaos-mesh --ignore-not-found --wait --timeout 5m
# StatefulSet PVCs outlive the release: delete them (and so their disks).
k delete pvc -n "$NS" --all --ignore-not-found --wait=false
k delete namespace "$NS" chaos-mesh --ignore-not-found --wait=false

# --- 4. wait for the disks and load balancers to go -------------------------------------------
left() { # the PVCs, PVs and LoadBalancer Services still there, one "<kind> <ns>/<name>" per line
	k get pvc,pv -A -o json 2>/dev/null | jq -r '.items[] | "\(.kind) \(.metadata.namespace // "-")/\(.metadata.name)"'
	k get services -A -o json 2>/dev/null | jq -r '.items[] | select(.spec.type == "LoadBalancer") | "\(.kind) \(.metadata.namespace)/\(.metadata.name)"'
}
echo "gke-teardown: waiting up to ${WAIT_SECONDS}s for PVCs, PVs and LoadBalancers to be gone"
deadline=$((SECONDS + WAIT_SECONDS))
while :; do
	remaining="$(left)"
	[ -z "$remaining" ] && break
	if [ "$SECONDS" -ge "$deadline" ]; then
		echo "$remaining" >"$EVIDENCE_DIR/stuck.txt"
		die "still present after ${WAIT_SECONDS}s, so the cluster was NOT destroyed (it would orphan their disks or forwarding rules):
$remaining"
	fi
	sleep "$POLL_SECONDS"
done

# --- 5. destroy -------------------------------------------------------------------------------
echo "gke-teardown: terraform destroy"
terraform -chdir="$TF_DIR" destroy -auto-approve -input=false -var-file="$TFVARS" ||
	die "terraform destroy failed; rerun it, then this script's checks"

# --- 6. leftovers, read-only ------------------------------------------------------------------
echo "gke-teardown: checking project $PROJECT for anything left"
found=0
check() { # check <what> <allowed regex or ''> <gcloud args...>
	local what="$1" allow="$2" items
	shift 2
	items="$(gcloud "$@" --project="$PROJECT" --format='value(name)' 2>&1)" || {
		echo "LEFT? $what: the listing failed: $items"
		found=1
		return
	}
	[ -n "$allow" ] && items="$(grep -Ev "$allow" <<<"$items")"
	items="$(grep -v '^[[:space:]]*$' <<<"$items")"
	if [ -n "$items" ]; then
		echo "LEFT  $what: $(tr '\n' ' ' <<<"$items")"
		found=1
	else
		echo "none  $what"
	fi
}
check "GKE clusters" "" container clusters list
check "VM instances" "" compute instances list
check "disks" "" compute disks list
check "forwarding rules" "" compute forwarding-rules list
check "target pools" "" compute target-pools list
check "backend services" "" compute backend-services list
check "health checks" "" compute health-checks list
check "IP addresses" "" compute addresses list
check "Cloud Routers/NAT" "" compute routers list
check "firewall rules" '^default-' compute firewall-rules list
check "networks" '^default$' compute networks list
check "Artifact Registry repositories" "" artifacts repositories list --location="$REGION"
bucket_allow=""
[ -n "$STATE_BUCKET" ] && bucket_allow="^(gs://)?${STATE_BUCKET}/?$"
check "storage buckets" "$bucket_allow" storage buckets list
if [ "$found" != 0 ]; then
	die "resources are left in project $PROJECT (above); they may still bill"
fi
echo "gke-teardown: nothing left in project $PROJECT$([ -n "$STATE_BUCKET" ] && echo " but the state bucket $STATE_BUCKET"). Check the billing report tomorrow."
