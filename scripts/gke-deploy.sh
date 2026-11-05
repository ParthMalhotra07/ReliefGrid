#!/usr/bin/env bash
# Deploy ReliefGrid to the S5.1 GKE cluster (D7: by hand; CI only publishes images).
#
#   make gke-deploy     (scripts/gke-deploy.sh)
#
# Order: check every input -> read the Terraform outputs (the target) -> kube credentials ->
# check each image exists in the session's Artifact Registry at its digest -> (app) render the
# app chart and check it has D8's middleware -> storage chart (values-gke.yaml, --wait for the
# bootstrap Job) -> app chart (values-app-gke.yaml, Traefik on the static IP behind the
# allowlist). Evidence (no secrets) goes to EVIDENCE_DIR. It never runs terraform apply and
# never creates anything in Google Cloud itself: Terraform made the cluster, the registry and
# the IP; Kubernetes makes the disks and the load balancer from the charts.
#
# Environment:
#   IMAGE_DIGESTS          image-digests.json from CI's digests job (required)
#   OPERATOR_TOKEN_FILE    the storage operator token, in a file outside the repository (required)
#   GKE_DEPLOY_APP         true (default) or false: storage only, no public URL (the leader-kill subset)
#   With the app (D8, before any public URL):
#   DISPATCHER_TOKEN_FILE  the dispatcher credential, in a file outside the repository
#   ALLOWED_CIDRS          comma-separated source CIDRs for the LoadBalancer; never 0.0.0.0/0 or ::/0
#   APP_VALUES             deploy/helm/reliefgrid/values-app-gke.yaml (A's chart, issue #115)
#   Optional: GKE_TF_DIR (infra/gcp), NAMESPACE (reliefgrid), STORAGE_RELEASE (storage),
#   APP_RELEASE (reliefgrid), KUBE_CONTEXT (if set, must be Terraform's), EVIDENCE_DIR
#   (docs/evidence/S5.1/deploy-<UTC time>), HELM_TIMEOUT (10m).
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

TF_DIR="${GKE_TF_DIR:-infra/gcp}"
NS="${NAMESPACE:-reliefgrid}"
STORAGE_RELEASE="${STORAGE_RELEASE:-storage}"
APP_RELEASE="${APP_RELEASE:-reliefgrid}"
DEPLOY_APP="${GKE_DEPLOY_APP:-true}"
APP_VALUES="${APP_VALUES:-deploy/helm/reliefgrid/values-app-gke.yaml}"
EVIDENCE_DIR="${EVIDENCE_DIR:-docs/evidence/S5.1/deploy-$(date -u +%Y%m%dT%H%M%SZ)}"
HELM_TIMEOUT="${HELM_TIMEOUT:-10m}"
IMAGES=(storagenode coordinator tools api matcher ui)

die() {
	echo "gke-deploy: $*" >&2
	exit 1
}

# secret_file <variable name>: the file exists, is not empty and is outside the repository.
secret_file() {
	local name="$1" f="${!1:-}" abs
	[ -n "$f" ] || die "$name is not set (a file outside the repository holding the secret)"
	[ -s "$f" ] || die "$name: $f is missing or empty"
	abs="$(realpath "$f")"
	case "$abs/" in
	"$ROOT"/*) die "$name: $f is inside the repository; keep secrets outside the repository" ;;
	esac
}

# --- 1. inputs, before any tool runs ---------------------------------------------------------
case "$DEPLOY_APP" in true | false) ;; *) die "GKE_DEPLOY_APP must be true or false" ;; esac
for t in terraform gcloud helm kubectl jq; do command -v "$t" >/dev/null || die "$t is not installed"; done

[ -n "${IMAGE_DIGESTS:-}" ] && [ -f "$IMAGE_DIGESTS" ] || die "IMAGE_DIGESTS: no image-digests.json at '${IMAGE_DIGESTS:-}' (CI's digests artifact)"
declare -A DIGEST
for name in "${IMAGES[@]}"; do
	d="$(jq -r --arg n "$name" '.images[$n] // ""' "$IMAGE_DIGESTS")" || die "image-digests: $IMAGE_DIGESTS is not JSON"
	[[ "$d" =~ ^sha256:[0-9a-f]{64}$ ]] || die "image-digests: $name has no sha256 digest ('$d')"
	DIGEST[$name]="$d"
done
secret_file OPERATOR_TOKEN_FILE

CIDRS=()
if [ "$DEPLOY_APP" = true ]; then
	secret_file DISPATCHER_TOKEN_FILE
	[ -n "${ALLOWED_CIDRS:-}" ] || die "ALLOWED_CIDRS is not set: the public URL needs an IP allowlist (D8)"
	IFS=',' read -r -a CIDRS <<<"$ALLOWED_CIDRS"
	for c in "${CIDRS[@]}"; do
		case "$c" in
		0.0.0.0/0 | ::/0) die "ALLOWED_CIDRS contains $c: that is the whole internet, not an allowlist" ;;
		esac
		[[ "$c" =~ ^[0-9]{1,3}(\.[0-9]{1,3}){3}/[0-9]{1,2}$ || "$c" =~ ^[0-9a-fA-F:]+/[0-9]{1,3}$ ]] ||
			die "ALLOWED_CIDRS: '$c' is not a CIDR"
	done
	[ -f "$APP_VALUES" ] || die "APP_VALUES: $APP_VALUES is missing (values-app-gke.yaml, A's chart: issue #115); GKE_DEPLOY_APP=false deploys storage only"
fi

# --- 2. the target, from Terraform's outputs --------------------------------------------------
tf="$(terraform -chdir="$TF_DIR" output -json)" || die "terraform output failed in $TF_DIR (init with the state bucket first)"
out() { jq -r --arg k "$1" '.[$k].value // ""' <<<"$tf"; }
PROJECT="$(out project_id)" ZONE="$(out zone)" CLUSTER="$(out cluster_name)"
CTX="$(out kube_context)" REGISTRY="$(out registry)" IP="$(out ingress_ip)"
for v in PROJECT ZONE CLUSTER CTX REGISTRY IP; do
	[ -n "${!v}" ] || die "Terraform output for $v is empty: is the cluster applied?"
done
if [ -n "${KUBE_CONTEXT:-}" ] && [ "$KUBE_CONTEXT" != "$CTX" ]; then
	die "KUBE_CONTEXT is $KUBE_CONTEXT, but Terraform's cluster is $CTX; unset it or point it there"
fi
echo "gke-deploy: project $PROJECT, cluster $CLUSTER in $ZONE, context $CTX"

# --- 3. credentials (writes the local kubeconfig only) and the images -------------------------
gcloud container clusters get-credentials "$CLUSTER" --zone "$ZONE" --project "$PROJECT" >/dev/null ||
	die "get-credentials for $CLUSTER failed"
for name in "${IMAGES[@]}"; do
	gcloud artifacts docker images describe "$REGISTRY/$name@${DIGEST[$name]}" --project "$PROJECT" >/dev/null 2>&1 ||
		die "$name@${DIGEST[$name]} is not in $REGISTRY: re-run CI's publish on main with the GCP_* repository variables set"
done

# --- 4. the app's chart must carry D8's limits before it gets a public IP ---------------------
APP_ARGS=()
if [ "$DEPLOY_APP" = true ]; then
	cidr_list="$(IFS=','; echo "${CIDRS[*]}")"
	APP_ARGS=(
		"$APP_RELEASE" deploy/helm/reliefgrid -n "$NS" -f "$APP_VALUES"
		--set storage.enabled=false
		--set "app.api.image.repository=$REGISTRY/api" --set "app.api.image.digest=${DIGEST[api]}"
		--set "app.matcher.image.repository=$REGISTRY/matcher" --set "app.matcher.image.digest=${DIGEST[matcher]}"
		--set "app.ui.image.repository=$REGISTRY/ui" --set "app.ui.image.digest=${DIGEST[ui]}"
		--set app.api.image.pullPolicy=IfNotPresent --set app.matcher.image.pullPolicy=IfNotPresent
		--set app.ui.image.pullPolicy=IfNotPresent
		--set traefik.service.spec.type=LoadBalancer
		--set "traefik.service.spec.loadBalancerIP=$IP"
		--set "traefik.service.spec.loadBalancerSourceRanges={$cidr_list}"
		--set-string "app.publicHost=$IP.sslip.io"
		--set-file "app.dispatcherToken=$DISPATCHER_TOKEN_FILE"
	)
	bash scripts/app-chart-deps.sh >/dev/null
	rendered="$(helm template "${APP_ARGS[@]}")" || die "the app chart does not render with $APP_VALUES"
	grep -q 'rateLimit:' <<<"$rendered" ||
		die "the app chart renders no Traefik rateLimit middleware: D8 requires rate and size limits before a public URL (issue #115)"
	grep -q 'maxRequestBodyBytes:' <<<"$rendered" ||
		die "the app chart renders no buffering.maxRequestBodyBytes middleware: D8 requires a size limit (issue #115)"
fi

# --- 5. evidence: what is deployed, never a secret --------------------------------------------
mkdir -p "$EVIDENCE_DIR"
{
	echo "# S5.1 GKE deploy $(date -u +%Y-%m-%dT%H:%M:%SZ)"
	echo "commit: $(git rev-parse HEAD) (images from $(jq -r .commit "$IMAGE_DIGESTS"))"
	echo "project: $PROJECT  zone: $ZONE  cluster: $CLUSTER  context: $CTX"
	echo "registry: $REGISTRY"
	for name in "${IMAGES[@]}"; do echo "image $name: $REGISTRY/$name@${DIGEST[$name]}"; done
	echo "operator token: sha256 $(sha256sum <"$OPERATOR_TOKEN_FILE" | cut -c1-12)..."
	if [ "$DEPLOY_APP" = true ]; then
		echo "ingress: https://$IP.sslip.io (LoadBalancer $IP, allowlist ${CIDRS[*]})"
		echo "dispatcher credential: sha256 $(sha256sum <"$DISPATCHER_TOKEN_FILE" | cut -c1-12)..."
	else
		echo "app: not deployed (GKE_DEPLOY_APP=false): no public URL"
	fi
} >"$EVIDENCE_DIR/deploy.txt"

# --- 6. storage, then the app -----------------------------------------------------------------
echo "gke-deploy: storage ($STORAGE_RELEASE)"
helm upgrade --install "$STORAGE_RELEASE" deploy/helm/storage -n "$NS" --create-namespace \
	-f deploy/helm/storage/values-gke.yaml \
	--set "image.repository=$REGISTRY/storagenode" --set "image.digest=${DIGEST[storagenode]}" \
	--set "coordinator.image.repository=$REGISTRY/coordinator" --set "coordinator.image.digest=${DIGEST[coordinator]}" \
	--set-file "operatorToken=$OPERATOR_TOKEN_FILE" \
	--wait --wait-for-jobs --timeout "$HELM_TIMEOUT" --kube-context "$CTX" ||
	die "storage install failed"

if [ "$DEPLOY_APP" = true ]; then
	echo "gke-deploy: app ($APP_RELEASE) on https://$IP.sslip.io"
	helm upgrade --install "${APP_ARGS[@]}" --wait --timeout "$HELM_TIMEOUT" --kube-context "$CTX" ||
		die "app install failed"
fi

kubectl --context "$CTX" -n "$NS" get pods -o wide >"$EVIDENCE_DIR/pods.txt" 2>&1 || true
kubectl --context "$CTX" -n "$NS" get service,pvc -o wide >"$EVIDENCE_DIR/services-pvcs.txt" 2>&1 || true
echo "gke-deploy: done; evidence in $EVIDENCE_DIR"
