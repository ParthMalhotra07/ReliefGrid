#!/usr/bin/env bash
# Chaos Mesh for the S4.3 suite, pinned and namespace-scoped (chaos/mesh/values.yaml).
#   chaos/mesh/install.sh install     verify the chart and images against tools.mk, import the
#                                     images into the kind nodes that run them, install
#   chaos/mesh/install.sh check       the install works: pods Ready, no RBAC errors, and a
#                                     5 s smoke NetworkChaos reaches AllInjected and recovers
#   chaos/mesh/install.sh uninstall   remove leftover chaos objects, the release, the extra RBAC
#                                     and the chaos-mesh.org CRDs
# Run through make (chaos-install, chaos-check, chaos-uninstall), which exports the pins:
# CHAOS_MESH_VERSION, CHAOS_MESH_CHART_SHA256, CHAOS_MESH_IMAGE_DIGEST, CHAOS_DAEMON_IMAGE_DIGEST.
# Environment: CHAOS_CONTEXT (kind-reliefgrid), CHAOS_NAMESPACE (reliefgrid), CHAOS_CACHE
# (~/.cache/reliefgrid/chaos-mesh: the verified chart).
#
# Why images are imported this way (S4.3 findings, kind v0.33 / containerd):
#   - `kind load` of these multi-arch images fails: Docker's store references the arm64
#     manifest it never downloaded. So: docker save --platform linux/amd64 | ctr images import.
#   - an imported image is known to the CRI only by its tag, so a pod spec pinned by digest
#     gets ErrImageNeverPull. So: after checking that the node's content-addressed target
#     digest equals the pin, tag it repo@digest. ctr images tag checks nothing; the
#     comparison before it is what makes this a pin.
set -uo pipefail

CTX="${CHAOS_CONTEXT:-kind-reliefgrid}"
NS="${CHAOS_NAMESPACE:-reliefgrid}"
MESH_NS=chaos-mesh
RELEASE=chaos-mesh
HERE="$(cd "$(dirname "$0")" && pwd)"
CACHE="${CHAOS_CACHE:-$HOME/.cache/reliefgrid/chaos-mesh}"
REGISTRY=ghcr.io/chaos-mesh

k() { kubectl --context "$CTX" "$@"; }
log() { printf '%s chaos-mesh: %s\n' "$(date +%H:%M:%S)" "$*" >&2; }
die() { log "FAILED: $*"; exit 1; }

need_pins() {
	local v
	for v in CHAOS_MESH_VERSION CHAOS_MESH_CHART_SHA256 CHAOS_MESH_IMAGE_DIGEST CHAOS_DAEMON_IMAGE_DIGEST; do
		[ -n "${!v:-}" ] || die "$v is not set (run through make, which reads tools.mk)"
	done
	[[ "$CHAOS_MESH_CHART_SHA256" =~ ^[0-9a-f]{64}$ ]] || die "CHAOS_MESH_CHART_SHA256 is not a sha256"
	[[ "$CHAOS_MESH_IMAGE_DIGEST" =~ ^sha256:[0-9a-f]{64}$ && "$CHAOS_DAEMON_IMAGE_DIGEST" =~ ^sha256:[0-9a-f]{64}$ ]] ||
		die "image digests must be sha256:<64 hex>"
}
chart() { echo "$CACHE/chaos-mesh-$CHAOS_MESH_VERSION.tgz"; }

# fetch_chart: the chart tgz in the cache, downloaded once, always checked against the pin
fetch_chart() {
	local tgz got
	tgz="$(chart)"
	if [ ! -f "$tgz" ]; then
		mkdir -p "$CACHE"
		helm pull chaos-mesh --repo https://charts.chaos-mesh.org --version "$CHAOS_MESH_VERSION" -d "$CACHE" >/dev/null ||
			die "helm pull chaos-mesh $CHAOS_MESH_VERSION"
	fi
	got="$(sha256sum "$tgz" | cut -d' ' -f1)"
	[ "$got" = "$CHAOS_MESH_CHART_SHA256" ] || die "chart $tgz has sha256 $got, pinned $CHAOS_MESH_CHART_SHA256"
	log "chart $CHAOS_MESH_VERSION sha256 $got (pinned)"
}

# node_target <node> <repo:tag>: the digest the node's containerd holds for the tag
node_target() { docker exec "$1" ctr --namespace=k8s.io images ls 2>/dev/null | awk -v r="$2" '$1 == r { print $3 }'; }

# place_image <name> <digest> <nodes...>: the pinned image on every node, tagged by digest
place_image() {
	local name="$1" digest="$2" ref node got
	shift 2
	ref="$REGISTRY/$name:v$CHAOS_MESH_VERSION"
	if ! docker image inspect "$ref" >/dev/null 2>&1; then
		docker pull -q "$REGISTRY/$name@$digest" >/dev/null || die "docker pull $REGISTRY/$name@$digest"
		docker tag "$REGISTRY/$name@$digest" "$ref" || die "docker tag $ref"
	fi
	for node in "$@"; do
		got="$(node_target "$node" "$ref")"
		if [ "$got" != "$digest" ]; then
			docker save --platform linux/amd64 "$ref" | docker exec -i "$node" ctr --namespace=k8s.io images import --digests --snapshotter=overlayfs - >/dev/null 2>&1 ||
				die "import $ref into $node"
			got="$(node_target "$node" "$ref")"
		fi
		[ "$got" = "$digest" ] || die "$node holds $ref at ${got:-nothing}, pinned $digest"
		docker exec "$node" ctr --namespace=k8s.io images tag --force "$ref" "$REGISTRY/$name@$digest" >/dev/null 2>&1 ||
			die "tag $REGISTRY/$name@$digest on $node"
		log "$node: $name at $digest (verified)"
	done
}

nodes_with_role() { k get nodes -l "role=$1" -o jsonpath='{range .items[*]}{.metadata.name}{"\n"}{end}'; }

install() {
	need_pins
	fetch_chart
	local infra data
	infra="$(nodes_with_role infra)" data="$(nodes_with_role data)"
	[ -n "$infra" ] && [ "$(wc -l <<<"$data")" -ge 3 ] || die "want an infra node and 3 data nodes; got infra '$infra', data '$data'"
	# shellcheck disable=SC2086
	place_image chaos-mesh "$CHAOS_MESH_IMAGE_DIGEST" $infra
	# shellcheck disable=SC2086
	place_image chaos-daemon "$CHAOS_DAEMON_IMAGE_DIGEST" $data
	k apply -f "$HERE/remotecluster-rbac.yaml" >/dev/null || die "apply remotecluster-rbac.yaml"
	helm --kube-context "$CTX" upgrade --install "$RELEASE" "$(chart)" -n "$MESH_NS" --create-namespace \
		-f "$HERE/values.yaml" --set "controllerManager.targetNamespace=$NS" \
		--set-string "controllerManager.image.tag=v$CHAOS_MESH_VERSION@$CHAOS_MESH_IMAGE_DIGEST" \
		--set-string "chaosDaemon.image.tag=v$CHAOS_MESH_VERSION@$CHAOS_DAEMON_IMAGE_DIGEST" \
		--wait --timeout 4m >/dev/null || die "helm upgrade --install $RELEASE"
	log "installed (namespace-scoped to $NS)"
}

# check: prints one JSON object (the facts) on stdout and exits 0 only if the install works
check() {
	local ctrl daemons want forbidden started injected="false" recovered="false" images
	ctrl="$(k -n "$MESH_NS" get deploy chaos-controller-manager -o jsonpath='{.status.readyReplicas}' 2>/dev/null)"
	daemons="$(k -n "$MESH_NS" get ds chaos-daemon -o jsonpath='{.status.numberReady}' 2>/dev/null)"
	want="$(nodes_with_role data | grep -c .)"
	forbidden="$(k -n "$MESH_NS" logs deploy/chaos-controller-manager 2>/dev/null | grep -c forbidden)"
	started="$(k -n "$MESH_NS" logs deploy/chaos-controller-manager 2>/dev/null | grep -c 'Starting workers')"
	images="$(k -n "$MESH_NS" get pods -o json 2>/dev/null | jq -c '[.items[].spec.containers[0].image] | unique')"
	# Smoke: a 1 ms delay on one storage pod for 5 s must be injected and then recovered.
	local pod
	pod="$(k -n "$NS" get pods -l app.kubernetes.io/component=storagenode -o jsonpath='{.items[0].metadata.name}' 2>/dev/null)"
	if [ -n "$pod" ]; then
		k -n "$NS" delete networkchaos chaos-smoke --ignore-not-found --wait=true >/dev/null 2>&1
		jq -n --arg ns "$NS" --arg pod "$pod" '{apiVersion: "chaos-mesh.org/v1alpha1", kind: "NetworkChaos",
			metadata: {name: "chaos-smoke", namespace: $ns, labels: {"reliefgrid.io/chaos": "true"}},
			spec: {action: "delay", mode: "all", selector: {pods: {($ns): [$pod]}}, delay: {latency: "1ms"}, duration: "5s"}}' |
			k apply -f - >/dev/null 2>&1
		k -n "$NS" wait networkchaos/chaos-smoke --for=condition=AllInjected --timeout=30s >/dev/null 2>&1 && injected=true
		k -n "$NS" wait networkchaos/chaos-smoke --for=condition=AllRecovered --timeout=30s >/dev/null 2>&1 && recovered=true
		k -n "$NS" delete networkchaos chaos-smoke --wait=true --timeout=60s >/dev/null 2>&1
	fi
	local facts
	facts="$(jq -n --arg v "${CHAOS_MESH_VERSION:-}" --argjson ctrl "${ctrl:-0}" --argjson d "${daemons:-0}" --argjson want "$want" \
		--argjson forbidden "$forbidden" --argjson started "$started" --argjson images "${images:-[]}" \
		--argjson injected "$injected" --argjson recovered "$recovered" --arg pod "$pod" \
		--arg server "$(k version -o json 2>/dev/null | jq -r .serverVersion.gitVersion)" \
		'{version: $v, kubernetes: $server, controllerReady: $ctrl, daemonsReady: $d, dataNodes: $want, forbiddenErrors: $forbidden,
		  controllersStarted: $started, images: $images, smoke: {pod: $pod, injected: $injected, recovered: $recovered},
		  ok: ($ctrl == 1 and $d == $want and $forbidden == 0 and $started > 0 and $injected and $recovered
		       and ($images | all(test("@sha256:[0-9a-f]{64}$"))))}')"
	echo "$facts"
	jq -e .ok <<<"$facts" >/dev/null
}

uninstall() {
	local kind
	for kind in networkchaos podchaos stresschaos; do
		k -n "$NS" delete "$kind" --all --wait=true --timeout=120s >/dev/null 2>&1
	done
	helm --kube-context "$CTX" uninstall "$RELEASE" -n "$MESH_NS" --wait >/dev/null 2>&1
	k delete -f "$HERE/remotecluster-rbac.yaml" --ignore-not-found >/dev/null 2>&1
	k get crd -o name 2>/dev/null | grep '\.chaos-mesh\.org$' | xargs -r kubectl --context "$CTX" delete --wait=true >/dev/null 2>&1
	k delete namespace "$MESH_NS" --ignore-not-found --wait=true >/dev/null 2>&1
	log "uninstalled"
}

case "${1:-}" in
install) install ;;
check) check ;;
uninstall) uninstall ;;
*) echo "usage: $0 install|check|uninstall" >&2; exit 2 ;;
esac
