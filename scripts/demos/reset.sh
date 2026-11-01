#!/usr/bin/env bash
# Clean deployment for the demos (S3.2): the documented starting state of every gate round.
#
#   make demo-reset     (scripts/demos/reset.sh; the gate runs it before each round)
#
# First uncordons any worker a demo cordoned (DEMO_CORDON_LABEL), so even a reset that fails
# part-way leaves no demo cordon behind. Then deletes the kind cluster, creates it again (make
# cluster-up), builds and loads the storage, coordinator and tools images (the tools image runs
# the failure harness's invcheck Jobs), installs deploy/helm/storage with --wait --wait-for-jobs (the
# bootstrap Job forms the three Raft groups), and waits until the coordinator reports all nine
# storage nodes UP. If deploy/helm/reliefgrid has a Chart.yaml (A's application chart), it is
# installed through app-deploy with storage.enabled=false and dependency preparation. Checks that no worker is cordoned. Prints the measured reset time (v3.1 §28 demo hygiene: measure
# reset time rather than promise it).
#
# It destroys whatever runs on the cluster, so it asks for the cluster name first unless
# RESET_CONFIRM is set to it; anything else exits 1 before any tool is called.
set -uo pipefail
# shellcheck source=scripts/demos/common.sh
. "$(dirname "$0")/common.sh"

NS="${NAMESPACE:-reliefgrid}"
CTX="kind-$CLUSTER"
INSTALL_TIMEOUT="${INSTALL_TIMEOUT:-6m}"

if [ "${RESET_CONFIRM:-}" != "$CLUSTER" ]; then
	echo "reset: this deletes and recreates the kind cluster '$CLUSTER' and everything on it."
	read -r -p "Type the cluster name to confirm: " RESET_CONFIRM || true
	if [ "${RESET_CONFIRM:-}" != "$CLUSTER" ]; then
		echo "reset: not confirmed; nothing was reset" >&2
		exit 1
	fi
fi

t0="$SECONDS"
step() { # step <description> <command...>
	local desc="$1"
	shift
	echo "reset: $desc"
	"$@" || { echo "reset: FAILED: $desc (after $((SECONDS - t0)) s)" >&2; exit 1; }
}
cd "$REPO_ROOT" || exit 1
restore_demo_cordons | sed 's/^/reset: restored a demo cordon on /'
step "delete the kind cluster" make -s cluster-down
step "create the kind cluster" make -s cluster-up
step "build and load the storage image" make -s storage-image
step "build and load the coordinator image" make -s coordinator-image
step "build and load the tools image" make -s tools-image
step "install deploy/helm/storage" helm --kube-context "$CTX" install storage deploy/helm/storage -n "$NS" --create-namespace \
	--wait --wait-for-jobs --timeout "$INSTALL_TIMEOUT"
pf_pid="" # nine_up's port-forward, also killed if the reset is interrupted
trap '[ -n "$pf_pid" ] && kill "$pf_pid" 2>/dev/null' EXIT
trap 'exit 130' INT
trap 'exit 143' TERM HUP
nine_up() {
	local tok port ok=1
	tok="$(kubectl --context "$CTX" -n "$NS" get secret storage-operator-token -o jsonpath='{.data.token}' | base64 -d)" || return 1
	port="$(python3 -c 'import socket; s=socket.socket(); s.bind(("127.0.0.1", 0)); print(s.getsockname()[1])')"
	kubectl --context "$CTX" -n "$NS" port-forward svc/storage-coordinator "$port:8080" >/dev/null 2>&1 &
	pf_pid=$!
	for _ in $(seq 120); do
		[ "$(curl -s -m 3 -H "Authorization: Bearer $tok" "http://127.0.0.1:$port/admin/nodes" 2>/dev/null |
			jq '[.nodes[] | select(.state == "UP")] | length' 2>/dev/null)" = 9 ] && { ok=0; break; }
		sleep 1
	done
	kill "$pf_pid" 2>/dev/null
	pf_pid=""
	return "$ok"
}
step "wait until the coordinator reports nine storage nodes UP" nine_up
if [ -f deploy/helm/reliefgrid/Chart.yaml ]; then
	step "install app against existing storage (including chart dependencies)" env \
		CLUSTER="$CLUSTER" NAMESPACE="$NS" APP_WITH_STORAGE=false make -s app-deploy
fi
no_cordons() { [ -z "$(cordoned_nodes)" ]; }
step "check that no worker is cordoned" no_cordons
echo "reset: clean deployment in $((SECONDS - t0)) s"
