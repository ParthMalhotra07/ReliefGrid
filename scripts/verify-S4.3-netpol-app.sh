#!/usr/bin/env bash
# Verify check for #40 with the application deployed (S4.3): the coordinator NetworkPolicy
# lets the deployed API and matcher reach the coordinator, and nothing unlabelled.
# Run from the repo root:
#   scripts/verify-S4.3-netpol-app.sh
# It deletes and recreates the local "reliefgrid" kind cluster, installs the storage chart
# (with its storage-coordinator NetworkPolicy) as release "storage", then the application
# with `make app-deploy` (release "reliefgrid", standalone storage) and checks:
#   - the policy is installed and selects the coordinator;
#   - the api and matcher Deployments are Available with every pod labelled
#     reliefgrid.io/coordinator-client=true and no restarts (the API exits at start, and the
#     matcher stays unready, if it cannot load the coordinator's ring); ui pods are not labelled;
#   - `make e2e-smoke` passes: create, register and match through Traefik, which needs the API
#     and the matcher (and its relay) to reach the coordinator;
#   - control: a bare pod from the deployed API image with the same COORDINATOR_URL fails to
#     start with "connect coordinator" when unlabelled, and stays up when labelled.
# A1.5's own evidence (app-deploy, e2e-smoke) goes to a temporary directory, not
# docs/evidence/A1.5. Writes docs/evidence/S4.3/coordinator-netpol-app-<host>.txt (code
# fingerprint, dirty flag, every PASS/FAIL line, the result). Leaves the cluster running.
# Exit 0 only if every check passed.
set -uo pipefail

CLUSTER=reliefgrid
CTX="kind-$CLUSTER"
NS=reliefgrid
EVIDENCE="docs/evidence/S4.3/coordinator-netpol-app-$(hostname -s).txt"
LABEL=reliefgrid.io/coordinator-client

tmp="$(mktemp -d)"
results="$tmp/results"
: >"$results"
passes=0 failures=0
k() { kubectl --context "$CTX" "$@"; }
kn() { k -n "$NS" "$@"; }
cleanup() {
	kn delete pod cnp-api-open cnp-api-client --ignore-not-found --wait=false >/dev/null 2>&1
	rm -rf "$tmp"
}
trap cleanup EXIT
pass() {
	printf 'PASS  %s\n' "$1" | tee -a "$results"
	passes=$((passes + 1))
}
fail() {
	{
		printf 'FAIL  %s\n' "$1"
		[ -n "${2:-}" ] && sed 's/^/      /' <<<"$2"
	} | tee -a "$results"
	failures=$((failures + 1))
}

write_evidence() {
	local code_paths code_tree code_dirty result
	code_paths=(go.mod go.sum Makefile cmd/api cmd/matcher cmd/coordinator cmd/storagenode internal deploy/helm scripts/app-deploy.sh scripts/e2e-smoke.py scripts/verify-S4.3-netpol-app.sh)
	code_tree="$(git ls-tree -r HEAD -- "${code_paths[@]}" | git hash-object --stdin)"
	code_dirty="$(git status --porcelain -- "${code_paths[@]}" | grep -q . && echo yes || echo no)"
	if [ "$failures" -eq 0 ]; then result="PASSED ($passes/$passes checks)"; else result="FAILED ($failures of $((passes + failures)) checks failed)"; fi
	mkdir -p "$(dirname "$EVIDENCE")"
	{
		echo "# S4.3 #40 coordinator NetworkPolicy with the application deployed"
		echo "result:     $result"
		echo "host:       $(hostname -s)"
		echo "date:       $(date -u +%Y-%m-%dT%H:%M:%SZ)"
		echo "code tree:  ${code_tree:0:12} (git ls-tree -r HEAD -- ${code_paths[*]} | git hash-object --stdin)"
		echo "code dirty: $code_dirty (uncommitted changes under those paths)"
		echo
		echo "## checks"
		cat "$results"
		echo
		echo "## kubectl get pods -o wide"
		kn get pods -o wide 2>&1
		echo
		echo "## pod labels ($LABEL)"
		kn get pods -L "$LABEL" -L app.kubernetes.io/component 2>&1
		echo
		echo "## control pods"
		cat "$tmp/control.log" 2>/dev/null
		echo
		echo "## e2e-smoke (tail)"
		tail -20 "$tmp/e2e.log" 2>/dev/null
	} 2>&1 | sed 's/[[:space:]]*$//' >"$EVIDENCE"
	echo "evidence: $EVIDENCE ($result)"
}
finish() {
	write_evidence
	if [ "$failures" -eq 0 ]; then echo "verify-S4.3-netpol-app: all checks passed"; exit 0; fi
	echo "verify-S4.3-netpol-app: $failures check(s) failed"
	exit 1
}
abort() {
	fail "$@"
	finish
}

for tool in kind kubectl helm docker jq python3; do
	command -v "$tool" >/dev/null || abort "tools: $tool is installed"
done

# --- 1. Fresh cluster and storage (with the policy) ----------------------------------------
make -s cluster-down >/dev/null 2>&1
make -s cluster-up >"$tmp/up.log" 2>&1 && pass "cluster: fresh reliefgrid cluster" || abort "cluster: fresh reliefgrid cluster" "$(tail -3 "$tmp/up.log")"
make -s storage-image coordinator-image >"$tmp/images.log" 2>&1 && pass "images: storagenode and coordinator built and loaded" ||
	abort "images: storagenode and coordinator built and loaded" "$(tail -5 "$tmp/images.log")"
helm --kube-context "$CTX" install storage deploy/helm/storage -n "$NS" --create-namespace --wait --wait-for-jobs --timeout 5m >"$tmp/storage.log" 2>&1 &&
	pass "storage: helm install --wait --wait-for-jobs" || abort "storage: helm install --wait --wait-for-jobs" "$(tail -5 "$tmp/storage.log")"
sel="$(kn get networkpolicy storage-coordinator -o json 2>/dev/null | jq -cS '.spec.podSelector.matchLabels')"
[ "$sel" = '{"app.kubernetes.io/component":"coordinator","app.kubernetes.io/name":"storage"}' ] &&
	pass "policy: storage-coordinator installed, selecting the coordinator" || abort "policy: storage-coordinator installed, selecting the coordinator" "selector '$sel'"

# --- 2. The application -----------------------------------------------------------------
A15_EVIDENCE_DIR="$tmp/a15" make -s app-deploy >"$tmp/app.log" 2>&1 && pass "app: make app-deploy (standalone storage)" ||
	abort "app: make app-deploy (standalone storage)" "$(tail -8 "$tmp/app.log")"
for c in api matcher; do
	dep="$(kn get deployment -l "app.kubernetes.io/component=$c" -o json | jq -c '.items[0]')"
	[ "$(jq -r '.status.conditions[]? | select(.type == "Available") | .status' <<<"$dep")" = True ] &&
		pass "$c: Deployment Available" || fail "$c: Deployment Available" "$(jq -c .status <<<"$dep")"
	pods="$(kn get pods -l "app.kubernetes.io/component=$c" -o json)"
	n="$(jq '.items | length' <<<"$pods")"
	bad="$(jq -r --arg l "$LABEL" '.items[] | select(.metadata.labels[$l] != "true") | .metadata.name' <<<"$pods")"
	[ "$n" -gt 0 ] && [ -z "$bad" ] && pass "$c: all $n pods labelled $LABEL=true" || fail "$c: all pods labelled $LABEL=true" "pods $n, unlabelled: $bad"
	restarts="$(jq '[.items[].status.containerStatuses[]?.restartCount] | add // 0' <<<"$pods")"
	ready="$(jq '[.items[] | select(any(.status.conditions[]?; .type == "Ready" and .status == "True"))] | length' <<<"$pods")"
	[ "$ready" = "$n" ] && [ "$restarts" = 0 ] && pass "$c: $ready/$n pods Ready, 0 restarts (reached the coordinator at start)" ||
		fail "$c: every pod Ready, 0 restarts" "ready $ready/$n, restarts $restarts"
done
ui_labelled="$(kn get pods -l app.kubernetes.io/component=ui -o json | jq -r --arg l "$LABEL" '.items[] | select(.metadata.labels[$l] == "true") | .metadata.name')"
[ -z "$ui_labelled" ] && pass "ui: no pod carries $LABEL (it doesn't call the coordinator)" || fail "ui: no pod carries $LABEL" "$ui_labelled"

# --- 3. End to end through Traefik ---------------------------------------------------------
A15_EVIDENCE_DIR="$tmp/a15" make -s e2e-smoke >"$tmp/e2e.log" 2>&1 && pass "e2e-smoke: create, register and match through Traefik" ||
	fail "e2e-smoke: create, register and match through Traefik" "$(tail -8 "$tmp/e2e.log")"

# --- 4. Control: the deployed API image, with and without the label -----------------------
api="$(kn get deployment -l app.kubernetes.io/component=api -o json | jq -c '.items[0].spec.template.spec.containers[0]')"
image="$(jq -r .image <<<"$api")" url="$(jq -r '.env[] | select(.name == "COORDINATOR_URL") | .value' <<<"$api")"
: >"$tmp/control.log"
# control_pod <name> <labels|"">: run the API image; prints its phase after it fails or 20 s up
control_pod() {
	local phase=""
	kn delete pod "$1" --ignore-not-found --wait=true >/dev/null 2>&1
	kn run "$1" --image "$image" --image-pull-policy Never --restart Never --env "COORDINATOR_URL=$url" ${2:+--labels "$2"} >/dev/null 2>&1
	for _ in $(seq 40); do
		phase="$(kn get pod "$1" -o jsonpath='{.status.phase}' 2>/dev/null)"
		[ "$phase" = Failed ] && break
		sleep 1
	done
	{ echo "### $1 (labels '${2:-none}', phase $phase)"; kn logs "$1" 2>&1 | tail -3; } >>"$tmp/control.log"
	echo "$phase"
}
phase="$(control_pod cnp-api-open "")"
[ "$phase" = Failed ] && kn logs cnp-api-open 2>&1 | grep -q "connect coordinator" &&
	pass "control: an unlabelled pod from $image fails at start with 'connect coordinator'" ||
	fail "control: an unlabelled pod from $image fails at start with 'connect coordinator'" "phase $phase: $(kn logs cnp-api-open 2>&1 | tail -2)"
phase="$(control_pod cnp-api-client "$LABEL=true")"
[ "$phase" = Running ] && ! kn logs cnp-api-client 2>&1 | grep -q "connect coordinator" &&
	pass "control: the same pod labelled $LABEL=true is still running after 40 s" ||
	fail "control: the same pod labelled $LABEL=true is still running after 40 s" "phase $phase: $(kn logs cnp-api-client 2>&1 | tail -2)"

finish
