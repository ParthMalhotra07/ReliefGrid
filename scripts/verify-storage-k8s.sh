#!/usr/bin/env bash
# Verify check for S1.5 (storage on Kubernetes). Run from the repo root:
#   scripts/verify-storage-k8s.sh
#   ELECTION_MAX_SECONDS=15 KEEP_CLUSTER=0 scripts/verify-storage-k8s.sh
# It deletes and recreates the local "reliefgrid" kind cluster, builds and loads the
# storage image, installs deploy/helm/storage and checks bootstrap, placement, the
# bootstrap guard and failover. It leaves the cluster running (KEEP_CLUSTER=0 deletes
# it). Writes evidence to docs/evidence/S1.5/storage-k8s-<host>.txt, identified by a
# fingerprint of the code under test and a dirty flag. Exits non-zero
# if any check fails; prints one PASS/FAIL line per check.
#
# Interface under test:
#   cmd/storagenode/Dockerfile   multi-stage; final stage distroless, pinned by digest,
#                                non-root; builds ./cmd/storagenode as /storagenode and
#                                ./cmd/storagectl as /storagectl (context: repo root)
#   make storage-image           docker build of that Dockerfile as reliefgrid/storagenode:dev,
#                                then kind load docker-image into the reliefgrid cluster
#   make storage-reset           asks for the namespace on stdin; only if it is typed exactly,
#                                uninstalls the release and deletes the storage PVCs and the
#                                bootstrap manifests; otherwise exits non-zero, deleting nothing
#   deploy/helm/storage          standalone chart, release "storage", namespace reliefgrid:
#     StatefulSet storage-shard-N (N=0..2): 3 replicas, Parallel, serviceName storage-shard-N,
#       pod label reliefgrid.io/shard=N, container "storagenode", nodeSelector role=data,
#       required podAntiAffinity on kubernetes.io/hostname per shard, one volumeClaimTemplate
#       (Raft state, DATA_DIR) and an emptyDir for FSM_DIR (M3), PROFILE=demo (M4),
#       startup and liveness probes GET /healthz and the readiness probe GET /readyz, all on
#       the mgmt port (8081); no not-ready/unreachable tolerations of its own (§24 sets 30 s
#       only on stateless pods), runAsNonRoot,
#       OPERATOR_TOKEN from Secret storage-operator-token (key token)
#     Service storage-shard-N: headless, publishNotReadyAddresses, ports raft 7000,
#       http 8080, mgmt 8081
#     PodDisruptionBudget storage-shard-N: minAvailable 2
#     NetworkPolicy storage-shard-N: ingress to 8081 only from pods labelled
#       reliefgrid.io/storage-operator=true
#     ConfigMap storage-shard-N-manifest (v3.1 §10, kept on uninstall): groupId shard-N,
#       shard, ringVersion (= contracts/ring.yaml), peers (JSON [{podName,address}] with the
#       stable FQDNs), phase PENDING -> BOOTSTRAPPING -> BOOTSTRAPPED, peerIds (JSON list of
#       podName/incarnation, written by the Job)
#     Job storage-bootstrap-<revision>: an ordinary Job (no helm.sh/hook), labelled
#       reliefgrid.io/storage-operator=true, runs /storagectl bootstrap; exit code 2 means
#       "refused" and fails the Job at once (podFailurePolicy); logs "refused" when it refuses
#       and "already initialized" when it finds a group it did not create in this run
#     value bootstrap.debugHoldSeconds: the Job sleeps that long after bootstrapping shard 0
#       and before recording BOOTSTRAPPED, logging "holding" (gives the kill a fixed window)
#   /storagectl status [--addr host:port] [--timeout d]
#                                GET /mgmt/status (default localhost:8081), JSON on stdout,
#                                non-zero exit if unreachable
set -uo pipefail

CLUSTER=reliefgrid
CTX="kind-$CLUSTER"
NS=reliefgrid
RELEASE=storage
CHART=deploy/helm/storage
DOCKERFILE=cmd/storagenode/Dockerfile
IMAGE=reliefgrid/storagenode:dev
SHARDS=(0 1 2)
WRITE_SHARD="${WRITE_SHARD:-1}"
INSTALL_TIMEOUT="${INSTALL_TIMEOUT:-5m}"
ELECTION_MAX_SECONDS="${ELECTION_MAX_SECONDS:-10}"
REJOIN_MAX_SECONDS="${REJOIN_MAX_SECONDS:-120}"
NO_SELF_BOOTSTRAP_SECONDS="${NO_SELF_BOOTSTRAP_SECONDS:-20}"
HOLD_SECONDS="${HOLD_SECONDS:-60}"
KEEP_CLUSTER="${KEEP_CLUSTER:-1}"
EVIDENCE="docs/evidence/S1.5/storage-k8s-$(hostname -s).txt"

failures=0
pass() { printf 'PASS  %s\n' "$1"; }
fail() { printf 'FAIL  %s\n' "$1"; [ -n "${2:-}" ] && printf '      %s\n' "$2"; failures=$((failures + 1)); }
check() { # check <description> <command...>
	local desc="$1"
	shift
	if "$@" >/dev/null 2>&1; then pass "$desc"; else fail "$desc"; fi
}
finish() {
	[ "$KEEP_CLUSTER" = 0 ] && make -s cluster-down >/dev/null 2>&1
	echo
	if [ "$failures" -eq 0 ]; then
		echo "verify-storage-k8s: all checks passed"
	else
		echo "verify-storage-k8s: $failures check(s) failed"
		exit 1
	fi
	exit 0
}
abort() { # abort <description> [detail]: a failure that makes later checks meaningless
	fail "$@"
	finish
}

tmp="$(mktemp -d)"
pf_pid=""
cleanup() {
	[ -n "$pf_pid" ] && kill "$pf_pid" 2>/dev/null
	rm -rf "$tmp"
}
trap cleanup EXIT

k() { kubectl --context "$CTX" "$@"; }
kn() { k -n "$NS" "$@"; }
sts_name() { printf 'storage-shard-%s' "$1"; }
pod_names() { printf 'storage-shard-%s-0 storage-shard-%s-1 storage-shard-%s-2' "$1" "$1" "$1"; }
fqdn() { printf '%s.storage-shard-%s.%s.svc.cluster.local:7000' "$2" "$1" "$NS"; } # fqdn <shard> <pod>
st() { kn exec "$1" -c storagenode -- /storagectl status 2>/dev/null; }             # status JSON or empty
field() { jq -r "$2 // empty" <<<"$1" 2>/dev/null; }                               # field <json> <jq path>
now() { date +%s.%N; }
since() { awk -v a="$1" -v b="$(now)" 'BEGIN { printf "%.1f", b - a }'; }
y2j() { python3 -c 'import json, sys, yaml; print(json.dumps([d for d in yaml.safe_load_all(sys.stdin) if d]))'; }
leaders() { # leaders <pods...>: "<pod> <term>" for every reachable pod in state Leader
	local p s
	for p in "$@"; do
		s="$(st "$p")"
		[ "$(field "$s" .state)" = Leader ] && echo "$p $(field "$s" .term)"
	done
}
wait_leader() { # wait_leader <seconds> <not-pod|-> <min-term> <pods...>: print "<pod> <term>"
	local deadline=$((SECONDS + $1)) not="$2" min="$3" l
	shift 3
	while [ "$SECONDS" -lt "$deadline" ]; do
		l="$(leaders "$@")"
		if [ "$(grep -c . <<<"$l")" -eq 1 ] && [ "${l%% *}" != "$not" ] && [ "${l##* }" -ge "$min" ]; then
			echo "$l"
			return 0
		fi
		sleep 0.5
	done
	return 1
}
one_group() { # one_group <shard>: exactly one Leader; every pod has state and names that leader
	local shard="$1" p s lead="" n=0 ids=""
	for p in $(pod_names "$shard"); do
		s="$(st "$p")"
		[ -n "$s" ] || { echo "$p unreachable"; return 1; }
		[ "$(field "$s" .state)" = Leader ] && n=$((n + 1))
		[ "$(field "$s" .hasState)" = true ] || { echo "$p has no Raft state"; return 1; }
		[ -z "$lead" ] && lead="$(field "$s" .leaderId)"
		[ -n "$lead" ] && [ "$(field "$s" .leaderId)" = "$lead" ] || { echo "$p names leader '$(field "$s" .leaderId)', others '$lead'"; return 1; }
		ids+="$(field "$s" .serverId)"$'\n'
	done
	[ "$n" -eq 1 ] || { echo "$n pods are Leader"; return 1; }
	local want
	want="$(kn get configmap "$(sts_name "$shard")-manifest" -o jsonpath='{.data.peerIds}' 2>/dev/null | jq -r '.[]' 2>/dev/null | sort)"
	[ "$(sort <<<"$ids" | grep .)" = "$want" ] || { echo "serverIds $(tr '\n' ' ' <<<"$ids")differ from manifest peerIds $(tr '\n' ' ' <<<"$want")"; return 1; }
}
phase() { kn get configmap "$(sts_name "$1")-manifest" -o jsonpath='{.data.phase}' 2>/dev/null; }
job_name() { # newest bootstrap Job
	kn get jobs -l app.kubernetes.io/component=storage-bootstrap --sort-by=.metadata.creationTimestamp \
		-o jsonpath='{range .items[*]}{.metadata.name}{"\n"}{end}' 2>/dev/null | tail -1
}
job_logs() { kn logs "job/$1" --tail=-1 2>/dev/null; }
port_forward() { # port_forward <pod>: forward a free local port to the pod's 8080; print the port
	[ -n "$pf_pid" ] && kill "$pf_pid" 2>/dev/null
	local port
	port="$(python3 -c 'import socket; s=socket.socket(); s.bind(("127.0.0.1", 0)); print(s.getsockname()[1])')"
	kn port-forward "pod/$1" "$port:8080" >"$tmp/pf.out" 2>&1 &
	pf_pid=$!
	for _ in $(seq 50); do (exec 3<>"/dev/tcp/127.0.0.1/$port") 2>/dev/null && break; sleep 0.1; done
	echo "$port"
}

for tool in kind kubectl helm docker jq python3 curl; do
	command -v "$tool" >/dev/null || abort "tools: $tool is installed"
done
ring_version="$(sed -n 's/^ringVersion:[[:space:]]*\([0-9]*\).*/\1/p' contracts/ring.yaml)"

# --- 1. Image and make target ------------------------------------------------------
if [ -f "$DOCKERFILE" ]; then
	pass "image: $DOCKERFILE exists"
	froms="$(grep -Ei '^FROM[[:space:]]' "$DOCKERFILE")"
	final="$(tail -1 <<<"$froms")"
	[ "$(grep -c . <<<"$froms")" -ge 2 ] && pass "image: multi-stage build" || fail "image: multi-stage build" "$froms"
	grep -Eq 'gcr\.io/distroless/[^ ]+@sha256:[0-9a-f]{64}' <<<"$final" &&
		pass "image: final stage is distroless, pinned by digest" || fail "image: final stage is distroless, pinned by digest" "$final"
	if grep -Eq ':nonroot@' <<<"$final" || grep -Eq '^USER[[:space:]]+(nonroot|65532)' "$DOCKERFILE"; then
		pass "image: runs as non-root"
	else
		fail "image: runs as non-root"
	fi
	for pkg in ./cmd/storagenode ./cmd/storagectl; do
		check "image: builds $pkg" grep -Eq "go build .*${pkg//./\\.}([[:space:]]|$)" "$DOCKERFILE"
	done
else
	fail "image: $DOCKERFILE exists"
fi
recipe="$(make -n storage-image 2>/dev/null)"
if grep -Fq -- "-f $DOCKERFILE" <<<"$recipe" && grep -Fq "$IMAGE" <<<"$recipe" && grep -Eq "kind load docker-image .*$IMAGE|kind load docker-image .*--name $CLUSTER" <<<"$recipe"; then
	pass "make storage-image: builds $IMAGE from $DOCKERFILE and kind-loads it"
else
	fail "make storage-image: builds $IMAGE from $DOCKERFILE and kind-loads it" "recipe: $(tr '\n' '|' <<<"$recipe")"
fi
check "make storage-reset: target exists" make -n storage-reset

# --- 2. Chart, rendered offline ----------------------------------------------------
[ -f "$CHART/Chart.yaml" ] || abort "chart: $CHART/Chart.yaml exists"
pass "chart: $CHART/Chart.yaml exists"
check "chart: helm lint passes" helm lint "$CHART" --namespace "$NS"
if ! helm template "$RELEASE" "$CHART" --namespace "$NS" >"$tmp/render.yaml" 2>"$tmp/render.err"; then
	abort "chart: helm template renders" "$(tr '\n' '|' <"$tmp/render.err")"
fi
y2j <"$tmp/render.yaml" >"$tmp/render.json" || abort "chart: rendered YAML parses"
obj() { jq -c --arg k "$1" --arg n "$2" '.[] | select(.kind == $k and .metadata.name == $n)' "$tmp/render.json"; }
q() { # q <json> [jq options...] <filter>
	local j="$1"
	shift
	jq -r "$@" <<<"$j" 2>/dev/null
}

secret="$(jq -c '.[] | select(.kind == "Secret" and .metadata.name == "storage-operator-token")' "$tmp/render.json")"
[ -n "$secret" ] && [ -n "$(q "$secret" '(.data.token // .stringData.token) // empty')" ] &&
	pass "secret: storage-operator-token has a token" || fail "secret: storage-operator-token has a token"

for s in "${SHARDS[@]}"; do
	n="$(sts_name "$s")"
	sts="$(obj StatefulSet "$n")"
	[ -n "$sts" ] || { fail "statefulset $n: rendered"; continue; }
	pass "statefulset $n: rendered"
	[ "$(q "$sts" .spec.replicas)" = 3 ] && pass "statefulset $n: 3 replicas" || fail "statefulset $n: 3 replicas"
	[ "$(q "$sts" .spec.podManagementPolicy)" = Parallel ] && pass "statefulset $n: podManagementPolicy Parallel" || fail "statefulset $n: podManagementPolicy Parallel"
	[ "$(q "$sts" .spec.serviceName)" = "$n" ] && pass "statefulset $n: serviceName $n" || fail "statefulset $n: serviceName $n"
	[ "$(q "$sts" '.spec.template.metadata.labels["reliefgrid.io/shard"]')" = "$s" ] &&
		pass "statefulset $n: pods labelled reliefgrid.io/shard=$s" || fail "statefulset $n: pods labelled reliefgrid.io/shard=$s"
	pod="$(q "$sts" -c .spec.template.spec)"
	c="$(q "$pod" -c '.containers[] | select(.name == "storagenode")')"
	[ -n "$c" ] || { fail "statefulset $n: container storagenode"; continue; }
	[ "$(q "$pod" '.nodeSelector.role')" = data ] && pass "statefulset $n: nodeSelector role=data" || fail "statefulset $n: nodeSelector role=data"
	aa="$(q "$pod" -c '.affinity.podAntiAffinity.requiredDuringSchedulingIgnoredDuringExecution // [] | map(select(.topologyKey == "kubernetes.io/hostname" and .labelSelector.matchLabels["reliefgrid.io/shard"] == "'"$s"'")) | length')"
	[ "${aa:-0}" -ge 1 ] && pass "statefulset $n: required anti-affinity on hostname for shard $s" ||
		fail "statefulset $n: required anti-affinity on hostname for shard $s"
	envv() { q "$c" --arg k "$1" '.env[] | select(.name == $k) | .value // empty'; }
	vct="$(q "$sts" -c '[.spec.volumeClaimTemplates[].metadata.name]')"
	data_dir="$(envv DATA_DIR)"
	fsm_dir="$(envv FSM_DIR)"
	vol_at() { q "$c" -r --arg p "$1" '.volumeMounts[] | select(.mountPath == $p) | .name'; }
	[ "$(q "$vct" length)" = 1 ] && [ "$(vol_at "$data_dir")" = "$(q "$vct" -r '.[0]')" ] &&
		pass "statefulset $n: one PVC, mounted at DATA_DIR ($data_dir)" || fail "statefulset $n: one PVC, mounted at DATA_DIR" "templates $vct, DATA_DIR '$data_dir'"
	fsm_vol="$(vol_at "$fsm_dir")"
	[ -n "$fsm_vol" ] && [ -n "$(q "$pod" -r --arg v "$fsm_vol" '.volumes[] | select(.name == $v and .emptyDir != null) | .name')" ] &&
		pass "statefulset $n: FSM_DIR ($fsm_dir) is an emptyDir (M3)" || fail "statefulset $n: FSM_DIR is an emptyDir (M3)" "FSM_DIR '$fsm_dir', volume '$fsm_vol'"
	[ "$(envv PROFILE)" = demo ] && pass "statefulset $n: PROFILE=demo (M4)" || fail "statefulset $n: PROFILE=demo (M4)" "got '$(envv PROFILE)'"
	[ "$(envv SHARD)" = "$s" ] && pass "statefulset $n: SHARD=$s" || fail "statefulset $n: SHARD=$s"
	adv="$(envv RAFT_ADVERTISE)"
	[[ "$adv" == *'$(POD_NAME)'".storage-shard-$s.$NS.svc.cluster.local:7000" ]] &&
		pass "statefulset $n: RAFT_ADVERTISE is the stable FQDN" || fail "statefulset $n: RAFT_ADVERTISE is the stable FQDN" "got '$adv'"
	[ "$(q "$c" -r '.env[] | select(.name == "OPERATOR_TOKEN") | .valueFrom.secretKeyRef | "\(.name)/\(.key)"')" = storage-operator-token/token ] &&
		pass "statefulset $n: OPERATOR_TOKEN from the Secret" || fail "statefulset $n: OPERATOR_TOKEN from the Secret"
	for pp in startupProbe:/healthz livenessProbe:/healthz readinessProbe:/readyz; do
		probe="${pp%%:*}" path="${pp#*:}"
		[ "$(q "$c" -r ".$probe.httpGet.path")" = "$path" ] && [[ "$(q "$c" -r ".$probe.httpGet.port")" =~ ^(8081|mgmt)$ ]] &&
			pass "statefulset $n: $probe GET $path on the mgmt port" || fail "statefulset $n: $probe GET $path on the mgmt port"
	done
	[ -z "$(q "$pod" -r '.tolerations[]? | select(.key == "node.kubernetes.io/not-ready" or .key == "node.kubernetes.io/unreachable") | .key')" ] &&
		pass "statefulset $n: no not-ready/unreachable tolerations of its own (§24)" || fail "statefulset $n: no not-ready/unreachable tolerations of its own (§24)"
	[ "$(q "$pod" .securityContext.runAsNonRoot)" = true ] || [ "$(q "$c" .securityContext.runAsNonRoot)" = true ] &&
		pass "statefulset $n: runAsNonRoot" || fail "statefulset $n: runAsNonRoot"

	svc="$(obj Service "$n")"
	[ "$(q "$svc" .spec.clusterIP)" = None ] && [ "$(q "$svc" .spec.publishNotReadyAddresses)" = true ] &&
		pass "service $n: headless, publishNotReadyAddresses" || fail "service $n: headless, publishNotReadyAddresses"
	[ "$(q "$svc" '.spec.selector["reliefgrid.io/shard"]')" = "$s" ] && pass "service $n: selects shard $s" || fail "service $n: selects shard $s"
	[ "$(q "$svc" -c '[.spec.ports[].port] | sort')" = '[7000,8080,8081]' ] && pass "service $n: ports 7000, 8080, 8081" || fail "service $n: ports 7000, 8080, 8081"

	pdb="$(obj PodDisruptionBudget "$n")"
	[ "$(q "$pdb" .spec.minAvailable)" = 2 ] && [ "$(q "$pdb" '.spec.selector.matchLabels["reliefgrid.io/shard"]')" = "$s" ] &&
		pass "pdb $n: minAvailable 2 for shard $s" || fail "pdb $n: minAvailable 2 for shard $s"

	np="$(obj NetworkPolicy "$n")"
	if [ -z "$np" ] || [ "$(q "$np" '.spec.podSelector.matchLabels["reliefgrid.io/shard"]')" != "$s" ]; then
		fail "networkpolicy $n: selects shard $s"
	else
		pass "networkpolicy $n: selects shard $s"
		# Every ingress rule that admits 8081 must admit only operator-labelled pods.
		bad="$(q "$np" -c '[.spec.ingress[] | select((.ports // []) | length == 0 or any(.port == 8081 or .port == "mgmt"))
			| select((.from // []) | length == 0 or any(.podSelector.matchLabels["reliefgrid.io/storage-operator"] != "true"))] | length')"
		ok="$(q "$np" -c '[.spec.ingress[] | select(any(.ports[]?; .port == 8081 or .port == "mgmt"))] | length')"
		[ "$bad" = 0 ] && [ "${ok:-0}" -ge 1 ] &&
			pass "networkpolicy $n: 8081 only from reliefgrid.io/storage-operator=true" || fail "networkpolicy $n: 8081 only from reliefgrid.io/storage-operator=true" "$np"
	fi

	cm="$(obj ConfigMap "$n-manifest")"
	if [ -z "$cm" ]; then
		fail "manifest $n-manifest: rendered"
	else
		[ "$(q "$cm" .data.groupId)" = "shard-$s" ] && [ "$(q "$cm" .data.shard)" = "$s" ] &&
			pass "manifest $n-manifest: groupId shard-$s, shard $s" || fail "manifest $n-manifest: groupId shard-$s, shard $s"
		[ "$(q "$cm" .data.ringVersion)" = "$ring_version" ] &&
			pass "manifest $n-manifest: ringVersion $ring_version (contracts/ring.yaml)" || fail "manifest $n-manifest: ringVersion $ring_version" "got '$(q "$cm" .data.ringVersion)'"
		want_peers="$(for p in $(pod_names "$s"); do printf '%s %s\n' "$p" "$(fqdn "$s" "$p")"; done)"
		[ "$(q "$cm" -r '.data.peers | fromjson | .[] | "\(.podName) \(.address)"')" = "$want_peers" ] &&
			pass "manifest $n-manifest: peers are the three stable FQDNs" || fail "manifest $n-manifest: peers are the three stable FQDNs" "got $(q "$cm" .data.peers)"
		[ "$(q "$cm" .data.phase)" = PENDING ] && pass "manifest $n-manifest: phase PENDING on a fresh install" || fail "manifest $n-manifest: phase PENDING on a fresh install"
		[ "$(q "$cm" '.metadata.annotations["helm.sh/resource-policy"]')" = keep ] &&
			pass "manifest $n-manifest: kept on helm uninstall" || fail "manifest $n-manifest: kept on helm uninstall"
	fi
done

job="$(jq -c '.[] | select(.kind == "Job")' "$tmp/render.json")"
if [ "$(jq -s length <<<"$job")" != 1 ]; then
	fail "job: exactly one bootstrap Job rendered" "got $(jq -s length <<<"$job")"
else
	[[ "$(q "$job" .metadata.name)" == storage-bootstrap-* ]] && pass "job: named storage-bootstrap-<revision>" || fail "job: named storage-bootstrap-<revision>" "$(q "$job" .metadata.name)"
	[ -z "$(q "$job" '.metadata.annotations // {} | keys[] | select(startswith("helm.sh/hook"))')" ] &&
		pass "job: ordinary Job, not a Helm hook (C2)" || fail "job: ordinary Job, not a Helm hook (C2)"
	jt="$(q "$job" -c .spec.template)"
	[ "$(q "$jt" '.metadata.labels["reliefgrid.io/storage-operator"]')" = true ] &&
		pass "job: pods labelled reliefgrid.io/storage-operator=true" || fail "job: pods labelled reliefgrid.io/storage-operator=true"
	q "$jt" -c '.spec.containers[0] | (.command // []) + (.args // [])' | grep -q '"/storagectl".*"bootstrap"\|"bootstrap"' &&
		pass "job: runs storagectl bootstrap" || fail "job: runs storagectl bootstrap"
	[ "$(q "$job" -c '.spec.podFailurePolicy.rules[]? | select(.action == "FailJob") | .onExitCodes.values' | grep -c '\b2\b')" -ge 1 ] &&
		pass "job: exit code 2 (refused) fails the Job at once" || fail "job: exit code 2 (refused) fails the Job at once"
	sa="$(q "$jt" .spec.serviceAccountName)"
	role="$(jq -c --arg sa "$sa" '[.[] | select(.kind == "RoleBinding" and any(.subjects[]; .name == $sa))][0].roleRef.name' "$tmp/render.json" 2>/dev/null)"
	verbs="$(jq -r --arg r "${role//\"/}" '.[] | select(.kind == "Role" and .metadata.name == $r) | .rules[] | select(.resources | index("configmaps")) | .verbs[]' "$tmp/render.json" | sort -u | tr '\n' ' ')"
	[[ "$verbs" == *get* && "$verbs" == *update* ]] && pass "job: service account may get and update the manifests" || fail "job: service account may get and update the manifests" "verbs '$verbs'"
fi

# --- 3. Fresh cluster, image, install ------------------------------------------------
make -s cluster-down >/dev/null 2>&1
if make -s cluster-up >"$tmp/up.out" 2>&1; then pass "cluster: fresh reliefgrid cluster"; else abort "cluster: fresh reliefgrid cluster" "$(tail -3 "$tmp/up.out" | tr '\n' '|')"; fi
if make -s storage-image >"$tmp/image.out" 2>&1; then pass "make storage-image: succeeds"; else abort "make storage-image: succeeds" "$(tail -5 "$tmp/image.out" | tr '\n' '|')"; fi
uid="$(docker image inspect "$IMAGE" --format '{{.Config.User}}' 2>/dev/null)"
[[ -n "$uid" && "$uid" != 0 && "$uid" != root ]] && pass "image: $IMAGE runs as user '$uid'" || fail "image: $IMAGE runs as a non-root user" "Config.User '$uid'"
for b in /storagenode /storagectl; do
	docker create --name s15-probe "$IMAGE" >/dev/null 2>&1 && docker cp "s15-probe:$b" "$tmp/bin" >/dev/null 2>&1 &&
		pass "image: contains $b" || fail "image: contains $b"
	docker rm -f s15-probe >/dev/null 2>&1
done

t0="$(now)"
if helm --kube-context "$CTX" install "$RELEASE" "$CHART" -n "$NS" --create-namespace --wait --wait-for-jobs --timeout "$INSTALL_TIMEOUT" >"$tmp/install.out" 2>&1; then
	pass "install: helm install --wait --wait-for-jobs succeeds ($(since "$t0") s)"
else
	kn get pods -o wide
	kn logs "job/$(job_name)" --tail=30 2>/dev/null
	abort "install: helm install --wait --wait-for-jobs succeeds" "$(tail -5 "$tmp/install.out" | tr '\n' '|')"
fi

# --- 4. Three groups, one leader each, placement --------------------------------------
for s in "${SHARDS[@]}"; do
	if out="$(one_group "$s")"; then
		pass "group shard-$s: one leader, all three replicas in it, IDs match the manifest"
	else
		fail "group shard-$s: one leader, all three replicas in it, IDs match the manifest" "$out"
	fi
	[ "$(phase "$s")" = BOOTSTRAPPED ] && pass "manifest shard-$s: phase BOOTSTRAPPED" || fail "manifest shard-$s: phase BOOTSTRAPPED" "got '$(phase "$s")'"
	for p in $(pod_names "$s"); do
		[ "$(field "$(st "$p")" .address)" = "$(fqdn "$s" "$p")" ] || fail "advertise: $p advertises $(fqdn "$s" "$p")" "got '$(field "$(st "$p")" .address)'"
	done
	[ "$(kn get pvc -l "reliefgrid.io/shard=$s" --no-headers 2>/dev/null | grep -c Bound)" = 3 ] &&
		pass "storage shard-$s: 3 PVCs Bound" || fail "storage shard-$s: 3 PVCs Bound"
done
data_nodes="$(k get nodes -l role=data -o jsonpath='{.items[*].metadata.name}')"
for node in $data_nodes; do
	got="$(kn get pods -l app.kubernetes.io/component=storagenode --field-selector "spec.nodeName=$node" \
		-o jsonpath='{range .items[*]}{.metadata.labels.reliefgrid\.io/shard}{"\n"}{end}' 2>/dev/null | sort | tr '\n' ' ')"
	[ "$got" = "0 1 2 " ] && pass "placement: $node runs one replica of every shard" || fail "placement: $node runs one replica of every shard" "shards on it: '$got'"
done

# --- 5. Management port is restricted ---------------------------------------------------
target="$(fqdn 0 storage-shard-0-0)"
target="${target%:7000}:8081"
np_run() { # np_run <name> <operator label true|false>: exit status of storagectl status from a fresh pod
	kn delete pod "$1" --ignore-not-found --wait=true >/dev/null 2>&1
	kn run "$1" --image "$IMAGE" --image-pull-policy Never --restart Never \
		--labels "reliefgrid.io/storage-operator=$2" --command -- /storagectl status --addr "$target" --timeout 3s >/dev/null 2>&1
	for _ in $(seq 60); do
		case "$(kn get pod "$1" -o jsonpath='{.status.phase}' 2>/dev/null)" in
		Succeeded) kn delete pod "$1" --wait=false >/dev/null 2>&1; return 0 ;;
		Failed) kn delete pod "$1" --wait=false >/dev/null 2>&1; return 1 ;;
		esac
		sleep 1
	done
	return 2
}
np_run np-operator true && pass "networkpolicy: an operator-labelled pod reaches $target" || fail "networkpolicy: an operator-labelled pod reaches $target"
np_run np-other false
rc=$?
[ "$rc" = 1 ] && pass "networkpolicy: an unlabelled pod cannot reach $target" || fail "networkpolicy: an unlabelled pod cannot reach $target" "exit $rc"

# --- 6. A write through the leader survives deleting that leader -------------------------
wpods=($(pod_names "$WRITE_SHARD"))
read -r leader term < <(wait_leader 10 - 0 "${wpods[@]}")
if [ -z "${leader:-}" ]; then
	fail "write shard-$WRITE_SHARD: leader found"
else
	port="$(port_forward "$leader")"
	key="" value="{\"verify\":\"S1.5-$(date +%s)\"}"
	for i in $(seq 0 200); do # first key the ring gives to WRITE_SHARD; a foreign key is 400 and never proposed
		code="$(curl -s -o "$tmp/put.json" -w '%{http_code}' -X PUT "http://127.0.0.1:$port/internal/data/cfg:s15-verify-$i" \
			-H 'Content-Type: application/json' -H 'If-None-Match: *' \
			-d "{\"operationId\":\"op-s15-$i-$(date +%s%N)\",\"at\":1792800000000,\"ringVersion\":$ring_version,\"value\":$value}")"
		[ "$code" = 400 ] && grep -q 'owned by shard' "$tmp/put.json" && continue
		key="cfg:s15-verify-$i"
		break
	done
	if [ "${code:-}" = 200 ] && [ "$(jq -r .result.version "$tmp/put.json")" = 1 ]; then
		pass "write shard-$WRITE_SHARD: PUT $key through leader $leader committed (version 1)"
	else
		fail "write shard-$WRITE_SHARD: PUT through leader $leader committed" "HTTP ${code:-none}: $(cat "$tmp/put.json" 2>/dev/null)"
	fi
	kill "$pf_pid" 2>/dev/null
	pf_pid=""
	t0="$(now)"
	kn delete pod "$leader" --wait=false >/dev/null 2>&1
	read -r newleader newterm < <(wait_leader "$ELECTION_MAX_SECONDS" "$leader" $((term + 1)) "${wpods[@]}")
	if [ -n "${newleader:-}" ]; then
		pass "failover shard-$WRITE_SHARD: $newleader leads at term $newterm, $(since "$t0") s after deleting $leader"
		port="$(port_forward "$newleader")"
		code="$(curl -s -o "$tmp/get.json" -w '%{http_code}' "http://127.0.0.1:$port/internal/data/$key")"
		if [ "$code" = 200 ] && [ "$(jq -c .record.value "$tmp/get.json")" = "$(jq -c . <<<"$value")" ] && [ "$(jq -r .record.version "$tmp/get.json")" = 1 ]; then
			pass "failover shard-$WRITE_SHARD: strong read of $key on $newleader returns the write"
		else
			fail "failover shard-$WRITE_SHARD: strong read of $key on $newleader returns the write" "HTTP $code: $(cat "$tmp/get.json")"
		fi
		kill "$pf_pid" 2>/dev/null
		pf_pid=""
	else
		fail "failover shard-$WRITE_SHARD: a new leader within ${ELECTION_MAX_SECONDS}s of deleting $leader"
	fi
	if kn wait --for=condition=Ready "pod/$leader" --timeout="${REJOIN_MAX_SECONDS}s" >/dev/null 2>&1 && out="$(one_group "$WRITE_SHARD")"; then
		pass "failover shard-$WRITE_SHARD: $leader rejoins the same group (same ID) and is Ready"
	else
		fail "failover shard-$WRITE_SHARD: $leader rejoins the same group (same ID) and is Ready" "${out:-not Ready}"
	fi
fi

# --- 7. Wiped disks with a kept manifest: no second cluster -------------------------------
helm --kube-context "$CTX" uninstall "$RELEASE" -n "$NS" --wait >/dev/null 2>&1
kn delete pvc -l app.kubernetes.io/component=storagenode --wait=true >/dev/null 2>&1
kept=0
for s in "${SHARDS[@]}"; do [ "$(phase "$s")" = BOOTSTRAPPED ] && kept=$((kept + 1)); done
[ "$kept" = 3 ] && pass "uninstall: the three manifests survive, phase BOOTSTRAPPED" || fail "uninstall: the three manifests survive, phase BOOTSTRAPPED" "$kept kept"
helm --kube-context "$CTX" install "$RELEASE" "$CHART" -n "$NS" >"$tmp/reinstall.out" 2>&1 ||
	fail "reinstall: helm install over wiped disks" "$(tail -3 "$tmp/reinstall.out" | tr '\n' '|')"
j="$(job_name)"
if kn wait --for=condition=Failed "job/$j" --timeout=180s >/dev/null 2>&1; then
	pass "guard: the bootstrap Job fails over wiped disks with a BOOTSTRAPPED manifest"
else
	fail "guard: the bootstrap Job fails over wiped disks with a BOOTSTRAPPED manifest" "$(job_logs "$j" | tail -3 | tr '\n' '|')"
fi
job_logs "$j" | grep -q refused && pass "guard: the Job logs 'refused'" || fail "guard: the Job logs 'refused'"
echo "waiting ${NO_SELF_BOOTSTRAP_SECONDS}s to confirm no pod bootstraps itself..."
sleep "$NO_SELF_BOOTSTRAP_SECONDS"
for s in "${SHARDS[@]}"; do
	formed=""
	for p in $(pod_names "$s"); do
		sj="$(st "$p")"
		[ -n "$sj" ] || { formed+="$p:unreachable "; continue; }
		[ "$(field "$sj" .hasState)" = true ] || [ -n "$(field "$sj" .leaderId)" ] && formed+="$p "
	done
	[ -z "$formed" ] && pass "guard shard-$s: no replica has Raft state or a leader" || fail "guard shard-$s: no replica has Raft state or a leader" "$formed"
	[ "$(phase "$s")" = BOOTSTRAPPED ] || fail "guard shard-$s: manifest left as BOOTSTRAPPED" "got '$(phase "$s")'"
done

# --- 8. make storage-reset ------------------------------------------------------------------
# The operator decides this is a new cluster. A wrong answer deletes nothing.
counts() { echo "$(kn get configmap -o name 2>/dev/null | grep -c -- '-manifest$') $(kn get pvc --no-headers 2>/dev/null | grep -c .)"; }
before="$(counts)"
if echo nope | make -s storage-reset >"$tmp/reset-no.out" 2>&1; then
	fail "storage-reset: a wrong answer exits non-zero"
else
	pass "storage-reset: a wrong answer exits non-zero"
fi
[ "$(counts)" = "$before" ] && [ "$before" != "0 0" ] && pass "storage-reset: a wrong answer deletes nothing ($before manifests/PVCs)" ||
	fail "storage-reset: a wrong answer deletes nothing" "before '$before', after '$(counts)'"
if echo "$NS" | make -s storage-reset >"$tmp/reset.out" 2>&1; then
	pass "storage-reset: typing '$NS' succeeds"
else
	fail "storage-reset: typing '$NS' succeeds" "$(tail -3 "$tmp/reset.out" | tr '\n' '|')"
fi
[ "$(counts)" = "0 0" ] && pass "storage-reset: manifests and PVCs deleted" || fail "storage-reset: manifests and PVCs deleted" "left: $(counts)"
helm --kube-context "$CTX" status "$RELEASE" -n "$NS" >/dev/null 2>&1 && fail "storage-reset: release uninstalled" || pass "storage-reset: release uninstalled"

# --- 9. Kill the bootstrap Job mid-run, re-run it ----------------------------------------
# A fresh install whose Job holds after bootstrapping shard 0, before it records
# BOOTSTRAPPED; it is deleted inside that window.
helm --kube-context "$CTX" install "$RELEASE" "$CHART" -n "$NS" --set "bootstrap.debugHoldSeconds=$HOLD_SECONDS" >"$tmp/install2.out" 2>&1 ||
	fail "rerun: helm install with a hold" "$(tail -3 "$tmp/install2.out" | tr '\n' '|')"
j="$(job_name)"
held=""
for _ in $(seq 180); do
	job_logs "$j" | grep -q holding && { held=1; break; }
	sleep 1
done
if [ -n "$held" ]; then
	pass "rerun: Job $j bootstrapped shard 0 and is holding"
	echo "phases at the kill: shard-0 $(phase 0), shard-1 $(phase 1), shard-2 $(phase 2)"
	kn delete job "$j" --cascade=foreground --wait=true >/dev/null 2>&1
	[ -z "$(kn get pods -l "job-name=$j" -o name 2>/dev/null)" ] && pass "rerun: Job $j and its pod deleted mid-run" || fail "rerun: Job $j and its pod deleted mid-run"
	[ "$(phase 0)" = BOOTSTRAPPING ] && [ "$(phase 1)" = PENDING ] && [ "$(phase 2)" = PENDING ] &&
		pass "rerun: killed with shard-0 BOOTSTRAPPING, shards 1 and 2 PENDING" || fail "rerun: killed with shard-0 BOOTSTRAPPING, shards 1 and 2 PENDING" "$(phase 0) $(phase 1) $(phase 2)"
	read -r l0 t0term < <(wait_leader 15 - 0 $(pod_names 0))
	[ -n "${l0:-}" ] && pass "rerun: shard-0 already has a leader ($l0, term $t0term) before the re-run" || fail "rerun: shard-0 already has a leader before the re-run"
else
	fail "rerun: Job $j bootstrapped shard 0 and is holding" "$(job_logs "$j" | tail -3 | tr '\n' '|')"
fi
t0="$(now)"
if helm --kube-context "$CTX" upgrade "$RELEASE" "$CHART" -n "$NS" --set bootstrap.debugHoldSeconds=0 --wait --wait-for-jobs --timeout "$INSTALL_TIMEOUT" >"$tmp/upgrade2.out" 2>&1; then
	pass "rerun: helm upgrade --wait --wait-for-jobs re-runs the Job and succeeds ($(since "$t0") s)"
else
	fail "rerun: helm upgrade --wait --wait-for-jobs re-runs the Job and succeeds" "$(tail -3 "$tmp/upgrade2.out" | tr '\n' '|')"
fi
j2="$(job_name)"
[ "$j2" != "$j" ] && pass "rerun: a new Job ran ($j2)" || fail "rerun: a new Job ran" "still '$j2'"
job_logs "$j2" | grep -q 'shard-0.*already initialized' && pass "rerun: the new Job found shard-0 already initialized" ||
	fail "rerun: the new Job found shard-0 already initialized" "$(job_logs "$j2" | tail -5 | tr '\n' '|')"
for s in "${SHARDS[@]}"; do
	if out="$(one_group "$s")"; then
		pass "rerun shard-$s: one group, one leader, IDs match the manifest (no second cluster)"
	else
		fail "rerun shard-$s: one group, one leader, IDs match the manifest (no second cluster)" "$out"
	fi
	[ "$(phase "$s")" = BOOTSTRAPPED ] && pass "rerun shard-$s: phase BOOTSTRAPPED" || fail "rerun shard-$s: phase BOOTSTRAPPED" "got '$(phase "$s")'"
done
if [ -n "${t0term:-}" ]; then
	now0="$(leaders $(pod_names 0))"
	[ "$(grep -c . <<<"$now0")" = 1 ] && [ "${now0##* }" -ge "$t0term" ] &&
		pass "rerun shard-0: same group as before the re-run (term ${now0##* } >= $t0term)" || fail "rerun shard-0: same group as before the re-run" "leaders now: $now0"
fi

# --- 10. Evidence ---------------------------------------------------------------------------
# Identify the code under test, not the commit: committing the evidence (or any
# docs change) must not make it stale, so only these paths are fingerprinted.
code_paths=(go.mod go.sum cmd/storagenode cmd/storagectl internal deploy/helm/storage scripts/verify-storage-k8s.sh)
code_tree="$(git ls-tree -r HEAD -- "${code_paths[@]}" | git hash-object --stdin)"
code_dirty="$(git status --porcelain -- "${code_paths[@]}" | grep -q . && echo yes || echo no)"
mkdir -p "$(dirname "$EVIDENCE")"
{
	echo "# S1.5 storage on Kubernetes"
	echo "host:       $(hostname -s)"
	echo "date:       $(date -u +%Y-%m-%dT%H:%M:%SZ)"
	echo "code tree:  ${code_tree:0:12} (git ls-tree -r HEAD -- ${code_paths[*]} | git hash-object --stdin)"
	echo "code dirty: $code_dirty (uncommitted changes under those paths)"
	echo
	echo "## kubectl get pods -o wide"
	kn get pods -o wide
	echo
	for s in "${SHARDS[@]}"; do
		echo "## shard-$s"
		for p in $(pod_names "$s"); do st "$p" | jq -c '{podName, serverId, state, term, leaderId, commitIndex, appliedIndex, caughtUp}'; done
		kn get configmap "$(sts_name "$s")-manifest" -o jsonpath='{.data}' | jq -c .
		echo
	done
	echo "## bootstrap Job logs ($j2)"
	job_logs "$j2"
} 2>&1 | sed 's/[[:space:]]*$//' >"$EVIDENCE"
check "evidence: $EVIDENCE written" test -s "$EVIDENCE"

finish
