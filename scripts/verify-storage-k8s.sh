#!/usr/bin/env bash
# Verify check for S1.5 (storage on Kubernetes), S1.5b (the coordinator in the chart) and
# S3.1 (readiness and placement).
# Run from the repo root:
#   scripts/verify-storage-k8s.sh
#   ELECTION_MAX_SECONDS=15 KEEP_CLUSTER=0 scripts/verify-storage-k8s.sh
# It deletes and recreates the local "reliefgrid" kind cluster, builds and loads the
# storage image, installs deploy/helm/storage and checks bootstrap, placement, the
# bootstrap guard and failover. It leaves the cluster running (KEEP_CLUSTER=0 deletes
# it). Writes evidence to docs/evidence/S1.5/storage-k8s-<host>.txt, identified by a
# fingerprint of the code under test and a dirty flag, every PASS/FAIL line and the
# result, once the result is known (a failed run is written, marked FAILED); the S2.1 checks
# (step 6b2) also go to docs/evidence/S2.1/snapshot-restore-<host>.txt, and the S3.1 checks
# (step 6e) to docs/evidence/S3.1/readiness-placement-<host>.txt. Exits non-zero
# if any check fails; prints one PASS/FAIL line per check.
#
# Interface under test:
#   cmd/storagenode/Dockerfile   multi-stage; final stage distroless, pinned by digest,
#                                non-root; builds ./cmd/storagenode as /storagenode and
#                                ./cmd/storagectl as /storagectl (context: repo root); no /coordinator
#   cmd/coordinator/Dockerfile   (S1.5b) multi-stage; every base pinned by digest, final stage
#                                distroless, non-root; builds ./cmd/coordinator as /coordinator
#   make coordinator-image       docker build of that Dockerfile as reliefgrid/coordinator:dev,
#                                then kind load docker-image into the reliefgrid cluster
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
#       OPERATOR_TOKEN from Secret storage-operator-token (key token),
#       COORDINATOR_URL http://storage-coordinator.<ns>.svc.cluster.local:8080 and
#       HTTP_ADVERTISE $(POD_NAME).storage-shard-N.<ns>.svc.cluster.local:8080 (S1.5b)
#     Deployment storage-coordinator (S1.5b): 1 replica, pod label
#       app.kubernetes.io/component=coordinator, container "coordinator" running
#       /coordinator from reliefgrid/coordinator:dev, nodeSelector role=infra and a toleration for
#       role=infra:NoSchedule, ConfigMap storage-ring mounted so that RING_PATH is its
#       ring.yaml, OPERATOR_TOKEN from Secret storage-operator-token (key token), readiness
#       probe GET /healthz on 8080, runAsNonRoot
#     Service storage-coordinator: ClusterIP, port 8080, selects the coordinator pod
#   coordinator /admin/nodes, /admin/distribution, /admin/locate/{key} with the operator
#     token, and the routed data routes (PUT/GET /internal/data/{key}) of contracts/internal.yaml
#     Service storage-shard-N: headless, publishNotReadyAddresses, ports raft 7000,
#       http 8080, mgmt 8081
#     PodDisruptionBudget storage-shard-N: minAvailable 2
#     NetworkPolicy storage-shard-N: ingress to 8081 only from pods labelled
#       reliefgrid.io/storage-operator=true, to 7000 only from shard N's own pods
#     no manifest ConfigMap: the chart renders none (Helm must never own or replay it)
#     Job storage-bootstrap-<revision>: an ordinary Job (no helm.sh/hook), labelled
#       reliefgrid.io/storage-operator=true, runs /storagectl bootstrap with MANIFESTS (JSON
#       specs {name, groupId, shard, ringVersion, peers}); exit code 2 means "refused" and
#       fails the Job at once (podFailurePolicy); logs "refused:" when it refuses,
#       "created manifest" when it creates one, "already initialized" when it finds a group
#       it did not create in this run, and "verified" for a BOOTSTRAPPED group
#     Role storage-bootstrap: get/update on the three manifests by name, and create on
#       configmaps (RBAC can't scope create by name); nothing else
#   ConfigMap storage-shard-N-manifest (v3.1 §10), created by the Job, never by Helm:
#     labels app.kubernetes.io/{name=storage,component=storage-manifest,managed-by=storagectl},
#     no Helm ownership; groupId shard-N, shard, ringVersion (= contracts/ring.yaml), peers
#     (JSON [{podName,address}] with the stable FQDNs), phase PENDING -> BOOTSTRAPPING ->
#     BOOTSTRAPPED, peerIds (JSON list of podName/incarnation); an existing one without
#     managed-by=storagectl, or whose groupId, shard, ringVersion or peers differ from the
#     Job's spec, is refused (exit 2) before any peer is contacted
#     value bootstrap.debugHoldSeconds: the Job sleeps that long after bootstrapping shard 0
#       and before recording BOOTSTRAPPED, logging "holding" (gives the kill a fixed window)
#   /storagectl status [--addr host:port] [--timeout d]
#                                GET /mgmt/status (default localhost:8081), JSON on stdout,
#                                non-zero exit if unreachable
#   /storagectl dial --addr host:port [--timeout d]
#                                exit 0 if a TCP connection opens
#   /storagectl snapshot [--addr host:port] [--timeout d]   (S2.1)
#                                POST /mgmt/snapshot with the pod's OPERATOR_TOKEN: Raft takes a
#                                snapshot now; JSON {podName, snapshotId, raftIndex, appliedIndex}
#                                on stdout
#   /storagectl dump [--addr host:port] [--timeout d]       (S2.1)
#                                GET /mgmt/dump with OPERATOR_TOKEN: the local FSM's rows read in one
#                                transaction; JSON {podName, appliedIndex, sha256, rows{table: n}}
#   GET /mgmt/status             also restoredIndex: the applied index of the last snapshot the FSM
#                                restored since the process started (0: none)
#   storagenode log              one line per snapshot restore containing "restored snapshot" and
#                                appliedIndex=<n>
#   make placement-check         (S3.1) exit 0 only if every shard has 3 storage pods on 3 distinct
#                                nodes labelled role=data; otherwise non-zero, naming the shard and the
#                                worker. PLACEMENT_PODS_JSON / PLACEMENT_NODES_JSON (env): check those
#                                saved `kubectl get pods|nodes -o json` files instead of the live cluster
#   /storagectl transfer-leader [--addr host:port] [--timeout d]   (S3.1)
#                                POST /mgmt/transfer-leader with OPERATOR_TOKEN: the leader hands
#                                leadership to another voter (raftstore.StepDown) and exits 0 once it
#                                is no longer leader; on a follower, non-zero naming NOT_LEADER
#   GET /mgmt/status             (S3.1) also leaderCommitIndex: the leader's commit index as this
#                                replica last learned it from the leader (the leader: its own);
#                                caughtUp (and /readyz) only once appliedIndex covers it
#   503 NOT_LEADER               (S3.1, #30) leaderHint.address is the leader's advertised
#                                HTTP_ADVERTISE, not its Raft host with this replica's port
set -uo pipefail

CLUSTER=reliefgrid
CTX="kind-$CLUSTER"
NS=reliefgrid
RELEASE=storage
CHART=deploy/helm/storage
DOCKERFILE=cmd/storagenode/Dockerfile
IMAGE=reliefgrid/storagenode:dev
COORD_DOCKERFILE=cmd/coordinator/Dockerfile
COORD_IMAGE=reliefgrid/coordinator:dev
SHARDS=(0 1 2)
WRITE_SHARD="${WRITE_SHARD:-1}"
INSTALL_TIMEOUT="${INSTALL_TIMEOUT:-5m}"
ELECTION_MAX_SECONDS="${ELECTION_MAX_SECONDS:-10}"
COORD_FAILOVER_SECONDS="${COORD_FAILOVER_SECONDS:-20}" # delete leader -> coordinator names the new one (S1.5b)
REJOIN_MAX_SECONDS="${REJOIN_MAX_SECONDS:-120}"
NO_SELF_BOOTSTRAP_SECONDS="${NO_SELF_BOOTSTRAP_SECONDS:-20}"
HOLD_SECONDS="${HOLD_SECONDS:-60}"
KEEP_CLUSTER="${KEEP_CLUSTER:-1}"
EVIDENCE="docs/evidence/S1.5/storage-k8s-$(hostname -s).txt"
EVIDENCE_S21="docs/evidence/S2.1/snapshot-restore-$(hostname -s).txt"
EVIDENCE_S31="docs/evidence/S3.1/readiness-placement-$(hostname -s).txt"
TRANSFER_MAX_SECONDS="${TRANSFER_MAX_SECONDS:-5}" # storagectl transfer-leader -> a new leader (S3.1)
TRANSFERS="${TRANSFERS:-3}"
FAILOVERS="${FAILOVERS:-20}"                      # consecutive leader deletes, each within ELECTION_MAX_SECONDS (S3.1)
LAG_WRITES="${LAG_WRITES:-300}"                   # writes while a follower restarts (S3.1 readiness)

failures=0 passes=0
results="" # every PASS/FAIL line, for the evidence; set to a file once $tmp exists
s21_results="" # step 6b2's PASS/FAIL lines too, while it runs
s31_results="" # step 6e's, likewise
pass() {
	printf 'PASS  %s\n' "$1" | tee -a "${results:-/dev/null}" "${s21_results:-/dev/null}" "${s31_results:-/dev/null}"
	passes=$((passes + 1))
}
fail() { # fail <description> [detail]: a multi-line detail is indented line by line
	{
		printf 'FAIL  %s\n' "$1"
		[ -n "${2:-}" ] && sed 's/^/      /' <<<"$2"
	} | tee -a "${results:-/dev/null}" "${s21_results:-/dev/null}" "${s31_results:-/dev/null}"
	failures=$((failures + 1))
}
check() { # check <description> <command...>
	local desc="$1"
	shift
	if "$@" >/dev/null 2>&1; then pass "$desc"; else fail "$desc"; fi
}
finish() {
	write_evidence
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
results="$tmp/results"
: >"$results"
pf_pid=""
cpf_pid="" # the coordinator port-forward, kept apart from pf_pid
cport=""   # its local port
coord_failover="" # step 6a's measured timeline, for the evidence
cordoned="" # a worker step 6e cordons; uncordoned again on any exit
cleanup() {
	[ -n "$cordoned" ] && kubectl --context "$CTX" uncordon "$cordoned" >/dev/null 2>&1
	[ -n "$pf_pid" ] && kill "$pf_pid" 2>/dev/null
	[ -n "$cpf_pid" ] && kill "$cpf_pid" 2>/dev/null
	rm -rf "$tmp"
}
trap cleanup EXIT

k() { kubectl --context "$CTX" "$@"; }
kn() { k -n "$NS" "$@"; }
sts_name() { printf 'storage-shard-%s' "$1"; }
pod_names() { printf 'storage-shard-%s-0 storage-shard-%s-1 storage-shard-%s-2' "$1" "$1" "$1"; }
fqdn() { printf '%s.storage-shard-%s.%s.svc.cluster.local:7000' "$2" "$1" "$NS"; } # fqdn <shard> <pod>
st() { timeout 5 kubectl --context "$CTX" -n "$NS" exec "$1" -c storagenode -- /storagectl status 2>/dev/null; } # status JSON or empty
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
# wait_leader <seconds> <min-term> <pods...>: print "<pod> <term>" once exactly one pod is
# Leader, at term >= min-term. Which pod does not matter: a deleted leader's pod comes back
# under the same name, often within an election, and may legitimately win (issue #19). Every
# poll goes to $tmp/polls ("<t> pod=state/term/leaderId ..."); polls_detail prints it for a FAIL.
wait_leader() {
	local t0 max="$1" min="$2" p s l line
	shift 2
	t0="$(now)"
	: >"$tmp/polls"
	while awk -v t="$(since "$t0")" -v m="$max" 'BEGIN { exit !(t < m) }'; do
		l="" line=""
		for p in "$@"; do
			s="$(st "$p")"
			line+=" $p=$(field "$s" .state)/$(field "$s" .term)/$(field "$s" .leaderId)"
			[ "$(field "$s" .state)" = Leader ] && l+="$p $(field "$s" .term)"$'\n'
		done
		l="${l%$'\n'}"
		printf '%5ss%s\n' "$(since "$t0")" "$line" >>"$tmp/polls"
		if [ "$(grep -c . <<<"$l")" -eq 1 ] && [ "${l##* }" -ge "$min" ]; then
			echo "$l"
			return 0
		fi
		sleep 0.5
	done
	return 1
}
polls_detail() { echo "polls (t after start, pod=state/term/leaderId):"; cat "$tmp/polls"; }
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
server_ids() { local p; for p in $(pod_names "$1"); do field "$(st "$p")" .serverId; done | sort | tr '\n' ' '; } # server_ids <shard>
manifest_rv() { kn get configmap "$(sts_name "$1")-manifest" -o jsonpath='{.metadata.resourceVersion}' 2>/dev/null; }
peer_ids() { kn get configmap "$(sts_name "$1")-manifest" -o jsonpath='{.data.peerIds}' 2>/dev/null; }
port_forward() { # port_forward <pod>: forward a free local port to the pod's 8080; print the port
	[ -n "$pf_pid" ] && kill "$pf_pid" 2>/dev/null
	local port
	port="$(python3 -c 'import socket; s=socket.socket(); s.bind(("127.0.0.1", 0)); print(s.getsockname()[1])')"
	kn port-forward "pod/$1" "$port:8080" >"$tmp/pf.out" 2>&1 &
	pf_pid=$!
	for _ in $(seq 50); do (exec 3<>"/dev/tcp/127.0.0.1/$port") 2>/dev/null && break; sleep 0.1; done
	echo "$port"
}

COORD=storage-coordinator
COORD_URL="http://$COORD.$NS.svc.cluster.local:8080"
http_adv() { printf '%s.storage-shard-%s.%s.svc.cluster.local:8080' "$2" "$1" "$NS"; } # http_adv <shard> <pod>
# coord_forward: forward a free local port to Service storage-coordinator:8080; sets cport
coord_forward() {
	[ -n "$cpf_pid" ] && kill "$cpf_pid" 2>/dev/null
	cport="$(python3 -c 'import socket; s=socket.socket(); s.bind(("127.0.0.1", 0)); print(s.getsockname()[1])')"
	kn port-forward "svc/$COORD" "$cport:8080" >"$tmp/cpf.out" 2>&1 &
	cpf_pid=$!
	for _ in $(seq 50); do (exec 3<>"/dev/tcp/127.0.0.1/$cport") 2>/dev/null && return 0; sleep 0.1; done
	return 1
}
op_token() { kn get secret storage-operator-token -o jsonpath='{.data.token}' 2>/dev/null | base64 -d; }
# cadmin <path>: GET a coordinator /admin path with the operator token; body to stdout, fails unless 200
cadmin() {
	local code
	code="$(curl -s -m 5 -o "$tmp/cadmin.json" -w '%{http_code}' -H "Authorization: Bearer $optok" "http://127.0.0.1:$cport$1")"
	cat "$tmp/cadmin.json"
	[ "$code" = 200 ]
}
# leader_id <shard>: the serverId of the pod /mgmt/status reports as Leader (one only)
leader_id() {
	local l
	l="$(leaders $(pod_names "$1"))"
	[ "$(grep -c . <<<"$l")" = 1 ] && field "$(st "${l%% *}")" .serverId
}
# nodes_ok <nodes json>: every shard has 3 UP nodes at their advertised addresses, all naming
# the leader /mgmt/status reports; prints the first mismatch
nodes_ok() {
	local j="$1" s p want n
	[ "$(jq '.nodes | length' <<<"$j")" = 9 ] || { echo "$(jq '.nodes | length' <<<"$j") nodes"; return 1; }
	for s in "${SHARDS[@]}"; do
		want="$(leader_id "$s")"
		[ -n "$want" ] || { echo "shard $s: no single leader in /mgmt/status"; return 1; }
		for p in $(pod_names "$s"); do
			n="$(jq -c --arg p "$p" '.nodes[] | select(.nodeId == $p)' <<<"$j")"
			[ "$(q "$n" .shardId)" = "$s" ] && [ "$(q "$n" .state)" = UP ] && [ "$(q "$n" .address)" = "$(http_adv "$s" "$p")" ] &&
				[ "$(q "$n" .leaderId)" = "$want" ] || { echo "$p: $n (want UP, shard $s, $(http_adv "$s" "$p"), leader $want)"; return 1; }
		done
	done
}
# write_key <shard> <tag>: PUT a fresh key the ring gives to <shard> through its leader;
# prints "<key> <value>" on success (version 1 committed), nothing on failure.
write_key() {
	local shard="$1" tag="$2" leader term port key="" value code i
	read -r leader term < <(wait_leader 15 0 $(pod_names "$shard"))
	[ -n "${leader:-}" ] || return 1
	port="$(port_forward "$leader")"
	value="{\"verify\":\"S1.5-$tag-$(date +%s)\"}"
	for i in $(seq 0 200); do # a foreign key is 400 and never proposed
		code="$(curl -s -o "$tmp/put.json" -w '%{http_code}' -X PUT "http://127.0.0.1:$port/internal/data/cfg:s15-$tag-$i" \
			-H 'Content-Type: application/json' -H 'If-None-Match: *' \
			-d "{\"operationId\":\"op-s15-$tag-$i-$(date +%s%N)\",\"at\":1792800000000,\"ringVersion\":$ring_version,\"value\":$value}")"
		[ "$code" = 400 ] && grep -q 'owned by shard' "$tmp/put.json" && continue
		key="cfg:s15-$tag-$i"
		break
	done
	kill "$pf_pid" 2>/dev/null
	pf_pid=""
	[ "$code" = 200 ] && [ "$(jq -r .result.version "$tmp/put.json")" = 1 ] || return 1
	echo "$key $value"
}
# read_key <shard> <key> <value>: a strong read through the shard's leader returns <value> at version 1
read_key() {
	local shard="$1" key="$2" value="$3" leader term port code
	read -r leader term < <(wait_leader 15 0 $(pod_names "$shard"))
	[ -n "${leader:-}" ] || return 1
	port="$(port_forward "$leader")"
	code="$(curl -s -o "$tmp/get.json" -w '%{http_code}' "http://127.0.0.1:$port/internal/data/$key")"
	kill "$pf_pid" 2>/dev/null
	pf_pid=""
	[ "$code" = 200 ] && [ "$(jq -c .record.value "$tmp/get.json")" = "$(jq -c . <<<"$value")" ] && [ "$(jq -r .record.version "$tmp/get.json")" = 1 ]
}
# exec_dial <from-pod> <host:port>: storagectl dial from inside a storage pod
exec_dial() { kn exec "$1" -c storagenode -- /storagectl dial --addr "$2" --timeout 3s >/dev/null 2>&1; }
# write_evidence: called by finish, once the result is known. Identifies the code
# under test, not the commit: committing the evidence (or any docs change) must not
# make it stale, so only these paths are fingerprinted.
write_evidence() {
	local code_paths code_tree code_dirty result s p
	code_paths=(go.mod go.sum Makefile contracts/ring.yaml cmd/storagenode cmd/storagectl cmd/coordinator internal deploy/helm/storage scripts/verify-storage-k8s.sh scripts/placement-check.sh)
	code_tree="$(git ls-tree -r HEAD -- "${code_paths[@]}" | git hash-object --stdin)"
	code_dirty="$(git status --porcelain -- "${code_paths[@]}" | grep -q . && echo yes || echo no)"
	if [ "$failures" -eq 0 ]; then result="PASSED ($passes/$passes checks)"; else result="FAILED ($failures of $((passes + failures)) checks failed)"; fi
	mkdir -p "$(dirname "$EVIDENCE")"
	{
		echo "# S1.5 storage on Kubernetes"
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
		kn get pods -o wide
		echo
		for s in "${SHARDS[@]}"; do
			echo "## shard-$s"
			for p in $(pod_names "$s"); do st "$p" | jq -c '{podName, serverId, state, term, leaderId, commitIndex, leaderCommitIndex, appliedIndex, caughtUp}'; done
			kn get configmap "$(sts_name "$s")-manifest" -o jsonpath='{.data}' | jq -c .
			echo
		done
		echo "## coordinator (S1.5b)"
		echo "failover: ${coord_failover:-not run}"
		echo
		echo "## bootstrap Job logs (${j2:-$(job_name)})"
		job_logs "${j2:-$(job_name)}"
	} 2>&1 | sed 's/[[:space:]]*$//' >"$EVIDENCE"
	echo "evidence: $EVIDENCE ($result)"
	[ -s "${tmp:-}/s21" ] && write_evidence_s21 "$code_tree" "$code_dirty"
	[ -s "${tmp:-}/s31" ] && write_evidence_s31 "$code_tree" "$code_dirty"
}
# write_evidence_s31 <code tree> <code dirty>: step 6e's checks, the measured timings and the
# placement-check outputs, live and doctored
write_evidence_s31() {
	local result n_fail n_all
	n_fail="$(grep -c '^FAIL' "$tmp/s31")" n_all="$(grep -Ec '^(PASS|FAIL)' "$tmp/s31")"
	if [ "$n_fail" -eq 0 ]; then result="PASSED ($n_all/$n_all checks)"; else result="FAILED ($n_fail of $n_all checks failed)"; fi
	mkdir -p "$(dirname "$EVIDENCE_S31")"
	{
		echo "# S3.1 readiness and placement (verify-storage-k8s.sh step 6e)"
		echo "result:     $result"
		echo "host:       $(hostname -s)"
		echo "date:       $(date -u +%Y-%m-%dT%H:%M:%SZ)"
		echo "code tree:  ${1:0:12} (same fingerprint as the S1.5 evidence)"
		echo "code dirty: $2"
		echo "bounds:     transfer-leader ${TRANSFER_MAX_SECONDS}s x$TRANSFERS, failover ${ELECTION_MAX_SECONDS}s x$FAILOVERS, $LAG_WRITES writes while a follower restarts"
		echo
		echo "## checks"
		cat "$tmp/s31"
		echo
		echo "## transfer-leader"
		printf '%s' "$s31_transfers"
		echo
		echo "## failovers (time = delete until the check saw the new leader; polls every ~0.5 s plus 3 execs)"
		printf '%s' "$s31_failovers"
		echo
		echo "## readiness during catch-up"
		printf '%s\n' "$s31_lag"
		echo
		echo "## placement-check output"
		printf '%s' "$s31_placement"
	} 2>&1 | sed 's/[[:space:]]*$//' >"$EVIDENCE_S31"
	echo "evidence: $EVIDENCE_S31 ($result)"
}
# write_evidence_s21 <code tree> <code dirty>: step 6b2's checks and the snapshot shard's state
# as 6b2 left it (s21_state), not at exit: later steps restart and reinstall the shards.
write_evidence_s21() {
	local result n_fail n_all p
	n_fail="$(grep -c '^FAIL' "$tmp/s21")" n_all="$(grep -Ec '^(PASS|FAIL)' "$tmp/s21")"
	if [ "$n_fail" -eq 0 ]; then result="PASSED ($n_all/$n_all checks)"; else result="FAILED ($n_fail of $n_all checks failed)"; fi
	mkdir -p "$(dirname "$EVIDENCE_S21")"
	{
		echo "# S2.1 snapshot and restore under failure (verify-storage-k8s.sh step 6b2)"
		echo "result:     $result"
		echo "host:       $(hostname -s)"
		echo "date:       $(date -u +%Y-%m-%dT%H:%M:%SZ)"
		echo "code tree:  ${1:0:12} (same fingerprint as the S1.5 evidence)"
		echo "code dirty: $2"
		echo "writes:     $SNAP_WRITES before the snapshot, $TAIL_WRITES in the tail"
		echo
		echo "## checks"
		cat "$tmp/s21"
		echo
		cat "$tmp/s21-state" 2>/dev/null
	} 2>&1 | sed 's/[[:space:]]*$//' >"$EVIDENCE_S21"
	echo "evidence: $EVIDENCE_S21 ($result)"
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
grep -Eq 'go build .*\./cmd/coordinator([[:space:]]|$)' "$DOCKERFILE" && fail "image: $DOCKERFILE does not build ./cmd/coordinator (own image)" ||
	pass "image: $DOCKERFILE does not build ./cmd/coordinator (own image)"
if [ -f "$COORD_DOCKERFILE" ]; then
	pass "coordinator image: $COORD_DOCKERFILE exists"
	froms="$(grep -Ei '^FROM[[:space:]]' "$COORD_DOCKERFILE")"
	[ "$(grep -c . <<<"$froms")" -ge 2 ] && pass "coordinator image: multi-stage build" || fail "coordinator image: multi-stage build" "$froms"
	[ -z "$(grep -Ev '@sha256:[0-9a-f]{64}' <<<"$froms")" ] && pass "coordinator image: every base pinned by digest" ||
		fail "coordinator image: every base pinned by digest" "$(grep -Ev '@sha256:[0-9a-f]{64}' <<<"$froms")"
	grep -Eq 'gcr\.io/distroless/' <<<"$(tail -1 <<<"$froms")" && pass "coordinator image: final stage is distroless" ||
		fail "coordinator image: final stage is distroless" "$(tail -1 <<<"$froms")"
	if grep -Eq ':nonroot@' <<<"$(tail -1 <<<"$froms")" || grep -Eq '^USER[[:space:]]+(nonroot|65532)' "$COORD_DOCKERFILE"; then
		pass "coordinator image: runs as non-root"
	else
		fail "coordinator image: runs as non-root"
	fi
	check "coordinator image: builds ./cmd/coordinator" grep -Eq 'go build .*\./cmd/coordinator([[:space:]]|$)' "$COORD_DOCKERFILE"
else
	fail "coordinator image: $COORD_DOCKERFILE exists"
fi
recipe="$(make -n coordinator-image 2>/dev/null)"
if grep -Fq -- "-f $COORD_DOCKERFILE" <<<"$recipe" && grep -Fq "$COORD_IMAGE" <<<"$recipe" && grep -Eq "kind load docker-image .*($COORD_IMAGE|--name $CLUSTER)" <<<"$recipe"; then
	pass "make coordinator-image: builds $COORD_IMAGE from $COORD_DOCKERFILE and kind-loads it"
else
	fail "make coordinator-image: builds $COORD_IMAGE from $COORD_DOCKERFILE and kind-loads it" "recipe: $(tr '\n' '|' <<<"$recipe")"
fi

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
	[ "$(envv COORDINATOR_URL)" = "$COORD_URL" ] && pass "statefulset $n: COORDINATOR_URL=$COORD_URL" ||
		fail "statefulset $n: COORDINATOR_URL=$COORD_URL" "got '$(envv COORDINATOR_URL)'"
	[ "$(envv HTTP_ADVERTISE)" = '$(POD_NAME)'".storage-shard-$s.$NS.svc.cluster.local:8080" ] &&
		pass "statefulset $n: HTTP_ADVERTISE is the stable FQDN on 8080" || fail "statefulset $n: HTTP_ADVERTISE is the stable FQDN on 8080" "got '$(envv HTTP_ADVERTISE)'"
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

	[ -z "$(obj ConfigMap "$n-manifest")" ] && pass "manifest $n-manifest: not rendered by the chart (storagectl owns it)" ||
		fail "manifest $n-manifest: not rendered by the chart (storagectl owns it)"
done
[ "$(jq '[.[] | select(.kind == "ConfigMap" and (.metadata.name | endswith("-manifest")))] | length' "$tmp/render.json")" = 0 ] &&
	pass "chart: renders no manifest ConfigMap at all" || fail "chart: renders no manifest ConfigMap at all"

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
	specs="$(q "$jt" -r '.spec.containers[0].env[] | select(.name == "MANIFESTS") | .value')"
	for s in "${SHARDS[@]}"; do
		sp="$(jq -c --argjson s "$s" '.[] | select(.shard == $s)' <<<"$specs" 2>/dev/null)"
		want_peers="$(for p in $(pod_names "$s"); do printf '%s %s\n' "$p" "$(fqdn "$s" "$p")"; done)"
		[ "$(q "$sp" .name)" = "$(sts_name "$s")-manifest" ] && [ "$(q "$sp" .groupId)" = "shard-$s" ] &&
			[ "$(q "$sp" .ringVersion)" = "$ring_version" ] && [ "$(q "$sp" -r '.peers[] | "\(.podName) \(.address)"')" = "$want_peers" ] &&
			pass "job: MANIFESTS spec for shard $s (name, groupId, ringVersion $ring_version, three stable FQDNs)" ||
			fail "job: MANIFESTS spec for shard $s" "got '$sp'"
	done
	sa="$(q "$jt" .spec.serviceAccountName)"
	role="$(jq -r --arg sa "$sa" '[.[] | select(.kind == "RoleBinding" and any(.subjects[]; .kind == "ServiceAccount" and .name == $sa))] | if length == 1 then .[0].roleRef.name else empty end' "$tmp/render.json")"
	[ "$(jq '[.[] | select(.kind == "ClusterRoleBinding")] | length' "$tmp/render.json")" = 0 ] && [ -n "$role" ] &&
		pass "job: one RoleBinding ($role), no ClusterRoleBinding" || fail "job: one RoleBinding, no ClusterRoleBinding" "role '$role'"
	rules="$(jq -c --arg r "$role" '.[] | select(.kind == "Role" and .metadata.name == $r) | [.rules[] | {apiGroups, resources, resourceNames: (.resourceNames // null), verbs: (.verbs | sort)}] | sort_by(.verbs)' "$tmp/render.json")"
	want_rules='[{"apiGroups":[""],"resources":["configmaps"],"resourceNames":null,"verbs":["create"]},{"apiGroups":[""],"resources":["configmaps"],"resourceNames":["storage-shard-0-manifest","storage-shard-1-manifest","storage-shard-2-manifest"],"verbs":["get","update"]}]'
	[ "$rules" = "$want_rules" ] && pass "job: Role is exactly get/update on the three manifests by name, plus create" ||
		fail "job: Role is exactly get/update on the three manifests by name, plus create" "got $rules"
fi

# Coordinator (S1.5b)
dep="$(obj Deployment "$COORD")"
if [ -z "$dep" ]; then
	fail "coordinator: Deployment $COORD rendered"
else
	pass "coordinator: Deployment $COORD rendered"
	[ "$(q "$dep" .spec.replicas)" = 1 ] && pass "coordinator: 1 replica" || fail "coordinator: 1 replica" "got '$(q "$dep" .spec.replicas)'"
	cpod="$(q "$dep" -c .spec.template.spec)"
	cc="$(q "$cpod" -c '.containers[] | select(.name == "coordinator")')"
	cenv() { q "$cc" --arg k "$1" '.env[] | select(.name == $k) | .value // empty'; }
	[ "$(q "$cc" -r '.image')" = "$COORD_IMAGE" ] && q "$cc" -c '(.command // []) + (.args // [])' | grep -q '"/coordinator"' &&
		pass "coordinator: runs /coordinator from $COORD_IMAGE" || fail "coordinator: runs /coordinator from $COORD_IMAGE" "$(q "$cc" -c '{image, command, args}')"
	[ "$(q "$cpod" '.nodeSelector.role')" = infra ] && pass "coordinator: nodeSelector role=infra" || fail "coordinator: nodeSelector role=infra"
	[ -n "$(q "$cpod" -r '.tolerations[]? | select(.key == "role" and (.value == "infra" or .operator == "Exists") and .effect == "NoSchedule") | .key')" ] &&
		pass "coordinator: tolerates role=infra:NoSchedule" || fail "coordinator: tolerates role=infra:NoSchedule" "$(q "$cpod" -c .tolerations)"
	ring_path="$(cenv RING_PATH)"
	ring_vol="$(q "$cc" -r --arg d "$(dirname "${ring_path:-/x/x}")" --arg f "${ring_path:-/x/x}" '.volumeMounts[]? | select(.mountPath == $d or .mountPath == $f) | .name')"
	[ -n "$ring_vol" ] && [ "$(q "$cpod" -r --arg v "$ring_vol" '.volumes[] | select(.name == $v) | .configMap.name')" = storage-ring ] &&
		pass "coordinator: ConfigMap storage-ring mounted at RING_PATH ($ring_path)" || fail "coordinator: ConfigMap storage-ring mounted at RING_PATH" "RING_PATH '$ring_path', volume '$ring_vol'"
	[ "$(q "$cc" -r '.env[] | select(.name == "OPERATOR_TOKEN") | .valueFrom.secretKeyRef | "\(.name)/\(.key)"')" = storage-operator-token/token ] &&
		pass "coordinator: OPERATOR_TOKEN from the Secret" || fail "coordinator: OPERATOR_TOKEN from the Secret"
	[ "$(q "$cc" -r .readinessProbe.httpGet.path)" = /healthz ] && [[ "$(q "$cc" -r .readinessProbe.httpGet.port)" =~ ^(8080|http)$ ]] &&
		pass "coordinator: readinessProbe GET /healthz on 8080" || fail "coordinator: readinessProbe GET /healthz on 8080" "$(q "$cc" -c .readinessProbe)"
	[ "$(q "$cpod" .securityContext.runAsNonRoot)" = true ] || [ "$(q "$cc" .securityContext.runAsNonRoot)" = true ] &&
		pass "coordinator: runAsNonRoot" || fail "coordinator: runAsNonRoot"
	csvc="$(obj Service "$COORD")"
	sel="$(q "$csvc" -c '.spec.selector // {}')"
	[ -n "$csvc" ] && [ "$(q "$csvc" -c '[.spec.ports[].port]')" = '[8080]' ] && [ "$(q "$csvc" -r '.spec.clusterIP // ""')" != None ] &&
		[ "$(q "$dep" -c --argjson sel "$sel" '.spec.template.metadata.labels | contains($sel) and ($sel | length > 0)')" = true ] &&
		pass "coordinator: Service $COORD on 8080 selects the coordinator pod" || fail "coordinator: Service $COORD on 8080 selects the coordinator pod" "$csvc"
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
docker create --name s15-probe "$IMAGE" >/dev/null 2>&1 && docker cp "s15-probe:/coordinator" "$tmp/bin" >/dev/null 2>&1 &&
	fail "image: $IMAGE does not contain /coordinator" || pass "image: $IMAGE does not contain /coordinator"
docker rm -f s15-probe >/dev/null 2>&1
if make -s coordinator-image >"$tmp/cimage.out" 2>&1; then pass "make coordinator-image: succeeds"; else abort "make coordinator-image: succeeds" "$(tail -5 "$tmp/cimage.out" | tr '\n' '|')"; fi
uid="$(docker image inspect "$COORD_IMAGE" --format '{{.Config.User}}' 2>/dev/null)"
[[ -n "$uid" && "$uid" != 0 && "$uid" != root ]] && pass "coordinator image: $COORD_IMAGE runs as user '$uid'" ||
	fail "coordinator image: $COORD_IMAGE runs as a non-root user" "Config.User '$uid'"
docker create --name s15-probe "$COORD_IMAGE" >/dev/null 2>&1 && docker cp "s15-probe:/coordinator" "$tmp/bin" >/dev/null 2>&1 &&
	pass "coordinator image: contains /coordinator" || fail "coordinator image: contains /coordinator"
docker rm -f s15-probe >/dev/null 2>&1

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
j1="$(job_name)"
[ "$(job_logs "$j1" | grep -c 'created manifest')" = 3 ] && pass "install: Job $j1 created the three manifests" ||
	fail "install: Job $j1 created the three manifests" "$(job_logs "$j1" | grep -c 'created manifest') created"
for s in "${SHARDS[@]}"; do
	cmj="$(kn get configmap "$(sts_name "$s")-manifest" -o json 2>/dev/null)"
	want_peers="$(for p in $(pod_names "$s"); do printf '%s %s\n' "$p" "$(fqdn "$s" "$p")"; done)"
	[ "$(q "$cmj" .data.groupId)" = "shard-$s" ] && [ "$(q "$cmj" .data.shard)" = "$s" ] && [ "$(q "$cmj" .data.ringVersion)" = "$ring_version" ] &&
		[ "$(q "$cmj" -r '.data.peers | fromjson | .[] | "\(.podName) \(.address)"')" = "$want_peers" ] &&
		pass "manifest shard-$s: groupId, shard, ringVersion $ring_version, the three stable FQDNs" || fail "manifest shard-$s: contents" "$(q "$cmj" -c .data)"
	[ "$(q "$cmj" '.metadata.labels["app.kubernetes.io/managed-by"]')" = storagectl ] &&
		[ -z "$(q "$cmj" '(.metadata.annotations // {}) | keys[] | select(startswith("meta.helm.sh/") or startswith("helm.sh/"))')" ] &&
		pass "manifest shard-$s: managed by storagectl, no Helm ownership" || fail "manifest shard-$s: managed by storagectl, no Helm ownership" "$(q "$cmj" -c .metadata)"
done
data_nodes="$(k get nodes -l role=data -o jsonpath='{.items[*].metadata.name}')"
for node in $data_nodes; do
	got="$(kn get pods -l app.kubernetes.io/component=storagenode --field-selector "spec.nodeName=$node" \
		-o jsonpath='{range .items[*]}{.metadata.labels.reliefgrid\.io/shard}{"\n"}{end}' 2>/dev/null | sort | tr '\n' ' ')"
	[ "$got" = "0 1 2 " ] && pass "placement: $node runs one replica of every shard" || fail "placement: $node runs one replica of every shard" "shards on it: '$got'"
done

# --- 4b. Coordinator: placement, registration, leaders, routing (S1.5b) -------------------
infra_node="$(k get nodes -l role=infra -o jsonpath='{.items[0].metadata.name}')"
cnode="$(kn get pods -l app.kubernetes.io/component=coordinator -o jsonpath='{.items[0].spec.nodeName}' 2>/dev/null)"
[ -n "$cnode" ] && [ "$cnode" = "$infra_node" ] && pass "coordinator: runs on the infra worker ($infra_node)" ||
	fail "coordinator: runs on the infra worker ($infra_node)" "node '$cnode'"
kn wait --for=condition=Available "deployment/$COORD" --timeout=60s >/dev/null 2>&1 && pass "coordinator: Deployment Available" ||
	fail "coordinator: Deployment Available"
optok="$(op_token)"
if coord_forward; then
	pass "coordinator: port-forward to Service $COORD"
	code="$(curl -s -m 5 -o /dev/null -w '%{http_code}' "http://127.0.0.1:$cport/admin/nodes")"
	[ "$code" = 401 ] && pass "coordinator: /admin/nodes without the token is 401" || fail "coordinator: /admin/nodes without the token is 401" "HTTP $code"
	t0="$(now)" why=""
	for _ in $(seq 30); do
		nodes="$(cadmin /admin/nodes)" && why="$(nodes_ok "$nodes")" && break
		why="${why:-$(head -c 300 <<<"$nodes")}"
		sleep 1
	done
	[ -z "$why" ] && pass "coordinator: /admin/nodes shows all 9 nodes UP at their HTTP_ADVERTISE addresses, naming each shard's leader ($(since "$t0") s)" ||
		fail "coordinator: /admin/nodes shows all 9 nodes UP, naming each shard's leader" "$why"
	if dist="$(cadmin /admin/distribution)"; then
		ok=1
		[ "$(q "$dist" .ringVersion)" = "$ring_version" ] && [ "$(q "$dist" '.shards | length')" = 3 ] || ok=""
		for s in "${SHARDS[@]}"; do
			[ "$(q "$dist" --argjson s "$s" '.shards[] | select(.shard == $s) | "\(.replicas) \(.leaderId)"')" = "3 $(leader_id "$s")" ] || ok=""
		done
		[ -n "$ok" ] && pass "coordinator: /admin/distribution answers (ringVersion $ring_version, 3 shards, 3 replicas and the leader each)" ||
			fail "coordinator: /admin/distribution answers with 3 shards, 3 replicas and the leader each" "$(head -c 400 <<<"$dist")"
	else
		fail "coordinator: /admin/distribution answers 200" "$(head -c 300 <<<"$dist")"
	fi
	ckey="cfg:s15b-coord-$(date +%s)" cval="{\"verify\":\"S1.5b-$(date +%s)\"}"
	code="$(curl -s -m 6 -o "$tmp/cput.json" -w '%{http_code}' -X PUT "http://127.0.0.1:$cport/internal/data/$ckey" \
		-H 'Content-Type: application/json' -H 'If-None-Match: *' \
		-d "{\"operationId\":\"op-s15b-$(date +%s%N)\",\"at\":1792800000000,\"ringVersion\":$ring_version,\"value\":$cval}")"
	[ "$code" = 200 ] && [ "$(jq -r .result.version "$tmp/cput.json" 2>/dev/null)" = 1 ] &&
		pass "coordinator: PUT $ckey through the coordinator committed (version 1)" ||
		fail "coordinator: PUT $ckey through the coordinator committed" "HTTP $code: $(cat "$tmp/cput.json" 2>/dev/null)"
	code="$(curl -s -m 6 -o "$tmp/cget.json" -w '%{http_code}' "http://127.0.0.1:$cport/internal/data/$ckey")"
	[ "$code" = 200 ] && [ "$(jq -c .record.value "$tmp/cget.json" 2>/dev/null)" = "$(jq -c . <<<"$cval")" ] &&
		pass "coordinator: GET $ckey through the coordinator returns the write" ||
		fail "coordinator: GET $ckey through the coordinator returns the write" "HTTP $code: $(cat "$tmp/cget.json" 2>/dev/null)"
else
	fail "coordinator: port-forward to Service $COORD" "$(cat "$tmp/cpf.out" 2>/dev/null)"
fi

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
raft0="$(fqdn 0 storage-shard-0-0)"
exec_dial storage-shard-0-1 "$raft0" && pass "networkpolicy: storage-shard-0-1 reaches $raft0 (same shard)" ||
	fail "networkpolicy: storage-shard-0-1 reaches $raft0 (same shard)"
exec_dial storage-shard-1-0 "${raft0%:7000}:8080" && pass "networkpolicy: storage-shard-1-0 reaches storage-shard-0-0:8080 (data, control)" ||
	fail "networkpolicy: storage-shard-1-0 reaches storage-shard-0-0:8080 (data, control)"
exec_dial storage-shard-1-0 "$raft0" && fail "networkpolicy: storage-shard-1-0 cannot reach $raft0 (other shard)" ||
	pass "networkpolicy: storage-shard-1-0 cannot reach $raft0 (other shard)"

# --- 6. A write through the leader survives deleting that leader -------------------------
wpods=($(pod_names "$WRITE_SHARD"))
read -r leader term < <(wait_leader 10 0 "${wpods[@]}")
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
	read -r newleader newterm < <(wait_leader "$ELECTION_MAX_SECONDS" $((term + 1)) "${wpods[@]}")
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
		fail "failover shard-$WRITE_SHARD: a new leader within ${ELECTION_MAX_SECONDS}s of deleting $leader" "$(polls_detail)"
	fi
	if kn wait --for=condition=Ready "pod/$leader" --timeout="${REJOIN_MAX_SECONDS}s" >/dev/null 2>&1 && out="$(one_group "$WRITE_SHARD")"; then
		pass "failover shard-$WRITE_SHARD: $leader rejoins the same group (same ID) and is Ready"
	else
		fail "failover shard-$WRITE_SHARD: $leader rejoins the same group (same ID) and is Ready" "${out:-not Ready}"
	fi
fi

# --- 6a. The coordinator follows a leader change (S1.5b) -----------------------------------
cs="$(cadmin "/admin/locate/${ckey:-none}" 2>/dev/null | jq -r '.shard // empty' 2>/dev/null)"
if [ -n "${ckey:-}" ] && [ -n "$cs" ]; then
	read -r cleader cterm < <(wait_leader 15 0 $(pod_names "$cs"))
	t0="$(now)"
	kn delete pod "${cleader:-none}" --wait=false >/dev/null 2>&1
	# The strict election bound is step 6's (issue #19); here the coordinator gets
	# COORD_FAILOVER_SECONDS from the delete to name the new leader, and the time is recorded.
	read -r cnew cnewterm < <(wait_leader "$COORD_FAILOVER_SECONDS" $((${cterm:-0} + 1)) $(pod_names "$cs"))
	t_elected="$(since "$t0")"
	# The winner may be the recreated pod, with the old leader's serverId; so the coordinator
	# must also report it at the new term, or a stale answer would pass.
	want="$( [ -n "${cnew:-}" ] && field "$(st "$cnew")" .serverId)" got="" gterm=0
	while [ -n "$want" ] && awk -v t="$(since "$t0")" -v m="$COORD_FAILOVER_SECONDS" 'BEGIN { exit !(t < m) }'; do
		got="$(cadmin "/admin/locate/$ckey" 2>/dev/null | jq -r '.leaderId // empty' 2>/dev/null)"
		gterm="$(cadmin /admin/nodes 2>/dev/null | jq -r --arg p "$cnew" '.nodes[] | select(.nodeId == $p) | .term' 2>/dev/null)"
		[ "$got" = "$want" ] && [ "${gterm:-0}" -ge "$cnewterm" ] 2>/dev/null && break
		sleep 0.5
	done
	if [ -n "$want" ] && [ "$got" = "$want" ] && [ "${gterm:-0}" -ge "${cnewterm:-1}" ] 2>/dev/null; then
		coord_failover="shard-$cs: deleted $cleader (term $cterm); $cnew leads at term $cnewterm after $t_elected s; the coordinator names $want after $(since "$t0") s"
		pass "coordinator failover shard-$cs: the coordinator names the new leader within ${COORD_FAILOVER_SECONDS}s ($(since "$t0") s; elected after $t_elected s)"
	else
		coord_failover="shard-$cs: deleted ${cleader:-?}; new leader '${cnew:-none}' after $t_elected s; the coordinator named '$got' (term '${gterm:-}') at the ${COORD_FAILOVER_SECONDS}s deadline"
		fail "coordinator failover shard-$cs: the coordinator names the new leader within ${COORD_FAILOVER_SECONDS}s" "$coord_failover"$'\n'"$( [ -z "${cnew:-}" ] && polls_detail)"
	fi
	code="$(curl -s -m 6 -o "$tmp/cget.json" -w '%{http_code}' "http://127.0.0.1:$cport/internal/data/$ckey")"
	[ "$code" = 200 ] && [ "$(jq -c .record.value "$tmp/cget.json" 2>/dev/null)" = "$(jq -c . <<<"$cval")" ] &&
		pass "coordinator failover shard-$cs: $ckey still reads back through the coordinator" ||
		fail "coordinator failover shard-$cs: $ckey still reads back through the coordinator" "HTTP $code: $(cat "$tmp/cget.json" 2>/dev/null)"
	kn wait --for=condition=Ready "pod/${cleader:-none}" --timeout="${REJOIN_MAX_SECONDS}s" >/dev/null 2>&1 &&
		pass "coordinator failover shard-$cs: ${cleader:-?} rejoins and is Ready" || fail "coordinator failover shard-$cs: ${cleader:-?} rejoins and is Ready"
else
	fail "coordinator failover: the coordinator locates the key written in 4b" "key '${ckey:-}', shard '$cs'"
fi
[ -n "$cpf_pid" ] && kill "$cpf_pid" 2>/dev/null
cpf_pid=""

# --- 6b. Full restart of every replica of one shard ----------------------------------------
RS=2
ids_before="$(server_ids "$RS")"
if kv="$(write_key "$RS" restart)"; then
	pass "restart shard-$RS: sentinel ${kv%% *} written before the restart"
	uids() { kn get pods $(pod_names "$RS") -o jsonpath='{.items[*].metadata.uid}' 2>/dev/null; }
	uids_before="$(uids)"
	t0="$(now)"
	kn delete pod $(pod_names "$RS") --wait=false >/dev/null 2>&1
	for _ in $(seq "$REJOIN_MAX_SECONDS"); do # all three replaced, so the Ready wait sees the new pods
		u="$(uids)"
		[ "$(wc -w <<<"$u")" = 3 ] && [ -z "$(comm -12 <(tr ' ' '\n' <<<"$uids_before" | sort) <(tr ' ' '\n' <<<"$u" | sort))" ] && break
		sleep 1
	done
	if kn wait --for=condition=Ready $(for p in $(pod_names "$RS"); do echo "pod/$p"; done) --timeout="${REJOIN_MAX_SECONDS}s" >/dev/null 2>&1 &&
		out="$(one_group "$RS")"; then
		pass "restart shard-$RS: all three pods deleted at once come back as one group ($(since "$t0") s)"
	else
		fail "restart shard-$RS: all three pods deleted at once come back as one group" "${out:-not Ready}"
	fi
	[ "$(server_ids "$RS")" = "$ids_before" ] && pass "restart shard-$RS: serverIds unchanged" || fail "restart shard-$RS: serverIds unchanged" "before '$ids_before', after '$(server_ids "$RS")'"
	read_key "$RS" "${kv%% *}" "${kv#* }" && pass "restart shard-$RS: the sentinel reads back" || fail "restart shard-$RS: the sentinel reads back" "$(cat "$tmp/get.json" 2>/dev/null)"
else
	fail "restart shard-$RS: sentinel written before the restart"
fi

# --- 6b2. Full restart restores every replica from a snapshot (S2.1) ------------------------
# Past the demo SnapshotThreshold, force a snapshot on all three replicas, write a tail,
# delete the whole shard: every replica must restore its snapshot, then replay the tail.
s21_results="$tmp/s21"
SS=0
SNAP_WRITES="${SNAP_WRITES:-1100}" # demo profile SnapshotThreshold is 1024
TAIL_WRITES=20
snap_status() { field "$(st "$1")" "$2"; } # snap_status <pod> <jq path>
dump_of() { kn exec "$1" -c storagenode -- /storagectl dump 2>/dev/null; }
# cas_updates <port> <key> <from-version> <n>: n CAS updates of <key>; prints the last version
cas_updates() {
	local port="$1" key="$2" v="$3" n="$4" i code
	for ((i = 0; i < n; i++)); do
		code="$(curl -s -o "$tmp/cas.json" -w '%{http_code}' -X PUT "http://127.0.0.1:$port/internal/data/$key" \
			-H 'Content-Type: application/json' -H "If-Match: \"$v\"" \
			-d "{\"operationId\":\"op-s21-$key-$v-$(date +%s%N)\",\"at\":1792800000000,\"ringVersion\":$ring_version,\"value\":{\"n\":$v}}")"
		[ "$code" = 200 ] && [ "$(jq -r .result.version "$tmp/cas.json")" = $((v + 1)) ] || { echo "$v"; return 1; }
		v=$((v + 1))
	done
	echo "$v"
}
# all_caught_up <shard>: every replica reports the same applied index as the leader, and caughtUp
all_caught_up() {
	local deadline=$((SECONDS + 60)) p a want ok
	while [ "$SECONDS" -lt "$deadline" ]; do
		ok=1 want=""
		for p in $(pod_names "$1"); do
			a="$(snap_status "$p" .appliedIndex)"
			[ "$(snap_status "$p" .caughtUp)" = true ] && [ -n "$a" ] || { ok=""; break; }
			[ -z "$want" ] && want="$a"
			[ "$a" = "$want" ] || { ok=""; break; }
		done
		[ -n "$ok" ] && return 0
		sleep 0.5
	done
	return 1
}
# same_dumps <shard>: prints "<appliedIndex> <sha256>" if all three replicas' dumps agree
same_dumps() {
	local p d first="" cur
	for p in $(pod_names "$1"); do
		d="$(dump_of "$p")" || return 1
		cur="$(jq -r '"\(.appliedIndex) \(.sha256)"' <<<"$d")"
		[ -z "$first" ] && first="$cur"
		[ "$cur" = "$first" ] || return 1
	done
	echo "$first"
}
declare -A snap_applied
if kvA="$(write_key "$SS" snapA)" && kvK="$(write_key "$SS" snapK)"; then
	pass "snapshot shard-$SS: sentinel ${kvA%% *} written before the snapshot"
	read -r sleader _ < <(wait_leader 15 0 $(pod_names "$SS"))
	port="$(port_forward "$sleader")"
	t0="$(now)"
	if kv_version="$(cas_updates "$port" "${kvK%% *}" 1 "$SNAP_WRITES")"; then
		pass "snapshot shard-$SS: $SNAP_WRITES writes through $sleader, past the demo threshold of 1024 ($(since "$t0") s)"
	else
		fail "snapshot shard-$SS: $SNAP_WRITES writes through $sleader" "stopped at version $kv_version: $(cat "$tmp/cas.json" 2>/dev/null)"
	fi
	kill "$pf_pid" 2>/dev/null
	pf_pid=""
	all_caught_up "$SS" || fail "snapshot shard-$SS: replicas caught up before the snapshot"
	for p in $(pod_names "$SS"); do
		out="$(kn exec "$p" -c storagenode -- /storagectl snapshot --timeout 60s 2>&1)"
		a="$(jq -r '.appliedIndex // empty' <<<"$out" 2>/dev/null)"
		snap_applied[$p]="$a"
		[ -n "$a" ] && [ "$a" -ge "$SNAP_WRITES" ] && pass "snapshot shard-$SS: $p took a snapshot at applied index $a ($(jq -r .snapshotId <<<"$out"))" ||
			fail "snapshot shard-$SS: $p took a snapshot past the threshold" "$out"
	done
	kvB="$(write_key "$SS" snapB)" && pass "snapshot shard-$SS: sentinel ${kvB%% *} written after the snapshot (in the tail)" ||
		fail "snapshot shard-$SS: sentinel written after the snapshot"
	read -r sleader _ < <(wait_leader 15 0 $(pod_names "$SS"))
	port="$(port_forward "$sleader")"
	cas_updates "$port" "${kvK%% *}" "$kv_version" "$TAIL_WRITES" >/dev/null && pass "snapshot shard-$SS: $TAIL_WRITES more writes in the tail" ||
		fail "snapshot shard-$SS: $TAIL_WRITES more writes in the tail" "$(cat "$tmp/cas.json" 2>/dev/null)"
	kill "$pf_pid" 2>/dev/null
	pf_pid=""
	if all_caught_up "$SS" && before="$(same_dumps "$SS")"; then
		pass "snapshot shard-$SS: the three dumps agree before the restart (applied index ${before%% *})"
	else
		fail "snapshot shard-$SS: the three dumps agree before the restart"
	fi

	suids() { kn get pods $(pod_names "$SS") -o jsonpath='{.items[*].metadata.uid}' 2>/dev/null; }
	suids_before="$(suids)"
	t0="$(now)"
	kn delete pod $(pod_names "$SS") --wait=false >/dev/null 2>&1
	for _ in $(seq "$REJOIN_MAX_SECONDS"); do
		u="$(suids)"
		[ "$(wc -w <<<"$u")" = 3 ] && [ -z "$(comm -12 <(tr ' ' '\n' <<<"$suids_before" | sort) <(tr ' ' '\n' <<<"$u" | sort))" ] && break
		sleep 1
	done
	if kn wait --for=condition=Ready $(for p in $(pod_names "$SS"); do echo "pod/$p"; done) --timeout="${REJOIN_MAX_SECONDS}s" >/dev/null 2>&1 &&
		out="$(one_group "$SS")"; then
		pass "snapshot shard-$SS: all three pods deleted at once come back as one group ($(since "$t0") s)"
	else
		fail "snapshot shard-$SS: all three pods deleted at once come back as one group" "${out:-not Ready}"
	fi
	all_caught_up "$SS" || fail "snapshot shard-$SS: replicas caught up after the restart"
	for p in $(pod_names "$SS"); do
		k="${snap_applied[$p]:-none}"
		r="$(snap_status "$p" .restoredIndex)"
		[ "$r" = "$k" ] && pass "snapshot shard-$SS: $p restored its snapshot at applied index $k" ||
			fail "snapshot shard-$SS: $p restored its snapshot at applied index $k" "restoredIndex '$r'"
		kn logs "$p" -c storagenode 2>/dev/null | grep 'restored snapshot' | grep -q "appliedIndex=$k\b" &&
			pass "snapshot shard-$SS: $p logged the restore at applied index $k" ||
			fail "snapshot shard-$SS: $p logged the restore at applied index $k" "$(kn logs "$p" -c storagenode 2>/dev/null | grep 'restored snapshot' | tail -2 | tr '\n' '|')"
		a="$(snap_status "$p" .appliedIndex)"
		[ -n "$a" ] && [ "$k" != none ] && [ "$a" -gt "$k" ] && pass "snapshot shard-$SS: $p then replayed the tail to applied index $a" ||
			fail "snapshot shard-$SS: $p then replayed the tail past $k" "applied index '$a'"
	done
	read_key "$SS" "${kvA%% *}" "${kvA#* }" && pass "snapshot shard-$SS: the sentinel from before the snapshot reads back" ||
		fail "snapshot shard-$SS: the sentinel from before the snapshot reads back" "$(cat "$tmp/get.json" 2>/dev/null)"
	[ -n "${kvB:-}" ] && read_key "$SS" "${kvB%% *}" "${kvB#* }" && pass "snapshot shard-$SS: the sentinel from the tail reads back" ||
		fail "snapshot shard-$SS: the sentinel from the tail reads back" "$(cat "$tmp/get.json" 2>/dev/null)"
	if after="$(same_dumps "$SS")" && [ "$after" = "${before:-}" ]; then
		pass "snapshot shard-$SS: the three dumps agree after the restart and equal the dump before it (${after%% *})"
	else
		fail "snapshot shard-$SS: the three dumps agree after the restart and equal the dump before it" "before '${before:-}', after '${after:-disagree}'"
	fi
else
	fail "snapshot shard-$SS: sentinels written before the snapshot"
fi
# s21_state: shard $SS as 6b2 left it, for the S2.1 evidence
{
	echo "## shard-$SS after the restart"
	for p in $(pod_names "$SS"); do st "$p" | jq -c '{podName, state, term, commitIndex, appliedIndex, restoredIndex, caughtUp}'; done
	echo
	echo "## dumps"
	for p in $(pod_names "$SS"); do dump_of "$p" | jq -c .; done
	echo
	echo "## restore log lines"
	for p in $(pod_names "$SS"); do kn logs "$p" -c storagenode 2>/dev/null | grep 'restored snapshot' | tail -1; done
} >"$tmp/s21-state" 2>&1
s21_results=""

# --- 6c. helm upgrade on healthy groups only verifies ---------------------------------------
declare -A rv_before ids_all
for s in "${SHARDS[@]}"; do rv_before[$s]="$(manifest_rv "$s")"; ids_all[$s]="$(server_ids "$s")"; done
if helm --kube-context "$CTX" upgrade "$RELEASE" "$CHART" -n "$NS" --wait --wait-for-jobs --timeout "$INSTALL_TIMEOUT" >"$tmp/upgrade1.out" 2>&1; then
	pass "upgrade: helm upgrade --wait --wait-for-jobs on healthy groups succeeds"
else
	fail "upgrade: helm upgrade --wait --wait-for-jobs on healthy groups succeeds" "$(tail -3 "$tmp/upgrade1.out" | tr '\n' '|')"
fi
ju="$(job_name)"
for s in "${SHARDS[@]}"; do
	job_logs "$ju" | grep -q "shard-$s: verified" && pass "upgrade: Job $ju verified shard-$s" || fail "upgrade: Job $ju verified shard-$s" "$(job_logs "$ju" | tail -3 | tr '\n' '|')"
	[ "$(manifest_rv "$s")" = "${rv_before[$s]}" ] && [ "$(phase "$s")" = BOOTSTRAPPED ] &&
		pass "upgrade: manifest shard-$s untouched (resourceVersion ${rv_before[$s]}, BOOTSTRAPPED)" || fail "upgrade: manifest shard-$s untouched" "rv ${rv_before[$s]} -> $(manifest_rv "$s"), phase $(phase "$s")"
	[ "$(server_ids "$s")" = "${ids_all[$s]}" ] || fail "upgrade: shard-$s serverIds unchanged" "${ids_all[$s]} -> $(server_ids "$s")"
done
job_logs "$ju" | grep -Eq 'created manifest|msg="?[^ ]*: bootstrapped' && fail "upgrade: the Job created or bootstrapped nothing" "$(job_logs "$ju" | tr '\n' '|')" ||
	pass "upgrade: the Job created or bootstrapped nothing"

# --- 6d. helm rollback to revision 1 cannot touch the manifests ----------------------------
declare -A pids_before
for s in "${SHARDS[@]}"; do pids_before[$s]="$(peer_ids "$s")"; done
job_uids() { kn get jobs -l app.kubernetes.io/component=storage-bootstrap -o jsonpath='{range .items[*]}{.metadata.uid}{"\n"}{end}' 2>/dev/null; }
uids_before_rb="$(job_uids)"
if helm --kube-context "$CTX" rollback "$RELEASE" 1 -n "$NS" --wait --wait-for-jobs --timeout "$INSTALL_TIMEOUT" >"$tmp/rollback.out" 2>&1; then
	pass "rollback: helm rollback $RELEASE 1 succeeds"
else
	fail "rollback: helm rollback $RELEASE 1 succeeds" "$(tail -3 "$tmp/rollback.out" | tr '\n' '|')"
fi
# Revision 1's Job was deleted by the upgrade (its name left the chart), so the
# rollback re-creates it and it runs again: it must verify, not create or bootstrap.
jr="$(job_name)"
jr_uid="$(kn get job "$jr" -o jsonpath='{.metadata.uid}' 2>/dev/null)"
[ -n "$jr_uid" ] && ! grep -qx "$jr_uid" <<<"$uids_before_rb" && pass "rollback: a new bootstrap Job ran ($jr, uid $jr_uid)" ||
	fail "rollback: a new bootstrap Job ran" "newest '$jr' uid '$jr_uid', existing before: $(tr '\n' ' ' <<<"$uids_before_rb")"
job_logs "$jr" | grep -Eq 'created manifest|msg="?[^ ]*: bootstrapped' && fail "rollback: the Job created or bootstrapped nothing" "$(job_logs "$jr" | tr '\n' '|')" ||
	pass "rollback: the Job created or bootstrapped nothing"
for s in "${SHARDS[@]}"; do
	job_logs "$jr" | grep -q "shard-$s: verified" && pass "rollback: Job $jr verified shard-$s" || fail "rollback: Job $jr verified shard-$s" "$(job_logs "$jr" | tail -3 | tr '\n' '|')"
	[ "$(manifest_rv "$s")" = "${rv_before[$s]}" ] && [ "$(phase "$s")" = BOOTSTRAPPED ] && [ "$(peer_ids "$s")" = "${pids_before[$s]}" ] &&
		pass "rollback: manifest shard-$s untouched (resourceVersion, BOOTSTRAPPED, peerIds)" ||
		fail "rollback: manifest shard-$s untouched" "rv ${rv_before[$s]} -> $(manifest_rv "$s"), phase $(phase "$s"), peerIds $(peer_ids "$s")"
done

# --- 6e. Readiness and placement (S3.1) ------------------------------------------------------
# placement-check on the live cluster and on doctored copies; transfer-leader; the not-leader
# hint (#30); readiness against the leader's commit index (#22), including a follower that
# restarts during a write burst; then FAILOVERS consecutive leader deletes (#19).
s31_results="$tmp/s31"
: >"$s31_results"
s31_transfers="" s31_failovers="" s31_lag="" s31_placement=""
TS="$WRITE_SHARD"
tpods=($(pod_names "$TS"))
# placement_check [pods json] [nodes json]: make placement-check, live when no files; output in $tmp/pc.out
placement_check() { PLACEMENT_PODS_JSON="${1:-}" PLACEMENT_NODES_JSON="${2:-}" make -s placement-check >"$tmp/pc.out" 2>&1; }
# doctor <name> <jq filter>: the saved pods JSON with one change, as $tmp/pods-<name>.json
doctor() { jq "$2" "$tmp/pods.json" >"$tmp/pods-$1.json"; }
# rejects <description> <name> <shard> <worker>: placement-check fails on pods-<name>.json, naming both
rejects() {
	if placement_check "$tmp/pods-$2.json" "$tmp/nodes.json"; then
		fail "placement-check: fails when $1" "exit 0: $(cat "$tmp/pc.out")"
	elif grep -Eq "shard[- ]$3([^0-9]|$)" "$tmp/pc.out" && grep -Fq -- "$4" "$tmp/pc.out"; then
		pass "placement-check: fails when $1, naming shard $3 and $4"
	else
		fail "placement-check: fails when $1, naming shard $3 and $4" "$(cat "$tmp/pc.out")"
	fi
	s31_placement+="## $1"$'\n'"$(cat "$tmp/pc.out")"$'\n'
}
optok="$(op_token)"

check "make placement-check: target exists" make -n placement-check
if placement_check; then
	pass "placement-check: the live placement passes"
else
	fail "placement-check: the live placement passes" "$(cat "$tmp/pc.out")"
fi
s31_placement+="## live"$'\n'"$(cat "$tmp/pc.out")"$'\n'
kn get pods -o json >"$tmp/pods.json"
k get nodes -o json >"$tmp/nodes.json"
podnode() { jq -r --arg p "$1" '.items[] | select(.metadata.name == $p) | .spec.nodeName' "$tmp/pods.json"; }
n10="$(podnode storage-shard-1-0)"
doctor shared "(.items[] | select(.metadata.name == \"storage-shard-1-2\") | .spec.nodeName) = \"$n10\""
rejects "storage-shard-1-0 and storage-shard-1-2 share a worker" shared 1 "$n10"
doctor infra "(.items[] | select(.metadata.name == \"storage-shard-2-1\") | .spec.nodeName) = \"$infra_node\""
rejects "storage-shard-2-1 is on the infra worker" infra 2 "$infra_node"
doctor unscheduled 'del(.items[] | select(.metadata.name == "storage-shard-0-2") | .spec.nodeName)'
rejects "storage-shard-0-2 is not scheduled" unscheduled 0 storage-shard-0-2
doctor missing 'del(.items[] | select(.metadata.name == "storage-shard-0-1"))'
rejects "storage-shard-0-1 is missing" missing 0 storage-shard-0-1

# transfer-leader: a follower refuses; the leader hands over within TRANSFER_MAX_SECONDS
read -r tl tterm < <(wait_leader 15 0 "${tpods[@]}")
kv_t="$(write_key "$TS" transfer)"
for p in "${tpods[@]}"; do [ "$p" != "${tl:-}" ] && tf="$p" && break; done
out="$(kn exec "$tf" -c storagenode -- /storagectl transfer-leader 2>&1)"
rc=$?
read -r al aterm < <(wait_leader 5 0 "${tpods[@]}")
[ "$rc" != 0 ] && grep -q NOT_LEADER <<<"$out" && [ "${al:-}" = "${tl:-x}" ] && [ "${aterm:-}" = "${tterm:-x}" ] &&
	pass "transfer-leader: on follower $tf exits non-zero with NOT_LEADER; ${tl:-?} still leads at term ${tterm:-?}" ||
	fail "transfer-leader: on follower $tf exits non-zero with NOT_LEADER; the leader is unchanged" "exit $rc: $(head -c 300 <<<"$out"); leader before ${tl:-?}/${tterm:-?}, after ${al:-?}/${aterm:-?}"
for i in $(seq "$TRANSFERS"); do
	read -r tl tterm < <(wait_leader 15 0 "${tpods[@]}")
	t0="$(now)"
	out="$(kn exec "${tl:-none}" -c storagenode -- /storagectl transfer-leader 2>&1)"
	rc=$?
	read -r nl nterm < <(wait_leader "$TRANSFER_MAX_SECONDS" $((${tterm:-0} + 1)) "${tpods[@]}")
	el="$(since "$t0")"
	ost="$(st "${tl:-none}")" nid="$( [ -n "${nl:-}" ] && field "$(st "$nl")" .serverId)"
	if [ "$rc" = 0 ] && [ -n "${nl:-}" ] && [ "$nl" != "$tl" ] && awk -v t="$el" -v m="$TRANSFER_MAX_SECONDS" 'BEGIN { exit !(t <= m) }' &&
		[ "$(field "$ost" .state)" = Follower ] && [ "$(field "$ost" .leaderId)" = "$nid" ]; then
		pass "transfer-leader $i/$TRANSFERS: $tl -> $nl (term $tterm -> $nterm) in $el s; $tl follows $nl"
	else
		fail "transfer-leader $i/$TRANSFERS: $tl hands leadership to another pod within ${TRANSFER_MAX_SECONDS}s and follows it" \
			"exit $rc: $(head -c 300 <<<"$out"); new leader '${nl:-none}' after $el s; $tl now $(field "$ost" .state) naming '$(field "$ost" .leaderId)'"$'\n'"$(polls_detail)"
	fi
	s31_transfers+="$i: ${tl:-?} (term ${tterm:-?}) -> ${nl:-none} (term ${nterm:-?}) in $el s, storagectl exit $rc"$'\n'
done
if out="$(one_group "$TS")"; then pass "transfer-leader: shard-$TS is one group afterwards"; else fail "transfer-leader: shard-$TS is one group afterwards" "$out"; fi
[ -n "$kv_t" ] && read_key "$TS" "${kv_t%% *}" "${kv_t#* }" && pass "transfer-leader: the key written before the transfers reads back" ||
	fail "transfer-leader: the key written before the transfers reads back" "key '${kv_t%% *}': $(cat "$tmp/get.json" 2>/dev/null)"
write_key "$TS" after-transfer >/dev/null && pass "transfer-leader: a write through the new leader commits" || fail "transfer-leader: a write through the new leader commits"
read -r nl nterm < <(wait_leader 15 0 "${tpods[@]}")
nid="$(field "$(st "${nl:-none}")" .serverId)" ok=""
if coord_forward; then
	t0="$(now)"
	while awk -v t="$(since "$t0")" -v m="$COORD_FAILOVER_SECONDS" 'BEGIN { exit !(t < m) }'; do
		nodes="$(cadmin /admin/nodes 2>/dev/null)"
		[ "$(jq -r --arg p "${nl:-none}" --arg id "$nid" --argjson t "${nterm:-0}" --argjson s "$TS" \
			'[.nodes[] | select(.shardId == $s)] | (length == 3 and all(.leaderId == $id) and any(.nodeId == $p and .term >= $t))' <<<"$nodes" 2>/dev/null)" = true ] && ok=1 && break
		sleep 0.5
	done
fi
[ -n "$ok" ] && pass "transfer-leader: the coordinator names $nl (term $nterm) for shard-$TS ($(since "$t0") s)" ||
	fail "transfer-leader: the coordinator names ${nl:-?} (term ${nterm:-?}) for shard-$TS within ${COORD_FAILOVER_SECONDS}s" "$(head -c 600 <<<"${nodes:-}")"

# #30: a follower's NOT_LEADER hint is the leader's advertised HTTP address
read -r hl _ < <(wait_leader 15 0 "${tpods[@]}")
for p in "${tpods[@]}"; do [ "$p" != "${hl:-}" ] && hf="$p" && break; done
port="$(port_forward "$hf")"
code="$(curl -s -m 6 -o "$tmp/hint.json" -w '%{http_code}' -X PUT "http://127.0.0.1:$port/internal/data/${kv_t%% *}" \
	-H 'Content-Type: application/json' -H 'If-None-Match: *' \
	-d "{\"operationId\":\"op-s31-hint-$(date +%s%N)\",\"at\":1792800000000,\"ringVersion\":$ring_version,\"value\":{\"hint\":1}}")"
kill "$pf_pid" 2>/dev/null
pf_pid=""
[ "$code" = 503 ] && [ "$(jq -r .code "$tmp/hint.json")" = NOT_LEADER ] && [ "$(jq -r .leaderHint.nodeId "$tmp/hint.json")" = "${hl:-x}" ] &&
	[ "$(jq -r .leaderHint.address "$tmp/hint.json")" = "$(http_adv "$TS" "${hl:-x}")" ] &&
	pass "leader hint: a write to follower $hf is 503 NOT_LEADER naming $hl at its HTTP_ADVERTISE $(http_adv "$TS" "${hl:-x}")" ||
	fail "leader hint: a write to follower $hf is 503 NOT_LEADER naming ${hl:-?} at its HTTP_ADVERTISE" "HTTP $code: $(cat "$tmp/hint.json" 2>/dev/null)"

# #22: every replica reports the leader's commit index, and caughtUp is judged against it
for s in "${SHARDS[@]}"; do
	read -r rl _ < <(wait_leader 15 0 $(pod_names "$s"))
	why="" t0="$(now)"
	while awk -v t="$(since "$t0")" 'BEGIN { exit !(t < 5) }'; do # writes are quiescent; followers learn within CommitTimeout
		lc="$(field "$(st "${rl:-none}")" .commitIndex)" why=""
		for p in $(pod_names "$s"); do
			ps="$(st "$p")"
			[ "$(jq -r '.leaderCommitIndex | type' <<<"$ps" 2>/dev/null)" = number ] || { why="$p: no leaderCommitIndex in $(head -c 300 <<<"$ps")"; break; }
			[ "$(field "$ps" .leaderCommitIndex)" = "$lc" ] && [ "$(field "$ps" .caughtUp)" = true ] ||
				{ why="$p: leaderCommitIndex $(field "$ps" .leaderCommitIndex), caughtUp $(field "$ps" .caughtUp); leader ${rl:-?} commitIndex $lc"; break; }
		done
		[ -z "$why" ] && break
		sleep 0.5
	done
	[ -z "$why" ] && pass "readiness shard-$s: every replica reports the leader's commitIndex ($lc) as leaderCommitIndex and is caughtUp" ||
		fail "readiness shard-$s: every replica reports the leader's commitIndex as leaderCommitIndex and is caughtUp" "$why"
done
# A follower held down for a whole write burst: its worker is cordoned and its PVC is
# node-local, so the recreated pod stays Pending until the uncordon. Sampled from then on,
# it must never report caughtUp below the leader's appliedIndex at the end of the burst.
read -r rl _ < <(wait_leader 15 0 "${tpods[@]}")
for p in "${tpods[@]}"; do [ "$p" != "${rl:-}" ] && rf="$p" && break; done
rnode="$(kn get pod "$rf" -o jsonpath='{.spec.nodeName}' 2>/dev/null)"
k cordon "$rnode" >/dev/null 2>&1 && cordoned="$rnode"
kn delete pod "$rf" --wait=true --timeout=60s >/dev/null 2>&1
sleep 3
[ "$(kn get pod "$rf" -o jsonpath='{.status.phase}' 2>/dev/null)" = Pending ] && [ -z "$(kn get pod "$rf" -o jsonpath='{.spec.nodeName}' 2>/dev/null)" ] &&
	pass "readiness lag: $rf is held Pending while $rnode is cordoned" ||
	fail "readiness lag: $rf is held Pending while $rnode is cordoned" "phase $(kn get pod "$rf" -o jsonpath='{.status.phase}' 2>/dev/null)"
port="$(port_forward "${rl:-none}")"
tl0="$(now)" n=0
for i in $(seq 0 $((LAG_WRITES * 6))); do # about a third of keys belong to this shard; foreign keys are 400 and never proposed
	code="$(curl -s -m 5 -o "$tmp/lag.json" -w '%{http_code}' -X PUT "http://127.0.0.1:$port/internal/data/cfg:s31-lag-${tl0%.*}-$i" \
		-H 'Content-Type: application/json' -H 'If-None-Match: *' \
		-d "{\"operationId\":\"op-s31-lag-$i-$(date +%s%N)\",\"at\":1792800000000,\"ringVersion\":$ring_version,\"value\":{\"lag\":$i}}")"
	[ "$code" = 200 ] && n=$((n + 1))
	[ "$n" -ge "$LAG_WRITES" ] && break
done
kill "$pf_pid" 2>/dev/null
pf_pid=""
a_end="$(field "$(st "${rl:-none}")" .appliedIndex)"
[ "$n" -ge "$LAG_WRITES" ] && pass "readiness lag: $n writes committed through ${rl:-?} while $rf was down ($(since "$tl0") s; leader appliedIndex $a_end)" ||
	fail "readiness lag: $LAG_WRITES writes committed through ${rl:-?} while $rf was down" "$n committed; last HTTP $code: $(head -c 300 "$tmp/lag.json")"
: >"$tmp/lag.samples"
: >"$tmp/lag.run"
( # sample the follower from the uncordon until told to stop: "<t> <caughtUp> <appliedIndex> <leaderCommitIndex>"
	ts0="$(now)"
	while [ -e "$tmp/lag.run" ]; do
		ps="$(st "$rf")"
		[ -n "$ps" ] && echo "$(since "$ts0") $(field "$ps" .caughtUp) $(field "$ps" .appliedIndex) $(field "$ps" .leaderCommitIndex)" >>"$tmp/lag.samples"
		sleep 0.1
	done
) &
sampler=$!
k uncordon "$rnode" >/dev/null 2>&1 && cordoned=""
kn wait --for=condition=Ready "pod/$rf" --timeout="${REJOIN_MAX_SECONDS}s" >/dev/null 2>&1
t0="$(now)"
while awk -v t="$(since "$t0")" 'BEGIN { exit !(t < 30) }'; do
	[ "$(field "$(st "$rf")" .appliedIndex)" -ge "${a_end:-0}" ] 2>/dev/null && break
	sleep 0.5
done
sleep 1
rm -f "$tmp/lag.run"
wait "$sampler" 2>/dev/null
bad="$(awk -v a="${a_end:-0}" '$2 == "true" && $3 < a' "$tmp/lag.samples")"
nsamp="$(grep -c . "$tmp/lag.samples")"
nbehind="$(awk -v a="${a_end:-0}" '$3 < a' "$tmp/lag.samples" | grep -c .)"
last="$(tail -1 "$tmp/lag.samples")"
[ "$(awk '{ print $2 }' <<<"$last")" = true ] && [ "$(awk '{ print $3 }' <<<"$last")" -ge "${a_end:-0}" ] 2>/dev/null &&
	kn wait --for=condition=Ready "pod/$rf" --timeout=1s >/dev/null 2>&1 &&
	pass "readiness lag: $rf catches up to appliedIndex >= $a_end and is caughtUp and Ready" ||
	fail "readiness lag: $rf catches up to appliedIndex >= ${a_end:-?} and is caughtUp and Ready" "last sample (t caughtUp applied leaderCommit): $last"
[ "$nsamp" -gt 0 ] && [ -z "$bad" ] &&
	pass "readiness lag: in $nsamp samples ($nbehind behind the leader), $rf never reported caughtUp below the leader's appliedIndex $a_end" ||
	fail "readiness lag: $rf never reports caughtUp below the leader's appliedIndex ${a_end:-?}" "$nsamp samples; offending (t caughtUp applied leaderCommit): $(head -5 <<<"$bad" | tr '\n' '|')"
s31_lag="follower $rf held Pending on cordoned $rnode; $n writes through ${rl:-?} (leader appliedIndex $a_end); sampled from the uncordon"$'\n'"samples (t caughtUp appliedIndex leaderCommitIndex): $nsamp, $nbehind behind the leader"$'\n'"first: $(head -1 "$tmp/lag.samples")"$'\n'"first caughtUp: $(awk '$2 == "true"' "$tmp/lag.samples" | head -1)"$'\n'"last: $last"

# #19: FAILOVERS consecutive leader deletes, each with a new leader (any pod) within ELECTION_MAX_SECONDS
kv_f="$(write_key "$TS" failovers)"
fo_ok=0 fo_times="" fo_same=0
for i in $(seq "$FAILOVERS"); do
	if ! kn wait --for=condition=Ready $(for p in "${tpods[@]}"; do echo "pod/$p"; done) --timeout="${REJOIN_MAX_SECONDS}s" >/dev/null 2>&1 || ! out="$(one_group "$TS")"; then
		fail "failover $i/$FAILOVERS: shard-$TS is one Ready group before the delete" "${out:-not Ready}"
		continue
	fi
	read -r fl fterm < <(wait_leader 15 0 "${tpods[@]}")
	t0="$(now)"
	kn delete pod "${fl:-none}" --wait=false >/dev/null 2>&1
	read -r nl nterm < <(wait_leader "$ELECTION_MAX_SECONDS" $((${fterm:-0} + 1)) "${tpods[@]}")
	el="$(since "$t0")"
	if [ -n "${nl:-}" ]; then
		fo_ok=$((fo_ok + 1)) fo_times+="$el"$'\n'
		[ "$nl" = "$fl" ] && fo_same=$((fo_same + 1))
		pass "failover $i/$FAILOVERS: $nl leads at term $nterm, $el s after deleting $fl$( [ "$nl" = "$fl" ] && echo ' (the recreated pod won)')"
	else
		fail "failover $i/$FAILOVERS: a leader at term > ${fterm:-?} within ${ELECTION_MAX_SECONDS}s of deleting ${fl:-?}" "$(polls_detail)"
	fi
	s31_failovers+="$i: deleted ${fl:-?} (term ${fterm:-?}) -> ${nl:-none} (term ${nterm:-?}) seen after $el s"$'\n'
done
dist="$(sort -n <<<"${fo_times%$'\n'}" | awk '{ v[NR] = $1 } END { if (NR) printf "min %s, median %s, max %s s", v[1], v[int((NR + 1) / 2)], v[NR] }')"
[ "$fo_ok" = "$FAILOVERS" ] && pass "failover: $FAILOVERS consecutive leader deletes, each with a new leader within ${ELECTION_MAX_SECONDS}s ($dist; the recreated pod won $fo_same)" ||
	fail "failover: $FAILOVERS consecutive leader deletes, each with a new leader within ${ELECTION_MAX_SECONDS}s" "$fo_ok passed ($dist)"
s31_failovers+="seen-by-check times: $dist; the recreated pod won $fo_same of $fo_ok"$'\n'
kn wait --for=condition=Ready $(for p in "${tpods[@]}"; do echo "pod/$p"; done) --timeout="${REJOIN_MAX_SECONDS}s" >/dev/null 2>&1
[ -n "$kv_f" ] && read_key "$TS" "${kv_f%% *}" "${kv_f#* }" && pass "failover: the key written before the $FAILOVERS deletes reads back" ||
	fail "failover: the key written before the $FAILOVERS deletes reads back" "key '${kv_f%% *}': $(cat "$tmp/get.json" 2>/dev/null)"
[ -n "$cpf_pid" ] && kill "$cpf_pid" 2>/dev/null
cpf_pid=""
s31_results=""

# --- 7. Wiped disks after upgrade + rollback: no second cluster ----------------------------
helm --kube-context "$CTX" uninstall "$RELEASE" -n "$NS" --wait >/dev/null 2>&1
kn delete pvc -l app.kubernetes.io/name=storage,app.kubernetes.io/component=storagenode --wait=true >/dev/null 2>&1
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
reason="$(kn get job "$j" -o jsonpath='{.status.conditions[?(@.type=="Failed")].reason}' 2>/dev/null)"
codes="$(kn get pods -l "job-name=$j" -o jsonpath='{range .items[*]}{.status.containerStatuses[0].state.terminated.exitCode}{" "}{end}' 2>/dev/null)"
[ "$reason" = PodFailurePolicy ] && [ "$codes" = "2 " ] && pass "guard: failed by podFailurePolicy after one pod exited 2" ||
	fail "guard: failed by podFailurePolicy after one pod exited 2" "reason '$reason', exit codes '$codes'"
job_logs "$j" | grep -q 'refused: .*no reachable peer has Raft state' && pass "guard: the Job refused because no reachable peer has Raft state" ||
	fail "guard: the Job refused because no reachable peer has Raft state" "$(job_logs "$j" | tail -3 | tr '\n' '|')"
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
	ids0="$(server_ids 0)"
	kv0="$(write_key 0 rerun)" && pass "rerun: sentinel ${kv0%% *} written to shard-0 during the hold" || fail "rerun: sentinel written to shard-0 during the hold"
	echo "phases at the kill: shard-0 $(phase 0), shard-1 $(phase 1), shard-2 $(phase 2)"
	kn delete job "$j" --cascade=foreground --wait=true >/dev/null 2>&1
	[ -z "$(kn get pods -l "job-name=$j" -o name 2>/dev/null)" ] && pass "rerun: Job $j and its pod deleted mid-run" || fail "rerun: Job $j and its pod deleted mid-run"
	[ "$(phase 0)" = BOOTSTRAPPING ] && [ "$(phase 1)" = PENDING ] && [ "$(phase 2)" = PENDING ] &&
		pass "rerun: killed with shard-0 BOOTSTRAPPING, shards 1 and 2 PENDING" || fail "rerun: killed with shard-0 BOOTSTRAPPING, shards 1 and 2 PENDING" "$(phase 0) $(phase 1) $(phase 2)"
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
if [ -n "${kv0:-}" ]; then
	read_key 0 "${kv0%% *}" "${kv0#* }" && pass "rerun shard-0: the sentinel written before the kill reads back (same group)" ||
		fail "rerun shard-0: the sentinel written before the kill reads back (same group)" "$(cat "$tmp/get.json" 2>/dev/null)"
	[ "$(server_ids 0)" = "$ids0" ] && pass "rerun shard-0: serverIds unchanged ($ids0)" || fail "rerun shard-0: serverIds unchanged" "before '$ids0', after '$(server_ids 0)'"
fi

# --- 10. Evidence ---------------------------------------------------------------------------
finish
