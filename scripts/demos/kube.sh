# shellcheck shell=bash
# Cluster helpers for the storage demos (1, 4, 6, 7). Source after lib.sh and demo_begin.
# Adapted from scripts/verify-storage-k8s.sh (status, leader and dump helpers); the
# coordinator is reached through a port-forward to Service storage-coordinator.
#
# Environment: KUBE_CONTEXT (kind-reliefgrid), NAMESPACE (reliefgrid).

CTX="${KUBE_CONTEXT:-kind-$CLUSTER}"
NS="${NAMESPACE:-reliefgrid}"
SHARDS=(0 1 2)
RING_VERSION="$(sed -n 's/^ringVersion:[[:space:]]*\([0-9]*\).*/\1/p' "$REPO_ROOT/contracts/ring.yaml")"
COORD_PORT="" # set by coord_forward
OPTOK=""      # the operator token, set by coord_forward

k() { kubectl --context "$CTX" "$@"; }
kn() { k -n "$NS" "$@"; }
field() { jq -r "$2 // empty" <<<"$1" 2>/dev/null; } # field <json> <jq path>
pod_names() { printf 'storage-shard-%s-0 storage-shard-%s-1 storage-shard-%s-2' "$1" "$1" "$1"; }
st() { timeout 5 kubectl --context "$CTX" -n "$NS" exec "$1" -c storagenode -- /storagectl status 2>/dev/null; } # status JSON or empty
dump_of() { timeout 30 kubectl --context "$CTX" -n "$NS" exec "$1" -c storagenode -- /storagectl dump 2>/dev/null; }
free_port() { python3 -c 'import socket; s=socket.socket(); s.bind(("127.0.0.1", 0)); print(s.getsockname()[1])'; }
now_ms() { echo $(($(date +%s%N) / 1000000)); }

# forward <target> <remote port>: port-forward a free local port to it and set FWD_PORT. Runs
# in this shell, never in $(...), so the forward is stopped when the demo exits.
FWD_PORT=""
forward() {
	local port pid
	port="$(free_port)"
	# kubectl itself in the background, not kn: $! must be the kubectl process, not a subshell
	# that would leave it running when killed (#50).
	kubectl --context "$CTX" -n "$NS" port-forward "$1" "$port:$2" >/dev/null 2>&1 &
	pid=$!
	demo_on_exit kill "$pid"
	for _ in $(seq 50); do (exec 3<>"/dev/tcp/127.0.0.1/$port") 2>/dev/null && { FWD_PORT="$port"; return 0; }; sleep 0.1; done
	return 1
}
coord_forward() {
	OPTOK="$(kn get secret storage-operator-token -o jsonpath='{.data.token}' 2>/dev/null | base64 -d)" || return 1
	[ -n "$OPTOK" ] && forward svc/storage-coordinator 8080 && COORD_PORT="$FWD_PORT"
}
coord() { echo "http://127.0.0.1:$COORD_PORT"; }

# cadmin <path>: GET a coordinator /admin path; body on stdout, fails unless 200
cadmin() {
	local code
	code="$(curl -s -m 10 -o "$DEMO_TMP/cadmin.json" -w '%{http_code}' -H "Authorization: Bearer $OPTOK" "$(coord)$1")"
	cat "$DEMO_TMP/cadmin.json"
	[ "$code" = 200 ]
}
# cput <key> <operationId> <json value>: create <key> through the coordinator (If-None-Match:
# *); prints "<http code> <body>"
cput() {
	local code
	code="$(curl -s -m 10 -o "$DEMO_TMP/cput.json" -w '%{http_code}' -X PUT "$(coord)/internal/data/$1" \
		-H 'Content-Type: application/json' -H 'If-None-Match: *' \
		-d "{\"operationId\":\"$2\",\"at\":1792800000000,\"ringVersion\":$RING_VERSION,\"value\":$3}")"
	echo "$code $(jq -c . "$DEMO_TMP/cput.json" 2>/dev/null)"
}
# cget <key> [consistency]: a read through the coordinator; prints "<http code> <body>"
cget() {
	local code
	code="$(curl -s -m 10 -o "$DEMO_TMP/cget.json" -w '%{http_code}' "$(coord)/internal/data/$1${2:+?consistency=$2}")"
	echo "$code $(jq -c . "$DEMO_TMP/cget.json" 2>/dev/null)"
}

# group_state <shard>: "<leader pod> <term>" if exactly one reachable pod is Leader and every
# reachable pod names it
group_state() {
	local p s n=0 lead="" term="" ids=""
	for p in $(pod_names "$1"); do
		s="$(st "$p")"
		[ -n "$s" ] || continue
		ids+="$(field "$s" .leaderId) "
		[ "$(field "$s" .state)" = Leader ] && { n=$((n + 1)) lead="$p" term="$(field "$s" .term)"; }
	done
	[ "$n" = 1 ] && [ "$(tr ' ' '\n' <<<"$ids" | grep . | sort -u | wc -l)" = 1 ] && echo "$lead $term"
}
# wait_leader <shard> <seconds> [min term] [not pod]: group_state once it holds with a term >=
# min term and a leader other than <not pod>
wait_leader() {
	local deadline=$((SECONDS + $2)) g
	while [ "$SECONDS" -lt "$deadline" ]; do
		g="$(group_state "$1")"
		[ -n "$g" ] && [ "${g##* }" -ge "${3:-0}" ] && [ "${g%% *}" != "${4:-}" ] && { echo "$g"; return 0; }
		sleep 0.5
	done
	return 1
}
# all_caught_up <shard> [seconds]: every replica caughtUp at one appliedIndex
all_caught_up() {
	local deadline=$((SECONDS + ${2:-60})) p s a want ok
	while [ "$SECONDS" -lt "$deadline" ]; do
		ok=1 want=""
		for p in $(pod_names "$1"); do
			s="$(st "$p")"
			a="$(field "$s" .appliedIndex)"
			[ "$(field "$s" .caughtUp)" = true ] && [ -n "$a" ] || { ok=""; break; }
			[ -z "$want" ] && want="$a"
			[ "$a" = "$want" ] || { ok=""; break; }
		done
		[ -n "$ok" ] && return 0
		sleep 0.5
	done
	return 1
}
# caught_up_to <status JSON> <raft index>: the replica has replayed through <raft index>.
# It uses the node's own caughtUp (raftstore Status, #45): the FSM has applied the last
# command at or below leaderCommitIndex (commandTarget). Requiring leaderCommitIndex to have
# reached <raft index> makes that cover it, since commandTarget never decreases. appliedIndex
# is never compared with a raw Raft index: no-ops, barriers and configuration entries never
# reach the FSM, so with strong reads running it can stay below the commit index for good.
caught_up_to() {
	[ -n "$2" ] && jq -e --argjson want "$2" '.caughtUp == true and (.leaderCommitIndex // 0) >= $want' <<<"$1" >/dev/null 2>&1
}
# --- comparing replicas while the app writes ---------------------------------------------------
# With the app installed, its matcher and background writes move records and Raft indexes at
# any time, so a snapshot (a strong read, a set of dumps, a catch-up check) can be overtaken
# before the check that uses it. The checks below tell "the app moved it" (exit 75, retried by
# retry_on within a bound) from a real fault (exit 1, never retried).

# retry_on <exit code> <seconds> <command...>: run the command (RETRY_ATTEMPT=1, 2, ...) while
# it exits with <exit code> and the bound has not passed. Prints only the last attempt's
# stdout and returns its status. Each retried attempt is a line in $DEMO_TMP/retried.
retry_on() {
	local want="$1" deadline=$((SECONDS + $2)) rc out
	shift 2
	out="$(mktemp "$DEMO_TMP/retry.XXXXXX")"
	RETRY_ATTEMPT=0
	while :; do
		RETRY_ATTEMPT=$((RETRY_ATTEMPT + 1))
		"$@" >"$out"
		rc=$?
		[ "$rc" = "$want" ] && [ "$SECONDS" -lt "$deadline" ] || break
		echo "$1 attempt $RETRY_ATTEMPT: exit $rc" >>"$DEMO_TMP/retried"
		sleep 0.5
	done
	cat "$out"
	rm -f "$out"
	return "$rc"
}

# replica_reads_agree <min version>: stdin has one "<pod> <http code> <stale-read body>" line
# per replica. 0 if every replica served the record from its own FSM (200, servedBy itself)
# and all hold the same record at one version >= <min version>; 75 if they are at different
# versions or all below <min version> (the app moved it between reads, or a replica lags);
# 1 otherwise (an error, another pod's answer, a different record at the same version). The
# reason goes to stdout.
replica_reads_agree() {
	local r
	r="$(jq -nrR --argjson min "${1:-null}" '
		[inputs | select(length > 0) | capture("^(?<pod>\\S+) (?<code>\\S+) ?(?<body>.*)$")
			| .body |= (try fromjson catch null)] as $r
		| if $min == null then "1 no strong-read version to compare with"
		  elif ($r | length) == 0 then "1 no replica reads"
		  elif any($r[]; .code != "200" or .body.read.servedBy != .pod or .body.record == null) then
			"1 not served by every replica from its own FSM: \([$r[] | select(.code != "200" or .body.read.servedBy != .pod or .body.record == null) | .pod] | join(", "))"
		  elif ([$r[].body.record.version] | unique | length) > 1 then
			"75 replicas at different versions: \([$r[] | "\(.pod)=\(.body.record.version)"] | join(" "))"
		  elif $r[0].body.record.version < $min then "75 all replicas at version \($r[0].body.record.version), below the strong read'"'"'s \($min)"
		  elif ([$r[].body.record | tojson] | unique | length) > 1 then "1 different records at version \($r[0].body.record.version)"
		  else "0 all \($r | length) replicas hold the same record at version \($r[0].body.record.version)"
		  end' 2>&1)" || { echo "unreadable reads: $r"; return 1; }
	echo "${r#* }"
	return "${r%% *}"
}

# not_caught_up <http code> <body file>: the replica refused a read because it is not caught
# up with the leader commit (storagenode's 503 STORAGE_UNAVAILABLE "<pod> is not caught up: ...")
not_caught_up() {
	[ "$1" = 503 ] && jq -e '.code == "STORAGE_UNAVAILABLE" and (.message | test(" is not caught up: "))' "$2" >/dev/null 2>&1
}
# stale_get <local port> <key> <body file>: one stale read from the replica forwarded to
# <local port>; prints the HTTP code; exit 75 if it answered "not caught up", else 0
stale_get() {
	local c
	c="$(curl -s -m 5 -o "$3" -w '%{http_code}' "http://127.0.0.1:$1/internal/data/$2?consistency=stale")"
	printf '%s\n' "$c"
	not_caught_up "$c" "$3" && return 75
	return 0
}

# dumps <shard>: one line per replica, "<pod> <appliedIndex> <sha256> records=<n>". The three
# dumps run at once, so the app has less time to move one past the others.
dumps() {
	local p pids=()
	for p in $(pod_names "$1"); do
		{
			d="$(dump_of "$p")" && jq -r --arg p "$p" '"\($p) \(.appliedIndex) \(.sha256) records=\(.rows.records)"' <<<"$d" || echo "$p unreachable"
		} >"$DEMO_TMP/dump.$p" 2>/dev/null &
		pids+=($!)
	done
	wait "${pids[@]}" # not a bare wait: the demo's port-forwards are background jobs too
	for p in $(pod_names "$1"); do cat "$DEMO_TMP/dump.$p"; done
}
# dumps_agree: stdin has the three lines of dumps. 0 if all three are at one appliedIndex with
# one sha256; 75 if they are at different appliedIndexes (the app wrote between dumps); 1 if a
# replica is unreachable, one is missing, or they differ at the same appliedIndex.
dumps_agree() {
	awk '$2 == "unreachable" || NF < 3 { bad = 1 } { n++; idx[$2]; sum[$2 " " $3] }
		END {
			if (bad || n != 3) exit 1
			ni = 0; for (i in idx) ni++
			ns = 0; for (s in sum) ns++
			if (ni > 1) exit 75
			exit ns == 1 ? 0 : 1
		}'
}
# agreed_dumps <shard> <seconds>: dump the three replicas until they agree (dumps_agree),
# retrying only while they are at different appliedIndexes, within <seconds>. The last dumps
# are in $DEMO_TMP/dumps-<shard>.
agreed_dumps() {
	_dumps_once() { dumps "$1" >"$DEMO_TMP/dumps-$1"; dumps_agree <"$DEMO_TMP/dumps-$1"; }
	retry_on 75 "$2" _dumps_once "$1"
}

# preflight: the storage deployment is there and healthy enough to demo on. Runs in this
# shell (it sets COORD_PORT and OPTOK); sets PREFLIGHT_WHY when it does not hold.
PREFLIGHT_WHY=""
preflight() {
	local t s j
	why() { PREFLIGHT_WHY="$1"; return 1; }
	for t in kubectl jq curl python3 go; do command -v "$t" >/dev/null || why "$t is not installed" || return 1; done
	k config get-contexts -o name 2>/dev/null | grep -qx "$CTX" || why "no kube context $CTX" || return 1
	j="$(kn get pods -l app.kubernetes.io/component=storagenode -o json 2>/dev/null)" || why "cannot list storage pods in $NS" || return 1
	[ "$(jq '[.items[] | select(.status.conditions[]? | .type == "Ready" and .status == "True")] | length' <<<"$j")" = 9 ] ||
		why "not all nine storage pods are Ready" || return 1
	coord_forward || why "cannot reach the coordinator (Service storage-coordinator)" || return 1
	j="$(cadmin /admin/nodes)" || why "coordinator /admin/nodes failed" || return 1
	[ "$(jq '[.nodes[] | select(.state == "UP")] | length' <<<"$j")" = 9 ] || why "the coordinator does not report nine nodes UP" || return 1
	for s in "${SHARDS[@]}"; do
		[ -n "$(group_state "$s")" ] || why "shard $s has no single agreed leader" || return 1
	done
}
# missing_replicas [shard...]: one line "shard N: <pods not Ready>" per listed shard (default
# all) that does not have all three replicas Ready. Reads only kubectl get pods. Exit 2 (and the
# reason) if the pods cannot be listed at all: a preflight failure, not a missing replica.
missing_replicas() {
	local j s
	j="$(kn get pods -l app.kubernetes.io/component=storagenode -o json 2>/dev/null)" || { echo "cannot list storage pods in $NS (context $CTX)"; return 2; }
	for s in "${@:-${SHARDS[@]}}"; do
		jq -r --arg s "$s" '[.items[] | select(.metadata.labels["reliefgrid.io/shard"] == $s)] as $p
			| ([$p[] | select(any(.status.conditions[]?; .type == "Ready" and .status == "True"))] | length) as $ready
			| if $ready == 3 and ($p | length) == 3 then empty
			  else "shard \($s): \($ready) of 3 replicas Ready; not Ready: \([$p[] | select(all(.status.conditions[]?; .type != "Ready" or .status != "True")) | .metadata.name] | join(", "))"
			  end' <<<"$j"
	done
}

# demo6_handoffs: the pending demo-6 hand-offs, one annotation value per line, from the workers
# demo 6 cordoned
demo6_handoffs() {
	k get nodes -l "$DEMO_CORDON_LABEL=true" -o json 2>/dev/null |
		jq -r --arg a "$DEMO_HANDOFF_ANNOTATION" '.items[] | .metadata.annotations[$a] // empty'
}
# handoff_field <value> <key>: one key of a hand-off annotation value
handoff_field() { tr ' ' '\n' <<<"$1" | sed -n "s/^$2=//p"; }

# require_preflight: end the demo as FAIL if preflight does not hold
require_preflight() {
	if preflight; then pass "preflight: nine storage pods Ready, nine nodes UP, one leader per shard"; return 0; fi
	fail "preflight: $PREFLIGHT_WHY"
	demo_end
}

# stop_leader_pod <shard>: stop the shard's leader and keep it down. Cordons the leader's worker
# (with the demos' label) and deletes the pod gracefully; the StatefulSet's replacement stays
# Pending, since its PV and the required anti-affinity both pin it to that worker. Sets STOP_POD,
# STOP_NODE, STOP_ID (its serverId), STOP_TERM, STOP_UID (the Pending replacement) and
# STOP_MS (when the delete was issued); PREFLIGHT_WHY-style STOP_WHY on failure. The caller
# registers the uncordon (demo_on_exit) before calling it.
stop_leader_pod() {
	local g old_uid i j
	STOP_POD="" STOP_NODE="" STOP_ID="" STOP_TERM="" STOP_UID="" STOP_MS="" STOP_WHY=""
	g="$(wait_leader "$1" 15)" || { STOP_WHY="shard $1 has no agreed leader"; return 1; }
	STOP_POD="${g% *}" STOP_TERM="${g#* }"
	STOP_ID="$(field "$(st "$STOP_POD")" .serverId)"
	STOP_NODE="$(kn get pod "$STOP_POD" -o jsonpath='{.spec.nodeName}' 2>/dev/null)"
	old_uid="$(kn get pod "$STOP_POD" -o jsonpath='{.metadata.uid}' 2>/dev/null)"
	[ -n "$STOP_NODE" ] && [ -n "$old_uid" ] || { STOP_WHY="cannot read $STOP_POD's node"; return 1; }
	demo_cordon "$STOP_NODE" || { STOP_WHY="kubectl cordon $STOP_NODE failed"; return 1; }
	STOP_MS="$(now_ms)"
	kn delete pod "$STOP_POD" --wait=false >/dev/null || { STOP_WHY="kubectl delete pod $STOP_POD failed"; return 1; }
	for ((i = 0; i < 120; i++)); do
		j="$(kn get pod "$STOP_POD" -o json 2>/dev/null)"
		if [ -n "$j" ] && [ "$(jq -r .metadata.uid <<<"$j")" != "$old_uid" ]; then
			[ "$(jq -r '.status.phase' <<<"$j")" = Pending ] && [ "$(jq -r '.spec.nodeName // ""' <<<"$j")" = "" ] &&
				{ STOP_UID="$(jq -r .metadata.uid <<<"$j")"; return 0; }
			STOP_WHY="the replacement of $STOP_POD was scheduled: $(jq -c '{phase: .status.phase, node: .spec.nodeName}' <<<"$j")"
			return 1
		fi
		sleep 0.5
	done
	STOP_WHY="no replacement of $STOP_POD appeared within 60 s"
	return 1
}
# still_stopped <pod> <uid>: the Pending replacement is still the pod, unscheduled
still_stopped() {
	local j
	j="$(kn get pod "$1" -o json 2>/dev/null)" || return 1
	[ "$(jq -r .metadata.uid <<<"$j")" = "$2" ] && [ "$(jq -r .status.phase <<<"$j")" = Pending ] && [ "$(jq -r '.spec.nodeName // ""' <<<"$j")" = "" ]
}
