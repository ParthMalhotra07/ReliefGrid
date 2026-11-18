# Rotation of both credential classes, following docs/operations/internal-auth.md phase by
# phase. Before phase 1 and after each phase: the mounted token files in every affected pod
# (hashes only, via an ephemeral debug container), old/new token acceptance, then demo-1 and a
# write/read-back through the coordinator. Tokens never reach a command line or a log.
. "$R/stages/lib.sh"
RT="$(umask 077 && mktemp -d /tmp/reliefgrid-rotation.XXXXXX)"
trap 'stop_pfs; internal_auth_cleanup; rm -rf "$RT"' EXIT
secret_key() { $K get secret "$1" -o json | jq -er --arg k "$2" '.data[$k]' | base64 -d; } # to stdout: only ever redirected to a file
sha() { sha256sum <"$1" | cut -d' ' -f1; }
(umask 077
secret_key reliefgrid-internal-auth token >"$RT/A0"; secret_key reliefgrid-node-auth token >"$RT/N0"
cmp -s "$RT/A0" <(secret_key reliefgrid-internal-auth sender-token) && cmp -s "$RT/N0" <(secret_key reliefgrid-node-auth sender-token)) ||
	die "initial Secrets: token and sender-token differ or unreadable"
(umask 077; python3 -c 'import secrets,sys; open(sys.argv[1],"w").write(secrets.token_hex(24)); open(sys.argv[2],"w").write(secrets.token_hex(24))' "$RT/A1" "$RT/N1")
for f in A0 N0 A1 N1; do (umask 077; { printf 'X-ReliefGrid-Internal-Token: '; cat "$RT/$f"; printf '\n'; } >"$RT/$f.h"); done
log "tokens: old app $(sha "$RT/A0" | head -c 12)…, old node $(sha "$RT/N0" | head -c 12)…, new app $(sha "$RT/A1" | head -c 12)…, new node $(sha "$RT/N1" | head -c 12)… (sha256 prefixes)"

# values <token-app> <prev-app|-> <sender-app> <token-node> <prev-node|-> <sender-node>: a 0600 values file
# The runbook's values() helper (docs/operations/internal-auth.md): JSON, so every byte survives.
values() {
	(umask 077; python3 - "$RT" "$@" >"$RT/values.yaml" <<'PY'
import json, sys
d, ta, pa, sa, tn, pn, sn = sys.argv[1:]
r = lambda n: '' if n == '-' else open(f'{d}/{n}').read()
json.dump({key: {'token': r(t), 'previousToken': r(p), 'senderToken': r(s)}
           for key, (t, p, s) in {'internalAuth': (ta, pa, sa), 'nodeAuth': (tn, pn, sn)}.items()},
          sys.stdout, indent=2)
PY
	)
}
upgrade() { helm --kube-context kind-reliefgrid -n reliefgrid upgrade storage deploy/helm/storage --reuse-values -f "$RT/values.yaml" --wait --wait-for-jobs --timeout 10m >"$R/rotation-upgrade-$1.log" 2>&1 || die "helm upgrade ($1): $(tail -3 "$R/rotation-upgrade-$1.log" | tr '\n' '|')"; ok "helm upgrade storage ($1)"; }
nine_up() {
	local i
	pf svc/storage-coordinator 8080 || return 1
	for i in $(seq 180); do
		[ "$(curl -s -m 3 -H "Authorization: Bearer $(optok)" "http://127.0.0.1:$PF_PORT/admin/nodes" | jq '[.nodes[] | select(.state == "UP")] | length' 2>/dev/null)" = 9 ] && { stop_pfs; return 0; }
		sleep 1
	done
	stop_pfs; return 1
}
restart_receivers() {
	$K rollout restart deployment/storage-coordinator >/dev/null && $K rollout status deployment/storage-coordinator --timeout=300s >/dev/null || die "coordinator restart"
	for s in 0 1 2; do
		$K rollout restart statefulset/storage-shard-$s >/dev/null && $K rollout status statefulset/storage-shard-$s --timeout=600s >/dev/null || die "storage-shard-$s restart"
	done
	nine_up || die "nine storage nodes UP after the receiver restart"
	for s in 0 1 2; do leader_of "$s" >/dev/null || die "shard $s has no leader after the restart"; done
	ok "restarted receivers: coordinator, storage-shard-0..2; nine nodes UP, a leader per shard"
}
restart_app_senders() {
	for d in reliefgrid-api reliefgrid-matcher; do
		$K rollout restart deployment/$d >/dev/null && $K rollout status deployment/$d --timeout=300s >/dev/null || die "$d restart"
	done
	ok "restarted app senders: api, matcher"
}
# mounts_of <pod> <container>: "<dir>/<file> <sha256>" for each mounted auth file, read from the
# target container's root through an ephemeral debug container (hashes only).
mounts_of() {
	local pod="$1" c="$2" name="authcheck-$(date +%s%N | tail -c 9)" i st
	$K debug "pod/$pod" -c "$name" --target="$c" --image="$DEBUG_IMAGE" --profile=general -- \
		sh -c 'for f in /proc/1/root/var/run/reliefgrid/*-auth/*; do [ -f "$f" ] && sha256sum "$f"; done; echo END' >/dev/null 2>&1 || return 2
	for i in $(seq 120); do
		st="$($K get pod "$pod" -o json | jq -r --arg n "$name" '.status.ephemeralContainerStatuses[]? | select(.name == $n) | .state | keys[0]')"
		[ "$st" = terminated ] && break
		sleep 1
	done
	[ "$st" = terminated ] || return 3
	$K logs "$pod" -c "$name" | awk '$1 == "END" { e = 1; next } { n = split($2, p, "/"); print p[n-1] "/" p[n], $1 } END { exit !e }'
}
# expect_mounts <label> <app token> <app prev|-> <app sender> <node token> <node prev|-> <node sender>
expect_mounts() {
	local label="$1" ta="$2" pa="$3" sa="$4" tn="$5" pn="$6" sn="$7" want got pod c attempt
	h() { [ "$1" = - ] || printf '%s %s\n' "$2" "$(sha "$RT/$1")"; }
	# The Secret objects first.
	for kv in "reliefgrid-internal-auth $ta $pa $sa" "reliefgrid-node-auth $tn $pn $sn"; do
		set -- $kv
		[ "$(secret_key "$1" token | sha256sum | cut -d' ' -f1)" = "$(sha "$RT/$2")" ] &&
		[ "$(secret_key "$1" sender-token | sha256sum | cut -d' ' -f1)" = "$(sha "$RT/$4")" ] || die "$label: Secret $1 token/sender-token not as intended"
		if [ "$3" = - ]; then $K get secret "$1" -o json | jq -e '.data | has("previous-token") | not' >/dev/null || die "$label: Secret $1 still has previous-token"
		else [ "$(secret_key "$1" previous-token | sha256sum | cut -d' ' -f1)" = "$(sha "$RT/$3")" ] || die "$label: Secret $1 previous-token not as intended"; fi
	done
	node_want="$(h "$tn" node-auth/token; h "$pn" node-auth/previous-token; h "$sn" node-auth/sender-token)"
	app_full="$(h "$ta" internal-auth/token; h "$pa" internal-auth/previous-token; h "$sa" internal-auth/sender-token)"
	targets=("$($K get pod -l app.kubernetes.io/component=coordinator -o jsonpath='{.items[0].metadata.name}') coordinator full")
	for p in $($K get pods -l app.kubernetes.io/component=storagenode -o jsonpath='{.items[*].metadata.name}'); do targets+=("$p storagenode node"); done
	for d in api matcher; do for p in $($K get pods -l "app.kubernetes.io/instance=reliefgrid,app.kubernetes.io/component=$d" -o jsonpath='{.items[*].metadata.name}'); do targets+=("$p $d app"); done; done
	[ "${#targets[@]}" -ge 14 ] || die "$label: expected at least 14 pods to check, found ${#targets[@]}"
	for t in "${targets[@]}"; do
		set -- $t; pod="$1" c="$2"
		case "$3" in
		full) want="$( (printf '%s\n' "$app_full" "$node_want") | sed '/^$/d' | sort)" ;;
		node) want="$(printf '%s\n' "$node_want" | sed '/^$/d' | sort)" ;;
		app) want="$(h "$sa" internal-auth/sender-token)" ;;
		esac
		# Unrestarted pods see Secret updates after the kubelet sync: retry up to ~3 min.
		for attempt in 1 2 3 4 5 6; do
			got="$(mounts_of "$pod" "$c" | sort)" || got="(debug container failed: exit $?)"
			[ "$got" = "$want" ] && break
			sleep 30
		done
		[ "$got" = "$want" ] || { printf 'want:\n%s\ngot:\n%s\n' "$(sed 's/ \(.\{12\}\).*/ \1…/' <<<"$want")" "$(sed 's/ \(.\{12\}\).*/ \1…/' <<<"$got")"; die "$label: mounted tokens in $pod ($c) are not as intended"; }
	done
	ok "$label: Secrets and mounted files in ${#targets[@]} pods as intended (coordinator, storage, api, matcher)"
}
# accepts <label> <app token file> <want code> <node token file> <want code>
accepts() {
	local label="$1" s c
	pf svc/storage-coordinator 8080 || die "no coordinator forward"
	c="$(curl -s -m 8 -o /dev/null -w '%{http_code}' -H "@$RT/$2.h" "http://127.0.0.1:$PF_PORT/internal/ring")"
	[ "$c" = "$3" ] && ok "$label: coordinator with app token $2: $c" || die "$label: coordinator with app token $2: $c, want $3"
	s="$(leader_of 0)" || die "no shard-0 leader"
	pf "pod/$s" 8080 || die "no forward to $s"
	c="$(curl -s -m 8 -o /dev/null -w '%{http_code}' -H "@$RT/$4.h" "http://127.0.0.1:$PF_PORT/internal/data/cfg:rotation-probe")"
	case "$5:$c" in 401:401 | ok:200 | ok:404 | ok:400) ok "$label: storage $s with node token $4: $c" ;; *) die "$label: storage $s with node token $4: $c, want $5" ;; esac
	stop_pfs
}
after_phase() { # demo-1 and a write/read-back through the coordinator with the current sender token
	local label="$1" key val code
	DEMO_RUN_ID="auth-official-rotation-$label" make -s demo-1 >"$R/rotation-demo1-$label.log" 2>&1 || die "$label: demo-1 failed: $(grep -E '^(FAIL|result)' "$R/rotation-demo1-$label.log" | head -5 | tr '\n' '|')"
	ok "$label: demo-1 PASS ($(grep -c '^PASS' "$R/rotation-demo1-$label.log") checks)"
	internal_auth_cleanup; INTERNAL_AUTH_ON=""
	internal_auth_setup kind-reliefgrid reliefgrid || die "$label: cannot read the sender tokens"
	pf svc/storage-coordinator 8080 || die "no coordinator forward"
	ringv="$(curl -s -m 6 "${INTERNAL_AUTH_CURL[@]}" "http://127.0.0.1:$PF_PORT/internal/ring" | jq -r .ringVersion)"
	key="cfg:rotation-$label-$(date +%s)" val="{\"phase\":\"$label\"}"
	code="$(curl -s -m 8 "${INTERNAL_AUTH_CURL[@]}" -o "$R/rot-put.json" -w '%{http_code}' -X PUT "http://127.0.0.1:$PF_PORT/internal/data/$key" -H 'Content-Type: application/json' -H 'If-None-Match: *' \
		-d "{\"operationId\":\"op-rot-$label-$(date +%s%N)\",\"at\":1792800000000,\"ringVersion\":$ringv,\"value\":$val}")"
	[ "$code" = 200 ] || die "$label: write through the coordinator: $code $(head -c 200 "$R/rot-put.json")"
	code="$(curl -s -m 8 "${INTERNAL_AUTH_CURL[@]}" -o "$R/rot-get.json" -w '%{http_code}' "http://127.0.0.1:$PF_PORT/internal/data/$key")"
	[ "$code" = 200 ] && [ "$(jq -c .record.value "$R/rot-get.json")" = "$(jq -c . <<<"$val")" ] || die "$label: read-back: $code $(head -c 200 "$R/rot-get.json")"
	ok "$label: write $key and read it back through the coordinator with the current sender token"
	stop_pfs
}

# The runbook's setup checks, before any rotation command.
kubectl config get-contexts -o name | quiet_grep -x kind-reliefgrid || die "no kube context kind-reliefgrid"
kubectl --context kind-reliefgrid get namespace reliefgrid >/dev/null || die "no namespace reliefgrid"
helm --kube-context kind-reliefgrid -n reliefgrid status storage >/dev/null || die "no storage release"
helm --kube-context kind-reliefgrid -n reliefgrid get values storage -a -o json | jq -e '.internalAuth.enabled == true' >/dev/null || die "internal auth is not on"
$K get secret reliefgrid-internal-auth reliefgrid-node-auth >/dev/null || die "the auth Secrets are missing"
[ "$(stat -c %a "$RT")" = 700 ] && [ -z "$(find "$RT" -type f ! -perm 600)" ] && ok "setup checks; token directory 700, every token file 600" || die "token files are not private"
# no_cluster_refusal <label> <since>: the runbook's refusal-log check for the phase.
no_cluster_refusal() { [ -z "$(in_cluster_refusals "$2")" ] && ok "$1: no in-cluster sender refused (refusal log)" || die "$1: in-cluster refusals: $(in_cluster_refusals "$2" | head -5)"; }

log "phase 0: the starting state"
expect_mounts "before phase 1" A0 - A0 N0 - N0
accepts "before phase 1" A0 200 N0 ok
accepts "before phase 1" A1 401 N1 401

log "phase 1: token=new, previous=old, sender=old; restart receivers"
p1="$(date -u +%FT%TZ)"
values A1 A0 A0 N1 N0 N0; upgrade phase1; restart_receivers
expect_mounts "after phase 1" A1 A0 A0 N1 N0 N0
accepts "after phase 1" A0 200 N0 ok
accepts "after phase 1" A1 200 N1 ok
after_phase phase1
no_cluster_refusal "after phase 1" "$p1"

log "phase 2: sender=new; restart senders (api, matcher, coordinator, storage)"
p2="$(date -u +%FT%TZ)"
values A1 A0 A1 N1 N0 N1; upgrade phase2; restart_app_senders; restart_receivers
expect_mounts "after phase 2" A1 A0 A1 N1 N0 N1
accepts "after phase 2" A0 200 N0 ok
accepts "after phase 2" A1 200 N1 ok
after_phase phase2
no_cluster_refusal "after phase 2" "$p2"

log "phase 3: clear previous; restart receivers"
p3="$(date -u +%FT%TZ)"
values A1 - A1 N1 - N1; upgrade phase3; restart_receivers
expect_mounts "after phase 3" A1 - A1 N1 - N1
accepts "after phase 3" A0 401 N0 401
accepts "after phase 3" A1 200 N1 ok
after_phase phase3
no_cluster_refusal "after phase 3" "$p3"
refusals "$p3" | quiet_grep -E 'reason="?unknown credential|"reason":"unknown credential"' &&
	ok "after phase 3: the old tokens' 401s are logged as unknown credential" || die "after phase 3: no unknown-credential refusal logged for the old tokens"
for f in A0 N0 A1 N1; do no_token_in_logs "$RT/$f" || die "a rotation token is in the logs"; done
ok "no old or new token appears in any pod log"
ok "rotation complete"
