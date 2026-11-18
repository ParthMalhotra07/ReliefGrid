# The 401 checks: no token -> 401 on coordinator read/write and storage read; the right token -> OK;
# the wrong credential class -> 401.
. "$R/stages/lib.sh"
since="$(date -u +%FT%TZ)"
internal_auth_setup kind-reliefgrid reliefgrid || die "cannot read the sender tokens"
[ "$INTERNAL_AUTH_ON" = true ] || die "internal auth is not on"
pf svc/storage-coordinator 8080 || die "no coordinator forward"; CP="$PF_PORT"
key="cfg:auth-official-$(date +%s)" val='{"official":"401-checks"}'
body() { printf '{"operationId":"op-%s-%s","at":1792800000000,"ringVersion":%s,"value":%s}' "$1" "$(date +%s%N)" "$ringv" "$val"; }
ringv="$(curl -s -m 6 "${INTERNAL_AUTH_CURL[@]}" "http://127.0.0.1:$CP/internal/ring" | jq -r .ringVersion)"
[[ "$ringv" =~ ^[0-9]+$ ]] || die "ring version with the app token: '$ringv'"
check() { # check <want code> <description> <curl args...>
	local want="$1" desc="$2" code; shift 2
	code="$(curl -s -m 8 -o "$R/last-body.json" -w '%{http_code}' "$@")"
	if [ "$code" = "$want" ]; then
		if [ "$want" = 401 ] && [ "$(jq -r .code "$R/last-body.json" 2>/dev/null)" != UNAUTHORIZED ]; then die "$desc: 401 without code UNAUTHORIZED: $(head -c 200 "$R/last-body.json")"; fi
		ok "$desc: $code"
	else die "$desc: got $code, want $want: $(head -c 300 "$R/last-body.json")"; fi
}
check 401 "coordinator GET /internal/ring, no token" "http://127.0.0.1:$CP/internal/ring"
check 401 "coordinator PUT /internal/data, no token" -X PUT "http://127.0.0.1:$CP/internal/data/$key-noauth" -H 'Content-Type: application/json' -H 'If-None-Match: *' -d "$(body noauth)"
check 401 "coordinator GET /internal/data, no token" "http://127.0.0.1:$CP/internal/data/$key"
check 401 "coordinator GET /internal/ring, node token (wrong class)" "${NODE_AUTH_CURL[@]}" "http://127.0.0.1:$CP/internal/ring"
check 200 "coordinator GET /internal/ring, app token" "${INTERNAL_AUTH_CURL[@]}" "http://127.0.0.1:$CP/internal/ring"
check 200 "coordinator PUT /internal/data, app token" "${INTERNAL_AUTH_CURL[@]}" -X PUT "http://127.0.0.1:$CP/internal/data/$key" -H 'Content-Type: application/json' -H 'If-None-Match: *' -d "$(body auth)"
check 200 "coordinator GET /internal/data, app token" "${INTERNAL_AUTH_CURL[@]}" "http://127.0.0.1:$CP/internal/data/$key"
[ "$(jq -c .record.value "$R/last-body.json")" = "$(jq -c . <<<"$val")" ] && ok "coordinator read-back returns the write" || die "coordinator read-back differs"
shard="$(curl -s -m 6 -H "Authorization: Bearer $(optok)" "http://127.0.0.1:$CP/admin/locate/$key" | jq -r .shard)"
[[ "$shard" =~ ^[0-2]$ ]] || die "cannot locate $key: '$shard'"
leader="$(leader_of "$shard")" || die "no leader for shard $shard"
pf "pod/$leader" 8080 || die "no forward to $leader"; SP="$PF_PORT"
check 401 "storage $leader GET /internal/data, no token" "http://127.0.0.1:$SP/internal/data/$key"
check 401 "storage $leader GET /internal/data, app token (wrong class)" "${INTERNAL_AUTH_CURL[@]}" "http://127.0.0.1:$SP/internal/data/$key"
check 200 "storage $leader GET /internal/data, node token" "${NODE_AUTH_CURL[@]}" "http://127.0.0.1:$SP/internal/data/$key"
[ "$(jq -c .record.value "$R/last-body.json")" = "$(jq -c . <<<"$val")" ] && ok "storage read returns the write" || die "storage read differs"
sleep 2
n="$(refusals "$since" | wc -l)"
[ "$n" = 6 ] && ok "six refusal log lines, one per refused request" || die "refusal log lines: $n, want 6: $(refusals "$since")"
for want in 'missing' 'unknown credential'; do
	refusals "$since" | quiet_grep -E "reason=\"?$want|\"reason\":\"$want\"" && ok "a refusal logged with reason $want" || die "no refusal with reason $want"
done
refusals "$since" | quiet_grep 'credential=node\|"credential":"node"' && refusals "$since" | quiet_grep 'credential=app\|"credential":"app"' &&
	ok "refusals name the credential class (app and node)" || die "refusals do not name both classes"
no_token_in_logs "$INTERNAL_AUTH_TOKEN_FILE" "$NODE_AUTH_SENDER_TOKEN_FILE" && ok "no sender token appears in any pod log" || die "a sender token is in the logs"
[ -z "$(in_cluster_refusals "$since")" ] && ok "no in-cluster sender refused" || die "in-cluster refusals: $(in_cluster_refusals "$since" | head -5)"
