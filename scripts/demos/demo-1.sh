#!/usr/bin/env bash
# demo-1 (owner S, goal S3.2): v3.1 §28 row 1. Written against the A0.1 public contract
# (contracts/public.yaml); PENDING until the public API (A1.1) is deployed.
#
# What to show:   Create request, retrieve it, inspect its logical shard and replicas
# Acceptance:     201 then 200; required fields and value agree
#
# Seed/reset: from the gate's clean deployment. DEMO1_SEED (default 1) fixes the request body
# (a MEDICINE request at a seeded point near 28.61N 77.21E); every run uses a fresh
# Idempotency-Key, so it creates one new request. API_URL (default
# http://localhost:8080/api/v1, Traefik on the infra worker) is the public API.
#
# PENDING, before anything else, if the API does not answer at API_URL or has no /requests
# route. Then:
#
# Assertions:
#   1. POST /requests -> 201; Location names /requests/<requestId>; the body has a UUID
#      requestId, the submitted requestType, location, details, peopleAffected and urgency, a
#      valid status, a version, and UTC createdAt/updatedAt.
#   2. GET of the Location -> 200 with the same requestId, requestType, location, details,
#      peopleAffected, urgency and createdAt (status and version may have moved on: the matcher
#      runs).
#   3. Logical shard: /admin/locate/req:<requestId> names a shard, its three replicas and a
#      leader among them.
#   4. The stored record: a strong read of req:<requestId> through the coordinator has the same
#      ID, location and peopleAffected.
#   5. Replicas: after settling, each of the shard's three replicas serves the record from its
#      own FSM (a stale read) at the version the strong read saw.
. "$(dirname "$0")/lib.sh"
demo_begin 1 "Create a request, retrieve it, inspect its shard and replicas"

SEED="${DEMO1_SEED:-1}"
API_URL="${API_URL:-http://localhost:8080/api/v1}"
API_URL="${API_URL%/}"
lat="$(awk -v s="$SEED" 'BEGIN { printf "%.4f", 28.6 + (s % 100) / 1000 }')"
lng="$(awk -v s="$SEED" 'BEGIN { printf "%.4f", 77.2 + (s % 100) / 1000 }')"
body="{\"requestType\":\"MEDICINE\",\"location\":{\"lat\":$lat,\"lng\":$lng},\"details\":\"demo-1 seed $SEED: medical supplies at the north entrance\",\"peopleAffected\":4,\"urgency\":\"HIGH\"}"
idem="$(python3 -c 'import uuid; print(uuid.uuid4())')"
demo_reset "clean deployment; one new request (seed $SEED: MEDICINE at $lat,$lng, 4 people, HIGH), Idempotency-Key $idem, API $API_URL"

# --- PENDING until A1.1 is deployed ---------------------------------------------------------
probe="$(curl -s -m 5 -o /dev/null -w '%{http_code}' "$API_URL/requests?limit=1" 2>/dev/null)"
case "$probe" in
000 | "") demo_pending "public API not reachable at $API_URL (A1.1 not deployed)" ;;
404) demo_pending "public API at $API_URL has no /requests route (A1.1 not deployed)" ;;
esac
pass "api: GET $API_URL/requests answers $probe"

# shellcheck source=scripts/demos/kube.sh
. "$(dirname "$0")/kube.sh"
require_preflight

same_fields='{requestId, requestType, location, details, peopleAffected, urgency, createdAt}'
uuid_re='^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
statuses='["PENDING","WAITING","MATCHING","MATCHED","IN_PROGRESS","RETRYING","CANCELLING","FULFILLING","CANCELLED","FULFILLED"]'

# --- 1: create --------------------------------------------------------------------------------
run "POST /requests" curl -s -m 10 -D "$DEMO_TMP/created.h" -o "$DEMO_TMP/created.json" -w '%{http_code}\n' -X POST "$API_URL/requests" \
	-H 'Content-Type: application/json' -H "Idempotency-Key: $idem" -d "$body"
code="$(awk 'NR == 1 { print $2 }' "$DEMO_TMP/created.h")"
created="$(cat "$DEMO_TMP/created.json")"
id="$(field "$created" .requestId)"
location="$(awk 'tolower($1) == "location:" { print $2 }' "$DEMO_TMP/created.h" | tr -d '\r')"
demo_note "201 body: $created"$'\n'"Location: $location"
if [ "$code" = 201 ] && [[ "$id" =~ $uuid_re ]] && [[ "$location" == */requests/"$id" ]] &&
	jq -e --argjson want "$body" --argjson st "$statuses" '
		.requestType == $want.requestType and .location == $want.location and .details == $want.details and
		.peopleAffected == $want.peopleAffected and .urgency == $want.urgency and (.status as $s | $st | index($s)) != null and
		(.version | type == "string" and length > 0) and (.createdAt | test("(Z|\\+00:00)$")) and (.updatedAt | test("(Z|\\+00:00)$"))' <<<"$created" >/dev/null; then
	pass "create: 201, Location $location, the submitted fields, status $(field "$created" .status), version $(field "$created" .version)"
else
	fail "create: 201 with a Location, a UUID requestId and the submitted fields" "$code $created; Location: $location"
fi

# --- 2: retrieve ------------------------------------------------------------------------------
case "$location" in http*) url="$location" ;; /api/v1/*) url="${API_URL%/api/v1}$location" ;; *) url="$API_URL/requests/$id" ;; esac
run "GET the Location" curl -s -m 10 -o "$DEMO_TMP/got.json" -w '%{http_code}\n' "$url"
got="$(cat "$DEMO_TMP/got.json")"
gcode="$(curl -s -m 10 -o /dev/null -w '%{http_code}' "$url")"
[ "$gcode" = 200 ] && [ "$(jq -cS "$same_fields" <<<"$got")" = "$(jq -cS "$same_fields" <<<"$created")" ] &&
	pass "retrieve: 200 with the same requestId, type, location, details, peopleAffected, urgency and createdAt (status now $(field "$got" .status))" ||
	fail "retrieve: 200 and the required fields agree with the 201 body" "$gcode $got"

# --- 3: logical shard -------------------------------------------------------------------------
p="$(cadmin "/admin/locate/req:$id")"
run "placement of req:$id (/admin/locate)" jq . <<<"$p"
s="$(field "$p" .shard)"
leader="$(field "$p" .leaderId)"
[[ "$s" =~ ^[0-2]$ ]] && [ "$(jq -r '.replicas | sort | join(" ")' <<<"$p")" = "$(pod_names "$s")" ] && [[ " $(pod_names "$s") " == *" ${leader%%/*} "* ]] &&
	pass "shard: req:$id is on shard $s, replicas $(pod_names "$s"), leader $leader" || fail "shard: a placement with the shard's three replicas and a leader" "$p"

# --- 4: the stored record ----------------------------------------------------------------------
read -r rcode rec <<<"$(cget "req:$id")"
run "strong read of req:$id through the coordinator" jq . <<<"$rec"
[ "$rcode" = 200 ] && jq -e --arg id "$id" --argjson want "$body" \
	'.record.kind == "request" and .record.request.id == $id and .record.request.location == $want.location and .record.request.peopleAffected == $want.peopleAffected' <<<"$rec" >/dev/null &&
	pass "stored: req:$id holds the same ID, location and peopleAffected at version $(field "$rec" .record.version)" ||
	fail "stored: the record matches the request" "$rcode $rec"

# --- 5: every replica has it ---------------------------------------------------------------------
version="$(field "$rec" .record.version)"
[[ "$s" =~ ^[0-2]$ ]] && all_caught_up "$s" 30
bad=""
for pod in $(pod_names "${s:-0}"); do
	if forward "pod/$pod" 8080; then
		c="$(curl -s -m 5 -o "$DEMO_TMP/stale.json" -w '%{http_code}' "http://127.0.0.1:$FWD_PORT/internal/data/req:$id?consistency=stale")"
		demo_note "$pod: $c $(jq -c '{version: .record.version, servedBy: .read.servedBy, appliedIndex: .read.appliedIndex}' "$DEMO_TMP/stale.json" 2>/dev/null)"
		[ "$c" = 200 ] && [ "$(jq -r .record.version "$DEMO_TMP/stale.json")" = "$version" ] && [ "$(jq -r .read.servedBy "$DEMO_TMP/stale.json")" = "$pod" ] ||
			bad+="$pod: $c $(cat "$DEMO_TMP/stale.json")"$'\n'
	else
		bad+="$pod: no port-forward"$'\n'
	fi
done
[ -z "$bad" ] && pass "replicas: all three of shard ${s:-?} serve req:$id from their own FSM at version $version" ||
	fail "replicas: every replica serves the record at the strong read's version" "$bad"
demo_end
