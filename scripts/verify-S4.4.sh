#!/usr/bin/env bash
# S4.4 Verify (part b): on kind with tracing on, one public create is ONE
# Jaeger trace holding the API, the storage path, the outbox relay's publish
# and the matcher's consume and storage calls.
#
# Preconditions (this script changes nothing but creates one request):
#   make app-deploy     # API + matcher images built from this tree
#   make tracing-up     # Collector + Jaeger; tracing on in storage and the app
#
# It sends the create with its own sampled traceparent, so the trace ID is
# known; reads that trace from Jaeger's query API (port-forward, killed on
# exit); and checks, by service and span:
#   api        POST /api/v1/requests (server, child of our traceparent)
#              coordclient.CreateEntity under it
#   storage    coordinator + storagenode server spans, raft.apply
#   relay      publish (producer) in the matcher, child of the API span
#   matcher    consume (consumer), child of that publish (first delivery),
#              and at least one coordclient.* span under consume
# Evidence: docs/evidence/S4.4/app-trace-<UTC>.json (the Jaeger trace).
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"
# shellcheck source=scripts/port-forwards.sh
source scripts/port-forwards.sh

CLUSTER="${CLUSTER:-reliefgrid}"
NS="${NAMESPACE:-reliefgrid}"
CTX="kind-$CLUSTER"
API_URL="${API_URL:-http://127.0.0.1:8080}"
JAEGER_PORT="${JAEGER_PORT:-16687}"
WAIT_SECONDS="${WAIT_SECONDS:-60}"
EVIDENCE_DIR="docs/evidence/S4.4"

kn() { kubectl --request-timeout=10s --context "$CTX" -n "$NS" "$@"; }
fail() { printf 'FAIL %s\n' "$*" >&2; exit 1; }
pass() { printf 'PASS %s\n' "$*"; }

tmp="$(mktemp -d)"
pf=""
cleanup() {
	if [ -n "$pf" ]; then kill "$pf" 2>/dev/null || true; wait "$pf" 2>/dev/null || true; fi
	rm -rf "$tmp"
}
trap cleanup EXIT

echo "== preconditions"
kubectl config get-contexts "$CTX" >/dev/null 2>&1 || fail "no kube context $CTX (make cluster-up)"
for d in otel-collector jaeger; do
	kn rollout status "deploy/$d" --timeout=60s >/dev/null || fail "$d not Ready (make tracing-up)"
done
for comp in api matcher; do
	dep="$(kn get deploy -l "app.kubernetes.io/component=$comp" -o name | head -1)"
	[ -n "$dep" ] || fail "no $comp Deployment (make app-deploy)"
	kn rollout status "$dep" --timeout=120s >/dev/null || fail "$dep not Ready"
	kn get "$dep" -o jsonpath='{range .spec.template.spec.containers[0].env[*]}{.name}={.value}{"\n"}{end}' >"$tmp/$comp.env"
	grep -q '^OTEL_EXPORTER_OTLP_ENDPOINT=.*otel-collector.*:4318$' "$tmp/$comp.env" ||
		fail "$comp does not export to the Collector (make tracing-up turns tracing on in the app release)"
	grep -q '^OTEL_TRACES_SAMPLER_ARG=1.0$' "$tmp/$comp.env" ||
		fail "$comp samples below 1.0; run make tracing-up with TRACING_SAMPLE=1.0 for this Verify"
done
pass "Collector, Jaeger, API and matcher Ready; API and matcher export to the Collector at ratio 1.0"

echo "== one create"
trace_id="$(od -An -N16 -tx1 /dev/urandom | tr -d ' \n')"
parent_id="$(od -An -N8 -tx1 /dev/urandom | tr -d ' \n')"
traceparent="00-${trace_id}-${parent_id}-01"
key="$(cat /proc/sys/kernel/random/uuid)"
code="$(curl --silent --show-error --max-time 10 -o "$tmp/create.json" -w '%{http_code}' \
	-H 'Content-Type: application/json' -H "Idempotency-Key: $key" -H "traceparent: $traceparent" \
	-d '{"requestType":"WATER","location":{"lat":28.61,"lng":77.21},"details":"S4.4 Verify trace probe"}' \
	"$API_URL/api/v1/requests")" || fail "POST $API_URL/api/v1/requests did not answer"
[ "$code" = 201 ] || fail "create answered $code: $(cat "$tmp/create.json")"
request_id="$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["requestId"])' "$tmp/create.json")"
pass "created request $request_id under traceparent $traceparent"

echo "== Jaeger trace $trace_id"
kubectl --context "$CTX" -n "$NS" port-forward svc/jaeger "$JAEGER_PORT:16686" >"$tmp/pf.log" 2>&1 &
pf=$!
for _ in $(seq 1 30); do
	curl --silent --max-time 1 -o /dev/null "http://127.0.0.1:$JAEGER_PORT/" && break
	sleep 1
done
deadline=$((SECONDS + WAIT_SECONDS))
verdict=""
while :; do
	if curl --silent --fail --max-time 5 -o "$tmp/trace.json" "http://127.0.0.1:$JAEGER_PORT/api/traces/$trace_id"; then
		if verdict="$(python3 - "$tmp/trace.json" "$parent_id" <<'PY'
import json, sys

doc = json.load(open(sys.argv[1]))
want_parent = sys.argv[2]
traces = doc.get("data") or []
if len(traces) != 1:
    print(f"{len(traces)} traces for the ID"); sys.exit(1)
tr = traces[0]
procs = {k: v["serviceName"] for k, v in tr["processes"].items()}
spans = {s["spanID"]: s for s in tr["spans"]}

def svc(s): return procs[s["processID"]]
def tag(s, k):
    for t in s.get("tags", []):
        if t["key"] == k: return t["value"]
def kind(s): return tag(s, "span.kind")
def parent(s):
    for r in s.get("references", []):
        if r["refType"] == "CHILD_OF": return r["spanID"]
def ancestors(s):
    seen = []
    while (p := parent(s)) in spans:
        s = spans[p]; seen.append(s)
    return seen
def find(service, name, k=None):
    return [s for s in spans.values() if svc(s) == service and s["operationName"] == name and (k is None or kind(s) == k)]

missing = []
api = find("api", "POST /api/v1/requests", "server")
if len(api) != 1 or parent(api[0]) != want_parent:
    missing.append("api: one POST /api/v1/requests server span, child of our traceparent")
else:
    api = api[0]
    if not any(api in ancestors(s) for s in find("api", "coordclient.CreateEntity")):
        missing.append("api: coordclient.CreateEntity under the API span")
    if not any(s["operationName"].startswith("PUT /internal/") for s in spans.values() if svc(s) == "coordinator" and kind(s) == "server"):
        missing.append("coordinator: a routed server span")
    if not any(svc(s) == "storagenode" and kind(s) == "server" for s in spans.values()):
        missing.append("storagenode: a data-route server span")
    if not any(svc(s) == "storagenode" and s["operationName"] == "raft.apply" and api in ancestors(s) for s in spans.values()):
        missing.append("storagenode: raft.apply under the API span")
    pubs = [s for s in find("matcher", "publish", "producer") if api in ancestors(s)]
    if not pubs:
        missing.append("relay: a publish producer span under the API span")
    cons = [s for s in find("matcher", "consume", "consumer") if any(parent(s) == p["spanID"] for p in pubs)]
    if not cons:
        missing.append("matcher: a consume consumer span, child of the relay's publish")
    elif not any(c in ancestors(s) for c in cons for s in spans.values() if svc(s) == "matcher" and s["operationName"].startswith("coordclient.")):
        missing.append("matcher: a coordclient.* storage span under consume")
counts = {}
for s in spans.values():
    counts[svc(s)] = counts.get(svc(s), 0) + 1
if missing:
    print("; ".join(missing) + f" (spans by service: {counts})"); sys.exit(1)
print(f"spans by service: {counts}")
PY
)"; then
			break
		fi
	fi
	[ "$SECONDS" -lt "$deadline" ] || fail "trace $trace_id incomplete after ${WAIT_SECONDS}s: ${verdict:-not in Jaeger}"
	sleep 2
done
pass "one trace holds the API, storage, relay and matcher spans: $verdict"

mkdir -p "$EVIDENCE_DIR"
evidence="$EVIDENCE_DIR/app-trace-$(date -u +%Y%m%dT%H%M%SZ).json"
cp "$tmp/trace.json" "$evidence"
chmod 0644 "$evidence"
pass "evidence $evidence"

kill "$pf" 2>/dev/null || true
wait "$pf" 2>/dev/null || true
pf=""
leaked="$(repo_port_forwards "$ROOT")"
[ -z "$leaked" ] || fail "leaked port-forwards: $leaked"
pass "no port-forward outlived the Verify"
echo "S4.4 Verify: PASS"
