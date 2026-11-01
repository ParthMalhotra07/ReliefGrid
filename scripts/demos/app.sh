# shellcheck shell=bash
# App demo preflight reuses the storage demo's bounded forwards and cleanup.
. "$(dirname "${BASH_SOURCE[0]}")/kube.sh"
app_demo() {
 demo_reset "clean deployment via demo-reset -> app-deploy; recorded UUID seed and frozen create inputs; fixed workload and settling budgets"
 # Restoring admission after the preceding demo can briefly put replicas in
 # catch-up. Await the same Ready condition before taking the strict snapshot.
 run "await storage readiness after previous workload" kubectl --context "$CTX" -n "$NS" wait --for=condition=Ready pods -l app.kubernetes.io/component=storagenode --timeout=90s || { fail "storage readiness before app demo"; demo_end; }
 require_preflight
 export DEMO_COORD_URL="$(coord)" DEMO_OPERATOR_TOKEN="$OPTOK"
 if [[ "$1" =~ ^(2|3|5|8)$ ]]; then
  forward "svc/${APP_RELEASE:-reliefgrid}-rabbitmq" 15672 || { fail "preflight: broker management forward"; demo_end; }
  export DEMO_BROKER_URL="http://127.0.0.1:$FWD_PORT"
  forward "svc/${APP_RELEASE:-reliefgrid}-rabbitmq" 5672 || { fail "preflight: broker AMQP forward"; demo_end; }
  export DEMO_AMQP_ENDPOINT="amqp://127.0.0.1:$FWD_PORT"
  export DEMO_OUTBOX_DRAIN="$DEMO_TMP/drain-outbox"
  run "build bounded relay-only handoff drainer" go build -o "$DEMO_OUTBOX_DRAIN" "$REPO_ROOT/scripts/demos/drain-outbox" || { fail "build handoff drainer"; demo_end; }
 fi
 local out="$EVIDENCE_ROOT/$DEMO_RUN_ID/demo-$1-app"
 if run "public API assertions and recorded observations" python3 "$REPO_ROOT/scripts/demos/app_demo.py" "$1" --out "$out"; then
  pass "application assertions passed; inputs, interruptions and strong reads retained in $out"
 else
  fail "application assertions; first failure recorded in $out"
 fi
 demo_end
}
