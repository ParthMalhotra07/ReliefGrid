# shellcheck shell=bash
# App demo preflight reuses the storage demo's bounded forwards and cleanup.
. "$(dirname "${BASH_SOURCE[0]}")/kube.sh"
app_demo() {
 demo_reset "clean deployment via demo-reset -> app-deploy; recorded UUID seed and frozen create inputs; fixed workload and settling budgets"
 require_preflight
 export DEMO_COORD_URL="$(coord)" DEMO_OPERATOR_TOKEN="$OPTOK"
 if [ "$1" = 5 ]; then
  forward "svc/${APP_RELEASE:-reliefgrid}-rabbitmq" 15672 || { fail "preflight: broker management forward"; demo_end; }
  export DEMO_BROKER_URL="http://127.0.0.1:$FWD_PORT"
 fi
 local out="$EVIDENCE_ROOT/$DEMO_RUN_ID/demo-$1-app"
 if run "public API assertions and recorded observations" python3 "$REPO_ROOT/scripts/demos/app_demo.py" "$1" --out "$out"; then
  pass "application assertions passed; inputs, interruptions and strong reads retained in $out"
 else
  fail "application assertions; first failure recorded in $out"
 fi
 demo_end
}
