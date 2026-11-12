# shellcheck shell=bash
API_FORWARD_PID=""
start_api_forward() {
 local log="$EVIDENCE_ROOT/api-forward.log" port="" deadline=$((SECONDS+30))
 # Keep this parent-owned forward outside the source cwd so per-demo leaked-
 # forward checks do not mistake its deliberately longer lifetime for a leak.
 (cd /tmp; exec kubectl --context "$KUBE_CONTEXT" -n "$NAMESPACE" port-forward --address=127.0.0.1 svc/reliefgrid-traefik :80) >"$log" 2>&1 &
 API_FORWARD_PID=$!
 while [ "$SECONDS" -lt "$deadline" ]; do
  kill -0 "$API_FORWARD_PID" 2>/dev/null || { cat "$log" >&2; return 1; }
  port="$(sed -n 's/^Forwarding from 127\.0\.0\.1:\([0-9]*\) ->.*/\1/p' "$log" | head -1)"
  if [ -n "$port" ]; then
   export API_URL="http://127.0.0.1:$port/api/v1"
   if curl --noproxy '*' --fail --silent --max-time 2 "$API_URL/requests?limit=1" >/dev/null; then return 0; fi
  fi
  sleep .1
 done
 echo 'offline API forward did not become ready within 30s' >&2
 return 1
}
cleanup_api_forward() {
 if [ -n "$API_FORWARD_PID" ]; then
  kill "$API_FORWARD_PID" 2>/dev/null || true
  wait "$API_FORWARD_PID" 2>/dev/null || true
  API_FORWARD_PID=""
 fi
}
