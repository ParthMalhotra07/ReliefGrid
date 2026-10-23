#!/usr/bin/env bash
# A0.3 verification: confirmed persistent messages survive a RabbitMQ pod
# restart and are consumed with manual acknowledgements.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
CHART="$ROOT/deploy/helm/reliefgrid"
CONTEXT="${KUBE_CONTEXT:-kind-reliefgrid}"
NAMESPACE="${NAMESPACE:-reliefgrid}"
RELEASE="${RELEASE:-reliefgrid}"
LOCAL_PORT="${LOCAL_PORT:-35672}"
COUNT="${COUNT:-100}"
AMQP_URL="amqp://reliefgrid:reliefgrid@127.0.0.1:${LOCAL_PORT}/"
PORT_FORWARD_PID=""

fail() {
	printf 'FAIL  %s\n' "$1" >&2
	exit 1
}

require_file() {
	[ -f "$1" ] || fail "missing ${1#"$ROOT/"}"
}

stop_port_forward() {
	if [ -n "$PORT_FORWARD_PID" ] && kill -0 "$PORT_FORWARD_PID" 2>/dev/null; then
		kill "$PORT_FORWARD_PID" 2>/dev/null || true
		wait "$PORT_FORWARD_PID" 2>/dev/null || true
	fi
	PORT_FORWARD_PID=""
}

start_port_forward() {
	local log="$1"
	stop_port_forward
	kubectl --context "$CONTEXT" -n "$NAMESPACE" port-forward \
		"service/${RELEASE}-rabbitmq" "${LOCAL_PORT}:5672" >"$log" 2>&1 &
	PORT_FORWARD_PID=$!
	for _ in $(seq 1 60); do
		if ! kill -0 "$PORT_FORWARD_PID" 2>/dev/null; then
			cat "$log" >&2
			fail "RabbitMQ port-forward exited"
		fi
		if grep -q "Forwarding from 127.0.0.1:${LOCAL_PORT}" "$log"; then
			return
		fi
		sleep 1
	done
	cat "$log" >&2
	fail "RabbitMQ port-forward was not ready within 60s"
}

trap stop_port_forward EXIT

require_file "$CHART/Chart.yaml"
require_file "$CHART/values.yaml"
require_file "$ROOT/spike/mq/main.go"

tmp="$(mktemp -d)"
trap 'stop_port_forward; rm -rf "$tmp"' EXIT
mkdir -p "$tmp/chart"
cp -R "$CHART/." "$tmp/chart/"

helm dependency build "$tmp/chart"
helm upgrade --install "$RELEASE" "$tmp/chart" \
	--kube-context "$CONTEXT" \
	--namespace "$NAMESPACE" \
	--create-namespace \
	--wait \
	--timeout 5m

kubectl --context "$CONTEXT" -n "$NAMESPACE" rollout status \
	"statefulset/${RELEASE}-rabbitmq" --timeout=5m

pod="$(kubectl --context "$CONTEXT" -n "$NAMESPACE" get pod \
	-l "app.kubernetes.io/instance=${RELEASE},app.kubernetes.io/component=rabbitmq" \
	-o jsonpath='{.items[0].metadata.name}')"
[ -n "$pod" ] || fail "RabbitMQ pod was not created"

image="$(kubectl --context "$CONTEXT" -n "$NAMESPACE" get pod "$pod" \
	-o jsonpath='{.spec.containers[0].image}')"
[[ "$image" =~ ^rabbitmq:4\.3\.[0-9]+-management@sha256:[0-9a-f]{64}$ ]] || \
	fail "RabbitMQ image is not an exact 4.3.x tag and digest: $image"

node="$(kubectl --context "$CONTEXT" -n "$NAMESPACE" get pod "$pod" \
	-o jsonpath='{.spec.nodeName}')"
role="$(kubectl --context "$CONTEXT" get node "$node" -o jsonpath='{.metadata.labels.role}')"
[ "$role" = "infra" ] || fail "RabbitMQ scheduled on $node (role=$role), not the infra worker"

pvc_phase="$(kubectl --context "$CONTEXT" -n "$NAMESPACE" get pvc \
	-l "app.kubernetes.io/instance=${RELEASE},app.kubernetes.io/component=rabbitmq" \
	-o jsonpath='{.items[0].status.phase}')"
[ "$pvc_phase" = "Bound" ] || fail "RabbitMQ PVC is not Bound (phase=$pvc_phase)"

traefik_ready="$(kubectl --context "$CONTEXT" -n "$NAMESPACE" get deployment \
	"${RELEASE}-traefik" -o jsonpath='{.status.readyReplicas}')"
[ "${traefik_ready:-0}" -ge 1 ] || fail "Traefik deployment is not ready"

start_port_forward "$tmp/port-forward-before.log"

publish_output="$(cd "$ROOT" && go run ./spike/mq -mode publish -url "$AMQP_URL" -count "$COUNT")"
printf '%s\n' "$publish_output"
grep -Fxq "confirmed=${COUNT}" <<<"$publish_output" || fail "publisher did not confirm all $COUNT messages"

stop_port_forward
kubectl --context "$CONTEXT" -n "$NAMESPACE" delete pod "$pod" --wait=true
for _ in $(seq 1 60); do
	if kubectl --context "$CONTEXT" -n "$NAMESPACE" get pod "$pod" >/dev/null 2>&1; then
		break
	fi
	sleep 1
done
kubectl --context "$CONTEXT" -n "$NAMESPACE" wait --for=condition=Ready \
	"pod/$pod" --timeout=5m
start_port_forward "$tmp/port-forward-after.log"

consume_output="$(cd "$ROOT" && go run ./spike/mq -mode consume -url "$AMQP_URL" -count "$COUNT")"
printf '%s\n' "$consume_output"
grep -Fxq "received=${COUNT}" <<<"$consume_output" || fail "consumer did not receive all $COUNT messages"

priority_output="$(cd "$ROOT" && go run ./spike/mq -mode priority -url "$AMQP_URL")"
printf '%s\n' "$priority_output"
grep -Fxq 'priority=strict:[31 15 7 2 1]' <<<"$priority_output" || \
	fail "explicit nonzero priorities were not delivered in strict descending order"
grep -Fxq 'priority=omitted-default-4:[7 0 1]' <<<"$priority_output" || \
	fail "omitted priority did not behave as effective priority 4"

printf 'PASS  A0.3 RabbitMQ restart verification\n'
