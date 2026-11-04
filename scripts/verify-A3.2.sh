#!/usr/bin/env bash
# Live A3.2 acceptance on an isolated pinned broker, or explicit test URLs.
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

go test -count=1 ./cmd/api/... ./cmd/matcher/...
go test -tags integration -count=1 -run '^TestAppAPIRecoveryBudget$' ./scripts/...

broker=""
tmp="$(mktemp -d)"
cleanup() {
  result=$?
  if [ -n "$broker" ]; then
    if [ "$result" -ne 0 ]; then docker logs "$broker" >&2 || true; fi
    docker rm -f "$broker" >/dev/null
  fi
  rm -rf "$tmp"
}
trap cleanup EXIT

if [ -z "${MQ_AMQP_URL:-}" ] && [ -z "${MQ_MANAGEMENT_URL:-}" ]; then
  # Deleting an isolated vhost can reset AMQP sockets. kubectl port-forward
  # treats that as a fatal pod connection error and drops its management port
  # too. Direct ports keep that expected teardown inside the test connection.
  image="$(awk '$1 == "image:" {print $2; exit}' deploy/helm/reliefgrid/values.yaml)"
  [[ "$image" =~ ^rabbitmq:4\.3\.[0-9]+-management@sha256:[0-9a-f]{64}$ ]]
  broker="$(docker run -d --rm --memory 1g --label reliefgrid.goal=A3.2 \
    --hostname reliefgrid-a32-verify \
    -p 127.0.0.1::5672 -p 127.0.0.1::15672 \
    -e RABBITMQ_DEFAULT_USER=reliefgrid -e RABBITMQ_DEFAULT_PASS=reliefgrid \
    "$image")"
  ready=false
  : > "$tmp/readiness.err"
  for _ in $(seq 1 60); do
    # Docker Desktop may publish the host mappings after container creation.
    # Observe current addresses with readiness instead of freezing that sample.
    amqp_address="$(docker port "$broker" 5672/tcp)"
    management_address="$(docker port "$broker" 15672/tcp)"
    export MQ_AMQP_URL="amqp://reliefgrid:reliefgrid@${amqp_address}/"
    export MQ_MANAGEMENT_URL="http://${management_address}"
    if [[ ! "$amqp_address" =~ ^127\.0\.0\.1:[0-9]+$ ]] || [[ ! "$management_address" =~ ^127\.0\.0\.1:[0-9]+$ ]]; then
      sleep 1
      continue
    fi
    if curl --fail --silent --show-error --max-time 1 -u reliefgrid:reliefgrid \
      "$MQ_MANAGEMENT_URL/api/overview" -o "$tmp/overview.json" 2>"$tmp/readiness.err"; then
      ready=true
      break
    fi
    sleep 1
  done
  if [ "$ready" != true ]; then
    printf 'FAIL A3.2 pinned broker unavailable at %s\n' "$MQ_MANAGEMENT_URL" >&2
    cat "$tmp/readiness.err" >&2
    exit 1
  fi
  printf 'broker_image=%s\n' "$image"
elif [ -z "${MQ_AMQP_URL:-}" ] || [ -z "${MQ_MANAGEMENT_URL:-}" ]; then
  printf 'FAIL supply both MQ_AMQP_URL and MQ_MANAGEMENT_URL\n' >&2
  exit 1
fi

go test -race -count=1 -tags integration,a32 -run '^TestDrainIntegration' -v ./internal/mq/...
go test -race -count=1 -tags integration,a32 -run '^TestSupervisorKeepsAdmittedTransportThroughCancellation$' -v ./cmd/matcher/...
printf 'A3.2 VERIFY: ALL PASS\n'
