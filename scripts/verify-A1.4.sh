#!/usr/bin/env bash
# Full A1.4 acceptance owns the only broker it stops; never target shared kind.
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"
go test -race -count=1 ./internal/outbox/... ./internal/mq/... ./cmd/matcher/...
broker=""
tmp="$(mktemp -d)"
cleanup() {
  result=$?
  if [ -n "$broker" ]; then
    if [ "$result" -ne 0 ]; then docker logs "$broker" >&2 || true; fi
    docker rm -fv "$broker" >/dev/null
  fi
  rm -rf "$tmp"
}
trap cleanup EXIT
image="$(awk '$1 == "image:" {print $2; exit}' deploy/helm/reliefgrid/values.yaml)"
[[ "$image" =~ ^rabbitmq:4\.3\.[0-9]+-management@sha256:[0-9a-f]{64}$ ]]
# Keep the disposable container while stopped so its quorum data survives the
# test. The helper rediscovers host ports. Cleanup removes only its own volume.
broker="$(docker run -d --memory 1g --label reliefgrid.goal=A1.4 \
  --hostname reliefgrid-a14-verify \
  -p 127.0.0.1::5672 -p 127.0.0.1::15672 \
  -e RABBITMQ_DEFAULT_USER=reliefgrid -e RABBITMQ_DEFAULT_PASS=reliefgrid \
  "$image")"
export A14_BROKER_ID="$broker"
ready=false
: > "$tmp/readiness.err"
for _ in $(seq 1 60); do
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
  printf 'FAIL A1.4 pinned broker unavailable at %s\n' "$MQ_MANAGEMENT_URL" >&2
  cat "$tmp/readiness.err" >&2
  exit 1
fi
printf 'broker_image=%s\n' "$image"
go test -race -count=1 -tags integration -v ./internal/outbox/...
printf 'A1.4 VERIFY: ALL PASS\n'
