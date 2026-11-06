#!/usr/bin/env bash
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"
expected="v$(make -s print-NODE_VERSION)"
[ "$(node --version)" = "$expected" ] || { echo "A4.1 requires Node $expected (tools.mk)" >&2;exit 1; }
(
 cd ui
 npm ci --ignore-scripts --no-audit --no-fund
 npm run format:check
 npm run build
 npm test
 npm run test:browser
)
go test -count=1 ./ui ./internal/api ./cmd/api ./internal/mq
broker=""
cleanup(){ status=$?;trap - EXIT;if [ -n "$broker" ];then docker rm -f "$broker" >/dev/null || status=1;fi;exit "$status";}
trap cleanup EXIT
if [ -z "${MQ_AMQP_URL:-}" ] && [ -z "${MQ_MANAGEMENT_URL:-}" ];then
 image="$(awk '$1 == "image:" {print $2; exit}' deploy/helm/reliefgrid/values.yaml)"
 [[ "$image" =~ ^rabbitmq:4\.3\.[0-9]+-management@sha256:[0-9a-f]{64}$ ]]
 broker="$(docker run -d --rm --memory 1g --hostname reliefgrid-a41-verify -p 127.0.0.1::5672 -p 127.0.0.1::15672 -e RABBITMQ_DEFAULT_USER=reliefgrid -e RABBITMQ_DEFAULT_PASS=reliefgrid "$image")"
 ready=false
 for _ in $(seq 1 60);do
  amqp_address="$(docker port "$broker" 5672/tcp)";management_address="$(docker port "$broker" 15672/tcp)"
  export MQ_AMQP_URL="amqp://reliefgrid:reliefgrid@${amqp_address}/" MQ_MANAGEMENT_URL="http://${management_address}"
  if curl --fail --silent --max-time 1 -u reliefgrid:reliefgrid "$MQ_MANAGEMENT_URL/api/overview" >/dev/null;then ready=true;break;fi
  sleep 1
 done
 [ "$ready" = true ] || { echo 'A4.1 isolated broker unavailable' >&2;exit 1; }
elif [ -z "${MQ_AMQP_URL:-}" ] || [ -z "${MQ_MANAGEMENT_URL:-}" ];then
 echo 'Supply both isolated broker URLs or neither' >&2;exit 1
fi
go test -race -count=1 -tags integration,a41 -run '^TestProjection' -v ./internal/mq
printf 'A4.1 VERIFY: ALL PASS\n'
