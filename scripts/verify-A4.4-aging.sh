#!/usr/bin/env bash
set -euo pipefail
export PATH="$HOME/.local/go/bin:$HOME/.local/bin:$PATH"
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"
out="${A44_AGING_EVIDENCE_DIR:-$(mktemp -d)}"
mkdir -p "$out"
printf 'aging evidence: %s\n' "$out"
go test -race -count=3 -run TestAging ./internal/reconcile > "$out/unit.log" 2>&1
image="$(awk '$1=="image:"{print $2;exit}' deploy/helm/reliefgrid/values.yaml)"
[[ "$image" =~ ^rabbitmq:4\.3\.[0-9]+-management@sha256:[0-9a-f]{64}$ ]]
broker="$(docker run -d --rm --memory 1g -p 127.0.0.1::5672 -p 127.0.0.1::15672 -e RABBITMQ_DEFAULT_USER=reliefgrid -e RABBITMQ_DEFAULT_PASS=reliefgrid "$image")"
cleanup(){ status=$?;trap - EXIT;docker rm -f "$broker" >/dev/null || status=1;exit "$status"; }
trap cleanup EXIT
ready=false
for _ in $(seq 60);do
 amqp="$(docker port "$broker" 5672/tcp | head -1)"
 management="$(docker port "$broker" 15672/tcp | head -1)"
 if curl --fail --silent --max-time 1 -u reliefgrid:reliefgrid "http://$management/api/overview" >/dev/null;then ready=true;break;fi
 sleep 1
done
test "$ready" = true
MQ_AMQP_URL="amqp://reliefgrid:reliefgrid@$amqp/" go test -race -count=1 -timeout=60s -tags integration,aging -run '^TestAgingImmediateWake' -v ./internal/reconcile > "$out/broker.log" 2>&1
printf 'AGING LIVE BROKER: PASS\n'
