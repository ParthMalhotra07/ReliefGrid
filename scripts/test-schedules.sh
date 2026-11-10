#!/usr/bin/env bash
# Seeded protocol execution on M8, then the local kind cluster. Missing snapshot
# interruption capabilities are failures, never skipped acceptance cases.
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"
backend="${SCHEDULE_BACKEND:-both}"
case "$backend" in m8|cluster|both) ;; *) echo 'SCHEDULE_BACKEND must be m8, cluster or both' >&2;exit 1;; esac
export SCHEDULE_RUN_ID="${SCHEDULE_RUN_ID:-$(python3 -c 'import uuid;print(uuid.uuid4())')}"
[[ "$SCHEDULE_RUN_ID" =~ ^[A-Za-z0-9][A-Za-z0-9._-]{0,80}$ ]] || { echo 'Invalid SCHEDULE_RUN_ID' >&2;exit 1; }
export SEEDS="${SEEDS:-50}"
hold="${SCHEDULE_HOLD_SHARD:-0}"
case "$hold" in 0) ;;1) [ "${SCHEDULE_IDS:-}" = 14 ] && [ "$backend" = cluster ] || { echo 'SCHEDULE_HOLD_SHARD=1 requires cluster and SCHEDULE_IDS=14 only' >&2;exit 1; };;*) exit 1;;esac
out="$ROOT/docs/evidence/A2.3/$SCHEDULE_RUN_ID"
mkdir -p "$out"
if [ -n "${SCHEDULE_IDS:-}" ];then echo "SUBSET $SCHEDULE_IDS: not complete A2.3 acceptance";fi
broker="" proxy="" replicas="" shard_replicas="" storage_tag=""
cleanup(){
 local status=$? cleanup_status=0
 trap - EXIT
 if [ -n "$proxy" ];then kill "$proxy" 2>/dev/null || true;wait "$proxy" 2>/dev/null || true;fi
 if [ -n "$shard_replicas" ];then
  kubectl --context kind-reliefgrid -n reliefgrid scale statefulset/storage-shard-1 --replicas="$shard_replicas" >/dev/null || cleanup_status=1
  kubectl --context kind-reliefgrid -n reliefgrid rollout status statefulset/storage-shard-1 --timeout=90s >/dev/null || cleanup_status=1
 fi
 if [ -n "$storage_tag" ];then
  for shard in 0 1 2;do for replica in 0 1 2;do
   kubectl --context kind-reliefgrid -n reliefgrid label pod "storage-shard-$shard-$replica" reliefgrid.io/chaos-isolated- >/dev/null 2>&1 || cleanup_status=1
  done;done
  helm --kube-context kind-reliefgrid upgrade storage deploy/helm/storage -n reliefgrid --reset-then-reuse-values --set-string "image.tag=$storage_tag" --wait --timeout 15m > "$out/restore-image.log" 2>&1 || cleanup_status=1
  for shard in 0 1 2;do kubectl --context kind-reliefgrid -n reliefgrid rollout status "statefulset/storage-shard-$shard" --timeout=600s >> "$out/restore-image.log" 2>&1 || cleanup_status=1;done
 fi
 if [ -n "$replicas" ];then kubectl --context kind-reliefgrid -n reliefgrid scale deployment reliefgrid-matcher --replicas="$replicas" >/dev/null || cleanup_status=1;fi
 if [ -n "$broker" ];then docker rm -f "$broker" >/dev/null || cleanup_status=1;fi
 if [ "$status" = 0 ];then status=$cleanup_status;fi
 exit "$status"
}
trap cleanup EXIT
if [ -z "${MQ_AMQP_URL:-}" ] && [ -z "${MQ_MANAGEMENT_URL:-}" ];then
 image="$(awk '$1=="image:"{print $2;exit}' deploy/helm/reliefgrid/values.yaml)"
 [[ "$image" =~ ^rabbitmq:4\.3\.[0-9]+-management@sha256:[0-9a-f]{64}$ ]]
 broker="$(docker run -d --rm --memory 1g --label reliefgrid.goal=A2.3 -p 127.0.0.1::5672 -p 127.0.0.1::15672 -e RABBITMQ_DEFAULT_USER=reliefgrid -e RABBITMQ_DEFAULT_PASS=reliefgrid "$image")"
 ready=false
 for _ in $(seq 60);do
  amqp="$(docker port "$broker" 5672/tcp)";management="$(docker port "$broker" 15672/tcp)"
  export MQ_AMQP_URL="amqp://reliefgrid:reliefgrid@$amqp/" MQ_MANAGEMENT_URL="http://$management"
  if curl --fail --silent --max-time 1 -u reliefgrid:reliefgrid "$MQ_MANAGEMENT_URL/api/overview" >/dev/null;then ready=true;break;fi
  sleep 1
 done
 test "$ready" = true
elif [ -z "${MQ_AMQP_URL:-}" ] || [ -z "${MQ_MANAGEMENT_URL:-}" ];then
 echo 'Supply both isolated test broker URLs or neither' >&2;exit 1
fi
run_backend(){
 SCHEDULE_BACKEND="$1" go test -failfast -timeout=150m -race -count=1 -tags schedules -run '^TestSeededWorkflowSchedules$' -v ./tests/workflow > "$out/$1.log" 2>&1 || { tail -60 "$out/$1.log" >&2;return 1; }
 echo "$1: PASS (run $SCHEDULE_RUN_ID, SEEDS=$SEEDS, selection=${SCHEDULE_IDS:-all})"
}
if [ "$backend" != cluster ];then run_backend m8;fi
if [ "$backend" != m8 ];then
 # A clean dev fixture deployment ensures no unrelated pending outbox events
 # can be published into this run's isolated broker vhosts.
 if [ "${SCHEDULE_RESET:-1}" = 1 ];then
  A15_EVIDENCE_DIR="$out/deployment" RESET_CONFIRM=reliefgrid make demo-reset > "$out/deploy.log" 2>&1
 fi
 needs_seams=false
 if [ -z "${SCHEDULE_IDS:-}" ];then needs_seams=true;else
  case ",${SCHEDULE_IDS}," in *,1,*|*,01,*|*,12,*|*,13,*|*,13a,*|*,13b,*) needs_seams=true;;esac
 fi
 if [ "$needs_seams" = true ];then
  current_image="$(kubectl --context kind-reliefgrid -n reliefgrid get statefulset storage-shard-0 -o jsonpath='{.spec.template.spec.containers[0].image}')"
  [[ "$current_image" == reliefgrid/storagenode:* ]] || { echo 'Unexpected storage image; refusing to change it' >&2;exit 1; }
  storage_tag="${current_image##*:}"
  make storage-image-seams > "$out/seams-image.log" 2>&1
  helm --kube-context kind-reliefgrid upgrade storage deploy/helm/storage -n reliefgrid --reset-then-reuse-values --set image.tag=seams --wait --timeout 15m > "$out/seams-rollout.log" 2>&1
  for shard in 0 1 2;do kubectl --context kind-reliefgrid -n reliefgrid rollout status "statefulset/storage-shard-$shard" --timeout=600s >> "$out/seams-rollout.log" 2>&1;done
 fi
 replicas="$(kubectl --context kind-reliefgrid -n reliefgrid get deployment reliefgrid-matcher -o jsonpath='{.spec.replicas}')"
 shard_replicas="$(kubectl --context kind-reliefgrid -n reliefgrid get statefulset storage-shard-1 -o jsonpath='{.spec.replicas}')"
 kubectl --context kind-reliefgrid -n reliefgrid scale deployment reliefgrid-matcher --replicas=0 >/dev/null
 kubectl --context kind-reliefgrid -n reliefgrid wait --for=delete pods -l app.kubernetes.io/instance=reliefgrid,app.kubernetes.io/component=matcher --timeout=90s
 port="$(python3 -c 'import socket;s=socket.socket();s.bind(("127.0.0.1",0));print(s.getsockname()[1]);s.close()')"
 bash scripts/schedules-forward.sh "$port" > "$out/forward.log" 2>&1 &
 proxy=$!
 export COORDINATOR_URL="http://127.0.0.1:$port"
 ready=false
 for _ in $(seq 50);do if curl --fail --silent --max-time 1 "$COORDINATOR_URL/internal/ring" >/dev/null;then ready=true;break;fi;sleep .1;done
 if [ "$ready" != true ];then echo 'Coordinator loopback forward did not become ready' >&2;tail -10 "$out/forward.log" >&2;exit 1;fi
 # Every audited fixture now confirms and drains its own outbox after assertions.
 # Refuse foreign work before any cluster case, including non-relay subsets.
 for shard in 0 1 2;do
  curl --fail --silent --max-time 5 "$COORDINATOR_URL/internal/outbox?shard=$shard&limit=1&consistency=strong" |
   jq -e '(.events|type)=="array" and (.events|length)==0' >/dev/null || { echo 'Cluster must have no foreign pending outbox work before schedule audits' >&2;exit 1; }
 done
 if [ "$hold" = 1 ];then
  kubectl --context kind-reliefgrid -n reliefgrid scale statefulset/storage-shard-1 --replicas=0 >/dev/null
  kubectl --context kind-reliefgrid -n reliefgrid wait --for=delete pod/storage-shard-1-0 pod/storage-shard-1-1 pod/storage-shard-1-2 --timeout=30s
 fi
 run_backend cluster
fi
if [ -n "${SCHEDULE_IDS:-}" ];then
 echo 'A2.3 SUBSET PASS; full acceptance still required'
elif [ "$backend" = both ] && [ "$SEEDS" = 50 ];then
 echo 'A2.3 ALL APPLICABLE SCHEDULES PASS: M8=850, cluster=900 (50 seeds; SCH13a is cluster-only)'
else
 echo "A2.3 selected backend/seeds PASS: backend=$backend seeds=$SEEDS; full acceptance still required"
fi
