#!/usr/bin/env bash
# CI schedules job (A2.3, #110): every schedules-tagged test in tests/workflow, on the M8
# backend, with the race detector. That includes TestKillRejectsExtraRestartBeforeReady (the
# exact restartCount == before+1 regression) and the seeded schedules, whose SCH15/SCH16
# drivers need a broker: a disposable RabbitMQ from the app chart's digest-pinned image,
# bound to loopback and removed on exit. No kind cluster; the cluster backend stays in
# make test-schedules.
#
#   scripts/ci-schedules.sh          SEEDS (default 50, the full M8 matrix) and
#                                    SCHEDULES_LOG_DIR (default a temp dir) may be set
#
# The job fails unless go test passes, nothing (test or subtest) is skipped, and every test
# that `go test -list` finds, which must include the kill-restart regression, reports PASS.
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

pkg=./tests/workflow
required=TestKillRejectsExtraRestartBeforeReady
seeds="${SEEDS:-50}"
logs="${SCHEDULES_LOG_DIR:-$(mktemp -d)}"
mkdir -p "$logs"
log="$logs/schedules.log"

image="$(awk '$1=="image:"{print $2;exit}' deploy/helm/reliefgrid/values.yaml)"
if ! [[ "$image" =~ ^rabbitmq:4\.3\.[0-9]+-management@sha256:[0-9a-f]{64}$ ]]; then
	echo "ci-schedules: broker image $image is not a digest-pinned rabbitmq 4.3 (deploy/helm/reliefgrid/values.yaml)" >&2
	exit 1
fi

broker=""
cleanup() {
	if [ -n "$broker" ]; then docker rm -f "$broker" >/dev/null || true; fi
}
trap cleanup EXIT

broker="$(docker run -d --rm --memory 1g --label reliefgrid.goal=A2.3 -p 127.0.0.1::5672 -p 127.0.0.1::15672 \
	-e RABBITMQ_DEFAULT_USER=reliefgrid -e RABBITMQ_DEFAULT_PASS=reliefgrid "$image")"
ready=false
for _ in $(seq 90); do
	amqp="$(docker port "$broker" 5672/tcp | head -1)"
	management="$(docker port "$broker" 15672/tcp | head -1)"
	export MQ_AMQP_URL="amqp://reliefgrid:reliefgrid@$amqp/" MQ_MANAGEMENT_URL="http://$management"
	if curl --fail --silent --max-time 1 -u reliefgrid:reliefgrid "$MQ_MANAGEMENT_URL/api/overview" >/dev/null; then
		ready=true
		break
	fi
	sleep 1
done
if [ "$ready" != true ]; then
	echo "ci-schedules: broker $image not ready after 90s" >&2
	docker logs --tail 40 "$broker" >&2 || true
	exit 1
fi

mapfile -t tests < <(go test -tags schedules -list . "$pkg" | grep -E '^Test')
if ! printf '%s\n' "${tests[@]}" | grep -qx "$required"; then
	echo "ci-schedules: $required is not among the schedules-tagged tests (${tests[*]:-none})" >&2
	exit 1
fi

echo "ci-schedules: ${#tests[@]} tests, SEEDS=$seeds, backend m8, broker $image"
status=0
SCHEDULE_BACKEND=m8 SEEDS="$seeds" go test -race -count=1 -timeout 15m -tags schedules -v "$pkg" >"$log" 2>&1 || status=$?

grep -E '^--- (PASS|FAIL|SKIP): ' "$log" || true
if [ "$status" != 0 ]; then
	echo "ci-schedules: go test exited $status; last 150 lines:" >&2
	tail -150 "$log" >&2
	exit "$status"
fi
if grep -E '^\s*--- SKIP: ' "$log" >&2; then
	echo "ci-schedules: the tests above were skipped; a schedules test must run, not skip" >&2
	exit 1
fi
passed=0
for t in "${tests[@]}"; do
	if grep -qE "^--- PASS: $t \(" "$log"; then
		passed=$((passed + 1))
	else
		echo "ci-schedules: $t did not report PASS" >&2
		status=1
	fi
done
if [ "$status" != 0 ]; then exit 1; fi
echo "ci-schedules: $passed/${#tests[@]} schedules tests passed, none skipped"
