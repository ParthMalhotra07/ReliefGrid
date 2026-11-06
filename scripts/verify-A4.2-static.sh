#!/usr/bin/env bash
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"
go test -race -count=1 ./internal/appmetrics ./cmd/api ./cmd/matcher ./internal/api ./internal/workflow
REQUIRE_HELM=1 go test -count=1 -tags integration -run '^(TestAppMetricsArePrivateAndOnlyApplicationWorkersExposeThem|TestObservabilityChartIsPrivateAndBounded)$' ./scripts
helm lint deploy/helm/observability
config="$(mktemp -d)"
cleanup(){ status=$?;trap - EXIT;rm -f "$config/prometheus.yml" || status=1;rmdir "$config" || status=1;exit "$status"; }
trap cleanup EXIT
# The integration test writes the parsed rendered configuration to an explicit
# temporary path. Validate with the exact server's promtool, without network.
A42_PROM_CONFIG="$config/prometheus.yml" REQUIRE_HELM=1 go test -count=1 -tags integration -run '^TestObservabilityChartIsPrivateAndBounded$' ./scripts
image="prom/prometheus:$(make -s print-PROMETHEUS_VERSION)@$(make -s print-PROMETHEUS_IMAGE_DIGEST)"
docker run --rm --network none --user "$(id -u):$(id -g)" --entrypoint /bin/promtool -v "$config/prometheus.yml:/etc/prometheus/prometheus.yml:ro" "$image" check config /etc/prometheus/prometheus.yml
echo 'A4.2 STATIC: ALL PASS'

PYTHONDONTWRITEBYTECODE=1 python3 scripts/verify-storage-promql.py
