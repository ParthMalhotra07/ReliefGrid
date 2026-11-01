#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
# Chart.lock pins the version; registration also works on a clean CI runner.
timeout 120 helm repo add reliefgrid-traefik https://traefik.github.io/charts
timeout 120 helm dependency build deploy/helm/reliefgrid
