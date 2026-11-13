#!/usr/bin/env bash
# deploy-harness.sh <source tree> <work dir>: runs scripts/demos/reset.sh, then
# scripts/app-deploy.sh, from a copy of <source tree>'s deploy scripts, against fake kubectl,
# helm, make, kind, docker and curl (deploy-fakes/). Each fake logs its command line to
# $WORK/calls with the work dir shown as <work>. The fake helm stops app-deploy at its
# `upgrade --install` (exit 3), after logging it. INTERNAL_AUTH and FAKE_STORAGE_VALUES
# come from the environment.
set -u
src="$1" WORK="$2"
export WORK
tree="$WORK/tree"
mkdir -p "$tree/scripts/demos" "$tree/deploy/helm/reliefgrid" "$tree/deploy/helm/storage"
for f in scripts/demos/reset.sh scripts/demos/common.sh scripts/port-forwards.sh scripts/app-deploy.sh \
	scripts/app_evidence.py scripts/app-chart-deps.sh deploy/helm/reliefgrid/Chart.yaml; do
	cp "$src/$f" "$tree/$f"
done
for c in storage reliefgrid; do
	[ -f "$src/deploy/helm/$c/values-internal-auth.yaml" ] && cp "$src/deploy/helm/$c/values-internal-auth.yaml" "$tree/deploy/helm/$c/"
done
git -C "$tree" init -q && git -C "$tree" add -A && git -C "$tree" -c user.name=t -c user.email=t@t commit -qm t
here="$(cd "$(dirname "$0")" && pwd)"
export PATH="$here/deploy-fakes:$PATH" APP_IMAGE_TAG=a15-fixed A15_EVIDENCE_DIR="$WORK/a15" XDG_CACHE_HOME="$WORK/cache"
: >"$WORK/calls"
RESET_CONFIRM=reliefgrid bash "$tree/scripts/demos/reset.sh" >"$WORK/reset.out" 2>&1
echo "reset exit $?" >>"$WORK/calls"
bash "$tree/scripts/app-deploy.sh" >"$WORK/app.out" 2>&1
echo "app-deploy exit $?" >>"$WORK/calls"
