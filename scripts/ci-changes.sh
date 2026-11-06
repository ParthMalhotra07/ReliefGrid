#!/usr/bin/env bash
# CI change gate (S4.6): does a PR's diff touch what the images and smoke jobs test?
#
#   scripts/ci-changes.sh <base sha> <head sha>     prints code=, seams= and schedules=true|false
#
# Code is cmd/, internal/, deploy/, contracts/, scripts/, chaos/, loadtest/ (in the tools
# image), ui/ (built into an image), .github/ (CI itself), and the root Makefile, tools.mk,
# go.mod, go.sum and .dockerignore (every image's build context). Anything else (docs/, other
# root files) leaves those jobs skipped.
#
# Seams (S4.6b) is what the seams job tests (make check-seams): the seam packages
# internal/raftstore/, internal/storagenode/, cmd/storagenode/, internal/testseam/ and
# internal/fsm/ (memstore and the codec both use), the kind driver tests/seams/, and how they
# are built and run: go.mod, go.sum, the root Makefile, tools.mk (the golangci-lint pin),
# .github/ and this script.
#
# Schedules (A2.3) is what the schedules job runs (scripts/ci-schedules.sh): the
# schedules-tagged tests in tests/workflow/, the module packages they build (internal/api/,
# coordclient/, fsm/, geo/, internalapi/, mq/, obs/, outbox/, ring/, store/, testseam/ and
# workflow/; scripts/ci_schedules_test.go checks this list against go list -deps), contracts/
# (internal/api/gen and internalapi come from it), the broker pin in
# deploy/helm/reliefgrid/values.yaml, and how they are built and run: go.mod, go.sum, the root
# Makefile, tools.mk, .github/, this script and scripts/ci-schedules.sh.
#
# The diff is from the merge base, so commits on the base branch since the PR forked do not
# count. A base or head it cannot resolve is an error, never "no changes".
set -euo pipefail

if [ $# -ne 2 ]; then
	echo "usage: $0 <base sha> <head sha>" >&2
	exit 2
fi
base="$(git merge-base "$1" "$2")" || { echo "ci-changes: no merge base for $1 and $2 (fetch more history?)" >&2; exit 1; }
files="$(git diff --name-only "$base" "$2")"

code=false
seams=false
schedules=false
while IFS= read -r f; do
	case "$f" in
	cmd/* | internal/* | deploy/* | contracts/* | scripts/* | chaos/* | loadtest/* | ui/* | .github/* | Makefile | tools.mk | go.mod | go.sum | .dockerignore)
		code=true
		;;
	esac
	case "$f" in
	internal/raftstore/* | internal/storagenode/* | cmd/storagenode/* | internal/testseam/* | internal/fsm/* | tests/seams/* | \
		go.mod | go.sum | Makefile | tools.mk | .github/* | scripts/ci-changes.sh)
		seams=true
		;;
	esac
	case "$f" in
	tests/workflow/* | internal/api/* | internal/coordclient/* | internal/fsm/* | internal/geo/* | internal/internalapi/* | \
		internal/mq/* | internal/obs/* | internal/outbox/* | internal/ring/* | internal/store/* | internal/testseam/* | \
		internal/workflow/* | contracts/* | deploy/helm/reliefgrid/values.yaml | \
		go.mod | go.sum | Makefile | tools.mk | .github/* | scripts/ci-changes.sh | scripts/ci-schedules.sh)
		schedules=true
		;;
	esac
done <<<"$files"
echo "code=$code"
echo "seams=$seams"
echo "schedules=$schedules"
