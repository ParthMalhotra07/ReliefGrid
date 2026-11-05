#!/usr/bin/env bash
# CI change gate (S4.6): does a PR's diff touch what the images and smoke jobs test?
#
#   scripts/ci-changes.sh <base sha> <head sha>     prints code=true|false and seams=true|false
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
# Infra (S5.1) is what the terraform job checks (fmt and validate of infra/gcp, offline):
# infra/, tools.mk (the terraform pin), .github/, scripts/ci-tools.sh (installs it),
# scripts/gke_test.go (the checks) and this script.
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
infra=false
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
	infra/* | tools.mk | .github/* | scripts/ci-tools.sh | scripts/gke_test.go | scripts/ci-changes.sh)
		infra=true
		;;
	esac
done <<<"$files"
echo "code=$code"
echo "seams=$seams"
echo "infra=$infra"
