#!/usr/bin/env bash
# CI change gate (S4.6): does a PR's diff touch what the images and smoke jobs test?
#
#   scripts/ci-changes.sh <base sha> <head sha>     prints code=true or code=false
#
# Code is cmd/, internal/, deploy/, contracts/, scripts/, chaos/, loadtest/ (in the tools
# image), ui/ (built into an image), .github/ (CI itself), and the root Makefile, tools.mk,
# go.mod, go.sum and .dockerignore (every image's build context). Anything else (docs/, other
# root files) leaves those jobs skipped. The diff
# is from the merge base, so commits on the base branch since the PR forked do not count. A
# base or head it cannot resolve is an error, never "no changes".
set -euo pipefail

if [ $# -ne 2 ]; then
	echo "usage: $0 <base sha> <head sha>" >&2
	exit 2
fi
base="$(git merge-base "$1" "$2")" || { echo "ci-changes: no merge base for $1 and $2 (fetch more history?)" >&2; exit 1; }
files="$(git diff --name-only "$base" "$2")"

code=false
while IFS= read -r f; do
	case "$f" in
	cmd/* | internal/* | deploy/* | contracts/* | scripts/* | chaos/* | loadtest/* | ui/* | .github/* | Makefile | tools.mk | go.mod | go.sum | .dockerignore)
		code=true
		break
		;;
	esac
done <<<"$files"
echo "code=$code"
