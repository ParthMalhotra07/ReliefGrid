#!/usr/bin/env bash
# image-digests.json for one commit (S5.1): CI's digests job merges the publish job's per-image
# digest files into the one file scripts/gke-deploy.sh deploys from.
#
#   scripts/image-digests.sh <dir> <commit>   <dir>/<name> holds sha256:<64 hex> for each of
#                                             the six images; prints {"commit", "images"}
# Fails, naming the image, if one is missing or not a sha256 digest: a partial set is never
# written.
set -euo pipefail

if [ $# -ne 2 ]; then
	echo "usage: $0 <digest dir> <commit>" >&2
	exit 2
fi
dir="$1" commit="$2"
images=(storagenode coordinator tools api matcher ui)
args=()
for name in "${images[@]}"; do
	[ -f "$dir/$name" ] || { echo "image-digests: no digest for $name in $dir" >&2; exit 1; }
	d="$(tr -d '[:space:]' <"$dir/$name")"
	[[ "$d" =~ ^sha256:[0-9a-f]{64}$ ]] || { echo "image-digests: $name: '$d' is not a sha256 digest" >&2; exit 1; }
	args+=(--arg "$name" "$d")
done
jq -n --arg commit "$commit" "${args[@]}" \
	'{commit: $commit, images: {storagenode: $storagenode, coordinator: $coordinator, tools: $tools, api: $api, matcher: $matcher, ui: $ui}}'
