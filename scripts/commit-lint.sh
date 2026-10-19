#!/usr/bin/env bash
# Fails if any commit in <base>..<head> carries AI attribution: a
# Co-authored-by trailer or "Generated with" text (case-insensitive).
#   scripts/commit-lint.sh <base> <head>
set -uo pipefail

if [ $# -ne 2 ]; then
	echo "usage: $0 <base> <head>" >&2
	exit 2
fi

shas="$(git rev-list "$1..$2")" || exit 2
bad=0
for sha in $shas; do
	msg="$(git log -1 --format=%B "$sha")"
	if grep -Eiq '^[[:space:]]*co-authored-by:' <<<"$msg" || grep -Fiq 'generated with' <<<"$msg"; then
		echo "commit-lint: $(git log -1 --format='%h %s' "$sha") has an attribution trailer or 'Generated with' text"
		bad=1
	fi
done

if [ "$bad" -ne 0 ]; then
	echo "commit-lint: reword the commits above (git rebase -i) and force-push the branch"
	exit 1
fi
echo "commit-lint: $(wc -w <<<"$shas") commit(s) clean"
