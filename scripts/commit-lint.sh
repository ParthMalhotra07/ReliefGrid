#!/usr/bin/env bash
# Fails if any commit in <base>..<head> carries an AI attribution line: a
# Co-authored-by: trailer, or a line that starts with "Generated with",
# optionally after an emoji or other symbols ("🤖 Generated with ...").
# Case-insensitive. The words inside prose ("regenerated with make gen")
# pass. Tests: scripts/commitlint_test.go.
#   scripts/commit-lint.sh <base> <head>
set -uo pipefail

if [ $# -ne 2 ]; then
	echo "usage: $0 <base> <head>" >&2
	exit 2
fi

# Byte-wise matching: an emoji is a run of non-alphanumeric, non-space bytes.
export LC_ALL=C
trailer='^[[:space:]]*co-authored-by:'
generated='^[[:space:]]*([^[:alnum:][:space:]]+[[:space:]]*)?generated with'

shas="$(git rev-list "$1..$2")" || exit 2
bad=0
for sha in $shas; do
	msg="$(git log -1 --format=%B "$sha")"
	if lines="$(grep -Ei "$trailer|$generated" <<<"$msg")"; then
		echo "commit-lint: $(git log -1 --format='%h %s' "$sha") has an attribution line:"
		sed 's/^/  /' <<<"$lines"
		bad=1
	fi
done

if [ "$bad" -ne 0 ]; then
	echo "commit-lint: reword the commits above and force-push the branch"
	exit 1
fi
echo "commit-lint: $(wc -w <<<"$shas") commit(s) clean"
