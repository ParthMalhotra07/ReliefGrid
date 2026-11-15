# The helpers no_token_in_logs and quiet_grep, extracted unchanged from #181 head
# 68265a2dfeeb8183e207f4e1061fc7d0f69e2e26,
# docs/evidence/S-29-auth-enable/official-20261114/stages/lib.sh. Kept only as the
# #183 reproducer: a failed kubectl logs on the left of && is not caught, so unread
# logs are reported as token-free. Do not use; scripts/auth-log-checks.sh replaces it.
quiet_grep() { grep "$@" >/dev/null; }
# no_token_in_logs <token file>...: no pod log of the release contains any of the tokens.
no_token_in_logs() {
	local p f
	for p in $($K get pods -o jsonpath='{.items[*].metadata.name}'); do
		for f in "$@"; do
			$K logs "$p" --all-containers 2>/dev/null | quiet_grep -F -f "$f" && { echo "a token appears in the logs of $p"; return 1; }
		done
	done
	return 0
}
