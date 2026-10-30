# shellcheck shell=bash
# The demo gate (S3.2, v3.1 §28). Every scripts/demos/demo-N.sh sources this file:
#
#   . "$(dirname "$0")/lib.sh"
#   demo_begin 4 "Seed a deterministic dataset; logical counts and locate samples"
#   demo_reset "what state the demo starts from and how it gets there (seed, keys, reset)"
#   run "<what this shows>" <command...>     # recorded in the evidence; returns its status
#   demo_show "<title>" "<text>"             # a block of observed output in the evidence
#   check "<assertion>" <command...>         # PASS if the command exits 0
#   pass "<assertion>" / fail "<assertion>" [detail]
#   demo_end                                 # or demo_not_implemented / demo_pending <why>
#
# Every demo ends with exactly one result, printed and written to its evidence file
# $EVIDENCE_ROOT/$DEMO_RUN_ID/demo-N.txt (default docs/evidence/<UTC time>-<host>/), with
# the code-tree fingerprint, tool and image versions, timestamps, every check and every
# command's output. Exit code and result:
#
#   0 PASS             at least one check, none failed
#   1 FAIL             a check failed, no check ran, or the script exited before demo_end
#   2 NOT IMPLEMENTED  the demo has no body yet
#   3 PENDING          the demo is written but what it exercises is not deployed yet
#
# The gate (gate.sh) counts everything but PASS as a failure.
#
# demo_on_exit <command...> registers cleanup (run last-registered first, before the
# evidence is written, on every exit path).

# shellcheck source=scripts/demos/common.sh
. "$(dirname "${BASH_SOURCE[0]}")/common.sh"
set -uo pipefail

readonly DEMO_EXIT_PASS=0 DEMO_EXIT_FAIL=1 DEMO_EXIT_NOT_IMPLEMENTED=2 DEMO_EXIT_PENDING=3

demo_begin() {
	DEMO_N="$1" DEMO_TITLE="$2"
	DEMO_RUN_ID="${DEMO_RUN_ID:-$(date -u +%Y%m%dT%H%M%SZ)-$(hostname -s)}"
	DEMO_EVIDENCE="$EVIDENCE_ROOT/$DEMO_RUN_ID/demo-$DEMO_N.txt"
	DEMO_TMP="$(mktemp -d)"
	DEMO_RESET_TEXT="(not documented)"
	DEMO_STARTED="$(utc)" DEMO_T0="$(date +%s.%N)"
	DEMO_PASSES=0 DEMO_FAILURES=0 DEMO_FINISHED=""
	DEMO_CLEANUPS=()
	: >"$DEMO_TMP/checks"
	: >"$DEMO_TMP/commands"
	trap '_demo_exit $?' EXIT
	# An interrupt still runs the cleanups and writes the evidence (as a FAIL).
	trap 'exit 130' INT
	trap 'exit 143' TERM HUP
	printf '== demo-%s: %s (run %s)\n' "$DEMO_N" "$DEMO_TITLE" "$DEMO_RUN_ID"
}

demo_reset() { DEMO_RESET_TEXT="$1"; printf 'seed/reset: %s\n' "$1"; }
demo_on_exit() { DEMO_CLEANUPS+=("$(printf '%q ' "$@")"); }
demo_note() { printf '%s\n' "$*" | tee -a "$DEMO_TMP/commands"; } # a line in the command log
demo_show() { printf '\n# %s\n%s\n' "$1" "$2" | tee -a "$DEMO_TMP/commands"; } # demo_show <title> <text>: a block of observed output

pass() {
	printf 'PASS  %s\n' "$1" | tee -a "$DEMO_TMP/checks"
	DEMO_PASSES=$((DEMO_PASSES + 1))
}
fail() { # fail <assertion> [detail]: a multi-line detail is indented line by line
	{
		printf 'FAIL  %s\n' "$1"
		[ -n "${2:-}" ] && sed 's/^/      /' <<<"$2"
	} | tee -a "$DEMO_TMP/checks"
	DEMO_FAILURES=$((DEMO_FAILURES + 1))
}
check() { # check <assertion> <command...>
	local desc="$1"
	shift
	if "$@" >/dev/null 2>&1; then pass "$desc"; else fail "$desc"; fi
}

# run <description> <command...>: run it, record "$ command", its output, exit status and
# duration in the evidence, echo the output, and return its status
run() {
	local desc="$1" rc t0
	shift
	t0="$(date +%s.%N)"
	"$@" >"$DEMO_TMP/run.out" 2>&1
	rc=$?
	{
		printf '\n# %s\n$ %s\n' "$desc" "$*"
		cat "$DEMO_TMP/run.out"
		printf '[exit %s, %s s]\n' "$rc" "$(_secs "$t0")"
	} >>"$DEMO_TMP/commands"
	cat "$DEMO_TMP/run.out"
	return "$rc"
}

demo_end() {
	if [ "$DEMO_FAILURES" -gt 0 ]; then
		_demo_finish "FAIL ($DEMO_FAILURES of $((DEMO_PASSES + DEMO_FAILURES)) checks failed)" "$DEMO_EXIT_FAIL"
	elif [ "$DEMO_PASSES" -eq 0 ]; then
		_demo_finish "FAIL (no checks ran)" "$DEMO_EXIT_FAIL"
	fi
	_demo_finish "PASS ($DEMO_PASSES/$DEMO_PASSES checks)" "$DEMO_EXIT_PASS"
}
demo_not_implemented() { _demo_finish "NOT IMPLEMENTED ($1)" "$DEMO_EXIT_NOT_IMPLEMENTED"; }
demo_pending() { _demo_finish "PENDING ($1)" "$DEMO_EXIT_PENDING"; }

_secs() { awk -v a="$1" -v b="$(date +%s.%N)" 'BEGIN { printf "%.1f", b - a }'; }

_demo_exit() {
	[ -n "$DEMO_FINISHED" ] && return
	_demo_finish "FAIL (exited with status $1 before demo_end)" "$DEMO_EXIT_FAIL"
}

# _demo_finish <result> <exit code>: run the cleanups, write the evidence, print the result
# and exit
_demo_finish() {
	local result="$1" code="$2" i
	DEMO_FINISHED=1
	for ((i = ${#DEMO_CLEANUPS[@]} - 1; i >= 0; i--)); do
		eval "${DEMO_CLEANUPS[$i]}" >>"$DEMO_TMP/commands" 2>&1
	done
	mkdir -p "$(dirname "$DEMO_EVIDENCE")"
	{
		echo "# demo-$DEMO_N: $DEMO_TITLE"
		echo "result:     $result"
		echo "runId:      $DEMO_RUN_ID"
		echo "host:       $(hostname -s)"
		echo "started:    $DEMO_STARTED"
		echo "finished:   $(utc)"
		echo "duration:   $(_secs "$DEMO_T0") s"
		echo "code tree:  $(code_tree) (git ls-tree -r HEAD -- ${CODE_PATHS[*]} | git hash-object --stdin)"
		echo "code dirty: $(code_dirty) (uncommitted changes under those paths)"
		echo "seed/reset: $DEMO_RESET_TEXT"
		echo
		echo "## versions"
		versions
		echo
		echo "## checks"
		cat "$DEMO_TMP/checks"
		echo
		echo "## commands"
		cat "$DEMO_TMP/commands"
	} 2>&1 | sed 's/[[:space:]]*$//' >"$DEMO_EVIDENCE"
	rm -rf "$DEMO_TMP"
	printf '== demo-%s: %s\nevidence: %s\n' "$DEMO_N" "$result" "$DEMO_EVIDENCE"
	exit "$code"
}
