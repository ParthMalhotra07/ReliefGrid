#!/usr/bin/env bash
# The course gate (S3.2/S3.3, v3.1 §28): all nine demos, from a clean deployment, twice.
#
#   make gate        (scripts/demos/gate.sh)
#
# Each round first runs the clean-deployment reset (reset.sh: it deletes and recreates the
# kind cluster, so the gate asks once, up front, for the cluster name), then demo-1 to
# demo-9 in order on that deployment. Each demo writes its evidence under
# docs/evidence/<gateId>/round-<r>/; the summary table goes to docs/evidence/<gateId>/gate.txt
# and stdout. Only PASS counts: FAIL, NOT IMPLEMENTED, PENDING, an unknown exit code (ERROR),
# a missing script (MISSING) and a demo skipped after a failed reset (NOT RUN) are all
# failures. Exit 0 only if every demo passed in every round.
#
# No worker may stay cordoned (demos 6 and 7 cordon one to keep a storage pod down). At the
# start the gate restores the demos' own cordons (DEMO_CORDON_LABEL) and refuses to run if any
# other worker is cordoned. After every round it checks again: a cordon left behind is shown in
# the cordons row, fails the gate, and is restored. An interrupt restores them too.
#
# Environment: GATE_ROUNDS (2), GATE_DEMOS ("1 2 3 4 5 6 7 8 9"), GATE_RUN_ID
# (gate-<UTC time>-<host>), GATE_RESET (reset.sh), DEMOS_DIR (this directory), RESET_CONFIRM
# (the cluster name: skips the question), EVIDENCE_ROOT (docs/evidence).
set -uo pipefail
# shellcheck source=scripts/demos/common.sh
. "$(dirname "$0")/common.sh"

DEMOS_DIR="${DEMOS_DIR:-$DEMOS_DIR_SELF}"
GATE_RESET="${GATE_RESET:-$DEMOS_DIR/reset.sh}"
GATE_ROUNDS="${GATE_ROUNDS:-2}"
GATE_DEMOS="${GATE_DEMOS:-1 2 3 4 5 6 7 8 9}"
GATE_RUN_ID="${GATE_RUN_ID:-gate-$(date -u +%Y%m%dT%H%M%SZ)-$(hostname -s)}"

if [ "${RESET_CONFIRM:-}" != "$CLUSTER" ]; then
	echo "gate: every round starts from a clean deployment, which deletes and recreates the kind cluster '$CLUSTER'."
	read -r -p "Type the cluster name to confirm: " RESET_CONFIRM || true
	if [ "${RESET_CONFIRM:-}" != "$CLUSTER" ]; then
		echo "gate: not confirmed; nothing was reset and no demo ran" >&2
		exit 1
	fi
fi
export RESET_CONFIRM

trap 'restore_demo_cordons >/dev/null; echo "gate: interrupted; demo cordons restored" >&2; exit 130' INT
trap 'restore_demo_cordons >/dev/null; echo "gate: interrupted; demo cordons restored" >&2; exit 143' TERM HUP
restore_demo_cordons | sed 's/^/gate: restored a demo cordon on /'
if left="$(cordoned_nodes)" && [ -n "$left" ]; then
	echo "gate: $(paste -sd ' ' <<<"$left") is cordoned before the gate, not by a demo; uncordon it first" >&2
	exit 1
fi

label() { # label <exit code>
	case "$1" in
	0) echo PASS ;;
	1) echo FAIL ;;
	2) echo "NOT IMPLEMENTED" ;;
	3) echo PENDING ;;
	*) echo "ERROR (exit $1)" ;;
	esac
}

declare -A cell # cell[<demo>,<round>] = result label
resets=() dirs=() cordons=()
total=0 passed=0 leaked=""
started="$(utc)"
for ((r = 1; r <= GATE_ROUNDS; r++)); do
	run_id="$GATE_RUN_ID/round-$r"
	dirs+=("$EVIDENCE_ROOT/$run_id/")
	echo "== gate round $r of $GATE_ROUNDS: clean deployment"
	t0="$SECONDS"
	if bash "$GATE_RESET"; then reset_ok=1 resets+=("OK ($((SECONDS - t0)) s)"); else reset_ok="" resets+=("FAILED ($((SECONDS - t0)) s)"); fi
	for n in $GATE_DEMOS; do
		total=$((total + 1))
		script="$DEMOS_DIR/demo-$n.sh"
		if [ -z "$reset_ok" ]; then
			cell[$n,$r]="NOT RUN"
		elif [ ! -f "$script" ]; then
			cell[$n,$r]=MISSING
		else
			echo "== gate round $r: demo-$n"
			DEMO_RUN_ID="$run_id" EVIDENCE_ROOT="$EVIDENCE_ROOT" bash "$script"
			cell[$n,$r]="$(label $?)"
		fi
		[ "${cell[$n,$r]}" = PASS ] && passed=$((passed + 1))
	done
	left="$(cordoned_nodes | paste -sd ' ')"
	if [ -n "$left" ]; then cordons+=("LEAKED: $left") leaked=1; else cordons+=(none); fi
	restore_demo_cordons >/dev/null
done

row() { # row <name> <cells...>: padded columns, no trailing blanks
	local line
	line="$(printf '%-8s' "$1")"
	shift
	while [ $# -gt 1 ]; do line+="$(printf '%-22s' "$1") "; shift; done
	printf '%s%s\n' "$line" "$1"
}
if [ -n "$leaked" ]; then
	result="FAIL ($passed of $total passed; cordons leaked)"
elif [ "$passed" -eq "$total" ]; then
	result="PASS ($passed/$total passed)"
else
	result="FAIL ($passed of $total passed)"
fi
summary="$EVIDENCE_ROOT/$GATE_RUN_ID/gate.txt"
mkdir -p "$(dirname "$summary")"
{
	echo "# gate $GATE_RUN_ID"
	echo "host:       $(hostname -s)"
	echo "started:    $started"
	echo "finished:   $(utc)"
	echo "code tree:  $(code_tree) (git ls-tree -r HEAD -- ${CODE_PATHS[*]} | git hash-object --stdin)"
	echo "code dirty: $(code_dirty)"
	echo
	header=(demo)
	for ((r = 1; r <= GATE_ROUNDS; r++)); do header+=("round $r"); done
	row "${header[@]}"
	for n in $GATE_DEMOS; do
		cells=()
		for ((r = 1; r <= GATE_ROUNDS; r++)); do cells+=("${cell[$n,$r]}"); done
		row "demo-$n" "${cells[@]}"
	done
	row reset "${resets[@]}"
	row cordons "${cordons[@]}"
	echo
	echo "result: $result"
	echo "evidence: ${dirs[*]}"
} | tee "$summary"
[ "$passed" -eq "$total" ] && [ -z "$leaked" ]
