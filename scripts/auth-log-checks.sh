#!/usr/bin/env bash
# auth-log-checks.sh: the pod-log checks of the internal-auth gate (#29), failing closed (#183).
#
#   auth-log-checks.sh no-token            --summary F [opts] <token file>...
#       PASS (exit 0): every listed pod's logs were read and none contains any token.
#       FAIL (exit 1): a token appears in a pod's logs (the pod is named, the token never).
#   auth-log-checks.sh refusals            --summary F [opts] --since T --out O
#       PASS (exit 0): every listed pod's logs since T were read; O holds each
#       "internal auth refused" line as "<pod> <line>" (possibly none).
#   auth-log-checks.sh in-cluster-refusals --summary F [opts] [--since T] [--out O]
#       As refusals (without --since, over the pods' whole current logs), keeping the
#       refusals whose peer is not loopback (a port-forward from this host).
#       PASS (exit 0): none. FAIL (exit 1): some, written to O.
#
#   Any collection failure is INCOMPLETE (exit 2), never PASS: the pod list cannot be read,
#   is empty, has an unexpected name or count (--expect-pods), a pod's logs cannot be read
#   (RBAC, missing pod, timeout), a token file is missing, unreadable, empty or has a blank
#   line, or grep itself fails (exit >1). A token found anywhere is FAIL even if other
#   reads failed. For refusals, O exists only after a complete collection; partial lines
#   go to O.partial. Usage errors exit 64.
#
#   Options: --selector S (label selector; default: every pod for no-token, the
#   coordinator and storage pods for the refusal checks), --expect-pods N (exact count),
#   --since T (RFC3339, passed as kubectl logs --since-time).
#   Environment: KUBE_CONTEXT and NAMESPACE (required, never the current context),
#   AUTH_LOG_KUBECTL (default kubectl), AUTH_LOG_TIMEOUT (seconds per kubectl call, default 30).
#
# Every run writes a collection-completeness summary (key=value lines) to F and stderr:
# check, result, pods listed and expected, logs read, log failures and each failure.
# Logs are held in a 0700 temp dir only while scanned. Diagnostics name a pod and the kind
# of failure; they never include log content, kubectl stderr or token bytes.
set -uo pipefail

usage() {
	sed -n '2,/^set /{/^#/s/^# \{0,1\}//p}' "$0" >&2
	exit 64
}

check="${1:-}"
case "$check" in no-token | refusals | in-cluster-refusals) shift ;; *) usage ;; esac
summary="" selector="" expect="" since="" out=""
selector_set=false
while [ $# -gt 0 ]; do
	case "$1" in
	--summary) summary="${2:-}" ;;
	--selector) selector="${2:-}" selector_set=true ;;
	--expect-pods) expect="${2:-}" ;;
	--since) since="${2:-}" ;;
	--out) out="${2:-}" ;;
	--) shift; break ;;
	-*) usage ;;
	*) break ;;
	esac
	[ $# -ge 2 ] || usage
	shift 2
done
[ -n "$summary" ] || usage
[ -z "$expect" ] || [[ "$expect" =~ ^[1-9][0-9]*$ ]] || usage
if [ "$check" = no-token ]; then
	[ $# -gt 0 ] || usage
else
	[ $# -eq 0 ] || usage
	[ "$check" = in-cluster-refusals ] || { [ -n "$since" ] && [ -n "$out" ]; } || usage
	$selector_set || selector='app.kubernetes.io/component in (coordinator,storagenode)'
fi
tokens=("$@")

kubectl_bin="${AUTH_LOG_KUBECTL:-kubectl}"
limit="${AUTH_LOG_TIMEOUT:-30}"
[[ "$limit" =~ ^[1-9][0-9]*$ ]] || usage

pods_listed=0 logs_read=0 log_failures=0 found_in=0
failures=()
fail() { failures+=("$1"); }

write_summary() { # write_summary <result>
	{
		printf 'check=%s\nresult=%s\n' "$check" "$1"
		printf 'context=%s\nnamespace=%s\n' "${KUBE_CONTEXT:-}" "${NAMESPACE:-}"
		printf 'selector=%s\n' "${selector:-(all pods)}"
		printf 'since=%s\n' "${since:-(all)}"
		printf 'pods_expected=%s\npods_listed=%s\n' "${expect:-any}" "$pods_listed"
		printf 'logs_read=%s\nlog_failures=%s\n' "$logs_read" "$log_failures"
		[ "$check" != no-token ] || printf 'token_files=%s\ntokens_found_in=%s\n' "${#tokens[@]}" "$found_in"
		local f
		for f in ${failures[@]+"${failures[@]}"}; do printf 'failure=%s\n' "$f"; done
	} > "$summary.tmp" && mv "$summary.tmp" "$summary" || {
		printf 'INCOMPLETE  %s: cannot write the summary %s\n' "$check" "$summary"
		exit 2
	}
	cat "$summary" >&2
}

finish() { # finish <result> <message>
	write_summary "$1"
	printf '%s  %s: %s\n' "$1" "$check" "$2"
	case "$1" in PASS) exit 0 ;; FAIL) exit 1 ;; *) exit 2 ;; esac
}

# The kind of a failed kubectl call, from its exit status and stderr; stderr is not echoed.
why() { # why <exit status> <stderr file>
	if [ "$1" -eq 124 ] || [ "$1" -eq 137 ]; then
		echo "timed out after ${limit}s"
	elif grep -qi 'forbidden' "$2" 2>/dev/null; then
		echo "forbidden (kubectl exit $1)"
	elif grep -qi 'notfound\|not found' "$2" 2>/dev/null; then
		echo "not found (kubectl exit $1)"
	else
		echo "failed (kubectl exit $1)"
	fi
}

if [ -z "${KUBE_CONTEXT:-}" ] || [ -z "${NAMESPACE:-}" ]; then
	fail "KUBE_CONTEXT and NAMESPACE must be set"
	finish INCOMPLETE "no kube context or namespace"
fi

# Credential inputs first: no scan starts with a token file that cannot match.
for f in ${tokens[@]+"${tokens[@]}"}; do
	if [ ! -f "$f" ] || [ ! -r "$f" ]; then
		fail "token file $f: missing or unreadable"
	elif [ ! -s "$f" ]; then
		fail "token file $f: empty"
	elif grep -q -x -e '' "$f"; then
		# An empty pattern matches every line, so the scan would mean nothing.
		fail "token file $f: has a blank line"
	fi
done
[ "${#failures[@]}" -eq 0 ] || finish INCOMPLETE "credential inputs are not usable; no log was scanned"

work="$(mktemp -d)" || { fail "cannot create a temp dir"; finish INCOMPLETE "no temp dir"; }
chmod 700 "$work"
trap 'rm -rf "$work"' EXIT
k=("$kubectl_bin" --context "$KUBE_CONTEXT" -n "$NAMESPACE" --request-timeout="${limit}s")

# Pod inventory: the list call must succeed, and yield valid, distinct names.
list=(get pods -o 'jsonpath={range .items[*]}{.metadata.name}{"\n"}{end}')
[ -z "$selector" ] || list+=(-l "$selector")
timeout -k 5 "$limit" "${k[@]}" "${list[@]}" > "$work/pods" 2> "$work/err"
rc=$?
if [ "$rc" -ne 0 ]; then
	fail "pod list: $(why "$rc" "$work/err")"
	finish INCOMPLETE "the pod list could not be read"
fi
pods=()
while IFS= read -r p || [ -n "$p" ]; do
	[ -n "$p" ] || continue
	if [[ ! "$p" =~ ^[a-z0-9]([-a-z0-9.]{0,251}[a-z0-9])?$ ]]; then
		fail "pod list: an entry is not a pod name"
		finish INCOMPLETE "unexpected pod inventory"
	fi
	pods+=("$p")
done < "$work/pods"
pods_listed=${#pods[@]}
if [ "$pods_listed" -eq 0 ]; then
	fail "pod list: no pods"
	finish INCOMPLETE "the pod list is empty"
fi
if [ "$(printf '%s\n' "${pods[@]}" | sort -u | wc -l)" -ne "$pods_listed" ]; then
	fail "pod list: duplicate names"
	finish INCOMPLETE "unexpected pod inventory"
fi
if [ -n "$expect" ] && [ "$pods_listed" -ne "$expect" ]; then
	fail "pod list: $pods_listed pods, expected $expect"
	finish INCOMPLETE "unexpected pod inventory"
fi

# Per-pod reads: each one must succeed on its own; grep's status is judged separately.
logs=(logs --all-containers)
[ -z "$since" ] || logs+=(--since-time="$since")
lines="$work/refusals"
: > "$lines"
for p in "${pods[@]}"; do
	timeout -k 5 "$limit" "${k[@]}" "${logs[@]}" "$p" > "$work/log" 2> "$work/err"
	rc=$?
	if [ "$rc" -ne 0 ]; then
		log_failures=$((log_failures + 1))
		fail "pod $p: logs $(why "$rc" "$work/err")"
		continue
	fi
	logs_read=$((logs_read + 1))
	if [ "$check" = no-token ]; then
		for f in "${tokens[@]}"; do
			grep -a -F -f "$f" "$work/log" > /dev/null
			rc=$?
			case "$rc" in
			0) found_in=$((found_in + 1)); fail "pod $p: a token from $f appears in its logs"; break ;;
			1) ;;
			*) fail "pod $p: token scan failed (grep exit $rc)" ;;
			esac
		done
	else
		grep -a -F 'internal auth refused' "$work/log" > "$work/hit"
		rc=$?
		case "$rc" in
		0) awk -v p="$p" '{ print p " " $0 }' "$work/hit" >> "$lines" || fail "pod $p: cannot record refusal lines" ;;
		1) ;;
		*) fail "pod $p: refusal scan failed (grep exit $rc)" ;;
		esac
	fi
	: > "$work/log"
done

if [ "$check" = no-token ]; then
	[ "$found_in" -eq 0 ] || finish FAIL "a token appears in the logs of $found_in pod(s); logs withheld"
	[ "${#failures[@]}" -eq 0 ] || finish INCOMPLETE "$logs_read of $pods_listed pod logs scanned; not proof that no token is logged"
	finish PASS "no token in the logs of all $pods_listed pods"
fi

if [ "${#failures[@]}" -ne 0 ]; then
	[ -z "$out" ] || { rm -f "$out"; cp "$lines" "$out.partial" 2> /dev/null; }
	finish INCOMPLETE "$logs_read of $pods_listed pod logs read; refusals unknown"
fi
total=$(wc -l < "$lines")
if [ "$check" = refusals ]; then
	cp "$lines" "$out" || { fail "cannot write $out"; finish INCOMPLETE "refusal lines not recorded"; }
	finish PASS "$total refusal line(s) from all $pods_listed pods"
fi
grep -a -v -E 'peer=("?)(127\.0\.0\.1|\[::1\]):|"peer":"(127\.0\.0\.1|\[::1\]):' "$lines" > "$work/in-cluster"
rc=$?
case "$rc" in
0) n=$(wc -l < "$work/in-cluster")
	[ -z "$out" ] || cp "$work/in-cluster" "$out" || fail "cannot write $out"
	finish FAIL "$n in-cluster refusal(s) among $total from all $pods_listed pods" ;;
1) [ -z "$out" ] || : > "$out" || { fail "cannot write $out"; finish INCOMPLETE "cannot write $out"; }
	finish PASS "no in-cluster refusal among $total from all $pods_listed pods" ;;
*) fail "in-cluster filter failed (grep exit $rc)"
	finish INCOMPLETE "in-cluster refusals unknown" ;;
esac
