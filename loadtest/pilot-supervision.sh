# Shared by pilot.sh and the harmless process integration tests.
# Caller supplies ROOT and OUT. There is no cluster/host operation in this file.
supervisor_pid=""
supervisor_evidence=""
interrupt_pilot() {
  local number="$1"
  trap '' INT TERM HUP
  if [ -n "$supervisor_pid" ]; then
    kill -s "$number" "$supervisor_pid" 2>/dev/null || true
    wait "$supervisor_pid" || true
  fi
  echo "SCOPE_EXIT_EVIDENCE: $supervisor_evidence/result.json" >&2
  if [ -z "$supervisor_evidence" ] || ! jq -e '.exitConfirmed == true' "$supervisor_evidence/result.json" >/dev/null 2>&1; then
      echo "EXIT_UNCONFIRMED; RESTORATION_UNVERIFIED: preserve cluster and evidence in $OUT" >&2
      exit 7
  fi
  echo "CONFIRMED_SCOPE_EXIT: $supervisor_evidence/result.json" >&2
  if ! declare -F pilot_capture_restoration >/dev/null; then
    echo "RESTORATION_UNVERIFIED: no host snapshot callback; preserve evidence in $OUT" >&2
    exit 8
  fi
  local restoration_rc=0
  pilot_capture_restoration || restoration_rc=$?
  if [ "$restoration_rc" != 0 ]; then exit "$restoration_rc"; fi
  echo "INTERRUPTED: scope exited and host restoration checked; preserve cluster/evidence in $OUT" >&2
  exit "$((128 + number))"
}
trap 'interrupt_pilot 2' INT
trap 'interrupt_pilot 15' TERM
trap 'interrupt_pilot 1' HUP

supervise() {
  local stage="$1" rc
  shift
  supervisor_evidence="$OUT/$stage.supervisor"
  RUN_DIR="$OUT/$stage" A15_EVIDENCE_DIR="$OUT/deploy-a15" \
    python3 -I "$ROOT/loadtest/supervise.py" --evidence-dir "$supervisor_evidence" -- "$@" &
  supervisor_pid=$!
  rc=0
  wait "$supervisor_pid" || rc=$?
  supervisor_pid=""
  if ! jq -e '.exitConfirmed == true' "$supervisor_evidence/result.json" >/dev/null; then
    echo "EXIT_UNCONFIRMED; RESTORATION_UNVERIFIED: preserve cluster/evidence in $OUT" >&2
    return 7
  fi
  service_rc="$(jq -r .supervisorExit "$OUT/$stage.supervisor/result.json")"
  test "$rc" = "$service_rc" || return 7
}
