#!/usr/bin/env bash
# scripts/verify-A0.2.sh
# Verify: go vet ./... passes; every §27 mandatory adversarial schedule appears
# exactly once in tests/workflow/fixtures.go.
# Exit 0 = all pass, non-zero = failure.

set -euo pipefail
REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"

# Source Go env if available (sets PATH for snap go)
if [ -f "$REPO_ROOT/scripts/_go-env.sh" ]; then
  # shellcheck source=scripts/_go-env.sh
  source "$REPO_ROOT/scripts/_go-env.sh"
fi

PASS=0
FAIL=0

ok()   { echo "ok        $*"; PASS=$((PASS+1)); }
fail() { echo "FAIL      $*"; FAIL=$((FAIL+1)); }

echo "=== A0.2 Verify: Workflow fixtures ==="
echo ""

# ---- 1. go vet ----
if go vet ./... 2>&1; then
  ok "go vet ./..."
else
  fail "go vet ./..."
fi

# ---- 2. fixtures file exists ----
FIXTURES="$REPO_ROOT/tests/workflow/fixtures.go"
if [ -f "$FIXTURES" ]; then
  ok "tests/workflow/fixtures.go exists"
else
  fail "tests/workflow/fixtures.go missing"
fi

SCHEDULES_MD="$REPO_ROOT/tests/workflow/schedules.md"
if [ -f "$SCHEDULES_MD" ]; then
  ok "tests/workflow/schedules.md exists"
else
  fail "tests/workflow/schedules.md missing"
fi

# ---- 3. Each §27 mandatory schedule appears exactly once in fixtures.go ----
# The fixture names must match these canonical tags exactly.
# Each tag corresponds to one mandatory adversarial schedule from §27.
declare -a REQUIRED_TAGS=(
  "SCH01_BothCASQueuedBeforeEitherApplies"
  "SCH02_DroppedResponseAfterCommit"
  "SCH03_CrashBeforeMatchingStep"
  "SCH04_CrashAfterMatchingStep"
  "SCH05_CancelBeforeTryClaim"
  "SCH06_CancelAfterTryClaim"
  "SCH07_StaleTryClaimAfterCloseAttempt"
  "SCH08_ConfirmationRacingWithRetarget"
  "SCH09_StaleOperationResultReplayAfterClose"
  "SCH10_DuplicateTerminalEvents"
  "SCH11_RepeatedCancelFulfillCalls"
  "SCH12_RestartDuringSnapshotCreation"
  "SCH13_RestartDuringSnapshotRestore"
  "SCH14_CoordinatorRestartWithShardUnavailable"
  "SCH15_BrokerOutageDuringRetryScheduling"
  "SCH16_PoisonMessages"
  "SCH17_PartialCandidateScan"
)

echo ""
echo "--- §27 schedule coverage (fixtures.go) ---"

ALL_TAGS_FOUND=true
if [ -f "$FIXTURES" ]; then
  for tag in "${REQUIRED_TAGS[@]}"; do
    count=$(grep -c "$tag" "$FIXTURES" || true)
    if [ "$count" -eq 1 ]; then
      ok "$tag: found exactly once"
    elif [ "$count" -eq 0 ]; then
      fail "$tag: NOT FOUND in fixtures.go"
      ALL_TAGS_FOUND=false
    else
      fail "$tag: found $count times (expected exactly 1)"
      ALL_TAGS_FOUND=false
    fi
  done
else
  for tag in "${REQUIRED_TAGS[@]}"; do
    fail "$tag: fixtures.go missing, cannot check"
  done
  ALL_TAGS_FOUND=false
fi

echo ""
echo "=== Results: $PASS passed, $FAIL failed ==="
if [ "$FAIL" -eq 0 ]; then
  echo "A0.2 VERIFY: ALL PASS"
  exit 0
else
  echo "A0.2 VERIFY: $FAIL FAILURE(S)"
  exit 1
fi
