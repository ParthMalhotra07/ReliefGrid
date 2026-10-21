#!/usr/bin/env bash
# Verify script for goal A0.1 — Public contract.
# Must be run from the repo root.
# Usage: bash scripts/verify-A0.1.sh
# All checks must pass (exit 0) before the goal is complete.
# Prerequisites: go 1.27.x, python3 with openapi-spec-validator and jsonschema
set -uo pipefail

REPO_ROOT="$(git rev-parse --show-toplevel)"
cd "$REPO_ROOT"

PASS=0
FAIL=0
ERRORS=()

ok()   { printf 'ok        %s\n' "$1"; PASS=$((PASS+1)); }
fail() { printf 'FAIL      %s\n' "$1"; ERRORS+=("$1"); FAIL=$((FAIL+1)); }

echo "=== A0.1 Verify: Public contract ==="
echo ""

# ── Check 1: contracts/public.yaml exists ─────────────────────────────────────
if [ -f "contracts/public.yaml" ]; then
  ok "contracts/public.yaml exists"
else
  fail "contracts/public.yaml MISSING"
fi

# ── Check 2: OpenAPI spec is valid (python openapi-spec-validator) ─────────────
if python3 -c "import openapi_spec_validator" 2>/dev/null; then
  if [ -f "contracts/public.yaml" ]; then
    lint_out=$(python3 -m openapi_spec_validator contracts/public.yaml 2>&1)
    lint_rc=$?
    if [ $lint_rc -eq 0 ]; then
      ok "openapi-spec-validator: 0 errors"
    else
      echo "$lint_out" | head -20
      fail "openapi-spec-validator: spec invalid"
    fi
  else
    fail "openapi-spec-validator: skipped (public.yaml missing)"
  fi
else
  fail "openapi-spec-validator not installed (pip3 install --break-system-packages openapi-spec-validator)"
fi

# ── Check 3: all five event schemas exist ─────────────────────────────────────
EXPECTED_EVENTS=(
  "contracts/events/request_created.json"
  "contracts/events/resource_registered.json"
  "contracts/events/match_proposed.json"
  "contracts/events/request_state_changed.json"
  "contracts/events/resource_state_changed.json"
)
for f in "${EXPECTED_EVENTS[@]}"; do
  if [ -f "$f" ]; then
    ok "$f exists"
  else
    fail "$f MISSING"
  fi
done

# ── Check 4: every event schema example validates against its own schema ───────
if python3 -c "import jsonschema" 2>/dev/null; then
  for f in contracts/events/*.json; do
    [ -f "$f" ] || continue
    result=$(python3 - "$f" <<'PYEOF'
import sys, json, jsonschema
path = sys.argv[1]
with open(path) as fh:
    schema = json.load(fh)
examples = schema.get("examples", [])
if not examples:
    print("no-example")
    sys.exit(0)
errs = []
for ex in examples:
    try:
        jsonschema.validate(instance=ex, schema=schema)
    except jsonschema.ValidationError as e:
        errs.append(str(e.message))
if errs:
    print("ERRORS: " + "; ".join(errs))
    sys.exit(1)
print("ok")
PYEOF
)
    rc=$?
    case "$result" in
      ok|no-example) ok "$f: example(s) validate" ;;
      *) fail "$f: example validation failed — $result"; ;;
    esac
  done
else
  fail "jsonschema not installed (pip3 install --break-system-packages jsonschema)"
fi

# ── Check 5: make gen runs and leaves git status clean ────────────────────────
gen_out=$(make gen 2>&1)
gen_rc=$?
echo "$gen_out"
if [ $gen_rc -eq 0 ]; then
  dirty=$(git status --short -- internal/api/gen/ contracts/)
  if [ -z "$dirty" ]; then
    ok "make gen: git status clean after gen"
  else
    echo "Dirty files after make gen:"
    echo "$dirty"
    fail "make gen: git status NOT clean"
  fi
else
  fail "make gen: command failed (rc=$gen_rc)"
fi

# ── Check 6: generated code compiles ──────────────────────────────────────────
if [ -d "internal/api/gen" ] && ls internal/api/gen/*.go >/dev/null 2>&1; then
  build_out=$(go build ./internal/api/gen/... 2>&1)
  build_rc=$?
  if [ $build_rc -eq 0 ]; then
    ok "go build ./internal/api/gen/... compiles"
  else
    echo "$build_out"
    fail "go build ./internal/api/gen/...: compile error"
  fi
else
  fail "internal/api/gen/*.go not generated"
fi

# ── Summary ───────────────────────────────────────────────────────────────────
echo ""
echo "=== Results: $PASS passed, $FAIL failed ==="
if [ ${#ERRORS[@]} -gt 0 ]; then
  echo "Failed checks:"
  for e in "${ERRORS[@]}"; do echo "  - $e"; done
  exit 1
fi
echo "A0.1 VERIFY: ALL PASS"
exit 0
