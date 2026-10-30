#!/usr/bin/env bash
# Public contract, real-FSM idempotency/fault cases and process HTTP wiring.
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"
echo '=== A1.1 Verify: public API ==='
events="$(mktemp)"
trap 'rm -f "$events"' EXIT
A11_EVENTS_FILE="$events" go test -race -count=1 -v ./cmd/api/... ./internal/api/...
python3 - "$events" <<'PY'
import json
import pathlib
import sys

import jsonschema

rows = json.loads(pathlib.Path(sys.argv[1]).read_text())
if len(rows) != 4:
    raise SystemExit("expected four actual API outbox events")
for row in rows:
    name = row["eventType"].replace(".", "_") + ".json"
    schema = json.loads((pathlib.Path("contracts/events") / name).read_text())
    validator = jsonschema.Draft202012Validator(schema, format_checker=jsonschema.FormatChecker())
    validator.validate(row["payload"])
    print("ok    actual API event validates:", row["eventType"])
PY
echo 'A1.1 VERIFY: ALL PASS'
