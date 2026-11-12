#!/usr/bin/env bash
set -euo pipefail
root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
target="$1"
if [ -n "${DEMO_OUTBOX_DRAIN_BINARY:-}" ]; then
 [ -f "$DEMO_OUTBOX_DRAIN_BINARY" ] && [ -x "$DEMO_OUTBOX_DRAIN_BINARY" ] || { echo 'prebuilt drainer missing or not executable' >&2;exit 1; }
 cp -- "$DEMO_OUTBOX_DRAIN_BINARY" "$target"
else
 go build -o "$target" "$root/scripts/demos/drain-outbox"
fi
