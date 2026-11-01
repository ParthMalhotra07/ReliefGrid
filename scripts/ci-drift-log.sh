#!/usr/bin/env bash
# ci-drift-log.sh <logs.zip>: did the CI check job's contract-drift step (make gen-check) fail
# on internal/internalapi/types.gen.go? <logs.zip> is a run's log archive
# (gh api repos/<repo>/actions/runs/<id>/logs). Reads both archive layouts GitHub has used:
# one file per job (2_check.txt, current) and one file per step (check/5_<step>.txt, former).
# Only the "Run make gen-check" step counts: it must show the types.gen.go diff and end in an
# error. Prints the drift and exits 0; exits 1 when the archive doesn't show it, 2 when the
# archive can't be read. Used by verify-S4.6.sh (S4.6).
set -uo pipefail
[ $# -eq 1 ] || { echo "usage: $0 <logs.zip>" >&2; exit 2; }
exec python3 -I - "$1" <<'PY'
import re, sys, zipfile
try:
    z = zipfile.ZipFile(sys.argv[1])
except (OSError, zipfile.BadZipFile) as e:
    print(f"ci-drift-log: cannot read {sys.argv[1]}: {e}", file=sys.stderr)
    sys.exit(2)
names = z.namelist()
job = [n for n in names if re.fullmatch(r"\d+_check\.txt", n)]
if not job:
    steps = [(int(m.group(1)), n) for n in names if (m := re.fullmatch(r"check/(\d+)_.*\.txt", n))]
    job = [n for _, n in sorted(steps)]
if not job:
    print("ci-drift-log: no check job log in the archive", file=sys.stderr)
    sys.exit(1)
lines = []
for n in job:
    for line in z.read(n).decode("utf-8", "replace").splitlines():
        lines.append(re.sub(r"^﻿?\d{4}-\d\d-\d\dT[0-9:.]+Z ", "", line))
step, found = None, None
for i, line in enumerate(lines):
    if line.startswith("##[group]Run "):
        step = line[len("##[group]Run "):].strip()
        continue
    if step != "make gen-check":
        continue
    if line.startswith("diff --git a/internal/internalapi/types.gen.go "):
        found = i
    elif line.startswith("##[error]") and found is not None:
        print("\n".join(lines[found:found + 13]))
        sys.exit(0)
print("ci-drift-log: the make gen-check step did not fail on internal/internalapi/types.gen.go", file=sys.stderr)
sys.exit(1)
PY
