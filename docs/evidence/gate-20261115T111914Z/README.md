# Final gate, attempt 1 — stopped at the replacement check (15 November 2026) [S4.7]

Gate SHA `c8b2b609d384d84a80e85d85aee9f6946eae9380` (tree `d35c3e2e5747`), host `hrishabh-sharrma-ThinkPad-E14-Gen-7`. Run 11:19:15Z, stopped at 11:43:15Z. Scope: enhanced gate (scoped), internal auth on (`GATE_AUTH=true`), deployed aging `0s`, autoscaling off (fixed API 2 / matcher 2).

**Result: FAILED, stopped at the first failure.**
- Preflight passed, and both course rounds passed (`PASS (18/18 passed)`, resets 104 s / 97 s).
- `verify-S4.2` then aborted at `FAIL  control:  reaches storage-shard-1-2:7000 and :8080 before the wipe` (15 of 16 checks passed). Nothing after it ran.
- Read-only diagnosis on the kept cluster: a harness race, not a product defect. The check deleted the shard leader, counted the still-terminating old pod as restarted, and read the leader once inside the roughly 3.7 s election window.
- The fix is #185 (merged as `636b265`). The next attempt is `gate-20261115T175612Z`.

| File | What it is |
|---|---|
| `report.md` | Full report: per-section results, the failure, retained identities, read-only diagnosis with timeline, verdict and proposed fix, and the image ID note |
| `gate.txt` | Course gate summary, `PASS (18/18 passed)` |
| `round-1/`, `round-2/` | Per-demo reports and app-demo artifacts (large files omitted; see `omitted-sha256.txt`) |
| `run-notes.md` | Source identity, per-reset core/wrapper times, reset wrapper sha256, the stop, teardown |
| `source-commit.txt`, `source-tree.txt`, `gate-auth.txt` | Gate SHA, tree, and the auth profile (`true`) |
| `replacement/admission-replace-*.txt` | The failed `verify-S4.2` evidence: `FAILED (1 of 16 checks failed)` |
| `pod-images/{after-course,at-stop}/pod-images.tsv` | Every container's observed image ID, all namespaces |
| `stop-observations/` | Pods, Jobs and the nine shard statuses after the failure (events omitted; see `omitted-sha256.txt`) |
| `omitted-sha256.txt` | sha256 of the files over 100 KB kept only in the run root |

Not produced: the #157 fixed-profile file and the `auth-log-checks.sh` completeness summaries (their sections were not reached). This run certifies nothing beyond the results above.

`gate.txt` and `run-notes.md` are kept byte-for-byte as the run wrote them, so their paths point at the original run root (`~/.cache/reliefgrid/gate-20261115T111914Z/`), which keeps the console logs and the omitted files.
