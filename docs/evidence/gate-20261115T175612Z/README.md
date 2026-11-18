# Final gate, attempt 2 — stopped at the metrics fault proof (15 November 2026) [S4.7]

Gate SHA `636b265084fc2f855a0d3b17ec8ca6634209bef2` (tree `fa967e0db5c9`; #185 merged), host `hrishabh-sharrma-ThinkPad-E14-Gen-7`. Run 17:56:13Z, stopped at 19:17:22Z. Scope: enhanced gate (scoped), internal auth on (`GATE_AUTH=true`), deployed aging `0s`, autoscaling off (fixed API 2 / matcher 2).

**Result: FAILED, stopped at the first failure.**
- These passed:
  - preflight;
  - both course rounds (`PASS (18/18 passed)`, resets 105 s / 108 s);
  - replacement `verify-S4.2` (`PASSED (65/65 checks)`);
  - the chaos suite with the app quiesced (`PASSED (22/22 checks)`);
  - placement, e2e-smoke, UI (A4.1), API refinements and static metrics checks.
- `verify-A4.2-live.py --mode full` passed seven checks, then stopped at the start of the leader-fault proof with `HTTP 401: GET /internal/ring`. This happened before any traffic, cordon or pod delete, so no fault was injected.
- The coordinator logged one refusal, `credential=app method=GET route=/internal/ring reason=missing`.
- Cause: the metrics verifier takes the app sender token only from `INTERNAL_AUTH_TOKEN_FILE`, which the gate procedure leaves unset. This is a gate-procedure and harness gap for the auth-on profile, not a product defect. The fix is not applied here.
- Nothing after the failure ran: M6 study, internal-auth checks, deployed UI and forms, PS aliases, trace, CI evidence, Merkle and #157 collector, signoff.

| File | What it is |
|---|---|
| `report.md` | Full report: per-section results, the failure and its cause, image identities, not-certified items, required items and their status |
| `gate.txt` | Course gate summary, `PASS (18/18 passed)` |
| `round-1/`, `round-2/` | Per-demo reports and app-demo artifacts (large files omitted; see `omitted-sha256.txt`) |
| `run-notes.md` | Source identity, per-reset core/wrapper times, reset wrapper sha256, the stop, teardown |
| `source-commit.txt`, `source-tree.txt`, `gate-auth.txt` | Gate SHA, tree, and the auth profile (`true`) |
| `replacement/admission-replace-*.txt` | `verify-S4.2` evidence, `PASSED (65/65 checks)`, including the settled-group pre-wipe check |
| `chaos/chaos-suite-*.txt` | Chaos suite summary, `PASSED (22/22 checks)` |
| `metrics-stopped/` | The stopped verifier's `results.json` (`"result": "FAIL"`), `queries.json` and the single baseline fault sample |
| `pod-images/{after-course,at-stop}/` | `pod-images.tsv` (every container's kubelet-reported image ID, all namespaces), `node-images.tsv` (each kind node's containerd store: repo digests and config ID), `local-images.tsv` (local index ID and `linux/amd64` platform manifest) |
| `stop-observations/` | Pods, Jobs, nodes, the nine shard statuses and the coordinator refusal line after the failure (events omitted; see `omitted-sha256.txt`) |
| `omitted-sha256.txt` | sha256 of the files over 100 KB kept only in the run root |

Not produced: the #157 fixed-profile file and the `auth-log-checks.sh` completeness summaries (their sections were not reached). This run certifies nothing beyond the results above.

`gate.txt` and `run-notes.md` are kept byte-for-byte as the run wrote them, so their paths point at the original run root (`~/.cache/reliefgrid/gate-20261115T175612Z/`), which keeps the console logs, the full chaos evidence, the forward logs and the omitted files.
