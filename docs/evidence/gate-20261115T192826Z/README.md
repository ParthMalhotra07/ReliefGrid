# Final gate, attempt 3 — enhanced gate (scoped), auth on: PASS (15 November 2026) [S4.7]

Gate SHA `636b265084fc2f855a0d3b17ec8ca6634209bef2` (tree `fa967e0db5c9`; #185 merged), host `hrishabh-sharrma-ThinkPad-E14-Gen-7`, run 19:28:27–20:43:01Z. Scope: enhanced gate (scoped), internal auth on (`GATE_AUTH=true`), deployed aging `0s`, autoscaling off (fixed API 2 / matcher 2).

**Result: PASS.** Every checklist section completed:
- **Preflight** passed.
- **Course:** both rounds `PASS (18/18 passed)`, resets 106 s / 109 s.
- **Section 3:**
  - replacement 65/65;
  - chaos 22/22;
  - placement, e2e-smoke, UI and API refinements;
  - metrics full mode with the leader-fault proof and the M6 study;
  - internal-auth probes and all three `auth-log-checks.sh` checks, PASS and complete;
  - deployed UI and the #164 forms;
  - PS aliases, the traced request, and CI evidence.
- **Merkle:** `verify-S4.5 --kind` 8/8, with a clean strict invcheck (pending 0) and the #157 collector `ok: true` at API 2 / matcher 2.
- **Sign-off:** the checkout ended clean.

The only change from attempt 2 (`gate-20261115T175612Z`) is in the gate checklist: the metrics step now stages the app sender token. No code changed. Issue #187 tracks the verifier-side guard. Positive-interval aging, final-cohort autoscaling and #29 rotation/rollout are **not certified** (open goals).

| File | What it is |
|---|---|
| `report.md` | Full report: per-section results, auth summaries, image identities, #157 evidence, open goals, limitations |
| `gate.txt`, `round-1/`, `round-2/` | Course gate summary and per-demo reports/app artifacts (large files omitted; see `omitted-sha256.txt`) |
| `run-notes.md`, `source-commit.txt`, `source-tree.txt`, `gate-auth.txt`, `git-status-end.txt` | Run identity, per-reset times, the checklist's sha256, finish line, the auth profile, and the empty end-of-run `git status` |
| `replacement/`, `chaos/` | `verify-S4.2` evidence (65/65) and the chaos suite summary (22/22) |
| `metrics/` | Full-mode `results.json` (`PASS`), queries, the fault proof and samples, recovery statuses, scratch intents, and the M6 study JSON |
| `internal-auth/` | Probe output (`internal-auth-check.txt`), the three completeness summaries, the token-free refusal logs and the probe's write intent |
| `deployed-ui/` | Live/offline observations and the #164 form results (screenshots omitted; see `omitted-sha256.txt`) |
| `ps-aliases/`, `trace/`, `ci/` | Alias results, trace ID/continuity/request/cancel, gate PR and main CI JSON |
| `merkle/`, `fixed-profile.json` | `verify-S4.5 --kind` evidence, the retained strict invcheck, and the #157 fixed-profile collector output |
| `pod-images/{after-course,after-section3,final}/` | Kubelet image IDs for every container, each node's containerd store (config IDs), and local platform manifests |
| `end-observations/` | Nine shard statuses, pods and nodes after section 4 |
| `omitted-sha256.txt` | sha256 of the files over 100 KB kept only in the run root (demo-5 burst records, screenshots, a forward log) |

`gate.txt` and `run-notes.md` are kept byte-for-byte as the run wrote them, so their paths point at the original run root (`~/.cache/reliefgrid/gate-20261115T192826Z/`).
