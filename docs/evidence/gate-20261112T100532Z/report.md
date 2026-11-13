# Enhanced gate (scoped) — report, 12 November 2026

Run root: `~/.cache/reliefgrid/gate-20261112T100532Z` · gate SHA `1623c23fef7a881990f7470c7ddc3f85cf33c3b6` (main after PR #159), tree `fabe5acf73ce` · the run root keeps the checklist copy used (`checklist-used.md`) and its blocks (`stages/`). The files in this directory are listed in `README.md`.

**Overall: PASS. Every section of the enhanced gate (scoped) passed on the first attempt, with no stops, retries or fixes.** Both course rounds 18/18; replacement 63/63; chaos 12/12 runs (22/22 checks) with the app quiesced; UI, metrics/dashboards, trace, M6, Merkle 7/7, strict invcheck and the #157 collector all PASS. The checkout ended clean.

Scope: deployed aging `0s`, autoscaling off (fixed API 2 / matcher 2), #29 auth excluded. **This is not the full enhanced gate.** These stay open goals and are not certified here:
1. HPA/KEDA scale acceptance (`verify-A4.3`).
2. Positive-interval aging (needs the #159 prerequisite check to pass with owner approval, strict coordinator/storagenode provenance and all nine replicas verified).
3. #29 internal-call auth and its rollout/rotation acceptance (#156 is a helper library only; no route uses it).

## Precondition
- Main CI for the gate SHA: run 43189068864 `ci` success (push to main runs only `check`; the rest is skipped by design).
- PR #159 (merged as the gate SHA, head `085af88`): check, commit-lint, changes, images ×3, **smoke** all SUCCESS; seams/schedules skipped.
- `kind get clusters` empty before section 1 and again before section 2; laptop on AC throughout; sleep inhibitor held for the whole run (released on exit).

## Timeline (UTC, 12 Nov)
| Stage | Start–end | Duration | Result |
|---|---|---|---|
| 1. Preflight (doctor, browsers, gen-check, check, contracts, 20 read-fence seeds, M8 schedules, A1.5-static) | 10:05:31–10:17:00 | 11m29s | PASS |
| 2. Two course rounds | 10:17:00–10:28:53 | 11m53s | **PASS 18/18** |
| 3. Replacement, chaos (app quiesced), controllers, placement, e2e, UI, API, metrics, M6 | 10:28:53–11:10:37 | 41m44s | PASS |
| Deployed UI observation | 11:10:37–11:11:20 | 43s | PASS |
| Traced request | 11:11:21–11:11:24 | 3s | PASS |
| CI evidence | 11:11:24–11:11:26 | 2s | PASS |
| 4. Merkle, strict invcheck, #157 collector | 11:11:26–11:12:16 | 50s | PASS |
| 5. Final stash, clean checkout | 11:12:16 | <1s | PASS |
| **Total** | **10:05:31–11:12:16** | **66m45s** | **PASS** |

## Course rounds (`course/gate-20261112/gate.txt`)
| | Round 1 | Round 2 |
|---|---|---|
| demo-1 … demo-9 | all PASS (7, 2, 2, 9, 2, 9, 11, 2, 2 checks) | all PASS (same) |
| gate.txt reset row (whole wrapper) | **OK (107 s)** | **OK (116 s)** |
| Core `reset.sh` (run-notes) | **106 s** | **115 s** |
| Cordons | none | none |

`result: PASS (18/18 passed)`, code tree `d52c88cc214e`, `code dirty: no`, rounds 10:17:00–10:28:53Z (11m53s; 4 Nov: 12m14s with core resets 129 s + 104 s; dry run: 106 s + 108 s). After each reset the wrapper's assertions passed: fixed API2/matcher2, aging `0s`, autoscaling off, no HPA on api/matcher, placement OK. Wrapper sha256 `a5865110…2eab0e`. Two `app-reset-*/` records (dev images, no source binding).

## Section 3
| Check | Result |
|---|---|
| Replacement (`verify-S4.2`) | PASSED 63/63; ledger 1794 acked, 0 lost; invcheck clean and quiescent; #21 bootstrap verifies updated peerIds |
| Chaos (`verify-S4.3`), app api/matcher/ui 2/2/1 → 0, pods gone, then restored | **PASSED 12/12 runs, 22/22 checks**. All ten experiments PASS; `control-lost-write` and `control-stale-reader` caught their faults; 0 lost, 0 unresolved unknowns, 0 stale reads in every experiment; app restore OK (all three rollouts complete). Fresh cluster, so Chaos Mesh was installed by the suite and its "before Mesh" resource sample is genuine this time. |
| autoscaling-install → autoscaling-down → fixed-profile upgrade | PASS; no HPA/ScaledObject created |
| placement-check | PASS, 3 replicas per shard on distinct data workers |
| e2e-smoke | PASS (reciprocal claim, original create replay); evidence moved to `evidence/enhanced/app-evidence/` by the helper |
| verify-A4.1 (UI, fixture server) | ALL PASS; `ui/static` no diff |
| API refinement tests | PASS |
| metrics-up, tracing-up, A4.2 static | PASS |
| A4.2 live full + screenshots | **FULL ACCEPTANCE: PASS** (business latency, cancellation cleanup, workflow age, leader loss/DOWN member/index gap/catch-up; 3 Grafana PNGs) |
| M6 study | PASS (exit 0, reproduces the retained study) |
| Deployed UI | PASS: 200 and "Live"; first six API rows present with the same status; offline view shows "Offline" and "Last saved". Counters: 100 open (loaded view), 26 active assignments, 7 resources, 74 needs attention; API first page MATCHED 26 / WAITING 74 (`deployed-ui/`) |
| Traced request | PASS: one trace with api/coordinator/storagenode/matcher and publish/consume/raft.apply; continuity: 75 spans, publish, consume and raft.apply chain to `api:POST /api/v1/requests`, only the external `traceparent` parent unresolved (`trace-continuity.txt`); reached committed CANCELLED |
| CI evidence | PR #159 assertions and main-CI assertion PASS (`PR159-ci.json`, `main-ci.json`) |

## Section 4 and the #157 collector
- Merkle `verify-S4.5 --kind`, seed 20261112: **PASSED 7/7**. 216 acked, 0 unknown, 0 lost; equal roots on all three replicas of every shard (appliedIndex 91105 / 80908 / 86155); app restored on exit.
- Retained Job `s45-check-seed20261112` complete; strict invcheck `ok:true`, `verdict:clean`, `quiescent:true`, outbox pending `[0,0,0]`.
- Rollouts OK; `PASS fixed profile retained`; matcher `AGING_INTERVAL=["0s"]`.
- **Collector (`final-fixed-profile/fixed-profile.json`): `ok: true`**, `fixed-profile observation: PASS`. `fixedReplicas {api: 2, matcher: 2}`; four Ready app pods (2 api, 2 matcher) with image IDs; 0 HPAs and 0 ScaledObjects; `collectorCommit 1623c23`, tree `fabe5acf`, **`collectorDirty: false`**, `runtimeBuildBinding: NOT_ESTABLISHED` (expected for dev images).

## Limitations
- The open goals above are not certified.
- #128's PS Table 2 alias routes (Traefik IngressRoute) are deployed but no gate step exercises them: `verify-ps-aliases` is not in the checklist, and its chart test is `integration`-tagged, outside `make check` and `verify-A1.5-static`. The `TestPS*` API tests do run in `make check`.
- Images are dev builds: no source/runtime build binding (#158 is a separate procedure).
- Chaos ran with the app quiesced, as the checklist requires; it says nothing about storage chaos under app load.
- M8-only schedules in preflight; full cluster schedule acceptance is a separate campaign.

## How it was run
The checklist's bash blocks were extracted verbatim and run in order by a driver (`stages/` in the run root) under `systemd-inhibit`, stopping on the first non-zero exit. The driver re-checked `kind get clusters` before section 2 and stood in for the "separate shell" Jaeger port-forward (started before the trace block, stopped right after). The day-before block (validator install) was not re-run; the validator was installed on 11 Nov and section 1's `make doctor` passed. No checklist errata were found in this run.

## Evidence (run root)
`source-commit.txt`/`source-tree.txt`, `run-notes.md`, `env.sh`, `checklist-used.md`, `stages/`, `reset-scoped.sh`, `stash-evidence.sh`, every console log, `course/gate-20261112/` (gate.txt + both rounds), `app-reset-*/`, `evidence/{preflight,replacement,chaos,enhanced,merkle,final}/`, `deployed-ui/`, `trace*.json`/`trace-continuity.txt`, `PR159-ci.json`, `main-ci.json`, `retained-invcheck.json`, `final-fixed-profile/fixed-profile.json`, `git-status-end.txt` (empty).

## State after the run
- kind cluster deleted after the run (`make cluster-down`); `kind get clusters` reports none.
- Gate checkout ended clean at the gate SHA, then switched back to `main`. Nothing was committed during the run.
