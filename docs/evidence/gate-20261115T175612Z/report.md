# Final gate, attempt 2 — enhanced gate (scoped), auth on

**Result: FAILED (stopped at the first failure in section 3, metrics/dashboards).** This is not a PASS. Everything after the failure was not run.

| | |
|---|---|
| Run ID | `gate-20261115T175612Z` (run root `~/.cache/reliefgrid/gate-20261115T175612Z`) |
| Source SHA | `636b265084fc2f855a0d3b17ec8ca6634209bef2` (main, PR #185 squash-merged), tree `fa967e0db5c92bbe26b0afdb5fb715f9cb14b491` |
| Course run ID | `gate-20261115` (`$RUN_ROOT/course/gate-20261115/gate.txt`) |
| Host | `hrishabh-sharrma-ThinkPad-E14-Gen-7`, on AC, sleep inhibitor held for the whole run |
| Started / stopped | 2026-11-15T17:56:13Z / 2026-11-15T19:17:22Z |
| Profile | `GATE_AUTH=true`; deployed `app.agingInterval=0s` and matcher `AGING_INTERVAL=0s`; `app.autoscaling.enabled=false`; fixed API 2 / matcher 2 |
| Attempt 1 | `gate-20261115T111914Z` at `c8b2b60`, stopped in section 3 by the replacement-check harness race fixed in #185 (report `~/report-drafts/gate-final-20261115-report.md`) |
| Main CI for the SHA | run 43741867090 (push, main): `check` success; the other jobs are skipped on push to main by design |

## Per-section results

| Section | Check | Result |
|---|---|---|
| 1 Preflight | `make doctor` | PASS ("all tools match tools.mk; Docker, RAM and ports ok") |
| | browser preparation, `make gen-check` | PASS |
| | `make check` | PASS (no `FAIL`; `scripts` 130.074s) |
| | `make verify-A0.1` (contracts) | PASS (16 passed, 0 failed) |
| | `verify-S4.1` read fence | PASS (21/21 runs) |
| | `SCHEDULE_BACKEND=m8 SEEDS=50 make test-schedules` | PASS (M8 only, 50 seeds) |
| | `make verify-A1.5-static` | PASS |
| | clean checkout, no leaked port-forwards | PASS |
| | #178/#179/#183 present in the SHA (auth profile requirements) | PASS |
| 2 Course | two clean rounds, demos 1–9 | **PASS (18/18 passed)**, resets OK (105 s) / OK (108 s), cordons none, `code dirty: no` |
| 3 Enhanced | `verify-S4.2` replacement (with #185) | **PASS (65/65 checks)** |
| | chaos suite (`make verify-S4.3`, app quiesced and restored) | **PASS (22/22 checks)**: ten experiments and both negative controls, 0 lost, 0 unresolved unknowns, 0 stale reads; app restore OK |
| | KEDA CRD install, then autoscaling down; fixed-profile helm upgrade | PASS (exit 0) |
| | `make placement-check` | PASS (3 replicas on distinct data workers, every shard) |
| | `make e2e-smoke` | PASS (reciprocal claim, original create replay) |
| | `make verify-A4.1` (UI, fixture server) | PASS ("A4.1 VERIFY: ALL PASS"), `ui/static` unchanged |
| | API refinements (conditional, CAS, cursors) | PASS |
| | `metrics-up`, `tracing-up TRACING_SAMPLE=1.0`, `verify-A4.2-static` | PASS |
| | **`verify-A4.2-live.py --mode full --screenshots`** | **FAIL**: 7 checks passed, then `RuntimeError: HTTP 401: GET /internal/ring` |
| | M6 study | NOT RUN |
| | internal auth checks (probes, `auth-log-checks.sh` ×3) | NOT RUN |
| | deployed UI observation and #164 forms | NOT RUN |
| | PS alias routes (#128) | NOT RUN |
| | traced request | NOT RUN |
| | CI smoke evidence | NOT RUN |
| 4 #157 | Merkle `verify-S4.5 --kind`, retained invcheck, fixed-profile collector | NOT RUN |
| 5 Signoff | evidence inventory, final clean checkout | NOT RUN (checkout verified clean at the stop, after stashing the stopped verifier's evidence) |

### Section 2 details

- Course `started 18:24:12Z`, `finished 18:35:57Z` (11m45s). Every demo report says PASS with full assertion counts in both rounds: demo-1 7/7, demo-2 2/2, demo-3 2/2, demo-4 9/9, demo-5 2/2, demo-6 9/9, demo-7 11/11, demo-8 2/2, demo-9 2/2.
- Reset wrapper sha256 `1637f7b43273fd4462a2548a74ad500b64e834d3b251172a8c7055f1d255290c`, the same as attempt 1's. Per reset: core `reset.sh` 104 s, wrapper 105 s (`app-reset-kaJZIEjW`); core 107 s, wrapper 108 s (`app-reset-efrtvA1x`).
- Both resets used `INTERNAL_AUTH=true`. The wrapper checked fixed API 2 / matcher 2, aging `0s`, autoscaling false and no HPA on the API or matcher after each reset.

### Section 3 details before the stop

- **Replacement (`verify-S4.2`, 18:39:25Z, code tree `b7f2c72662c0`, dirty no).** The new fingerprint covers the verifier, its library and the sourced chaos/auth helpers.
  - Restart: the new pod object (UID `1fbb4efa…` gone) came back Ready with the same serverId.
  - The new pre-wipe precondition passed: "storage-shard-1-2 is a follower of a settled group before the wipe (leader storage-shard-1-0, leadership unchanged)".
  - The control dialled from `storage-shard-1-0`. No election happened while the wiped pod waited (term 5).
  - Crash-resume replacement and refusals passed. The ledger had 1609 acknowledged writes, 0 lost. Invcheck was clean and quiescent.
- **Chaos (19:12:25Z, 22/22).** Kill-follower and delay-follower passed, and so did the other eight experiments. The lost-write control (`s441`) and stale-reader control (`s442`) each caught their fault: their verify Jobs ended Failed, as designed.

## The failure

`scripts/verify-A4.2-live.py --mode full` ran its app checks, then entered the leader-fault proof (`scripts/metrics_fault.py:run_fault`).

- The baseline sample was recorded at 19:17:22Z: leader `storage-shard-0-2`, 9 targets, lag 0.
- Its next call, `get(coord, '/internal/ring')`, went to the coordinator without a credential, and the coordinator returned **401**. The verifier exited nonzero and wrote `results.json` with `"result": "FAIL"`.
- The traffic generator, the cordon and the leader delete come after that call, so **no fault was injected and no scratch write was made.**
- After the stop:
  - all nodes were schedulable;
  - every storage pod was healthy: one leader per shard, all admitted and caught up, commit = Raft applied on every member;
  - pods were Running or Completed, except the expected Error pods of the two chaos negative controls and the replacement check's refused replace.

**Coordinator log:** exactly one refusal since the reset: `WARN internal auth refused credential=app method=GET route=/internal/ring peer=127.0.0.1:46526 reason=missing`. It contains no token value.

**Cause (from the code):**
- The metrics verifier's internal calls use `internal_auth.Sender`, which takes the app sender token only from `INTERNAL_AUTH_TOKEN_FILE` (`scripts/internal_auth.py:37`). With the variable empty, it sends no credential.
- The checklist's `env.sh` explicitly unsets `INTERNAL_AUTH_TOKEN_FILE`, and section 3 does not stage the app token before `verify-A4.2-live.py`.
- The shell verifiers stage their own credentials through `scripts/internal-auth-env.sh`; this Python verifier does not.
- The auth-off gates (12 November) never needed the token, so the enabled metrics fault path was first exercised here.

**Verdict:** a gate-procedure and harness gap for `GATE_AUTH=true`, not a product defect. The coordinator refused an unauthenticated internal call, which is the intended behaviour with auth on.

**Proposed fix (not applied):** have the metrics verifier (or the checklist step before it) stage the app sender token the same way the shell verifiers do. For example, with auth detected on, write the Secret's `sender-token` to a 0600 file in a 0700 temp dir, export `INTERNAL_AUTH_TOKEN_FILE` for that one command, and remove it on exit. It must fail closed if detection is unknown. Add an offline test that the full mode refuses to start the fault proof without a sender when the release has auth on. Then run attempt 3 under a new run ID.

## Image identity

Development images are not source-bound (`runtimeBuildBinding` not established; strict provenance is the separate #158 procedure). Records are in `$RUN_ROOT/pod-images/{after-course,at-stop}/`:
- `pod-images.tsv`: every container in every namespace, with kubelet's reported image ID. 38 pods after the course and 93 at the stop, 0 without an image ID.
- `node-images.tsv`: each kind node's containerd store.
- `local-images.tsv`: local index ID and `linux/amd64` platform manifest.

| Image (tag) | linux/amd64 platform manifest (local, at stop) | containerd config ID (all 5 nodes, after course and at stop) | kubelet-reported image ID (pods) |
|---|---|---|---|
| `storagenode:dev` | `sha256:7ca18b85b0b0b4519cbb42d0745075b90e0db5a4e819a43c346e575472a89373` | `sha256:57f26544d4861af5a288ab62192deb37b005270dc1dbc090d1a41a0ecd9f60c1` | after course `import-2026-11-15@sha256:536b3e92…`; storage pods after the replacement check's reload `…@sha256:2a81d614…` |
| `coordinator:dev` | `sha256:827d4685862ffcbbe1c9fa2839a69e60f6276bf0760bdd7d8eecac1d1ff3756f` | `sha256:df4832a008f76f798b86eb82c101064f2b0cdc675284107a2395cd2b7413560b` | `import-2026-11-15@sha256:33766342…` |
| `tools:dev` | `sha256:dfb4c66ffb228c8c0b051225ea2b304e84c1d3d74d0ada4270374714cba82ee6` | `sha256:f7cad31ab01746a8bac11d03a041af7844138c3c7a7dffe4f4c7de85fee67979` | Jobs before the reload `…@sha256:75665586…`, after it `…@sha256:5cac6b72…` |
| `api:a15-20261115183104` | `sha256:3450519ac63f386e953be9f60db2ef7ffe246938721fa00d36d9a676c4e8810c` | `sha256:6b4bbd3bccc8bea1735757b6aa81b3401a4f5db077f1306b9ab2b8524cd64b1b` | `import-2026-11-15@sha256:a8b21396…` |
| `matcher:a15-20261115183104` | `sha256:b148668f4f3c2ffdc38ea6857a013ccfbc36f0335215becc68e2e96339b6d83f` | `sha256:31e301737a3fc0669f0bb3aad915149e858c673e1c32fcbbbdd9ac262073cb96` | `import-2026-11-15@sha256:6471163f…` |
| `ui:a15-20261115183104` | `sha256:d3d1751497bcf9b91a361e6171f091ddf8dade1e28aa6fd046c65033108d889b` | `sha256:f90d0c052a635c20fdc0885eb42c34d60084b6e756f0d17dd3edc093fd9341d7` | `import-2026-11-15@sha256:580cb933…` |

- The storagenode platform manifest `7ca18b85…` is the same as in attempt 1. No storagenode input changed between `c8b2b60` and `636b265`; #185 changed only the verifier scripts.
- The app images are single-manifest images, so their local ID and platform manifest are the same digest.
- **Reading the IDs:** kubelet reports `import-<date>@sha256:…`, the digest of the image index as imported by `kind load`. It changes with every rebuild because the index carries per-build metadata. The containerd config ID did not change for any image on any node across the run, including the replacement check's rebuild and reload. Pods started before a reload keep the earlier import digest.
- Attempt 1's open question was why only storagenode's in-cluster ID changed after the rebuild. The answer: the storage pods are restarted after the reload and so pick up the new import digest, while the content (config ID, platform manifest) is unchanged.
- The coordinator pod, also restarted by the replacement check, still reports `33766342…`. Its node lists three import digests for the same config ID, so the digest kubelet reports for an image with several is not established here.
- Compare platform manifests or config IDs, never index or import digests.

## Not certified (open goals)

- **Positive-interval aging:** deployed `0s` throughout; `app.agingInterval` > 0 is not certified.
- **Final-cohort autoscaling (HPA/KEDA scale acceptance, `verify-A4.3`):** autoscaling off; only the KEDA CRD was installed for #157's lists; not certified.
- **#29 rotation and rollout onto an existing deployment:** excluded by scope. This attempt also did **not** reach the enabled-profile auth checks.

## Required retained items, and their status

- Run ID, source SHA and tree: above, and in `source-commit.txt`, `source-tree.txt` and `run-notes.md`.
- Platform manifest digests and observed image IDs for every pod: above, and in `pod-images/`.
- **#157 fixed-replica evidence: NOT PRODUCED.** Section 4 was not reached. The fixed profile was checked only by the reset wrapper (twice) and the section 3 helm upgrade; that is not the collector's evidence.
- **auth-log-checks completeness summaries: NOT PRODUCED.** The internal-auth block was not reached. No "no token in logs" or "no in-cluster refusal" claim is made for this attempt. The single coordinator refusal above is a manual log read, not an `auth-log-checks.sh` result.

## Stopped-run evidence

- The stopped verifier's files (`docs/evidence/A4.2/live-20261115T191714Z/`: results, queries, forward logs, the baseline fault sample) and the e2e-smoke app evidence were moved to `$RUN_ROOT/evidence/enhanced-stopped/`. The checkout was clean afterwards.
- Stop observations are in `$RUN_ROOT/stop-observations/`: pods, jobs, nodes, events, the nine shard statuses and the coordinator refusal line.
- The cluster was removed with `make cluster-down` after these observations.
