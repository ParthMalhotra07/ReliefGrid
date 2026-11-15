# Final gate, attempt 3 — enhanced gate (scoped), auth on

**Result: PASS.** Every section of the checklist ran to completion with exit 0, and the checkout was clean at the end. This is the **enhanced gate (scoped)**, not the full enhanced gate. The open goals below are not certified.

| | |
|---|---|
| Run ID | `gate-20261115T192826Z` (run root `~/.cache/reliefgrid/gate-20261115T192826Z`) |
| Source SHA | `636b265084fc2f855a0d3b17ec8ca6634209bef2` (main; PR #185 squash-merged), tree `fa967e0db5c92bbe26b0afdb5fb715f9cb14b491` |
| Course run ID | `gate-20261115` (`$RUN_ROOT/course/gate-20261115/gate.txt`) |
| Host | `hrishabh-sharrma-ThinkPad-E14-Gen-7`, on AC, sleep inhibitor held for the whole run |
| Started / finished | 2026-11-15T19:28:27Z / 2026-11-15T20:43:01Z (1h15m) |
| Profile | `GATE_AUTH=true`; deployed `app.agingInterval=0s` and matcher `AGING_INTERVAL=0s`; `app.autoscaling.enabled=false`; fixed API 2 / matcher 2 |
| Checklist | `~/report-drafts/gate-20261112-checklist.md`, sha256 recorded in `run-notes.md` |
| Main CI for the SHA | run 43741867090 (push): success. Gate PR #185 CI: `smoke` SUCCESS (run 43731619242), `check` SUCCESS |
| Earlier attempts | 1: `gate-20261115T111914Z` at `c8b2b60`, stopped by the replacement-check harness race (fixed in #185). 2: `gate-20261115T175612Z` at `636b265`, stopped at the metrics fault proof (HTTP 401, the app sender token not staged) |

## Change since attempt 2: the checklist only

Code and SHA are unchanged since attempt 2; only the checklist changed.

**The metrics step now stages the app sender token.** `verify-A4.2-live.py --mode full` runs in a subshell that does the following, using `scripts/internal-auth-env.sh`, the same helper the shell verifiers use:
- sources the helper;
- runs `internal_auth_setup kind-reliefgrid reliefgrid`, which writes the app sender token to a 0600 file in a 0700 temp dir and exports `INTERNAL_AUTH_TOKEN_FILE`;
- checks that the detected auth state equals `GATE_AUTH`;
- removes the token on exit.

Detection fails closed on an unreadable or malformed release; I checked this offline with no cluster (exit 1, no temp dir left). With auth off, nothing is staged.

**Read-only audit of every later step** for internal calls with auth on. The other steps either make no internal call (M6, deployed UI and forms, PS aliases, trace, CI evidence, the #157 collector, sign-off) or already get their own credential:
- the auth probes and log checks read the Secrets themselves;
- the `verify-S4.5` drain and Jobs stage through the same helper, and its audit uses `OPERATOR_TOKEN`.

None needed a code change. The audit table is in the checklist.

## Per-section results

| Section | Check | Result |
|---|---|---|
| 1 Preflight | `make doctor` | PASS |
| | browser preparation, `make gen-check` | PASS |
| | `make check` | PASS (no `FAIL`; `scripts` 129.197s) |
| | `make verify-A0.1` | PASS ("A0.1 VERIFY: ALL PASS") |
| | `verify-S4.1` read fence | PASS (21/21 runs) |
| | M8 schedules, 50 seeds | PASS |
| | `make verify-A1.5-static` | PASS |
| | clean checkout, no leaked port-forwards, #178/#179/#183 in the SHA | PASS |
| 2 Course | two clean rounds, demos 1–9 | **PASS (18/18 passed)**, 19:44:45–19:56:29Z, resets OK (106 s) / OK (109 s), cordons none, `code dirty: no` |
| 3 | `verify-S4.2` replacement | **PASS (65/65)**, 20:00:19Z |
| | chaos suite (app quiesced, then restored) | **PASS (22/22)**, 20:33:14Z |
| | KEDA CRD for #157 lists, autoscaling down, fixed-profile upgrade | PASS |
| | `placement-check` | PASS |
| | `e2e-smoke` | PASS |
| | `verify-A4.1` (UI, fixture server), `ui/static` unchanged | PASS |
| | API refinements (conditional, CAS, cursors) | PASS |
| | `metrics-up`, `tracing-up` (sample 1.0), `verify-A4.2-static` | PASS |
| | **`verify-A4.2-live.py --mode full --screenshots`** (token staged) | **PASS** ("A4.2 FULL ACCEPTANCE: PASS"), run `live-20261115T203818Z` |
| | M6 study | PASS |
| | internal auth probes | **PASS** |
| | `auth-log-checks.sh` no-token / refusals / in-cluster-refusals | **PASS / PASS / PASS** (complete; summaries below) |
| | deployed UI observation | PASS |
| | #164 forms | PASS |
| | PS aliases (#128) | PASS (7 routes, plus 4 live body/ETag comparisons) |
| | traced request | PASS (44 spans) |
| | CI smoke evidence | PASS |
| 4 | `verify-S4.5 --kind` (Merkle, #178 drain, strict check) | **PASS (8/8)**, 20:42:22Z |
| | retained invcheck `s45-check-seed20261115` | PASS: `ok`, `clean`, quiescent, outbox pending `[0,0,0]` |
| | #157 fixed-profile collector | **PASS**: `ok: true`, `fixedReplicas {api: 2, matcher: 2}` |
| 5 | evidence inventory, `git status` empty, back on main | PASS ("checkout clean at 636b265…") |

### Details

**Course**
- Every demo report says PASS with full counts in both rounds: demo-1 7/7, demo-4 9/9, demo-6 9/9, demo-7 11/11, all others 2/2.
- Reset wrapper sha256 `1637f7b4…290c` (same as attempts 1 and 2). Core `reset.sh` took 105 s and 107 s; the wrapper took 106 s and 109 s.

**Replacement**
- The new pre-wipe check passed: "storage-shard-1-2 is a follower of a settled group before the wipe (leader storage-shard-1-0, leadership unchanged)".
- The control dialled from `storage-shard-1-0`, and no election happened while the wiped pod waited (term 5).
- The evidence fingerprint `b7f2c72662c0` covers the sourced harness files.
- As in attempt 2 the target was a follower, so the leadership-transfer path was not exercised live; it is covered by the offline tests.

**Chaos**
- All ten experiments passed: kill-leader, kill-follower, restart-shard, isolate-replica, isolate-leader, delay-isolate-leader, lose-quorum, delay-follower, stop-worker and restart-coordinator.
- Both negative controls caught their fault: control-lost-write and control-stale-reader.
- Every experiment had 0 lost writes, 0 unresolved unknowns and 0 stale reads.

**Metrics (full mode)**
- The log shows "metrics: internal auth on; app sender token staged for this command only".
- Seven app checks passed. The fault proof then passed: "real leader loss, DOWN member, raw index-gap movement, growing workflow age and authoritative catchup observed".
- Three live Grafana screenshots were captured. I looked at the storage dashboard: it shows observations, the fault annotation and the Raft-to-FSM gap spike at the fault.

**Internal auth probes**
- Coordinator `GET /internal/ring` and `PUT /internal/data/cfg:gate-auth-…`, and storage `GET /internal/scan`, each returned **401** with no token.
- A wrong token got 401.
- With the app token, the ring read, the write, the read-back and the cleanup delete returned **200**. With the node token, the storage scan returned **200**.
- The refusal logs name the credential class, method, route and reason, and no configured or presented token appears in them.

**Auth log-check completeness summaries** (`$RUN_ROOT/internal-auth/*-summary.txt`):

| Check | Result | Pods listed / logs read | Log failures | Restarted containers / previous logs read / failed | Other |
|---|---|---|---|---|---|
| `no-token` (every pod, whole logs) | PASS | 71 / 71 | 0 | 1 / 1 / 0 | 5 token files, found in 0 logs |
| `refusals` (coordinator + 9 storage, since 20:39:41.118854Z) | PASS | 10 / 10 (expected 10) | 0 | 0 / 0 / 0 | 4 refusal lines |
| `in-cluster-refusals` (same 10 pods, whole logs since the reset) | PASS | 10 / 10 (expected 10) | 0 | 0 / 0 / 0 | no in-cluster refusal among 4 |

None is INCOMPLETE. The whole-log count of 4 refusals equals the probes' own 4, so the metrics run, chaos, Merkle and the other checks caused no refusal. In attempt 2, the unstaged metrics call caused one.

**Deployed UI**
- The live view loaded, and the first six API rows match the UI. The API's first page had 19 MATCHED and 81 WAITING; the UI counters show 100 open requests, 19 assignments and 81 needing attention.
- The offline view is visibly stale ("Offline · saved view", "Last saved 02:09:58 AM", create actions disabled). I looked at both screenshots.

**#164 forms**
- Request `1adb3bd9-…` (WAITING) and resource `71b12248-…` (AVAILABLE).
- One keyed POST each, and no match POST.
- Cleaned up: the request is CANCELLED and the resource OFFLINE.

**Trace `013b95a0cd1e95771b46435e1fd46602`**
- publish ← API POST; consume ← publish ← API POST.
- `raft.apply` ← storagenode `PUT /internal/data/{key}` ← coordinator forward ← coordinator PUT ← API `coordclient.CompareAndSet` ← API POST.
- The only unresolved reference is the external traceparent parent.
- The traced request reached committed CANCELLED.

**Merkle**
- 216 acknowledged writes, 0 lost, 0 unknown.
- The app was quiesced (11 Deployments at 0), and the outbox drain ran with the app at 0.
- The strict check was clean and quiescent with pending 0.
- Equal roots on all three replicas of every shard: shard 0 at applied index 49648, shard 1 at 46037, shard 2 at 56149.

## Image identity

Development images, not source-bound (`runtimeBuildBinding = NOT_ESTABLISHED`; strict provenance is #158). Records in `$RUN_ROOT/pod-images/{after-course,after-section3,final}/`:
- `pod-images.tsv`: every container in every namespace with kubelet's image ID. 38, 93 and 99 pods; 0 without an image ID.
- `node-images.tsv`: each kind node's containerd store.
- `local-images.tsv`: local index ID and `linux/amd64` platform manifest.

| Image | linux/amd64 platform manifest | containerd config ID (all 5 nodes) | kubelet-reported image ID (pods, at the end) |
|---|---|---|---|
| `storagenode:dev` | `sha256:7ca18b85b0b0b4519cbb42d0745075b90e0db5a4e819a43c346e575472a89373` | `sha256:57f26544d4861af5a288ab62192deb37b005270dc1dbc090d1a41a0ecd9f60c1` | `import-2026-11-15@sha256:9cb71e63…` (15 containers; `dc35c085…` after the course, before the replacement check's reload) |
| `coordinator:dev` | `sha256:827d4685862ffcbbe1c9fa2839a69e60f6276bf0760bdd7d8eecac1d1ff3756f` | `sha256:df4832a008f76f798b86eb82c101064f2b0cdc675284107a2395cd2b7413560b` | `…@sha256:59ce0637…` (`f9bd8d05…` after the course) |
| `tools:dev` | `sha256:dfb4c66ffb228c8c0b051225ea2b304e84c1d3d74d0ada4270374714cba82ee6` | `sha256:f7cad31ab01746a8bac11d03a041af7844138c3c7a7dffe4f4c7de85fee67979` | `…@sha256:3acb853a…` (44, including the retained `s45-check` Job); 3 earlier Jobs `…@sha256:61fc1dc4…` |
| `api:a15-20261115195136` | `sha256:3450519ac63f386e953be9f60db2ef7ffe246938721fa00d36d9a676c4e8810c` | `sha256:6b4bbd3bccc8bea1735757b6aa81b3401a4f5db077f1306b9ab2b8524cd64b1b` | `…@sha256:fd313b2b…` |
| `matcher:a15-20261115195136` | `sha256:b148668f4f3c2ffdc38ea6857a013ccfbc36f0335215becc68e2e96339b6d83f` | `sha256:31e301737a3fc0669f0bb3aad915149e858c673e1c32fcbbbdd9ac262073cb96` | `…@sha256:4d3b992a…` |
| `ui:a15-20261115195136` | `sha256:d3d1751497bcf9b91a361e6171f091ddf8dade1e28aa6fd046c65033108d889b` | `sha256:f90d0c052a635c20fdc0885eb42c34d60084b6e756f0d17dd3edc093fd9341d7` | `…@sha256:53438c38…` |

- Every platform manifest and every config ID is identical to attempt 2's. For storagenode, the platform manifest is also attempt 1's.
- The config IDs on all five nodes are identical between after the course and the end, across the replacement check's rebuild and reload.
- Only the kubelet-reported `import-…@sha256` digests change with each reload. They are index digests, not a content identity.

## #157 fixed-replica evidence

`$RUN_ROOT/final-fixed-profile/fixed-profile.json`:
- `ok: true`; scope "fixed-profile observation only; not a gate or build-source certification".
- Collector commit `636b265`, tree `fa967e0`, `collectorDirty: false`.
- Helm values: app API 2 / matcher 2, autoscaling false, storage disabled in the app release.
- Deployments `reliefgrid-api` and `reliefgrid-matcher` at 2 replicas, 2 ready, with current ReplicaSets.
- Four Ready distinct app pods with immutable image IDs.
- HPAs `[]` and ScaledObjects `[]`, both listed successfully.
- Retained tools Job `s45-check-seed20261115`: 1 succeeded, 0 failed, tools image ID recorded.

The checklist's independent assertions passed: the invcheck JSON check, "PASS fixed profile retained", matcher `AGING_INTERVAL=["0s"]`, and `fixedReplicas == {api: 2, matcher: 2}`.

## End state

- After section 4, all nine storage members were admitted and caught up, with commit = Raft applied: shard 0 at 49761, shard 1 at 46133, shard 2 at 56287, one leader each. No node was cordoned.
- The checkout was clean at `636b265` and switched back to main (`git-status-end.txt` empty).
- The cluster was removed with `make cluster-down`.

## Not certified (open goals)

- **Positive-interval aging:** deployed `0s` throughout; `app.agingInterval` > 0 is not certified.
- **Final-cohort autoscaling (HPA/KEDA scale acceptance, `verify-A4.3`):** autoscaling off; KEDA only supplied its CRD for #157's lists. Not certified.
- **#29 credential rotation and rollout onto an existing deployment:** not exercised. This gate checks only the enabled fresh-install profile.

## Limitations

- **Live coverage of the transfer path.** The replacement target was already a follower, so `step_off`'s leadership-transfer path ran only in the offline tests, not live.
- **The metrics fix is in the checklist, not the verifier.** `verify-A4.2-live.py` still does not refuse to start its fault step without a token when auth is on. Issue #187 (A-owned, after submission) tracks that.
- **Build provenance.** Development image records are not source-bound.
- **Course run ID.** The course run ID `gate-20261115` is shared by all three attempts' `gate.txt` files; each sits under its own run root.
