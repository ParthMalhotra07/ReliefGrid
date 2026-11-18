# Final enhanced gate (scoped), 15 November 2026: STOPPED at section 3 (replacement FAILED)

| Item | Value |
|---|---|
| Run ID | `gate-20261115T111914Z` (gate.txt run `gate-20261115`) |
| Run root | `~/.cache/reliefgrid/gate-20261115T111914Z` |
| Source SHA | `c8b2b609d384d84a80e85d85aee9f6946eae9380` (PR #184), tree `d35c3e2e57476432d2486fd51a7325e95b81c501` |
| Main CI for the SHA | run 43707170240 (`ci`, push to main): success, confirmed before the run |
| Profile | `GATE_AUTH=true`; aging `0s`; autoscaling off; fixed API 2 / matcher 2 |
| Checklist | `~/report-drafts/gate-20261112-checklist.md`, executed verbatim except `GATE_AUTH` and `GATE_SHA` in section 1 |
| Overall | **FAIL (stopped on first failure; no retry, no fix)** |

## Per-section results

| Section | Result | Evidence |
|---|---|---|
| 1. Preflight | **PASS** | `doctor.log` (all ok), `gen-check.log`, `check.log`, `contracts.log` (A0.1 16/16), `read-fence.log` (21/21 runs), `schedules.log` (M8, SEEDS=50, run e23b0891-5ad4-41ef-a6d9-fb0c4012595e), `ci-smoke-static.log`, `evidence/preflight/` |
| 2. Course rounds (2 × demos 1–9) | **PASS 18/18** | `course/gate-20261115/gate.txt`. Resets OK (104 s) and OK (97 s); core reset.sh 103 s / 96 s; cordons none; code dirty no. Each demo report's check counts all pass (demo-1 7/7, demo-4 9/9, demo-6 9/9, demo-7 11/11, others 2/2) |
| 3a. Replacement (`verify-S4.2`) | **FAIL** (15/16, aborted) | `replacement.log`, `evidence/replacement/docs/evidence/S4.2/admission-replace-*.txt` |
| 3b. Chaos suite (app quiesced) | NOT RUN | — |
| 3c. Controllers, placement, e2e-smoke, UI (A4.1), API refinements | NOT RUN | — |
| 3d. Metrics/dashboards (A4.2 full), M6 study | NOT RUN | — |
| 3e. Internal auth probes and `auth-log-checks.sh` (no-token, refusals, in-cluster-refusals) | NOT RUN: no completeness summaries exist | — |
| 3f. Deployed UI and #164 forms | NOT RUN | — |
| 3g. PS alias routes (#128) | NOT RUN | — |
| 3h. Traced request | NOT RUN | — |
| 3i. CI smoke evidence (PR #184) | NOT RUN (main CI was checked before the run; PR smoke not recorded) | — |
| 4. Merkle and #178 drain, retained tools Job, #157 fixed-profile collector | NOT RUN: no `fixed-profile.json` | — |
| 5. Evidence inventory/signoff | Not run as a block. Checkout was clean after the replacement stash | `run-notes.md` |

## The failure

```
== 3. wipe storage-shard-1-2: the empty disk stays unadmitted
FAIL  control:  reaches storage-shard-1-2:7000 and :8080 before the wipe
evidence: docs/evidence/S4.2/admission-replace-hrishabh-sharrma-ThinkPad-E14-Gen-7.txt (FAILED (1 of 16 checks failed))
```

- Passed before the abort: 3 chart guards, coordinator and three StatefulSet restarts on rebuilt images, shard-1 three committed voters caught up, #21 committed configuration on all three members, the bootstrap fence, the admission Role, and an intact-disk restart without admission.
- The control picks `peer0="$(leader_pod)"` (`scripts/verify-S4.2.sh:223`). The blank name in the message shows `leader_pod` returned empty: no shard-1 pod reported `state=Leader` at that instant. The fallback on line 224 only replaces a leader equal to the target, so the dial ran with no source pod and the script aborted before the wipe. No storage was wiped.
- Read-only observations about 2 minutes later (`stop-observations/`): shard-1 leader `storage-shard-1-1`, term 5, all three members `caughtUp`, applied = commit = 5133, status calls about 0.1 s. Events show readiness probe 503s on `storage-shard-1-0` (5) and `storage-shard-1-2` (1) around the control.
- Interpretation (not verified): a transient leaderless or not-ready window after the verifier's restarts, sampled once by the control. It does not indicate a replacement-admission defect, but the gate is not PASS. A fresh run under a new run ID is needed.

## Retained identities

**Image identities.** Observed `imageID` of every container in every pod (all namespaces), captured by `pod-images.sh`: `pod-images/after-course/pod-images.tsv` (after round 2) and `pod-images/at-stop/pod-images.tsv` (after the failure). Both have 38 pods, 38 containers and 0 without an image ID. At the stop:

| Image | Observed image ID | Containers |
|---|---|---|
| reliefgrid/api:a15-20261115113648 | `import-2026-11-15@sha256:517bb75771911dfd89a5f87b6683fda7010b41226345bca0812893d86d11ab1e` | 2 |
| reliefgrid/matcher:a15-20261115113648 | `import-2026-11-15@sha256:cadd57855dc22b1f00810bcd26ae964e0f006b3c85a912d75dacd12fb12f5147` | 2 |
| reliefgrid/ui:a15-20261115113648 | `import-2026-11-15@sha256:a49fcbc094de7292dbd16473de37caf77939355a98f54fcca45730d0c3b2cd68` | 1 |
| reliefgrid/coordinator:dev | `import-2026-11-15@sha256:7aafcb0bace763e95a9f0f5e28db99fe16de3391874c3616680fc15480e48e6c` | 1 |
| reliefgrid/storagenode:dev | `import-2026-11-15@sha256:75d1753bb75c25a230cdd274c65ae8b6fc8833e55521d3e73004247a0479b536` | 10 |
| reliefgrid/tools:dev | `import-2026-11-15@sha256:c0f25f1f0566436fb8cbb8f14f8937f0104933b7e48df62c9a7ebc95023245a0` | 3 |
| rabbitmq | `rabbitmq@sha256:57bcedccc5c75e55ac33e5f3eaeb28529317b5a02fe5e2722a025f2674c8063e` | 1 |
| traefik | `traefik@sha256:1336e85aaed293bd63d005086e0dd013b04078b26aaeb1c18b26a2f928af7e65` | 1 |

The kind system images (kindnetd, kube-proxy, coredns, etcd, control plane, local-path) are listed in the TSV. During the course rounds the storagenode image ID was `sha256:e4dd3612ee899d9eb36fc59691a60da7ac8332917462302016b43bbb70861fd1`. `verify-S4.2` rebuilds and restarts by design, and the rebuild at the same SHA produced a different storagenode ID; the coordinator and tools IDs were unchanged. Cause not investigated. These are development images with no source binding (`APP_BUILD_PROVENANCE=0`), not strict provenance. The per-reset app image records are in `app-reset-UiR8ecBb/` and `app-reset-2YPKoXOE/`.

**Fixed-replica evidence (#157 collector).** NOT COLLECTED: section 4 did not run. The section 2 reset wrapper asserted API 2 / matcher 2, aging `0s`, autoscaling false and no HPA targeting the app after each reset. That is wrapper evidence, not the collector.

**Auth log check completeness summaries.** NONE: the GATE_AUTH block did not run. No "no token" or "no in-cluster refusal" claim is made for this run. INCOMPLETE is not PASS, and NOT RUN is not PASS either. What the run does show: both releases had `internalAuth.enabled=true`, both auth secrets were present, and the two enabled-profile course rounds passed.

## Not certified (open goals, excluded by agreement)

1. HPA/KEDA scale acceptance and final-cohort autoscaling: autoscaling stayed off; `autoscaling-up` and `verify-A4.3` were not run.
2. Positive-interval aging: `agingInterval` stayed `0s`; not certified.
3. #29 credential rotation and rollout onto an existing deployment: excluded. Because this run stopped before section 3's auth block, enabled-profile auth acceptance is also not established by it.

## State at the end

- Checkout clean at `c8b2b60` (detached). Section 5's `git switch main` did not run.
- The kind cluster `reliefgrid` was kept up for the read-only diagnosis below, then removed with `make cluster-down`. This run is attempt 1 of the final gate. The fix for the harness race is PR #185 (not yet run live).

## Read-only diagnosis (cluster kept up, nothing changed)

**Shard 1 at 12:14Z.** Leader `storage-shard-1-1` at term 5. Three committed Voters (`1-0/0ee8e7eb…`, `1-1/a4be713a…`, `1-2/39fdeb9d…`), matching the manifest `peerIds`. All three `admitted=true` and `caughtUp=true`, applied = commit = last = 13495. Manifest phase BOOTSTRAPPED, ringVersion 1. The index keeps rising from app traffic (the app was never quiesced; chaos did not run).

**Timeline (events, evidence `date:`, Raft logs):**

| UTC | Event |
|---|---|
| 11:42:31–11:42:40 | `verify-S4.2` rollout restarts shard-1 (pods 2, 1, 0). `storage-shard-1-2` becomes leader at term 4 (its election lines were in the deleted instance's log). `1-0` runs 4 failed pre-votes (11:42:43–48; peers still had leader `1-2`) and follows `1-2` at 11:42:50 |
| 11:43:14 | Step 1 deletes `storage-shard-1-2`, **the current leader**, with `--wait=false` |
| ≤11:43:15 | `group_ok` and the "back as the same serverId" check pass, and the s42 load Job is created (Job event 11:43:15). The new container starts only at 11:43:16, so these checks sampled the **old, terminating** instance |
| 11:43:15 | The control's single `leader_pod` sample finds no Leader, `peer0` is empty, and the script aborts (evidence `date: 11:43:15Z`). The load pod is stopped by cleanup at 11:43:16 |
| 11:43:16.70 | `1-0` heartbeat timeout; its pre-vote fails (`1-1` still believed it had a leader; requestVote to the old `1-2` got EOF) |
| 11:43:17.749 | `storage-shard-1-1` wins term 5 (pre-vote 2/2, vote 2/2) |

Leaderless window: about 3.7 s (11:43:14 → 11:43:17.749). For shard 1 that is two leaderships (terms 4 and 5) in this window and 6 pre-vote campaigns in the current logs. The cause is deleting the leader pod, followed by normal heartbeat and election timeouts. No leadership transfer happens on shutdown.

**Internal-auth refusals.** 0 `internal auth refused` lines in the current logs of all nine storage pods (restartCount 0) and of the coordinator (started 11:42:08, covering the window). The previous `storage-shard-1-2` instance (11:42:32–11:43:14) was deleted with its log, so that instance cannot be checked. This is a spot check, not the `auth-log-checks.sh` completeness procedure.

**Readiness 503s.** `/readyz` returns 200 only when the FSM covers the leader's commit (probe period 2 s, failureThreshold 2). 503s appear intermittently on every shard, including during the passing course rounds. They are not tied to this failure.

**Positive control.** It reads the leader once (`peer0="$(leader_pod)"`, `scripts/verify-S4.2.sh:223`). The fallback on line 224 handles only "leader == target", not "no leader". A bounded wait for a non-empty leader would have found `storage-shard-1-1` about 2.7 s later, and the new `1-2` was serving :7000/:8080 from 11:43:16.6, so the dial would very likely have passed. That is inferred from timestamps, not a rerun.

**Verdict: harness race, not a product defect.** Step 1's restart check returns before the restart happens, so the control runs inside the failover window it never waits out. The leader loss itself is normal Raft behaviour.

**Proposed fix (not applied):**
1. Step 1: record the pod UID before `kn delete pod "$POD" --wait=false`. Wait for a new UID that is Ready before `wait_for 120 group_ok`, so the restart check observes the new incarnation.
2. Control: `wait_for 60` until `leader_pod` is non-empty (a helper that fails on empty). Then pick `peer0`; abort with "no shard-$SHARD leader within 60 s" if that times out, never with an empty name.
3. Harden `group_ok`: require a non-empty `leaderId` on every member, not just agreement, which an empty first sample can bypass.
4. Add an offline test for "no leader at the control" and "pod not yet replaced", then run a new gate under a new run ID. The failed run stays as recorded.

## Note: storagenode image ID changed after the verifier's rebuild (read-only check, time-boxed)

- All three storagenode builds that day were BuildKit cache hits (build history: 14/14 steps, about 0.1 s each). Round-1 reset (11:30Z), round-2 reset (11:36Z) and `verify-S4.2` (11:41Z) produced the **same `linux/amd64` platform manifest `sha256:7ca18b85b0b0b4519cbb42d0745075b90e0db5a4e819a43c346e575472a89373`**. The binaries and layers are identical.
- Only the top-level OCI image index differs (local image IDs `8630d08e…`, `7ae84847…`, `a8f493cc…`). The coordinator and tools local IDs also changed at every build (coordinator `a87bec…` → `b5ae263b…` → `b2e3c00d…`). Nothing in the build context changed between the builds (no git-ignored files; the only mtime changes were the identical `gen-check` regeneration at 11:19Z and an excluded `_test.go`).
- Likely cause: the index wraps per-build metadata (BuildKit's default provenance attestation records the build invocation and time), so each build gets a new image ID for the same content. kind loads by Docker image ID, and kubelet reports containerd's `import-<date>@sha256:…` digest, which is not the local Docker ID.
- Not resolved within the time box: why the coordinator's in-cluster ID stayed `7aafcb0b…` after its rebuild and restart while storagenode's changed. The ID change is therefore not evidence of a source or binary change. For identity, compare platform manifest digests, or use strict provenance (#158). Not fixed here.
