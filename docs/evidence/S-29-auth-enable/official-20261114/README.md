# #29 internal auth: official enable and rotation run, 14 Nov 2026

**Commit 7c4f8299bd8b9a6b59fa51824cf499e6124db706** (main after #179; `source.txt`). Run from a
detached worktree at that commit, clean before and after (`git status --porcelain` empty).
kind on the local laptop, on AC, sleep inhibited for the whole run; `make cluster-down` at the
end, no cluster left.

**Result: every required step PASSED.** The optional chaos suite ran in full with auth on and
had one failed check out of 22: a read-coverage criterion, not a safety check (see "Chaos").

| Stage (run attempt 3, UTC) | Duration | Result |
|---|---|---|
| a. `make demo-reset INTERNAL_AUTH=true` (fresh cluster, both releases with auth on) | 115 s | PASS |
| b. Auth really on: values, Secrets, every component's settings, one 401 and its refusal log line | 5 s | PASS 13/13 |
| c. All nine demos, one gate round on that deployment (`gate/gate.txt`) | 244 s | **PASS 9/9** |
| d. 401 checks and refusal logging | 8 s | **PASS 18/18** |
| e. `scripts/verify-S4.6b.sh` (seams on kind) | 117 s | PASS |
| f. Strict invcheck: `scripts/verify-S4.5.sh --kind` (drain, then the strict check) | 45 s | **PASS 8/8** |
| g. Rotation of both credential classes, three phases, per `docs/operations/internal-auth.md` | 352 s | **PASS 40/40** |
| h. Chaos suite with the app quiesced (optional) | 1986 s | 21/22 checks; 1 coverage FAIL |

Timeline: `timeline.tsv`, `driver.log`; every stage's checks: `logs/<stage>.log`; the exact stage
scripts and environment: `stages/`.

## make check on the exact commit

`make-check/make-check-attempts.tsv`, with each attempt's log:

| Attempt | Time (UTC) | Exit | Classification |
|---|---|---|---|
| 0 | 11:27:34–11:29:24 | 2 | known flake: `TestDeployInternalAuthTrue` and `…UnsetIsUnchanged`, only the coordinator port-forward line moved |
| 1 | 12:06:14–12:07:34 | 0 | PASS |

The rule, fixed before any rerun, allowed up to three reruns only if every failure was one of
the two known flakes, as decided mechanically by `make-check/classify-check.py`; any other
failure stopped the run. **Both flakes are test-only and are fixed in #180**:

- the deploy-switch golden comparison raced `reset.sh`'s background port-forward against its
  first poll (about one run in five);
- `TestHeartbeatAfterCloseIsRejected` saw a closed connection as ECONNRESET behind msgpack's
  `Cause()` under load (the handler assertion held).

No production code is involved in either.

## Auth on, the 401 checks and refusal logging (stages b, d)

- Both releases' computed values have `internalAuth.enabled: true`. Both Secrets
  (`reliefgrid-internal-auth`, `reliefgrid-node-auth`) have `token` and `sender-token` and no
  `previous-token`. The coordinator and the three storage StatefulSets run with
  `INTERNAL_AUTH_ENABLED=true`, and the API and matcher have `INTERNAL_AUTH_TOKEN_FILE`.
- Without a token: coordinator `GET /internal/ring`, `PUT /internal/data` and
  `GET /internal/data`, and a storage leader's `GET /internal/data`, all got **401
  UNAUTHORIZED**. The wrong credential class got 401: the node token at the coordinator, the app
  token at storage.
- With the right token: coordinator ring 200, write 200, read-back 200 with the value; storage
  read 200 with the same value.
- **Refusal logging:**
  - exactly six `internal auth refused` lines for the six refused requests, with reasons
    `missing` and `unknown credential` and credential classes `app` and `node`;
  - no in-cluster sender refused;
  - **no sender token in any pod log**, checked by scanning every pod's log for the token
    bytes; the scan was shown to catch a planted copy.

## Demos, seams, strict invcheck (stages c, e, f)

- `gate/gate.txt`: demo-1 … demo-9 PASS, no cordons left. The app demos' audit Jobs and
  handoff drains ran with the app token (#172, #175), and demo 6's invcheck Jobs with the
  mounted token (#174).
- verify-S4.6b: SCH01, SCH12, SCH13a and SCH13b PASS, with the readiness probe and `tests/seams`
  authenticated.
- verify-S4.5 (`check-jobs/kind-20261114T161618Z.txt`): 216 acknowledged, 0 lost. The drain ran
  with the app at 0 (nothing was pending this time). The strict check: claim invariants clean,
  no write during the walk, outbox pending 0 on every shard. The retained check Job ran with the
  app sender token mounted (`strict-invcheck.json`).

## Rotation (stage g)

Followed the merged runbook, for both credential classes together:
- the setup checks first (context, namespace, release, auth on, Secrets);
- token files in a 0700 directory, every file 0600;
- each phase's values file written as JSON with `values()`, passed with `-f`;
- storage restarted one StatefulSet at a time, then nine nodes UP and a leader per shard.

| | Mounted files in 14 pods (coordinator, 9 storage, 2 api, 2 matcher) | Old token | New token | After the phase |
|---|---|---|---|---|
| Before phase 1 | as intended (old, no previous) | accepted | **401** | — |
| After phase 1 (token new, previous old, sender old; receivers restarted) | as intended | accepted | accepted | demo-1 PASS, write/read-back OK, 0 in-cluster refusals |
| After phase 2 (sender new; api, matcher, coordinator, storage restarted) | as intended | accepted | accepted | same |
| After phase 3 (previous cleared; receivers restarted) | as intended | **401** (logged as `unknown credential`) | accepted | same |

- The mounted files were read by an ephemeral debug container (distroless `debug-nonroot`,
  pinned by digest), as hashes only.
- No old or new token appears in any pod log.
- Tokens appear in the logs only as sha256 prefixes.

## Chaos (stage h, optional)

`check-jobs/chaos/summary.txt`:
- all ten experiments and both negative controls ran with auth on;
- **every experiment: 0 lost, 0 unresolved unknowns, 0 stale reads**;
- both controls caught their faults;
- Chaos Mesh smoke, no NetworkChaos left behind, every node Ready.

**One check failed:** `restart-coordinator: coverage: shard 1 had 2 read(s) after the grace, want
at least 3`. This is the judge's minimum-sample rule (`chaos/judge/judge.go`, `MinCoverage`),
not a safety assertion.
- While the coordinator is down, every call waits out a 5 s timeout, so which shards the two
  readers sample is chance. The judge already counts write coverage across shards for this
  experiment for that reason, but still counts reads per shard.
- The same experiment without auth (12 Nov gate) had 3881 reads, 6397 acknowledged writes and
  33.7 s recovery. Here it had 3898, 6573 and 32.9 s, with the first ack 2.2 s after the heal
  (2.6 s on 12 Nov).
- So auth did not slow recovery, and the shortfall is sampling. The app was restored afterwards.

## Deviations, in order

1. **Run attempt 1** (`run-attempt-1/`) stopped at stage b on a defect in the check script, not
   the system. A `refusals | grep -q` pipeline under `pipefail` exited 141 (SIGPIPE) although the
   expected refusal line was present (`b-auth-on.log`). Every `| grep -q` was replaced with a
   SIGPIPE-safe grep. The cluster was deleted and the run restarted from a fresh reset.
2. **Run attempt 2** (`run-attempt-2/`) stopped at stage b on another check-script defect:
   `refusals` itself failed under errexit and pipefail when a pod had no refusal lines (grep exit
   1). Fixed so that no match is not an error. Before restarting, every check helper was
   validated live on that cluster: stage b passed, the token scan caught a planted copy, the
   in-cluster filter worked, and the mount check ran on a storage pod and the matcher. The
   cluster was deleted and the run restarted from a fresh reset (attempt 3, above).
3. **Paths and whitespace:** `$HOME` was replaced with `~` in every committed file, and trailing
   whitespace was stripped from the raw tool logs (the repository's whitespace check).
   `off-repo/ORIGINALS.sha256` has the checksums from before both edits.
4. **Off-repo files:** the bulky raw files stay off-repo: demo 5's and 8's workload JSONL and
   settled requests, and the full chaos suite reports. `off-repo/SHA256SUMS.full-run` lists every
   file of the run root with its sha256, size and location.

## Qualification (#183, added after the run)

The negative log findings above depend on every pod log having been read successfully. They
are: no sender token in any pod log (stage d), no old or new token in any pod log (stage g), and
no in-cluster sender refused (stages b, d, g). The helpers in `stages/lib.sh` did not enforce
that. `no_token_in_logs` returned 0 when a pod's `kubectl logs` failed, and `refusals` /
`in_cluster_refusals` used `|| true`, which also hid pod list and log read failures (#183
reproduces this offline). No read failure is known from this run, and the planted-copy check
shows that the scan matched when logs were read. But the run does not record per-pod
read success, so these findings are observations, not proof of complete collection. They also
cover only the pods that existed when each check ran: previous logs of restarted containers were
not scanned, and pods replaced by the rotation's rollout restarts were gone. The scripts and logs of this run are left
unchanged. Later gates use `scripts/auth-log-checks.sh` (#184), which fails closed and writes
a completeness summary.

## Not covered

- Raft TCP is not covered by this header (network policy is its boundary).
- Operator `/admin` and `/mgmt` keep their bearer tokens.
- Enabling auth on an existing deployment (the runbook's quiesced-app procedure) was not run:
  this run enabled it with a fresh reset.
