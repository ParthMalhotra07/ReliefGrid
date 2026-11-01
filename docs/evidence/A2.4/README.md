# A2.4 — durable reconciliation and wakeups

Source: branch `a/A2.4-reconcilers`, based on main `c5fe7ca` (includes #77,
#80 and #82). Implementation commits `ada4f4e`, `2de2d23`. A3.1 #84 is
independent; this PR is not stacked. Application workers use existing labelled
matcher pods. No storage contracts, S-owned implementation or policy edits.

## Red-first evidence and corrections

- `public-cleanup-baseline-red.log` and `public-cleanup-baseline/`: the prior
  app left public cancellation CANCELLING without a manual match helper.
- `initial-red.log`, `workflow-red.log`, `wakeup-red.log`, `resource-red.log`,
  `replay-red.log`, `health-red.log`: the initial missing recovery behaviours.
- `cluster-first-failure.log` and `cluster/20261101-first/first-failure.json`:
  a known losing concurrent helper updated retry metadata and invalidated the
  winner's exact conditional version. The first live run is a failure, not
  acceptance evidence. Barrier tests reproduce that race, cleanup handoff
  invalidation and immediate wake postponement (`concurrent-red.log`,
  `cleanup-concurrent-red.log`, `immediate-wake-red.log`).
- `periodic-age-red.log`: periodic filtering hid an old immutable MATCHING
  intent behind a newer future schedule. The fix lets recovery inspect intent
  age before backoff, using the original read snapshot when fencing it.
- `future-hint-red.log`: a future event timestamp must not postpone work.
- `broker-compile-red.log`, `final-helper-lint-red.log`: verifier build/lint
  errors corrected before acceptance; neither is a feature-failure claim.

No test was removed, skipped or weakened. Existing ordinary broker budgets
remain; the new 25 x fixed 5-second transport test has an explicit 180-second
budget. The live cluster bounds are unchanged across corrections.

## Final acceptance

- `final-check.log`: `make check`, both lint passes `0 issues.`, all ordinary
  and chaos race tests pass, including the standalone test-worker build.
- `final-gen.log`: `make gen-check` passes with no generated contract diff.
- `final-race-three.log`: workflow, reconcile, mq and matcher race tests x3;
  `final-periodic-race-three.log`: the final periodic-age regression x3.
- `final-broker.log`: pinned RabbitMQ 4.3; TTL returns to matching.wakeups;
  malformed input is dead-lettered; corrected replay confirms before dead ack;
  25 failures / 25 confirmed deferrals recover with deliveryLimit20 and
  businessRetryCount1. `A2.4 VERIFY: ALL PASS`.
- `final-cluster.log` and `cluster/20261101-accepted/`: both public cleanup
  cases, 120-second future waiter wakeup, all 13 crash/restart cases (including
  an old claim hidden behind a future schedule), complete 60-second age audit,
  exact closures and late CLOSED probes pass. Strict zero-backlog invcheck is
  clean and quiescent. `A2.4 CLUSTER VERIFY: ALL PASS`.

The live setup began with a clean `demo-reset`. Corrections used app-deploy
without erasing durable data, and unique synthetic fixtures on each run. The
latest deployment's selected image IDs, pod placement and shard status are in
`accepted-deployment/`; repeated build/node/Deployment dumps are omitted.

## Bounds and limitations

Periodic pages: 32 per participating shard, 16 visits per step, 2-second poll,
15-second work budget, 2-second remote-call deadline, failure backoff up to
30 seconds. Resource hints accelerate at most 16 compatible nearby WAITING
schedules. Due scans remain the fallback. The acceptance workload must settle
within fixed per-case deadlines and have no overdue workflow beyond 60 seconds;
this is not a latency guarantee under unbounded load or unavailable shards.

Poison work remains in matching.dead for an explicit single-event corrected
replay, with reason/depth alerts. No automatic poison purge, broker-node HA or
SSE delivery is claimed. The cluster fault image is test-only; production has
no pause/kill flags. Joint gate and main post-merge verification remain pending.
