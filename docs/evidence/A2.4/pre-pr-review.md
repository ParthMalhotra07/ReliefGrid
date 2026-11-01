# A2.4 pre-PR review — READY

Reviewed implementation against main c5fe7ca, including final source 2de2d23.

1. **Invariants:** authoritative reads and committed CAS/claim/closure rules
   remain the ownership boundary. Historical operation results are diagnostic
   only. Unknown outcomes retain exact commands/intent. Partial/failed scans
   cannot advance the cursor or prove absence. The live final run confirms
   current reciprocal claims, terminal no-live-claim state and late CLOSED.
2. **Idempotency:** schema-validated identity-only hints; repeated processing
   rereads state. Unknown resource-wake results retry identical commands.
   Routed mandatory confirms precede source/dead-original acknowledgement.
   Retry copy loss/duplication cannot grant ownership. Broker 25-hop acceptance
   retains delivery-limit20 and businessRetryCount1.
3. **Concurrency:** recovered abandonment uses the read snapshot's conditional
   version. Barrier tests protect a newer intent, a winning matching helper,
   committed cleanup and an immediate wake schedule. The losing helper cannot
   overwrite the exact version being completed. Worker call/work budgets and
   shutdown joins are bounded; affected packages pass race count3, with the
   final periodic-age path also passing count3.
4. **Errors:** transient reads/handoffs stay errors until durable handoff;
   unknown outcomes never take the known-concurrency shortcut. Poison reject
   reason/depth are visible. If transport publish fails, the original stays
   unacknowledged with bounded backoff. Permanent absence rejects once.
5. **Tests:** initial missing behaviour, public cancellation and real cluster
   failure outputs retained. The cluster failure became barrier regressions.
   Final old-claim case persists a newer future schedule without a hint before
   killing the worker. All 13 boundaries, public cleanup, early wake, complete
   age audit and strict invcheck pass. No assertion was weakened or test
   skipped; ordinary test budgets remain. The 25-hop test's explicit 180-second
   budget covers its measured fixed-TTL path.
6. **Scope:** app/workflow/mq/reconcile/Helm app definitions and verifier/docs
   only. No contracts, S-owned source, storage chart or .github edits. Embedded
   event schemas are byte-identical to frozen contracts. Fault hooks are in a
   separate test image and absent from production app images. Existing matcher
   labels satisfy the merged coordinator NetworkPolicy.
7. **Hygiene:** make check passes (both lint runs 0 issues; ordinary and chaos
   race suites pass); generation has no diff. Commits use Parth's author and
   Conventional Commit goal IDs, with no attribution trailers. Only selected
   concise evidence is included; no credentials, kubeconfig, binaries or local
   playbook/progress/viva files staged.

Resolved review findings: retry metadata invalidating a concurrent winner,
cleanup or immediate wake; age checks behind future retry gates; abandonment
rereading and targeting a newer intent; future hint timestamps postponing work.
Red-first regressions and final green output are indexed in README.md.

READY for PR. CI, owner review and post-merge main verification remain required;
this does not assert that the joint gate has run.
