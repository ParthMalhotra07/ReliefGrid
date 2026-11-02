# A2.3 pre-PR review

- Protocol: harness uses real M8/coordclient Store operations, production
  workflow and broker helpers. It asserts canonical versions and reciprocal
  ownership; partial results cannot become empty/NO_CANDIDATE.
- Retry/dedupe: dropped replies retain operation identity; pre-apply loss is
  unknown, historical success does not reacquire a claim, RETRYING rejects
  stale and current-version confirmation, terminal replay emits one result.
- Concurrency: explicit proposer barrier, bounded contexts, isolated broker
  vhosts and geographic fixtures. Race count3 passed. Coordinator forward
  supervisor reconnects and joins its child on exit. Outer rollback attempts
  both storage and matcher restoration even if one restoration fails.
- Tests: full missing drivers fail explicitly. Subsets and proposer-only
  probes are labelled as such. The measured aggregate timeout changed from
  Go's default 10m to 20m for 50 physical replacements; per-case/rollout bounds
  and final-state assertions were retained.
- Scope: A/shared tests, scripts, Makefile and compact evidence only. No
  S-owned production source, frozen contract, dependency or protocol changes.
- Hygiene: make check, tagged lint and generation pass. Explicit staging
  excludes raw logs, local progress/playbooks/PDF and unrelated files.

Verdict: **NOT READY for full A2.3 acceptance / draft checkpoint only**.
Owner Hrishabh must expose deterministic log/apply and snapshot boundaries
in #88. After integration, run all 17 schedules/50 seeds on both backends;
do not remove a missing-case failure until its real fault driver is tested.
