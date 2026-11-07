# Live demonstration rehearsals

Status: preparation only. No rehearsal is recorded as completed by this document.
The course requires a live demonstration. The recording is a backup and must
include every deployment step. The classroom target is kind; cloud is parked.

## Before a rehearsal

- Agree the source SHA and intended disposable kind cluster. Record both.
- Use the pinned tools from [CONTRIBUTING](../../CONTRIBUTING.md) and
  [tools.mk](../../tools.mk); run `make doctor`.
- Follow the [Makefile](../../Makefile): `make dev`, or `make demo-reset` for a
  clean disposable dataset, followed by `make app-deploy` when needed. Confirm
  standalone storage ownership (`storage.enabled=false` in the app release).
- Run the [gate](../../scripts/demos/gate.sh) against the agreed revision and
  retain its actual reports. Historical gate evidence does not certify a new SHA.
- Confirm public repository access from a signed-out browser. Keep a tested
  offline kit available only if all nine offline demos have passed on its SHA.
- Record terminal and UI from the first deployment command. Keep credentials
  outside the recording. Test projector readability and microphone input.

## Three rounds with swapped presenters

| Round | Parth | Hrishabh | Required observation |
|---|---|---|---|
| 1 | App walkthrough and demos 2/3/5/8/9 | Storage walkthrough and demos 1/4/6/7 | Both can deploy, find evidence and explain their own components |
| 2 | Explain and operate the storage side | Explain and operate the app side | Both can answer questions and recover without the other taking over |
| 3 | Start from the documented setup; share randomly assigned demos | Ask and answer questions; share randomly assigned demos | Reproducible setup, clear transitions, working backup and honest failure handling |

Run every required demonstration across each complete round. Record actual
start/end times; use them to choose the presentation order rather than guessing
the runtime. Record interruptions and repeat any failed step with its cause.

## Questions both presenters should answer

1. What does an acknowledged write prove, and what does an unknown result require?
2. How do intent, durable closure and reciprocal claims prevent double allocation?
3. Why must an empty storage disk remain unadmitted?
4. How do outbox commits, routed confirms and manual acknowledgement form a durable handoff?
5. What does API 202 mean while matching continues? Why can CANCELLING remain visible?
6. Why are partial scans and missing metric observations different from empty results?
7. How are API and matcher scaling controlled independently, and how is shutdown verified?
8. Which invariants does invcheck establish, and what does a not-quiescent result mean?
9. Which revision and test evidence support each claim being shown?

## Record actual results

Create one note per completed round under `docs/evidence/A6.2/<run-id>.md`:

```text
Round / presenters:
Source SHA / cluster / tool versions:
Start / end / deployment duration:
Deployment commands and result:
Demo 1..9: result and evidence links for each:
Swapped-presenter questions answered:
Failures, cause, fix PR and repeated result:
Recording location and playback check:
Open actions, owner and readiness decision:
```

An unexecuted round stays pending. A failed or missing demo stays failed or
missing. A6.2 is complete only after three real rounds and their notes are
reviewed; this runbook is preparation for that work.

## Final package handoff

Check a clean clone can follow the setup commands; README, contracts, ADRs,
report, current evidence and rehearsal notes are accessible; the backup recording
shows deployment and plays correctly; repository visibility is public; and the
actual submission receipt is recorded. `make cluster-down` is the supported
teardown command. See the [demo scripts](../../scripts/demos) for assertions.
