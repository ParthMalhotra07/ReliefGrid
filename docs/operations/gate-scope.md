# Agreed core gate and separate enhancement acceptance

Recorded 11 November 2026 from both owners' agreed plan. The planned 12 November gate
proves the core system on its recorded source/images. Configuration for that run:
application aging 0s; autoscaling off with fixed API/matcher counts; #29 internal
authentication implementation after the gate. This is a **scoped core-system
gate**, not evidence that every A4.4/A4.5 enhancement is complete.

Open follow-ups retain their own evidence and remain open until accepted:

- Aging: planned 13 November, run verify-A4.4-aging plus the #159 rollout checker
  after its merge. Verify the approved coordinator/all-nine-replica cohort,
  application behavior and policy before enablement; the core gate stays 0s.
- Autoscaling: off for the core gate. If included in the final demonstration,
  validate it separately on13–14 November on the final cohort, including controller
  ownership, workload/termination audit and fixed API2/matcher2 restoration. If
  omitted, label it untested on that final cohort. Preserve qualified historical
  #123 results; do not erase them or promote them to final-revision acceptance.
- Auth #29: A owns internal/auth (#156, reviewed/approved); S supplies coordinator/
  storage/client/node wiring by 15 November. Both owners review contracts, app
  wiring, credentials/rotation and unknown-outcome behavior before acceptance.

The agreed freeze target is 17 November. These are planned dates, not completed
executions or reasons to defer independent preparation that can be done earlier.
Each later acceptance record must retain its own source/tree/image identity,
scope, configuration and raw results. No gate result is declared by this file.
