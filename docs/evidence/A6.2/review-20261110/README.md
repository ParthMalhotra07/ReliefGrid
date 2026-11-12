# Rehearsal runbook review corrections

Review of 68da993 identified an incomplete fresh installation, destructive reset
scope presented as dataset cleanup, an undocumented demo6-to7 recovery handoff,
and missing tested-artifact fields. This is a documentation correction, not a
new protocol implementation or a fabricated completed rehearsal.

Source comparison against merged main42741b1 verified:
- scripts/demos/reset.sh deletes/recreates kind, builds/loads storage images,
  installs/bootstrap-waits storage, then prepares and deploys the app.
- scripts/demos/gate.sh defaults to two clean-reset automated rounds.
- demo-6.sh preserves its worker cordon/Pending replica via the node annotation;
  demo-7.sh consumes it and asserts catch-up, same-index equal dumps, missed-write
  readback and all nine nodesUP. A Ready node/pod alone is insufficient.
- make cluster-down deletes the selected cluster, rather than only stopping apps.

The runbook now specifies complete prerequisites/setup, immediate6->7 ordering,
inspection and interruption recovery, and exact source/dirty/image/retest/stable
artifact identity. Three joint presenter rehearsals, recording, playback and
submission remain pending external execution. No kind/reset/gate/demo commands
were executed to verify this documentation edit. The fenced shell examples were
syntax checked; local links resolve. No prose-mirroring regression was added.
