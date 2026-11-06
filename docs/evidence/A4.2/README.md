# A4.2 draft status — full goal INCOMPLETE

Current draft is rebased onto merged UI main b454dc0. Private metrics and the
UI event projection coexist; current full check, generation and static checks
pass. Storage dashboard queries now implement S113's exact contract, including
leader/success/freshness/reachability filtering, and coordinator heartbeat/
forwarding families. Pinned promtool tests prove a lagging follower cannot
inflate the leader count and failed/stale/down observations are absent rather
than fresh zero. An observed empty outbox remains0.

The live application screenshots/results under live-20261106T063905Z are
historical evidence from before this rebase. They are not presented as a new
full-goal fault run. Full acceptance still needs S's exporters from draft126 on
main, then a fresh observed leader-kill health/lag/workflow-age run. Draft121
must not be marked complete or merge-ready solely from this query wiring.
