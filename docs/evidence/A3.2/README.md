# A3.2 pod recovery

Stacked on A2.4 #86 so both the request and wakeup consumers receive the drain
behaviour in production source. Demo 8 acceptance also uses A3.1 #84's scripts.

SIGTERM stops HTTP admission while admitted API work keeps its own bounded
context. Server.Shutdown drains for at most 20 seconds, then closes remaining
connections and joins Serve. Readiness reports initialization/draining; temporary
dependency outages remain per-route errors. Liveness remains process-local.

The matching consumer stops admitting deliveries at cancellation, lets its one
admitted delivery finish within its existing 15-second work budget, then closes
the channel. Its dedicated connection lives through that drain. Unprocessed
prefetched deliveries return to RabbitMQ. Commit/confirmed handoff still precedes
acknowledgement. Cleanup RPCs are bounded; the pod grace remains 30 seconds.

The API PDB is policy/v1, minAvailable=1, and selects only this release's API.

Red evidence: admitted HTTP create returned 503 at shutdown; admitted broker
handoff remained PENDING; API PDB was missing. New barrier tests use the real M8
FSM and pinned live broker. No production fault flags are introduced.

Passing verification: core tests, PDB render, pinned-broker drain test and the
existing A1.3 live MQ suite. Affected packages passed race count3. The corrected
demo 8 records a real interrupted create, requires identical-input recovery,
replaces API/matcher pods under traffic and ends with pause/drain/strict invcheck.
Only compact final results/checks are committed; deployment dumps stay local.

Limit: a handler must honor its work deadline. Abrupt pod loss can still cause an
unknown client outcome, which uses the existing idempotent retry path. The joint
gate and post-merge checks remain pending. Both consumers now drain in actual
source; the prior patch artifact has been removed. The real-broker verifier
exercises both workers. Current recovery source combined with #84 passed a
fresh clean-deployment demo8; see acceptance/ for exact provenance and audit.
The PR is ready for review; human merges and main post-merge checks remain.
