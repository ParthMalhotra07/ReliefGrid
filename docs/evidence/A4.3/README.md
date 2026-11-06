# A4.3 application autoscaling evidence

Final live run: `20261106T071340Z`, **PASS**. Exactly20,000 persistent valid
duplicate hints routed with zero failed checks, zero failed HTTP requests and
zero dropped iterations. Ready matchers rose from1 to5
and returned to1 after both ready/unacknowledged counts drained; peak sampled
ready backlog1083; no restarts observed. No manual scaling.

The chart configures API CPU HPA2–6/60% and KEDA matcher1–8. Static tests cover
single controller ownership, default manual demo counts and refusing app-owned
storage. Five negative/positive acceptance tests reject empty observations,
desired-but-not-Ready scale-up, missing broker statistics, incomplete drain and
observed restarts. Final check.txt, gen-check.txt and static.txt are passing real
command output. The full check's earlier storage failure is retained in
red-and-diagnostics.txt and S-owned issue122; it is not claimed fixed here.

Normal demo ownership was restored after acceptance: API2/matcher2, no app HPA
or ScaledObject. Nine storage replicas remain Ready. runtime-images.json records
controller/application image IDs and the unchanged broker PVC identity.

This is a chart-only acceptance using the already deployed a42-20261106-090066f
app images (main plus private application observations). KEDA reads RabbitMQ
and HPA reads metrics-server; neither uses A4.2 application exporters. No A4.2
source commits are stacked in this branch. This does not claim full A4.2
storage-dashboard/fault acceptance or a user-request throughput benchmark.

source.json inside the run records the precommit main head and working-tree
changes. The PR contains the corresponding implementation and this evidence.
Only one final round is retained; development rounds are archived locally.
