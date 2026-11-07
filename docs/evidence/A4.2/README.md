# A4.2 full acceptance — PASS

Full run live-20261107T133542Z passed on the branch rebased onto main acc020a, including merged #130. Four application exporters, all seven canonical queues, complete authoritative observations, all three dashboard queries, real asynchronous matching and durable cancellation passed.

The real shard-0 leader-kill run observed storage-shard-0-1 DOWN and storage-shard-0-0 as replacement leader. 41 samples, 6,484 committed synthetic writes, raw index-gap panel movement (peak 65 in the sampled fault window), unfinished workflow age +47.227s, and authoritative caughtUp on all nine recovered members. All owned scratch keys were tombstoned by the successful load helper; no unresolved intents remain. Raw Raft/FSM index gaps include barriers/no-ops and are not themselves proof of unapplied business data.

Three live screenshots were captured and visually inspected: cluster.png shows member DOWN/UP movement and index gap; matching.png shows growing unfinished age and business outcomes; storage.png shows mutation and outbox observations. Empty/unobserved series are labelled rather than fabricated. Screenshots include pre-fault startup catchup data outside the narrower sampled fault window. The cluster contract note still references the initial S PR126 split; the actual metrics implementation is merged #130.

The previous failed development attempts are retained as honest history under fault-checkpoint-20261107. The new noisy-forward regression reproduces the old blocked pipe (TimeoutError) and passes with file-backed logs. Logs use separate read/write handles, so neither undrained pipes nor shared seek offsets can block requests or overwrite logging. Static checks include five PromQL cases, seven strict fault/envelope tests and the noisy-forward test. make check, generation and static verification passed before the physical run.

Runtime API/matcher/UI images: a42-nov7-resume from this rebased branch; standalone storage/coordinator remain the independently reviewed fceb26e metrics images (the same S implementation subsequently rebased/merged as #130). The native broker chart overlay is approved #133. No storage image/PVC replacement was needed for this round. One complete current fault round is retained; verbose connection logs are archived locally.

PRE-PR REVIEW: READY for review. No S-owned or public contract changes, missing/flat observations cannot pass, and failed runs are not relabelled. The goal is not ticked as merged until owner review and post-merge checks.
