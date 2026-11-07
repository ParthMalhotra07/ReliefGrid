# A5.4 draft checkpoint — NOT READY

Revisioned source, pinned image identities/archives, packaged charts, prebuilt
outbox drainer and isolated runner are implemented. Integrity/path/name/drainer/
precise-export-failure unit tests PASS5cases. Full makecheck/genPASS. Red-first
output and compact integration results are retained here. Large artifacts remain
outsideGit; no kubeconfig/Secret/binary/archive is committed.

Live bootstrap eventually proved all5nodesReady on an internal Docker network,
with a local DNS anchor and private API access. Source tests reject missing/corrupt
assets and prevent silently falling back to networked Go builds. Default demos
continue to build the helper normally. Actual source/image identities are recorded
in each local artifact manifest, not guessed from a mutable tag.

Full acceptance is INCOMPLETE: image archive import hit a Docker/containerd
multi-platform content hole. All9offline demos have NOT run. Three integration
attempts were used; the next batch must export complete Linux/amd64 archives,
validate their closure first and rerun the real offline acceptance. See the
linked A5.4 follow-up issue. No invariant or offline check was weakened.

PRE-PR REVIEW: NOT READY for merge. Draft PR preserves the tested implementation
and exact remaining failure. Original development cluster/PVCs were not reset.
