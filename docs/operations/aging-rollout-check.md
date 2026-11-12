# Read-only aging rollout prerequisite check

Keep app.agingInterval=0s until S and A agree the capability/policy and the
coordinator plus all nine storage replicas are verified. This checker performs
no deployment, enablement, mutation probe or Raft membership operation.

First obtain successful strict build-provenance records for coordinator and
storagenode from the approved source cohort. The recorder is a separate PR;
these scripts exchange versioned JSON and have no code-import/branch dependency.
Print the canonical JSON hashes for the exact supplied records, in argument order:

```sh
python3 scripts/aging_rollout.py --record-hashes \
  --build-record /tmp/cohort/build-provenance.json
```

Create an owner-reviewed approval file with these fields. This deliberately
incomplete example cannot pass until real approval/source/hashes are supplied:

```json
{
  "schemaVersion": 1,
  "sourceCommit": "",
  "context": "kind-reliefgrid",
  "namespace": "reliefgrid",
  "capability": "AgeRequest",
  "approvedBy": {"A": "", "S": ""},
  "interval": "1m",
  "canonicalBuildRecordSHA256": []
}
```

Owners approve capability after source review; neither a label nor Pod readiness
proves that AgeRequest works. The declarations are not digital signatures or
automatic verification of GitHub approvals. Canonical hashes intentionally cover
JSON meaning (sorted compact JSON), unlike the runtime binder's raw-file hashes.
Use canonical integer-unit policy durations such as 1ms, 1s, 1m or 1h; this small
approval format does not parse every valid compound Go duration.

```sh
python3 scripts/aging_rollout.py --repo . \
  --approval /tmp/aging-approved.json \
  --build-record /tmp/cohort/build-provenance.json \
  --context kind-reliefgrid --namespace reliefgrid \
  --output /tmp/aging-rollout-UNIQUE.json
```

Requires Git history containing the approved commit and the merged #150 seam,
kubectl read/list permissions and read-only exec of /storagectl status. Git must
prove the approved commit descends from e50685db3fb4a9b683c5e7f3688583dd545e570e;
the recorded source tree must match that commit. A later revert cannot be ruled
out by ancestry alone, hence the explicit owner capability approval is essential.

Two observations must cover one available coordinator, three fully rolled-out
StatefulSets and exactly nine distinct Ready storage pods with approved immutable
images. Pod/controller ownership, revision hashes and observed generations must
match. Every replica must be admitted, caught up and in its expected shard, with
three committed voters and one agreed current leader/term per shard. Node status
must identify the pod actually queried. Advancing applied indexes are permitted:
the checker never compares SQLite command indexes with Raft log indexes.

The deployed matcher configurations must explicitly remain at 0s. Pod/image and
leader identities must agree across observations. Selected state is retained for
rejudging; unrelated env, Secrets, annotations and command stderr are omitted.
Each kubectl request has a 10-second timeout, storage status a 3-second deadline,
and each host subprocess a 20-second timeout. Existing output is not overwritten.

A PASS means this **storage rollout prerequisite observation** passed under the
supplied approval. It does not verify the coordinator heartbeat routing view,
execute aging, certify application binaries/functional behavior, replace the
ordinary demo preflight, or enable anything. Run the existing aging functional
Verify and integrated gate separately. Aging remains disabled until the complete
rollout/policy/functional checks have been accepted by the owners.

Offline fixtures: `python3 scripts/aging_rollout_test.py -v`; ordinary make check
also executes an embedded-input Go wrapper. No live rollout is established by
those fixture results.
