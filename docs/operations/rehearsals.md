# Live demonstration rehearsals

Status: preparation only. No rehearsal is recorded as completed by this document.
The classroom target is local kind; cloud is parked. A live demonstration is
mandatory. The recording is a backup, must show deployment from scratch and
must be checked for successful playback.

## Before a rehearsal

- Agree the tested source SHA and intended disposable kind cluster. Record the
  dirty-tree status and exact deployed image references as well as the SHA.
- Install the pinned tools from [CONTRIBUTING](../../CONTRIBUTING.md) and
  [tools.mk](../../tools.mk), plus Bash, make, Python 3, jq, curl, GNU timeout
  and a C compiler for Go race tests. Docker must be reachable from the Linux
  or WSL2 shell. On an uncached machine, allow network access for Go modules,
  container images and the Traefik dependency chart; `make doctor` does not
  check every auxiliary prerequisite.
- Start recording before the first deployment command. Keep credentials out of
  the recording and check projector readability and microphone input.
- Use the complete disposable-cluster path below. **`make demo-reset` deletes
  and recreates the entire selected kind cluster and everything running on it**;
  it is not just a dataset reset. Review the selected cluster and its confirmation
  prompt before proceeding. The default context is `kind-reliefgrid`.

```sh
make doctor
make check
make demo-reset
kubectl --context kind-reliefgrid -n reliefgrid get pods
curl --fail 'http://localhost:8080/api/v1/requests?limit=1'
```

The [reset script](../../scripts/demos/reset.sh) creates the cluster, builds and
loads storage/coordinator/tools images, installs standalone storage and runs its
bootstrap Job, waits for nine storage nodes UP, then prepares chart dependencies
and builds/deploys the app. **It already deploys the app** against that storage
release with `storage.enabled=false`; a fresh run does not need a second
`make app-deploy`. In contrast, `make dev` only creates/selects kind. A later
`make app-deploy` is an app upgrade requiring existing standalone storage, not
a complete fresh installation.

Run the [automated gate](../../scripts/demos/gate.sh) on the agreed revision and
retain its actual reports. By default, `make gate` performs **two automated
rounds**, each with a destructive full-cluster reset before demos 1 through 9.
These two automated gate rounds do not count as the three joint presenter
rehearsals below. Historical gate evidence does not certify a new SHA. Arrange
the gate before a presenter round or after its recovery sequence; never insert
its reset between demos 6 and 7.

Confirm public repository access from a signed-out browser. Keep a tested
offline kit available only for its recorded artifact/SHA after all nine offline
demos pass; historical offline acceptance is not certification of later images.

## Three joint rounds with swapped presenters

| Round | Parth | Hrishabh | Required observation |
|---|---|---|---|
| 1 | App walkthrough and demos 2/3/5/8/9 | Storage walkthrough and demos 1/4/6/7 | Both can deploy, find evidence and explain their own components |
| 2 | Explain and operate the storage side | Explain and operate the app side | Both can answer questions and recover without the other taking over |
| 3 | Start from the documented setup; share randomly assigned demos | Ask and answer questions; share randomly assigned demos | Reproducible setup, clear transitions, working backup and honest failure handling |

Run all nine required demonstrations in each complete presenter round. Record
actual start/end times and use them to choose the presentation order. Preserve
the storage recovery handoff: **demo 7 must immediately follow demo 6, without
an intervening reset, gate, repeated demo 6 or unrelated demo**.

## Demo 6 to 7 handoff and interrupted recovery

[Demo 6](../../scripts/demos/demo-6.sh) deliberately cordons the stopped leader's
worker and leaves its replacement storage pod Pending. On success, it stores the
handoff in the worker annotation `reliefgrid.io/demo6-handoff`, including the
shard, pod, worker and identity. This is expected intermediate state, not a
finished healthy round. [Demo 7](../../scripts/demos/demo-7.sh) consumes that
handoff, uncordons the worker, and verifies replay and recovery. Rerunning demo 6
while a replica remains missing is refused to protect the surviving majority.

If the presentation is interrupted after demo 6, pause the sequence and preserve
its evidence. Inspect the default cluster without clearing the handoff:

```sh
kubectl --context kind-reliefgrid -n reliefgrid get pods -o json | jq -r '.items[] | [.metadata.name, .metadata.uid, .status.phase, (.spec.nodeName // "")] | @tsv'
kubectl --context kind-reliefgrid get nodes -o json | jq -r '.items[] | select(.metadata.annotations["reliefgrid.io/demo6-handoff"] != null) | [.metadata.name, (.spec.unschedulable // false), .metadata.annotations["reliefgrid.io/demo6-handoff"]] | @tsv'
```

Before running recovery, require exactly one still-live matching handoff: its
named worker is still cordoned, its named replacement pod remains Pending and
unscheduled with the annotated UID, and the recorded shard/identity agrees with
the interrupted demo 6 evidence. Validate those observations with the storage
presenter. For another agreed cluster/context, use that selection consistently.

If the handoff is stale, ambiguous or missing, or another replica is unexpectedly
absent, **stop this interrupted-round procedure and involve the storage owner**.
Do not run demo 7, clear annotations or reset merely to continue the presentation.
Standalone demo 7 supports healing a stale handoff and then starting a new fault,
or a fresh fault from a healthy shard; that behavior is not proof that it resumed
the interrupted demo 6. Retain the failed/interrupted result until its specific
recovery and any retest are established.

Only after validating the single still-live handoff, continue with:

```sh
make demo-7
```

Demo 7 consumes that handoff and has cleanup on exit. A failed recovery remains
a failure; pause the sequence and work with the storage owner rather than
starting another fault to make the presentation continue.

Before repeating a failed step or proceeding, retain successful recovery proof:
the recovered replica reaches the shard's captured leader commit position and
reports `caughtUp`; the replicas settle to equal committed state at the same
applied index; the missed writes read back from the recovered replica; and the
coordinator reports **all nine storage nodes UP**. Demo 7 asserts those conditions
and identity/leader agreement. A Running pod, Kubernetes Ready node or Ready pod
alone does not establish recovery. If any required proof fails or is missing,
record the interruption and unresolved owner action; do not claim the round passed.

## Enhanced-gate final fixed-profile evidence

This is an execution requirement, not retained evidence that restoration has
already passed. The historical A4.3 round finishes its measured autoscaling
state at API2/matcher1. Its audit restores saved workers and removes the KEDA
pause; neither that cleanup nor a successful Helm wait proves fixed API2/matcher2.

After the measured scaling round and strict audit, **before any further reset**,
the presenter must explicitly restore the fixed profile. First confirm the app
release does not own storage; never remove an app-owned storage subchart as part
of this procedure. For the default deployment:

```sh
helm --kube-context kind-reliefgrid get values reliefgrid -n reliefgrid --all -o json | jq -e '.storage.enabled == false' >/dev/null
helm --kube-context kind-reliefgrid upgrade reliefgrid deploy/helm/reliefgrid -n reliefgrid --reset-then-reuse-values --set storage.enabled=false --set app.autoscaling.enabled=false --set app.api.replicas=2 --set app.matcher.replicas=2 --wait --timeout 5m
kubectl --context kind-reliefgrid -n reliefgrid rollout status deployment/reliefgrid-api --timeout=120s
kubectl --context kind-reliefgrid -n reliefgrid rollout status deployment/reliefgrid-matcher --timeout=120s
```

Keep a new run-specific `fixed-profile/` evidence directory. Retain only selected
non-secret fields from the actual observations:

- Context, cluster UID, timestamp, Helm release revision/chart and resolved
  `storage.enabled`, `app.autoscaling.enabled`, API and matcher replica values.
- Both Deployment UIDs/generations and status. Require observedGeneration at
  least generation; desired, total, updated, Ready and available replicas all
  exactly 2; unavailable replicas zero.
- Current ReplicaSet-to-Deployment ownership and exactly two nonterminating
  Ready pods per component, owned by the current ReplicaSet. Retain their UIDs,
  declared image references and observed container image IDs.
- Successful HPA and KEDA ScaledObject observations showing no controller still
  targets either Deployment, including paused controllers. If a CRD is absent,
  establish that through successful discovery; never treat an arbitrary API
  error as an empty controller list.

Use a bounded wait for those predicates, not an assumed fixed sleep. If any
observation is missing, stale, incomplete or fails, restoration remains failed
or unverified and the enhanced-gate result must not claim that requirement passed.
Publish whitelisted workload/controller fields, not raw Helm manifests/values or
full pod specifications that can include credentials and annotations.

Bind the run to its clean source/build-input manifest, build commands, image
identities and kind imports. Capture the audit tools Job's observed image ID
before its cleanup, alongside API/matcher image IDs. Where immutable build-to-
source mapping is absent, record the limitation rather than inferring it from
tags. These new artifacts do not retroactively establish the historical A4.3
restoration, replace the strict invariant audit, or count as presenter rehearsals.

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

Create one note per executed round under `docs/evidence/A6.2/<run-id>.md`:

```text
Round / presenters:
Tested source SHA / source dirty status / cluster / tool versions:
Deployed image references and observed image IDs:
Start / end / deployment duration:
Deployment commands, destructive-reset confirmation and actual result:
Demo 1..9: actual result and evidence links for each:
Demo 6 -> 7 handoff / interruption / recovery proof:
Swapped-presenter questions answered:
Failures and cause / fix PR:
Retest source SHA and dirty status / retest deployed images / actual repeated result:
Recording location / deployment-from-scratch coverage / playback check:
Final stable tag and exact commit / whether this round tested that artifact:
Open actions, owner and readiness decision:
```

An unexecuted round stays pending. A failed or missing demo stays failed or
missing. A fix PR number alone does not identify the artifact that passed: retain
the retested revision and images. A6.2 is complete only after three real joint
rounds and their notes are reviewed. This runbook is preparation for that work.

## Final package handoff

Check that a clean clone follows the documented setup; README, contracts, ADRs,
reviewed report, current evidence and real rehearsal notes are accessible; the
backup recording shows deployment from scratch and plays correctly; and the
submitted public GitHub repository URL and actual submission receipt are recorded.
Report length and format remain pending professor confirmation. Record the final
stable tag and exact commit, including any changes after rehearsals that need
retesting. None of these outcomes is established merely by writing this runbook.

`make cluster-down` **deletes the selected local kind cluster and its data**;
it is not just a process shutdown. See the [demo scripts](../../scripts/demos)
for assertions and supported recovery paths.
