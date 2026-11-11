# Capture final fixed-profile evidence

After the autoscaling round, drain/audit and the operator's fixed-profile
restoration, run the read-only collector from the exact checkout being recorded:

```sh
python3 scripts/capture_fixed_profile.py \
  --context kind-reliefgrid --namespace reliefgrid --release reliefgrid \
  --tools-job THE-RETAINED-SUCCESSFUL-AUDIT-JOB \
  --output /tmp/reliefgrid-final-fixed-profile-UNIQUE-RUN
```

Replace the tools Job name and output path. Output must be a new directory; keep
it outside the checkout so writing evidence does not change its dirty status.
Use the actual Helm release name, which is not necessarily the resource name
prefix. kubectl, Helm and Git must be available, with read access in the selected
context/namespace. KEDA's ScaledObject API must be available: a missing CRD, RBAC
denial, timeout or failed list is a failure, never proof of no autoscalers.
No deployment, scaling, pause, drain, install, deletion or other mutation is run.

Retain the successful tools/audit Job and its pod until capture completes, before
TTL/cleanup removes their image IDs. The collector requires one successfully
completed owned pod with clean exits and SHA-256 image identities; a retried or
multi-pod Job needs separate evidence review rather than silently choosing one.
The Job's exit status is observed, not a replacement for its strict invcheck
report. Keep that report and measured-round/termination evidence separately.

`fixed-profile.json` records two read passes around the tools observation. Both
must agree on the sanitized state: fixed app Helm values, storage.enabled=false,
autoscaling disabled, current observed Deployment generations with exactly two
updated/Ready/available replicas each, current ReplicaSet ownership/revision,
four nonterminating Ready pods, requested images and observed immutable image
IDs. Namespace-wide HPA/ScaledObject lists rule out controllers targeting these
Deployments even when controller labels differ. Selected environment variables,
Secrets, arbitrary annotations, raw Helm output and error stderr are not saved.
Each kubectl request has a 15-second server timeout and each subprocess a
25-second host timeout. The collector is bounded and exits nonzero on failure.

The source SHA/tree and dirty flag describe the **collector checkout**, not an
inferred source of deployed images. Build/source binding is explicitly
NOT_ESTABLISHED. Attach existing immutable build records separately before
claiming reviewed-source acceptance; image tags, image IDs or a clean collector
checkout alone do not establish that binding. Dirty checkouts are recorded as
such. The two read passes are sequential observations, not an atomic Kubernetes
snapshot or a promise that the state cannot change immediately afterward.

A PASS establishes only the recorded fixed API2/matcher2 observation. It does
not execute or certify the enhanced gate, aging rollout, load results, strict
invcheck, protocol invariants or historical evidence. Failures preserve a
sanitized result when collection has started; existing artifact directories are
never overwritten. The previous #123 run's final restoration remains unverified.

Offline verification (no cluster required):

```sh
python3 scripts/capture_fixed_profile_test.py -v
go test -race -run TestFixedProfileCaptureRegressions ./scripts
```
