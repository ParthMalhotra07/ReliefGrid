# Deployment and live demonstration

## Local platform and ownership

The supported classroom target is kind: one control-plane node, three data workers
and one infra worker. Data pods use placement/anti-affinity; RabbitMQ and local
platform services use the infra worker. Helm installs the standalone storage
release before the app. The app uses storage.enabled=false in this arrangement,
so there is exactly one owner for storage objects. See [kind config](../../deploy/kind),
[storage chart](../../deploy/helm/storage) and [app deployment](../../scripts/app-deploy.sh).

The coordinator/storage headless discovery and bootstrap job admit the fixed
three-replica groups. An empty disk is not permission to self-bootstrap; replacement
requires the operator protocol. Kubernetes Ready and Raft data-ready are distinct.
See [operator recovery documentation](../operations/test-seams.md) and
[storage entrypoint](../../cmd/storagenode).

## Operator sequence

1. Install the pinned tools in [CONTRIBUTING](../../CONTRIBUTING.md) and
   [tools.mk](../../tools.mk); run make doctor.
2. Run make dev for the development environment, or make demo-reset for a clean
   disposable demonstration deployment. A reset is intentionally destructive to
   that development dataset, so preserve anything needed before using it.
3. Use make app-deploy for an app upgrade on the standalone storage release.
   Confirm Ready pods and placement before client demonstrations.
4. Run make demo-1 through make demo-9. The [gate wrapper](../../scripts/demos/gate.sh)
   executes two clean rounds and retains every assertion report.
5. Use make cluster-down to remove the local cluster when finished. There are no
   make teardown/demo-ready/verify/reset targets in the supported command set.

The [Makefile](../../Makefile) and [reset script](../../scripts/demos/reset.sh) are
the command authority. Never improvise an empty-disk Raft bootstrap or delete
production PVCs to make a failed demo appear clean.

## Presentation and submission

The live walkthrough should show deployment steps, create/register/match, queued
asynchronous work, shard routing, a storage failure/recovery, an app pod replacement
and manual scaling. Explain observed failures and bounded retries. Keep the
recorded gate as evidence, not a replacement for the mandatory live demonstration.

An offline kit should contain the source revision, immutable image inventory,
preloaded image archives, packaged charts, browser assets/fonts and deterministic
seed inputs. Its acceptance is the demos with external networking disconnected,
not merely a successful archive command. That verification belongs to A5.4 and is
not claimed by this report. Cloud/Terraform work remains parked; no GCP credits,
deployment or teardown is required for the agreed local submission.

Before submission, verify a clean clone, setup instructions, public repository
visibility, contracts, ADRs, evidence, final report, live-demo rehearsal notes and
backup recording. Submission and repository-publication actions need their actual
external outcome recorded; this document does not claim they have happened.
