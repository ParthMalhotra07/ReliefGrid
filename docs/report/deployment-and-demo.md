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

## Storage responsibilities and recovery demonstration

The standalone storage chart owns the coordinator, nine replicas, headless
services, PVCs, policies, admission manifests/RBAC and ordinary bootstrap Job.
S supplies storage images, readiness/recovery behavior, invcheck, chaos, tracing
and CI tooling; Parth owns app/messaging/UI and app deployment. See
[bootstrap](../../deploy/helm/storage/templates/bootstrap-job.yaml#L1),
[coordinator](../../deploy/helm/storage/templates/coordinator.yaml#L1),
[policies](../../deploy/helm/storage/templates/networkpolicy.yaml#L1) and
[ownership](../../CONTRIBUTING.md#L9).

Use #145's reviewed clean-clone narrative when integrated. `make dev` creates or
selects the cluster; it is not a complete app installation. The clean demo path
uses `make demo-reset` and explicit deployment/readiness checks. During the live
demo, S should explain liveness versus Raft data readiness, majority loss,
follower catch-up, snapshot replay and admission-gated replacement. All kind
workers share one physical host: anti-affinity is not independent host/zone
failure tolerance. Infra-worker/coordinator loss may block routing without
destroying committed shard history. An eviction/toleration interval is not a
measured recovery SLA.

## Operator sequence

1. Install the pinned tools in [CONTRIBUTING](../../CONTRIBUTING.md) and
   [tools.mk](../../tools.mk); run make doctor.
2. Run make dev to create/select the development cluster, then make demo-reset
   for a clean disposable demonstration deployment. A reset is intentionally
   destructive to that development dataset, so preserve anything needed before using it.
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
not merely a successful archive command. The [retained A5.4 acceptance](https://github.com/ParthMalhotra07/ReliefGrid/tree/b4ae2487df19c18d0cc44d2f6e1dd5225fba4701/docs/evidence/A5.4/accepted-20261107)
supports the recorded artifact's nine passes, not this integrated source tree.
#139's host-prerequisite documentation and timeout cleanup remain under review.
Cloud/Terraform work remains parked; no GCP credits,
deployment or teardown is required for the agreed local submission.

Before submission, verify a clean clone, setup instructions, public repository
visibility, contracts, ADRs, evidence, final report, live-demo rehearsal notes and
backup recording. Repository public visibility was observed on 7 November; that
does not establish submission, presenter rehearsals, recording creation or playback. The public
GitHub link, mandatory live demonstration and deployment-from-scratch backup
recording are resolved submission requirements. Report length/format alone
still needs professor confirmation; execution of those deliverables remains
pending until recorded.
