# ReliefGrid

Distributed emergency resource matching with three Raft-replicated shards,
a deterministic SQLite state machine, coordinator-owned routing and leader
discovery, and RabbitMQ-driven matching workflows. The application services are
the public API and matcher; coordinator and storage provide the distributed data
service. A dispatch UI is served through Traefik.

## Run a local demonstration

Use Linux, or WSL2 with Docker Desktop integration enabled. Install the exact
versions in [tools.mk](tools.mk) using [CONTRIBUTING](CONTRIBUTING.md#tools).
Docker must be running and reachable from that shell. The default kind profile
uses one control plane, three data workers and one infra worker; see the
[cluster configuration](deploy/kind/cluster.yaml).

Also install Bash, make, Python 3, **jq, curl, GNU timeout, and a C compiler**
(for the Go race tests) before running the commands below. `make doctor` does
not check every auxiliary prerequisite; in particular the reset uses jq when
confirming that all nine storage nodes are UP.

An uncached machine needs network access to download the pinned Go/tooling and
Go modules, pull container/base/kind images, and fetch the pinned Traefik chart
through [app-chart-deps.sh](scripts/app-chart-deps.sh). The standard setup below
is online preparation, not an offline installation. Prepare and verify the
offline kit separately before attempting a network-isolated demonstration.

```sh
git clone https://github.com/ParthMalhotra07/ReliefGrid.git
cd ReliefGrid
make doctor
make check
make demo-reset
```

`demo-reset` asks for the cluster name. It **deletes and recreates the entire
selected kind cluster and everything running on it** (default `reliefgrid`),
builds/loads images, bootstraps the three
storage groups and installs the application. Storage has a single Helm owner:
the app is installed over the standalone storage release with `storage.enabled=false`.

Check the deployment and open the dispatch UI:

```sh
kubectl --context kind-reliefgrid -n reliefgrid get pods
curl --fail 'http://localhost:8080/api/v1/requests?limit=1'
```

The UI is at **http://localhost:8080/**. The canonical public API is under
`/api/v1`; its routes, fields and error semantics are in the
[public contract](contracts/public.yaml).

## Verify the demonstrations

```sh
make e2e-smoke
make demo-2
make gate
```

`demo-1` through `demo-9` run individual asserted demonstrations. `gate` runs all
nine twice, starting each round from a clean deployment, and asks before resets.
Read its actual PASS/FAIL summary; a historical green run does not certify a new
source revision. Logs and assertion reports go under `docs/evidence/<run-id>/`.
The [demo scripts](scripts/demos) define the fixtures, bounds and pass criteria.

The gate includes storage failure/recovery and app interruption scenarios.
Kubernetes readiness alone does not establish storage invariants; the demos use
authoritative reads and invcheck where required. An interrupted write with an
unknown outcome must retain its operation identity rather than be treated as a
definite rejection.

## Development and cleanup

| Command | Purpose |
|---|---|
| `make dev` | Create/select the kind cluster; this target alone does not install the full application |
| `make app-deploy` | Build/load and upgrade the app against an existing standalone storage deployment |
| `make check` | Formatting, vet, lint and race tests |
| `make gen-check` | Verify generated contract code has no drift |
| `make cluster-down` | Delete the local kind cluster and its data |

For a fresh full setup use `make demo-reset` as above. For an app-only upgrade use
`make app-deploy`; it preserves the standalone storage release. These commands
target the local demonstration environment.

## Report and presentation

The classroom deployment target is local kind. Cloud deployment is parked.
Submission must include the **public GitHub repository URL**; verify signed-out
access to [this repository](https://github.com/ParthMalhotra07/ReliefGrid). A live
demonstration is **mandatory**. The recording is a backup and must show deployment
from scratch, including every deployment step; verify that the recording plays
back successfully. The professor requires the report to be **under 10 pages**.
The shared outline, final formatting and exported page count still need review.

The [report core](https://github.com/ParthMalhotra07/ReliefGrid/pull/137) and
[three-round rehearsal runbook](https://github.com/ParthMalhotra07/ReliefGrid/pull/144)
are prepared for review. Their PR state distinguishes prepared material from
merged submission content. Final readiness also requires the owners' reviewed
report, actual rehearsal notes, a playable backup recording and the submission
receipt; this README does not claim those actions have happened.
