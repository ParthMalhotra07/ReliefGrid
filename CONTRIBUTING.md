# Contributing to ReliefGrid

Two people build ReliefGrid: **S** ([@coDEmaVEriCK06](https://github.com/coDEmaVEriCK06)) and
**A** ([@ParthMalhotra07](https://github.com/ParthMalhotra07)). These are the working rules for
both of us and for any tool we use.

## Ownership

`.github/CODEOWNERS` is authoritative. A change outside your area goes through a PR that the
owner approves.

| Owner | Paths |
| --- | --- |
| S | `internal/fsm`, `internal/raftstore`, `internal/ring`, `internal/obs`, `internal/storagenode`, `internal/coordinator`, `internal/coordclient`, `cmd/storagenode`, `cmd/coordinator`, `cmd/invcheck`, `chaos/`, `loadtest/`, `infra/gcp/`, `deploy/kind/`, `deploy/helm/reliefgrid/templates/storage/`, `spike/raft/`, `.github/` |
| A | `cmd/api`, `cmd/matcher`, `internal/workflow`, `internal/geo`, `internal/mq`, `internal/outbox`, `internal/reconcile`, `internal/auth`, `ui/`, `deploy/helm/` (except storage templates), `spike/mq/`, `tests/workflow/` |
| Both | `contracts/`, `docs/`, `internal/resilience`, `deploy/platform/`, `Makefile`, `tools.mk`, `scripts/`, other `tests/`, other `spike/`, root files |

Contracts in `contracts/` are frozen after 21 October. A change needs both owners' approval and the
regenerated code in the same PR.

## Layout

```
contracts/   public.yaml, internal.yaml, events/*.json
cmd/         api, matcher, coordinator, storagenode, invcheck
internal/    ring, raftstore, fsm, storagenode, coordinator, coordclient, store, workflow, geo, mq, outbox, reconcile, obs, resilience, auth
deploy/      helm/reliefgrid, kind, platform
chaos/  loadtest/  tests/  ui/  infra/gcp/
docs/        architecture, adr, demos, evidence, operations
spike/       throwaway experiments (raft/, mq/)
```

`spike/` holds throwaway experiments that prove a library or platform behaviour before we build on
it. Spike code is not production code: nothing outside `spike/` may import it, and it can be deleted
once the real implementation lands. Keep the scripts that produced spike evidence runnable until then.

## Tools

Every version is pinned once, in `tools.mk`. `make doctor` compares what you have installed with
those pins; run it after any pin change.

| Tool | Pin | Install |
| --- | --- | --- |
| Go | `GO_VERSION` | tarball from go.dev/dl into `~/.local/go`; put `~/.local/go/bin` and `~/go/bin` on `PATH`. `go.mod`'s `toolchain` line matches the pin. |
| golangci-lint | `GOLANGCI_LINT_VERSION` | release binary from GitHub into `~/.local/bin` (not `go install`, as its maintainers recommend) |
| kind | `KIND_VERSION` | release binary into `~/.local/bin`. Its node image is pinned as `KIND_NODE_IMAGE` (tag and digest from that kind release's notes); `make cluster-up` pulls it. |
| kubectl | `KUBECTL_VERSION` | release binary from dl.k8s.io into `~/.local/bin` |
| helm | `HELM_VERSION` | Helm 3 tarball from get.helm.sh into `~/.local/bin` |
| oapi-codegen | `OAPI_CODEGEN_VERSION` | nothing to install: it is a `tool` in `go.mod`, run as `go tool oapi-codegen` |
| openapi-spec-validator | `OPENAPI_SPEC_VALIDATOR_VERSION` | Python package used by `scripts/verify-A0.1.sh`: `pip3 install --user --break-system-packages openapi-spec-validator==<pin>` (or in a virtualenv) |
| Node (UI build only) | `NODE_VERSION` | Exact Node24 release; `ui/.node-version` matches. Verify the official SHA-256. Runtime images embed the built assets and need no Node. |
| Docker | not pinned | Docker Engine (Linux) or Docker Desktop with WSL2 integration; your user must reach the daemon without `sudo`. |

Check each download against its published SHA-256 before installing it.

`make doctor` also checks that the Docker daemon answers, that enough RAM is available
(`MemAvailable`: 8 GiB for the full five-node profile, 4 GiB with `PROFILE=lean`; override with
`MIN_AVAILABLE_RAM_GIB=<n>`), and that the cluster's host ports (6443, 8080, 8443 on 127.0.0.1)
are free or already held by the `reliefgrid` cluster.

## Make targets

| Target | What it does |
| --- | --- |
| `make check` | gofmt, go vet, golangci-lint, `go test -race ./...`. Must pass before every push. |
| `make gen` | Regenerates code from `contracts/`. Must leave no diff. |
| `make doctor` | Checks tool versions against `tools.mk`, Docker, available RAM and host ports. |
| `make cluster-up` | Creates the `reliefgrid` kind cluster from `deploy/kind/cluster.yaml` (1 control plane, 3 `role=data` workers, 1 `role=infra` worker tainted `role=infra:NoSchedule`) and switches kubectl to `kind-reliefgrid`. No-op if it exists. |
| `make cluster-down` | Deletes the `reliefgrid` kind cluster. No-op if it is absent. |
| `make dev` | Development stack. For now it only runs `make cluster-up`. |

## One goal, one branch, one PR

1. Branch from an up-to-date `main`: `s/<ID>-slug` or `a/<ID>-slug`, e.g. `s/S1.2-fsm-commands`.
2. Write the failing test, or the goal's Verify script, first. Read its assertions yourself.
3. Implement in small commits.
4. Run `make check` and the goal's Verify command. Save demo or cluster evidence under `docs/evidence/`.
5. Open a PR using the template: Behaviour, Failure case handled, How tested (real output pasted),
   Limitations, Goal ID. Wait for CI to pass.
6. The other person reviews. Squash-merge with a Conventional Commit title that carries the goal ID.

`main` is protected: no direct pushes, CI and one approval required.

## Commits

- Conventional Commits with a scope and the goal ID in brackets:
  `feat(fsm): TryClaim with durable closure [S1.2]`. Types: `feat`, `fix`, `test`, `docs`, `chore`, `ci`.
- Commit under your own name, from your own machine, with `git config user.email` set to an email
  verified on your GitHub account. No commits from cloud or web sessions.
- No AI attribution: no `Co-authored-by` trailers and no "Generated with" lines in commits or PRs. The
  `commit-lint` CI job fails a PR that has either.
- Never commit secrets, kubeconfigs, `.env` files or Terraform state.

## Code conventions

- Go: `net/http`, `log/slog`, `context.Context` as the first parameter, and a deadline on every
  remote call. Wrap errors with `%w`; check them with `errors.Is` / `errors.As`.
- Never invent a library API: check the pinned version's documentation or source.
- When a PR says tests pass, paste the real command output.

## Invariants

Code and tests cite these by number (for example "CONTRIBUTING.md, Invariants, 5"). Never weaken one.

1. All preconditions (CAS version, claim rules, operationId dedupe) are checked inside the committed
   FSM apply, in one SQLite transaction.
2. FSM apply is deterministic: no wall clock, randomness, map-iteration-order effects or
   floating-point decisions. Proposers supply IDs and integer timestamps.
3. No resource has two active claims; no request has two live claims; MATCHED/IN_PROGRESS requests
   have a reciprocal claim; terminal requests hold no live claim.
4. A historical operation result is never proof of current ownership.
5. Unknown outcome is not rejection: keep the intent; retry the same operationId or close via
   RETRYING.
6. Never ack a RabbitMQ delivery before the durable handoff it depends on has committed.
7. A failed or partial shard scan is never an empty result.
8. Pods never self-bootstrap Raft; an empty disk stays unadmitted until `storagectl replace` admits
   it (M7).
