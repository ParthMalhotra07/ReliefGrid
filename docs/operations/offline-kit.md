# Offline demonstration kit

The accepted 7 November artifact passed all nine demos with runner and node
external HTTPS blocked. See [accepted reports](../evidence/A5.4/accepted-20261107).
Acceptance belongs to the recorded artifact source SHA and manifest checksum,
not automatically to a later checkout or newly prepared artifact. The review
follow-up below changes host cleanup and documentation; it does not relabel that
historical run as acceptance of the new source.

## Online preparation prerequisites

Prepare on a Linux/amd64 host, or an amd64 WSL2 distribution integrated with a
Linux Docker daemon. Required before downloading/building the kit:

- A complete repository checkout on a **named branch**, with the intended source
  committed and no tracked changes. Detached HEAD preparation is unsupported:
  preparation uses `git branch --show-current` and clones that named branch into
  the source archive. Check `git branch --show-current` is nonempty first.
- The exact tools in [tools.mk](../../tools.mk), with installation instructions in
  [CONTRIBUTING.md](../../CONTRIBUTING.md#tools): Go, kind, kubectl, Helm and the
  checks/build tools. `make doctor` must pass; download pins
  and checksums are part of preparation, not offline installation.
- Python **3.12 or newer** (the controller uses tar extraction's `filter='data'`),
  with PyYAML installed for `prepare`. Git, Bash, Make and ordinary Linux build
  utilities must be available. Use the checkout's lockfiles and chart dependency
  preparation; do not substitute unrecorded Helm dependencies. The UI image
  embeds the committed browser assets: use pinned Node/npm to regenerate those
  assets before committing a UI change, rather than assuming kit preparation
  rebuilds JavaScript. Node/npm are not invoked by the kit controller itself.
- A Docker CLI/daemon pair that supports `docker image inspect --platform` and
  `docker save --platform=linux/amd64`. The user must have working daemon access
  without an interactive sudo prompt. Docker Desktop must be running and its
  WSL integration enabled when preparing inside WSL. Docker itself is not pinned
  by this repository; record the versions used and verify those commands work.
- The `docker`, `kind`, `kubectl` and `helm` executables on PATH must be usable
  Linux/amd64 binaries: preparation copies them into the Linux runner image.
  Windows `.exe` files or WSL placeholder launchers are not runner tools.
- Online registry/package access for base images, Docker build dependencies,
  npm modules, Helm chart dependencies and Go modules. Enough writable space is
  needed for image layers, both image archives, the source archive and evidence.
  Keep the destination outside the checkout and choose a new, nonexistent path.

```sh
git branch --show-current
git status --short
make doctor
python3 scripts/offline-kit.py prepare --kit /absolute/path/to/new-kit
python3 scripts/offline-kit.py verify --kit /absolute/path/to/new-kit
```

Preparation builds the app/storage/tools and runner images, exports one explicit
platform, validates OCI blob closure and hashes, packages charts and saves a
prebuilt outbox drainer. It retains source and a checksummed image/file inventory.
The kit includes local demo credentials; keep its private values and kubeconfig
out of public Git. No binary bundle or credential is committed here.

## What must travel to the offline host

Copy the **entire prepared kit directory**, preserving names, bytes and executable
permissions, including `manifest.json`, `source.tar.gz`, both `*-images.tar`
archives, `kind.json`, `charts/`, `values/` and `bin/drain-outbox`. Verify it with
the controller before installation. A chart tarball or image archive by itself
is not the kit.

The host also needs a runnable copy of the matching `scripts/offline-kit.py`
controller. Keep it under `scripts/` in an existing parent directory (the
controller uses that parent as its command working directory). Offline actions
use this script plus host Python, Docker and kind; they do not need the other
host checkout files. It is not automatically launched from the source archive.
Safest for source provenance and documented commands is to carry the complete
named checkout at the manifest's source revision separately alongside the kit. If using this later host-timeout
cleanup fix to drive an older accepted kit, retain both the artifact's source
revision and the separate host-controller revision: the latter is a control-flow
fix, not new nine-demo acceptance of a different artifact.

During `install`, the kit's archived checkout is extracted as `<kit>/source`.
That archived checkout supplies the Makefile, unchanged demo scripts,
`deploy/offline/runner.sh`, API-forward helper and their supporting files. The
prepared runner image embeds its own `/offline-runner` entrypoint and tools;
copying a newer host checkout does not rebuild that image. The separately
carried host controller and the archived demo checkout have different roles.

## Offline-host prerequisites

Before disconnecting external access, have all of the following ready:

- A Linux/amd64 Docker daemon with local access from the host user, Linux Docker
  CLI, pinned kind and Python 3.12+. PyYAML, Go, Node/npm and host Helm/kubectl
  are preparation dependencies; offline actions use the cached runner tools and
  prebuilt drainer, so they must not download/build replacements.
- Docker support for loading/inspecting the exported platform, internal bridge
  networks, privileged kind node containers and bind mounts. The host controller
  must reach the daemon and the runner must be able to mount
  `/var/run/docker.sock`; Docker Desktop/WSL setups must expose compatible Linux
  paths and working bind mounts. Do not run against a remote daemon that cannot
  see the local kit and temporary kubeconfig paths.
- Sufficient CPU, RAM and free disk for the **full five-node** topology and its
  images/PVCs; the repository's full-profile doctor check requires at least
  8 GiB available RAM. A single-node lean profile does not reproduce this kit.
- A writable kit/evidence location. `install` intentionally refuses an existing
  `<kit>/source` extraction, an existing cluster, or an unrelated cluster name.
  Use a fresh copy for a clean reinstall; preserve old evidence first.
- A free, dedicated `reliefgrid-offline[-suffix]` cluster/network name and no
  conflicting Docker address pool. The internal envelope is `10.240.0.0/12`,
  with node allocations `10.240.0.0/24` and routed pod CIDR `10.244.0.0/16`.
  Existing development clusters and PVCs are not reset by this workflow.

## Install and run without external access

From the separately carried controller checkout:

```sh
python3 scripts/offline-kit.py verify --kit /absolute/path/to/new-kit
python3 scripts/offline-kit.py install --kit /absolute/path/to/new-kit --cluster reliefgrid-offline-demo
python3 scripts/offline-kit.py run --kit /absolute/path/to/new-kit --cluster reliefgrid-offline-demo
```

Installation creates a new five-node kind cluster on a Docker internal network,
uses the private API, loads cached images and installs standalone storage followed
by the app. Docker Desktop's unavailable host-published API port is handled only
after the specific export error and a separate five-node Ready verification.
External probes must fail on all five nodes; runner external access must fail too.
New installations retain each named node, exact probe command and result in
`<kit>/node-isolation.json`, including a failing probe before propagating its error.
An empty or duplicate node inventory cannot pass. This new capture does not
retroactively provide names for the original generic probe lines.

The runner reaches the public API through a managed Traefik loopback forward,
keeps that parent-owned forward outside per-demo leak detection and stops it on
exit. It runs demo1 through demo9 without changing their assertions. Source and
charts are mounted read-only, Go downloads are disabled and the prebuilt drainer
is mandatory. Results live under the kit's evidence directory.

The host creates each runner with a unique known name and cluster label, records
its exact Docker ID, and attaches to that ID. On normal completion, failure,
interruption or the 2,400-second host timeout, host-side cleanup explicitly stops
that runner, waits for exit, confirms it is not running and then removes it.
The original run error is propagated after successful cleanup. A Docker cleanup
failure remains a failed host command; it cannot be counted as acceptance.
`evidence/runner-host-<id>.json` retains the name, ID and confirmed exit status.
If cleanup cannot be confirmed, use that exact owned ID to investigate; do not
stop containers selected only by a broad name or network filter.

The final runner marker is `OFFLINE ACCEPTANCE PASS:9/9 demos with runner/node
external egress denied`, and the host command must also exit successfully.
Node readiness, image import or successful Helm install alone is not acceptance.
Preserve failures and use a new revisioned kit after a source fix; do not patch a
supposedly immutable accepted bundle in place.

## Cleanup

After preserving evidence, delete only the named offline cluster and its dedicated
DNS anchor/network. The original development cluster and storage PVCs are separate:

```sh
kind delete cluster --name reliefgrid-offline-demo
docker rm --force reliefgrid-offline-demo-dns-anchor
docker network rm reliefgrid-offline-demo-net
```

Docker rejects conflicting address pools rather than replacing another network.
The kit supports the pinned kind/Docker workflow and Linux/amd64; it does not
deploy cloud resources or replace the mandatory live presentation.
