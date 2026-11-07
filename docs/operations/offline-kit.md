# Offline demonstration kit

The accepted 7 November artifact passed all nine demos with runner and node
external HTTPS blocked. See [accepted reports](../evidence/A5.4/accepted-20261107).
Acceptance belongs to the recorded artifact source SHA and manifest checksum,
not automatically to a later checkout or newly prepared artifact.

## Prepare while online

Use the pinned Linux/amd64 tools, Docker and Python with PyYAML. Commit source
changes first; choose a new output directory outside the checkout:

```sh
python3 scripts/offline-kit.py prepare --kit /absolute/path/to/new-kit
python3 scripts/offline-kit.py verify --kit /absolute/path/to/new-kit
```

Preparation builds the app/storage/tools and runner images, exports one explicit
platform, validates OCI blob closure and hashes, packages charts and saves a
prebuilt outbox drainer. It retains source and a checksummed image/file inventory.
The kit includes local demo credentials; keep its private values and kubeconfig
out of public Git. No binary bundle or credential is committed here.

## Install and run without external access

```sh
python3 scripts/offline-kit.py install --kit /absolute/path/to/new-kit --cluster reliefgrid-offline-demo
python3 scripts/offline-kit.py run --kit /absolute/path/to/new-kit --cluster reliefgrid-offline-demo
```

Installation refuses an existing cluster or an unrelated name. It creates a new
five-node kind cluster on a Docker internal network, uses the private API, loads
the cached images and installs standalone storage followed by the app. Docker
Desktop's unavailable host-published API port is handled only after the specific
export error and a separate five-node Ready verification. External probes must
fail on all five nodes; runner external access must fail too.

The runner reaches the public API through a managed Traefik loopback forward,
keeps that parent-owned forward outside per-demo leak detection and stops it on
exit. It runs demo1 through demo9 without changing their assertions. Source and
charts are mounted read-only, Go downloads are disabled and the prebuilt drainer
is mandatory. Results live under the kit's evidence directory.

The final success marker is `OFFLINE ACCEPTANCE PASS:9/9 demos with runner/node
external egress denied`. Node readiness, image import or successful Helm install
alone is not acceptance. Preserve failures and use a new revisioned kit after a
source fix; do not patch a supposedly immutable accepted bundle in place.

## Cleanup

After preserving evidence, delete only the named offline cluster and its dedicated
DNS anchor/network. The original development cluster and storage PVCs are separate:

```sh
kind delete cluster --name reliefgrid-offline-demo
docker rm --force reliefgrid-offline-demo-dns-anchor
docker network rm reliefgrid-offline-demo-net
```

The demonstrated private network contains the fixed pod CIDR and a disjoint node
allocation pool. Docker rejects conflicting address pools rather than replacing
another network. The kit supports the pinned kind/Docker workflow and Linux/amd64;
it does not deploy cloud resources or replace the mandatory live presentation.
