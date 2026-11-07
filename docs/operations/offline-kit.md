# Offline demonstration kit (draft)

Current acceptance is incomplete: kind's --all-platforms importer found a missing
multi-platform blob. Do not present this draft as a verified offline demo kit.
See docs/evidence/A5.4 and the packaging follow-up issue.

Online preparation (Linux/amd64, pinned project tools installed):

    python3 scripts/offline-kit.py prepare --kit /absolute/new/path/outside/repo

Preparation builds source-revisioned application/storage/tool images, imports the
pinned RabbitMQ/Traefik/kind references, builds a runner, packages charts and a
prebuilt drainer, and writes an SHA256 inventory plus exact Docker image IDs.
It preserves a shallow Git source tree for the demo's code-tree evidence. UI
assets are bundled in its image; system-font stacks and offline list fallback
avoid a remote-font dependency. Seed/fixture programs travel with the source.

Offline integrity check:

    python3 scripts/offline-kit.py verify --kit /absolute/kit

Install/run (after the archive issue is fixed and verified):

    python3 scripts/offline-kit.py install --kit /absolute/kit --cluster reliefgrid-offline-check
    python3 scripts/offline-kit.py run --kit /absolute/kit --cluster reliefgrid-offline-check

Only a new reliefgrid-offline[-suffix] cluster is accepted; existing clusters are
never replaced. The standard development cluster is untouched. A private internal
Docker network denies external access from bootstrap onward. A local DNS-address
anchor satisfies kind's embedded-DNS setup without adding egress. On Desktop,
only the precise missing host-port export error is recoverable, and all5nodes
must independently be Ready through the private API before loading/installing.
Charts and demos execute in the private runner; no external host API is claimed.
The pinned kind custom-network environment is experimental and locally verified.

The runner refuses Docker pulls/builds, disables Go dependency downloads and uses
the prebuilt drainer. It proves external HTTPS fails, requires internal API access,
and runs all9demos with unchanged assertions. Success must be recorded in
kit/evidence/result.txt; an artifact hash or Ready node alone is insufficient.
Kubeconfigs are generated only into temporary0600files and never added to the kit.
Artifacts can consume gigabytes and stay outsideGit. This is not the joint A4.5
enhanced gate, the human rehearsal or the final submitted package.
