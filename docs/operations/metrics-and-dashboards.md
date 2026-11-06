# Application metrics and private dashboards [A4.2]

The API and matcher expose `/metrics` on a separate listener when `METRICS_ADDR`
is set. The application chart enables port 9090 for these workers; the public
HTTP listener and Traefik ingress do not route operator metrics. Set
`app.metrics.enabled=false` to disable the listeners and sampling work.

## Deploy and inspect

Use the existing standalone-storage kind profile. Monitoring images are pinned
to verified Linux/amd64 platform manifests in `tools.mk` and the observability
chart. No cloud deployment is involved.

```sh
make app-deploy
make metrics-up
make metrics-ui
```

Grafana is available only through loopback port forwarding on port 3000. The
local demonstration account is `admin` / `reliefgrid-local`; it is deliberately
a local example, not a public-deployment credential. Supply
`grafana.existingSecret` with `admin-user` and `admin-password` keys, or a
file-based Helm value override, when using different credentials. Never put a
real password in a command argument, evidence or committed values file.

Prometheus discovers annotated named metrics ports for each pod individually,
using read-only pod discovery restricted to the release namespace. Grafana has
no service-account token. Both Services are ClusterIP; neither has an ingress.
The broker scrape uses the pinned image's built-in Prometheus plugin, the root
vhost, and only the seven canonical queue names. Temporary projection queues,
test vhosts, per-connection IDs and arbitrary object labels are excluded.

Monitoring runs on the infra worker. Its data is ephemeral: Prometheus keeps at
most six hours / 512 MB of samples in a bounded emptyDir. Pod replacement or
monitoring reconfiguration can reset this history; export evidence first.
`make metrics-down` removes only this monitoring release and its ephemeral data.
Application/storage releases and PVCs are separate owners.

## Meaning of the application observations

- `relief_match_outcomes_total` counts workflow invocations and their observed
  outcome. Repeated invocations can appear more than once; this is not a durable
  unique-request ledger. Failed local attempts and unknown remote outcomes are
  distinct from committed domain rejection or cancellation.
- `relief_match_latency_seconds` measures creation to the observed successful
  committed MATCHED response, including commit/response delay. Only non-replayed
  responses contribute. A lost response can omit a sample; a replay cannot
  invent another completion. This is separate from HTTP handler execution time.
- `relief_cas_conflicts_total` counts non-replayed committed VERSION_CONFLICT
  responses. Network errors never become committed conflicts.
- `relief_workflow_age_seconds` and `relief_workflow_pending` describe the last
  complete paged scan of unfinished requests, by fixed state and urgency. Age is
  since request creation, so repeated retries cannot reset an old request's age.
  The walk is bounded and is an observation, not a globally atomic snapshot.
- `relief_metrics_observation_success` and
  `relief_metrics_last_success_timestamp_seconds` distinguish complete, stale and
  never-observed data. Partial, malformed or failed pages preserve the previous
  values and mark them invalid. The dashboards exclude invalid/stale workflow
  data instead of treating it as an empty system.
- `relief_shard_read_available` reports shard observations, not Kubernetes pod
  readiness or settled business workflows. A reachable exporter alone proves
  neither of those.
- `relief_outbox_batch_*` is explicitly the first batch of at most 100 pending
  events. It is a lower bound, with validity/truncation flags. It cannot supply an
  exact backlog count or the full backlog's oldest timestamp.

The wrappers delegate the original commands without changing IDs, timestamps,
payloads, retries or results. Observers do no remote I/O on mutation/ack paths.
The scrape path reads local counters/gauges; a background worker owns bounded
remote sampling. Parent cancellation stops and joins the private server/sampler
without consuming the public API readiness propagation/drain budget.

Labels supplied by application code are fixed service, command, outcome, state,
urgency and configured shard values. Unknown urgency/outcome input collapses to a
fixed bucket. Request/resource/attempt/operation IDs, raw errors and payload text
never become labels. Prometheus adds its ordinary per-target job/instance labels;
discovery and retained time are bounded by this local profile.

## Verification and evidence

```sh
make check
make gen-check
make verify-A4.2-static
make verify-A4.2-app
make verify-A4.2
```

Static verification runs race tests, checks chart isolation and the three JSON
dashboards, and validates the rendered Prometheus configuration with the exact
pinned server's `promtool` with network access disabled. The live application
check observes four exporters, seven queues, every dashboard query, a real
asynchronous match/cancellation, and a waiting request whose age grows across
complete samples. Synthetic requests are cancelled on exit.

To record the three browser screenshots, install `@playwright/test` 1.64.1 and
its Chromium runtime in a developer tools directory (it is not a production
dependency), set `A42_PLAYWRIGHT_MODULE` to its absolute `index.mjs` path, and run:

```sh
python3 scripts/verify-A4.2-live.py --mode app --screenshots
```

The capture helper requires Node and that exact Playwright package version. It
logs in through Grafana's session endpoint, validates loopback origin, keeps
credentials out of URLs and discards the browser context afterwards. Override
the demonstration password via `A42_GRAFANA_PASSWORD_FILE`, never a CLI value.
Results and screenshots are saved under `docs/evidence/A4.2/`.

### Pending storage handoff: #113

The final A4.2 acceptance is **not complete** with application metrics alone.
S owns the storage/coordinator exporters, exact outbox statistics and their
private scrape policy. Expected family/label contracts and access are requested
in [#113](https://github.com/ParthMalhotra07/ReliefGrid/issues/113). Those panels
currently say **No observation**. Their proposed query names must be confirmed
against the owner's actual exporter before final acceptance.

`make verify-A4.2` exits 2 as INCOMPLETE while these sources or the observed
leader-kill health/replication-lag/workflow-age evidence are missing. A successful
application-only check must never be reported as the full phase goal. Once S
supplies the contract/access, wire the fault-evidence stage, run a real observed
leader kill, verify recovery and capture its panels before marking A4.2 complete.

## Tooling notes

The pinned RabbitMQ image already enables `rabbitmq_prometheus`; A4.2 adds only a
private Service/container port, not a workflow topology or publication change.
Grafana plugin preinstall/auto-update is disabled, so startup does not fetch new
plugin binaries or fill the bounded data volume with unpinned updates.

The kind loader first pulls an immutable platform digest, gives those verified
bytes a named local tag for the Docker archive, and loads that tag. The pod still
uses the immutable digest. This avoids unnamed import references and incomplete
multi-architecture archives in the local containerd store.
