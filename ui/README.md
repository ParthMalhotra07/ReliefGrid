# ReliefGrid dispatch UI [A4.1]

The Go UI server embeds the pinned React/Leaflet build. The existing same-origin
Traefik routes continue to send `/api/v1/` to the API. The UI server never proxies
API, storage or operator routes. `/ui/status` returns only bounded queue aggregates;
its RabbitMQ management credentials remain in the UI pod's environment.

## Build and verify

Use the exact Node version in `tools.mk` / `.node-version`, and the Go/Helm tools
already used by this repository. Install browser support once:

```sh
cd ui
npm ci --ignore-scripts
npx playwright install --with-deps chromium  # Linux; installs Chromium's OS libraries
cd ..
make verify-A4.1
```

`make ui-build` rebuilds the committed offline assets from the exact lockfile.
`make check` checks the embedded files/source manifest without requiring Node on
storage-only developer machines. A browser CI job should run `make verify-A4.1`
and then `git diff --exit-code -- ui/static` to enforce reproducible output.

Run `HTTP_ADDR=127.0.0.1:8081 go run ./ui` behind the existing same-origin API
routing for development; `make app-deploy` builds and deploys all three app images.
There are no CDN scripts, external fonts, tile downloads or API keys. The map is
an explicitly labelled coordinate schematic; request/resource positions and
reciprocal assignment lines come from the API. Third-party licenses are bundled
under `/assets/licenses.txt`.

## Read and offline semantics

- Each refresh reads both public lists with limit 100. All three shard observations
  must be present and the result must be complete and strong. A failed or partial
  read preserves the saved view and disables mutations until a successful refresh.
- The public paging contract is A4.4. This version explicitly warns when more
  records exist; search/status/type/urgency filters apply to the loaded view.
  Counts are labelled as counts within that view, not global totals.
- SSE IDs are deduplicated in a bounded 2000-ID window. Every connection/open,
  reconnect and refresh hint rereads records. Incoming events never set assignment
  or terminal state. A 15-second refresh also repairs missed best-effort hints.
- The service worker atomically precaches the versioned app shell and local map
  assets. It never caches API or telemetry requests. Local storage retains the last
  successful bounded view and up to 12 observed status changes per loaded request.
- Offline navigation/reload shows the saved list with its timestamp. Actions are
  disabled. A device with no saved view shows unknown data, not zero requests.
- Timeline entries are observations, not a durable audit journal. CANCELLING and
  FULFILLING remain visible until a later authoritative record changes them.
- Match/start/cancel/fulfil actions use the existing public domain endpoints only.
  Request/resource creation remains available through the existing public API.
  Unknown outcomes are shown as unconfirmed and trigger a reread; claims are never
  edited from the browser.

The default local chart has no STREAM_TOKEN. If protecting SSE with that API
option, use authenticated same-origin proxy routing that injects the credential
server-side; never put a shared token in this bundle. A rejected stream is shown
as unavailable and snapshot refresh remains available. Public cloud authentication
is a separate deployment requirement.

## Health and queue observations

Shard read availability means each shard answered the last authoritative reads.
It does not claim Kubernetes pod readiness or that business workflows have settled.
The UI says explicitly when current availability is unconfirmed.

The UI backend polls only `matching.requests` (ready/unacknowledged) and
`matching.dead` (total), with a 1.5-second aggregate budget and a 10-second cache.
Missing or malformed counts produce nullable unknown values. Broker URLs, passwords,
payloads and operator endpoints are never returned to the browser. This is not an
outbox/TTL-queue metrics replacement; A4.2 provides the wider dashboards.
