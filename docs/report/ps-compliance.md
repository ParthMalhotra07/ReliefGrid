# Problem statement compliance: client API and fields

PS Table 2's seven client operations are served by the same API handlers at the
canonical `/api/v1` prefix and through the kind Traefik compatibility aliases.
An AddPrefix middleware adds `/api/v1`; it does not rewrite response bodies,
create IDs or choose another dispatcher namespace. A keyed create retried across
prefixes therefore uses the same durable idempotency record. Location headers
remain canonical.

| PS operation | Compatibility route | Normal success |
|---|---|---|
| Submit request | `POST /requests` | 201 |
| Request details | `GET /requests/{id}` | 200 |
| Request status | `PUT /requests/{id}/status` | 200 |
| Register resource | `POST /resources` | 201 |
| List resources | `GET /resources` | 200 |
| Resource status | `PUT /resources/{id}/status` | 200 |
| Match a request | `POST /match/{requestId}` | 200; 202 while work continues |

The aliases match HTTP methods, exact collection paths and UUID-shaped entity
paths. They preserve query strings. Named SPA paths such as `/requests/new` and
`/resources/register` remain under UI routing, including its normal 404 for unimplemented pages. `/events` is deliberately not aliased;
the UI's event stream remains `/api/v1/events`. Non-PS operations, including the
request collection listing, retain only their canonical contract server.

| PS field/concept | ReliefGrid representation | Meaning |
|---|---|---|
| Request `id` | `requestId` | Stable UUID identifying a request |
| Resource `id` | `resourceId` | Stable UUID identifying a resource |
| Request/resource type | `requestType` / `resourceType` in entity bodies; `type` in list filters and indexed storage metadata | Entity kind-specific fields retain the PS type; the shared filter/index name selects that same domain value |
| Creation time | `createdAt` | UTC timestamp persisted when created; keyed retries retain it |
| Availability | `windowStart`, `windowEnd`, `status` | A usable resource is AVAILABLE in its time window and has no active claim; ASSIGNED/OFFLINE resources cannot be freely assigned |

HTTP 202 from match means the workflow is still running or deferred. It is not
a rejection or proof that a claim exists. The caller follows the canonical
Location and reads current request state; MATCHED/IN_PROGRESS must have the
reciprocal durable resource claim. The regression test exercises a committed
WAITING request with a future retry time, then verifies 202 and its poll URL.

The UI provides create-request and register-resource forms. They freeze the
body and UUID Idempotency-Key across double-clicks, transport retries, unknown
outcomes and same-tab reloads, then require a current record read and successful
view refresh before allowing another submission. Matching is automatic. No bearer/operator credentials are added by these aliases.
