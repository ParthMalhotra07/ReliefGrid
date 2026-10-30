# Contracts

| File | What it fixes |
| --- | --- |
| `ring.yaml` | The consistent-hash ring: ringVersion, shards, vnodes and the token-map checksum. |
| `internal.yaml` | Coordinator and storage-node routes (plan v3.1 §40). Generated types: `internal/internalapi`. |
| `public.yaml` | The public API (v3.1 §7). `make gen-public` generates `internal/api/gen`. |
| `events/*.json` | Broker event schemas. |

Contracts are frozen after 21 October. A change needs both owners' approval, and the regenerated
code (`make gen`) must be in the same PR. `go test ./contracts` checks `internal.yaml` against
`internal/store`, `ring.yaml` and the table below.

`make gen` runs both `gen-public` and `gen-internal`; neither generator replaces the other.
Public contract tests also validate examples and reject unsupported request fields,
invalid geography/urgency and client assignment writes. Public IDs/types map to the stored
entities; public versions are opaque strings. Event `schemaVersion` is an integer, matching
the outbox. Public urgency NORMAL/HIGH/CRITICAL maps to stored and explicit AMQP priority
values 1/2/3; zero is never an application urgency.

## Store outcome → HTTP status

Both specs use this table. Every committed outcome, rejections included, is stored under its
operationId and returned again, with `replayed: true`, when the same command is retried. On the
internal API a committed rejection carries the store `Result` in the error body's `result` field.

- **Internal HTTP** follows v3.1 §40 and the store.go comments.
- **Public HTTP** is what the API returns. Public `VERSION_CONFLICT` is 409 until If-Match/412
  lands in week three.

Every error body has `code`, `message`, `retryable` and `outcomeUnknown`.
Public errors additionally require `correlationId` and may carry validation `details`.

| Store result | Code | Internal HTTP | Public HTTP | Notes |
| --- | --- | --- | --- | --- |
| `OK` | — | 200 with the Result | 200 or 201, per route | Committed. |
| `SUCCESS` | — | 200 with the Result | — | Claim granted. Internal only; the API translates it. |
| `CONFLICT_CLOSED` | — | 200 with the Result | — | Held by another attempt; this attempt is now closed. |
| `REJECTED_CLOSED` | — | 200 with the Result | — | Ineligible (type or window); this attempt is now closed. |
| `CLOSED` | — | 200 with the Result | — | A committed closure already fences this attempt. |
| `INVALID` | `INVALID` | 400 | 400 | Malformed command, or rejected by validation. retryable=false. |
| `NOT_FOUND` | `NOT_FOUND` | 404 | 404 | The command's entity does not exist. |
| `ErrNotFound` | `NOT_FOUND` | 404 | 404 | A read found no committed record, closure or operation. |
| `DOMAIN_CONFLICT` | `DOMAIN_CONFLICT` | 409 | 409 | Forbidden by a domain rule: illegal transition, protected key, immutable-input mismatch, update of an assigned resource. |
| `OPERATION_ID_REUSED` | `OPERATION_ID_REUSED` | 409 | 409 | Same operationId, different payload. Never stored. |
| idempotency mismatch | `CONFLICT_IDEMPOTENCY_KEY` | — | 409 | Public only: same Idempotency-Key, different payload hash. |
| `VERSION_CONFLICT` | `VERSION_CONFLICT` | 412 on the generic data routes (`PUT`/`DELETE /internal/data`); 409 on the domain transition routes | 409 | Expected version, status or attempt did not match. §40: 412 for a failed generic CAS, 409 for a domain or attempt conflict. The FSM reports all three mismatches as this one outcome, so the status follows the route. Public becomes 412 with If-Match in week three. |
| `EXISTS` | `EXISTS` | 412 | replay the original 201 + Location if the payload hash matches, else 409 | Create on an existing key (internal: `If-None-Match: *` failed). |
| `ErrRingVersionMismatch` | `RING_VERSION_MISMATCH` | 409 | 503 | Never proposed; the body gives the server's ringVersion. The coordinator refreshes and retries; the public API sees 503 only when that fails. retryable=true, outcomeUnknown=false. A scan cursor made under another ring version gets this too; refreshing cannot fix it, so the walk restarts with an empty cursor. |
| `ErrUnknownOutcome` | `OUTCOME_UNKNOWN` | 503 | 503 | May have committed. retryable=true, outcomeUnknown=true, with Retry-After. Retry the same operationId or read the operation; never treat it as a rejection. |
| not leader | `NOT_LEADER` | 503 | 503 | Never proposed. leaderHint names the leader. retryable=true, outcomeUnknown=false. The coordinator retries at the hint within its budget. |
| no leader | `NO_LEADER` | 503 | 503 | No leader known, or a read could not be fenced; nothing was proposed. A leader that loses quorum after proposing answers `OUTCOME_UNKNOWN`. retryable=true, outcomeUnknown=false, with Retry-After. |
| missing or bad operator token | `UNAUTHORIZED` | 401 | — | Internal `/admin` only. retryable=false, outcomeUnknown=false. |
| token lacks the operator role | `FORBIDDEN` | 403 | — | Internal `/admin` only. retryable=false, outcomeUnknown=false. |
| local storage failure | `STORAGE_UNAVAILABLE` | 503 | 503 | The node's local storage failed (for example a SQLite read error); nothing was committed by this request. retryable=true, outcomeUnknown=false. |
