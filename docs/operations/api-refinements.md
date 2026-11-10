# Conditional updates and list continuations

The A4.4 implementation is reviewed against contract PR134. ETags are quoted opaque
strong tags for entity versions. Request GET, create and successful status PUT
responses expose them. A keyed create replay deliberately returns its original
body and ETag; GET the current entity before changing it.

If-Match is optional on status PUTs. A nonmatching/weak tag returns412 with
PRECONDITION_FAILED. The committed CAS also returns412 if the entity changes
between read and apply. Without If-Match the existing409conflict behavior remains.
Unknown outcomes remain503 and are never converted to a precondition rejection.
Lists of strong tags and wildcard existence are supported; malformed tags are400.

Request/resource list responses may contain nextCursor. Send it as cursor with
the same endpoint, filters and limit. Do not decode or alter it. Under static data,
a complete walk returns each match once; mutations between pages can change the
result membership. A page is not a global cross-shard snapshot. The server retains
unconsumed merged rows by resuming the same source page, rather than advancing
past rows that did not fit the client limit. Empty/partial failures cannot be
treated as exhausted data. A reported ring-version mismatch requires a new walk.

Run `go test -race -count=3 ./internal/api -run
'TestAPIConditional|TestAPIResourceConditional|TestAPICursor|TestAPISpatialCursor'`
for the focused real-FSM checks. Aging requires S135; internal authentication
requires the S29 handoff. This implementation does not claim those are delivered.
