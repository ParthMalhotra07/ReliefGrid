# Internal authentication building blocks

These helpers implement the A-owned portion of the interface proposed in issue
#29. They do not enable authentication on any route and do not change contracts.
S still supplies coordinator/storage receivers, node registration/heartbeat and
`coordclient.WithInternalAuth`, followed by joint route/contract acceptance.

Validate `SenderConfig{Token: current}` before constructing a remote client or
issuing its initial ring request. `auth.NewClient(existingClient, config)` returns
an owned shallow copy preserving timeout, transport and cookie jar, installs a
credential-replacing transport and rejects all redirects. The transport also
strips inherited Authorization/Proxy-Authorization headers so forwarding an
incoming request cannot carry an operator/client credential to another service.
Operator requests use their separate client. It never mutates the
shared client or request headers. The transport and jar remain shared resources:
injected transports must be trusted and callers must not mutate them concurrently.
Callers select the trusted destination and supply a context with a deadline or a
nonzero client timeout. Requests without either fail before transport I/O, as do
already cancelled/expired contexts. CloseIdleConnections delegates to a transport
that supports it, preserving normal client cleanup.
Do not wrap an already authenticated client in another credential class's client;
construct each from the underlying unauthenticated transport instead.

`ReceiverConfig{Token: current, PreviousToken: previous}.Authorize(request)` is a
fail-closed check, including on invalid configurations. Wire it before protected
handlers and use the agreed structured UNAUTHORIZED response, not a helper-local
response format. A receiver accepts one header with the current or optional
previous credential. Duplicate, comma-joined, whitespace-bearing, missing and
malformed values are rejected. Digest comparisons use fixed-size constant-time
comparisons. HTTP parsing and config validation are not claimed constant-time.
Keep application/node secrets different, and operator Bearer authentication
separate. Syntax validation does not certify entropy: provision random secrets.

Config formatting through fmt/slog and JSON/YAML field serialization redacts the
credential. This cannot protect deliberately logged `.Token` values, raw request
headers, debugger memory or a malicious transport. Do not put secrets in URLs,
logs or evidence. Errors contain no credential values.

The agreed Secret names proposed on #29 are `reliefgrid-internal-auth` and
`reliefgrid-node-auth`. Senders receive only the current token; receivers may
receive previous-token for overlap. One release/provisioner must own each Secret.
UI must not receive either credential. Initial enablement needs a separate safe
rollout plan; an empty token is never a compatibility/fail-open mode. An auth
refusal must not erase an earlier potentially committed mutation's unknown outcome.

Verify with `go test -race -count=3 ./internal/auth`. This is helper acceptance,
not end-to-end authenticated deployment or gate completion.
