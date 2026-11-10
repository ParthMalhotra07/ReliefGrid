# A6.1 failed-verifier scratch recovery

Stacked on #121. After workers stop, unresolved immutable intents are replayed with their original operationId, At, bytes and conditional version. A definite matching VERSION_CONFLICT is recorded as a rejection, never as ownership. Cleanup then requires a current strong/same-ring scan covering all shards, an exact private scratch namespace/type/value and committed CAS. Partial/weak/wrong-ring/foreign responses fail closed. Lost cleanup responses retry the same cleanup operation.

Original acceptance errors are never cleared: recovering scratch data cannot turn a failed run into PASS. The failed stop is not repeated by the outer finally. Six transport/recovery regression cases PASS; existing fault/envelope, forward, PromQL/chart tests and make check/gen PASS. The response fixtures test client handling, not FSM correctness.

A separate real-FSM injection committed one scratch PUT and deliberately retained its original pending intent as if its response were lost. Recovery replayed that same operation, used current authoritative state for one CAS cleanup, verified no live owned key and retained the injected failure. Real proof is in real-fsm.txt. No business requests/resources/claims or S-owned source changed. PRE-PR REVIEW: READY.
