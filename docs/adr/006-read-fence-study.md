# ADR 006: use the barrier-versus-fence study as the M6 experiment

Status: Proposed for joint agreement by A and S. This record does not declare
the enhanced gate complete, change its authentication scope, or select a second
major experiment. The read-fence implementation itself is already merged in
PR #71; selecting its bounded comparative study as M6 still requires agreement.

## Context and proposed decision

Part 1 amendment M6 permits at most one major experiment. S has proposed using
the existing S4.1 strong-read comparison instead of adding another protocol
extension. A proposes accepting that study as the one experiment, with the
existing implementation, negative tests and reproducible demonstration below.
Other major extensions remain future work unless this decision is replaced
jointly. The experiment studies an existing optimization; it does not claim
that the optimization or a new consensus algorithm was invented here.

Compare a committed log barrier (`ReadStrong`) with the quorum/term/FSM-gated
linearizable read (`ReadLinearizable`) on the same three-node loopback cluster.
Both must retain the strong-read contract. A faster stale read is not an
acceptable alternative. The fence is guarded by a current-term committed
entry, a captured read index, fresh quorum acceptance, same-term checks and an
FSM-applied command target. A first-read term gate can still append a barrier;
zero entries/read is the measured steady-state result, not a promise that no
barrier is ever appended during leadership changes.

Implementation references: [read fence](../../internal/raftstore/fence.go),
[baseline and read modes](../../internal/raftstore/node.go), and
[three-node benchmark and negative tests](../../internal/raftstore/fence_test.go).

## Method and retained observations

The [original benchmark](../evidence/S4.1/bench-20261031T083641Z.txt) records
commit `5ad15ac`, a clean code tree, Linux/amd64, Go benchmark CPU suffix 8 and
an Intel Core Ultra 7 258V. The host was shared with other work (load about 2).
The command used `-benchtime 2000x -count 5`: five samples for each mode under
serial and parallel readers, 2,000 iterations in every sample. The benchmark
records elapsed ns/op and Raft log entries appended/read. It initializes a
committed key before measuring; it is a read microbenchmark rather than an
end-to-end workflow or crash-during-load benchmark.

Serial median elapsed time was 449,566 ns/op for the barrier and 126,084 ns/op
for the fence. Parallel median elapsed time was 112,730 and 23,335 ns/op,
respectively. Every retained barrier sample appended 1 entry/read; every fence
sample appended 0. The [verified summary](../evidence/A5.3-read-fence/summary.json)
derives medians, ranges and ratios directly from the retained data. These are
ratios of separate sample medians, not paired-trial speedups or confidence bounds.

The [historical safety campaign](../evidence/S4.1/fence-seeds-20261031T082917Z.txt)
retains twenty successful shuffled race-test seeds plus one route/configuration
check (21/21). The seed controls test order; it is not twenty distinct workload
or network models. This is historical evidence from the named tree and host,
not a fresh certification of today's deployment.

## Negative tests and demonstration

Permanent negative tests reject an isolated leader, stale acknowledgments and
a term change before the own-term gate; other tests exercise waiting for the
SQLite FSM, deadline/shutdown behavior, compaction and read-your-writes. In
particular, `TestFencedReadTermChangeBeforeOwnTermGate` covers the unsafe combined
term/ack mutation documented in `fence.go`; the safety claim does not depend on
removing a redundant same-term check alone failing a test.

To demonstrate the retained experiment without creating new benchmark claims:

```sh
python3 scripts/verify-M6-study.py
go test -race -count=1 ./internal/raftstore -run '^(TestFencedReadIsolatedLeaderRefuses|TestFencedReadStaleAckNotCounted|TestFencedReadTermChangeBeforeOwnTermGate)$'
```

The verifier requires all four five-sample groups, the expected iteration and
log-entry measurements, historical source metadata and all twenty safety seeds.
It emits the machine-readable historical comparison. Current focused safety
output is retained separately; it does not relabel the older twenty-seed run.
To reproduce a new measurement on a chosen clean tree and host, separately run:

```sh
go test ./internal/raftstore -run '^$' -bench StrongRead -benchtime 2000x -count 5
```

Record that run's commit, host, Go version and raw output separately. Do not
replace the original historical records or silently reuse their provenance.

## Limitations and acceptance

One shared loopback host is not a multi-host or kind-network performance study.
Fixed mode ordering, cache/warm-up effects and shared-host noise can influence
the comparison. Parallel ns/op reflects aggregate benchmark throughput and is
not an individual request tail-latency distribution. No p95/p99, saturation,
cloud, nine-replica system, cross-machine speedup or present-cluster guarantee
is established. This study does not replace #126 load measurements, strict
invariant checks, enhanced-gate evidence or live presenter rehearsals.

M6 selection becomes agreed only when both owners accept this record. If that
agreement is not reached, report this as an S4.1 engineering study and leave
M6 unselected; do not advertise a completed additional extension. Gate scope
for application aging and post-gate authentication is a separate decision.
