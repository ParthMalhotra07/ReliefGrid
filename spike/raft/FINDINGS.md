# Raft spike findings (S0.3)

Throwaway experiment: one HashiCorp Raft group of three StatefulSet pods on the local kind
cluster, with a single-table SQLite CAS state machine. Reproduce with `spike/raft-kill.sh`
(cluster up); evidence from the last run is in `docs/evidence/spike/raft-<host>.txt`.

Setup: `hashicorp/raft` v1.8.0, `raft-boltdb/v2` v2.4.2 (log and stable store), file snapshot
store (retain 2), all on the pod's PVC; SQLite FSM (`modernc.org/sqlite` v1.60.1) on an
emptyDir with WAL and `synchronous=OFF`. Library-default timeouts (heartbeat 1 s, election 1 s,
leader lease 500 ms); M4 snapshot settings (threshold 1024, trailing logs 512, interval 30 s).
The Verify's default load is 20 000 CAS writes (runs 1 to 7 below used 2 000).
Host: ThinkPad E14 Gen 7, 8 CPUs, native Linux, kind v0.33.0, Kubernetes v1.37.0.

## Election time

Measured from `kubectl delete pod <leader>` to a survivor reporting `Leader` at a higher term,
polled through `kubectl exec` (about 0.2 to 0.5 s resolution):

| Run | Load before kill | Election |
| --- | --- | --- |
| 1 | 2 000 CAS writes | 2.5 s |
| 2 | 50 000 CAS writes, all 8 host CPUs saturated | 1.6 s |
| 3 | 2 000 | 2.5 s |
| 4 | 2 000 | 2.5 s |
| 5 | 2 000 | 2.1 s |
| 6 | 2 000 | 1.3 s |
| 7 | 2 000 | 1.8 s |
| 8 | 20 000 (failed the rejoin read; see Applied index) | 2.1 s |
| 9 | 20 000 | 2.8 s |
| 10 | 20 000 | 2.1 s |
| 11 (evidence file) | 20 000 | 1.6 s |

Range 1.3 to 2.8 s with default timeouts. Almost all of it is failure detection: in run 2 the
survivor logged `heartbeat timeout reached` at 17:07:51.159 and `election won: term=3 tally=2`
13 ms later (pre-vote, then vote). A follower's heartbeat timeout is randomised between 1 and
2 s, which matches the spread. Run 9 is the slow one because its first candidate round
(17:43:04.639) did not win and the survivor won the second round at 17:43:05.755, one more
randomised timeout later. The leader exits on SIGTERM without a leadership transfer, so this
is the crash case; a graceful `LeadershipTransfer` would be faster and is not what we measured.

For later goals: Raft fails over well inside the 3 s SUSPECT target the coordinator uses for
heartbeats (v3.1 section 11), so the coordinator will see the new leader before it marks the old
one SUSPECT. A 10 s limit in Verify scripts leaves room for slower laptops.

## DNS advertise

**It does not work as-is.** `raft.NewTCPTransport(bind, advertise net.Addr, ...)` requires the
advertise address to be a `*net.TCPAddr` with a concrete IP: `newTCPTransport` in
`tcp_transport.go` (v1.8.0) returns `errNotTCP` for any other `net.Addr` and
`errNotAdvertisable` for a nil or unspecified IP. Passing a resolved pod IP would work at first,
but that IP is what followers learn as the leader address, and it goes stale when the pod is
recreated with a new IP.

**Design used (`spike/raft/transport.go`):** a custom `raft.StreamLayer` passed to
`raft.NewNetworkTransport`:

- `dnsAddr` is a `net.Addr` whose `String()` is the FQDN
  (`storage-spike-N.storage-spike.<ns>.svc.cluster.local:7000`) and whose `Network()` is `tcp`.
- `dnsStreamLayer` embeds the `net.Listener` bound on `:7000` and overrides `Addr()` to return
  that `dnsAddr`. `NetworkTransport.LocalAddr()` is `ServerAddress(stream.Addr().String())`, so
  the leader puts its FQDN in every AppendEntries, and followers report it as the leader address.
- `Dial(address, timeout)` calls `net.Dialer{Timeout: timeout}.Dial("tcp", address)` with the
  peer's `ServerAddress`, which is also an FQDN taken from the explicit bootstrap configuration.

**Name resolution on every dial.** The layer keeps no address of its own; each `Dial` hands the
name to Go's resolver, which has no in-process cache and asks cluster DNS each time.
`NetworkTransport` pools up to 3 connections per peer; when a peer restarts, the pooled
connection fails on its next RPC, is discarded, and the next RPC dials again, so it resolves the
name again and reaches the pod's new IP. Nothing on the path caches the answer: the image is
distroless (no nscd), and kind's CoreDNS Corefile disables both success and denial caching for
`cluster.local`, so the `kubernetes` plugin answers from its live EndpointSlice watch (its
`ttl 30` would only matter to a client-side cache). A recreated peer is reachable as soon as its
new IP is in the EndpointSlice. The real cluster must keep that property: a client-side DNS
cache (for example NodeLocal DNSCache) would delay reconnects by up to the TTL. In run 11
the old leader came back on a new IP (10.244.3.60 -> 10.244.3.62) and the new leader replicated
to it without any configuration change; every run since run 7 records the IP change. The headless Service publishes unready pods
(`publishNotReadyAddresses: true`) so peers resolve each other before readiness.

In every run, all three pods reported the leader and all three configuration entries by FQDN;
no pod IP appears in the Raft configuration.

## Spurious elections

None with the default timeouts on this host. In every run the Verify checked that all three pods
were still at term 2 under the bootstrap leader after the load; in the logs of runs 2 and 9 the
only `heartbeat timeout reached` line came after the kill. Run 2 is the strongest case: 50 000
CAS writes over 55 s (910 ops/s, 8 workers) while 8 busy loops held all host CPUs, with no term
change. Without the CPU hog, writes run at about 2 550 ops/s (20 000 in 7.8 s).

Not covered: WSL2. The plan's tuning step (raise `HeartbeatTimeout` and `ElectionTimeout` to 2 s
and `LeaderLeaseTimeout` to 1 s for the kind profile) should be decided by running
`LOAD_OPS=50000 spike/raft-kill.sh` on the WSL2 laptop. Native Linux does not need it.

## Applied index

**`raft.AppliedIndex()` is not "visible to reads".** In v1.8.0, `processLogs` (raft.go) queues
committed entries on `fsmMutateCh` (up to 128 batches of up to 64 entries) and then calls
`setLastApplied`; `FSM.Apply` runs later on the FSM goroutine. After a restart that replays a
long log, `AppliedIndex()` can be thousands of entries ahead of the state machine.

Run 8 hit this: the restarted old leader (killed before its first snapshot, so it replayed all
20 006 entries) reported the leader's applied index, but a read still returned `alpha@1`, not
`beta@2`. Polling a replaying pod directly showed the gap:

| Raft `AppliedIndex()` | FSM applied | Commit |
| --- | --- | --- |
| 0 | 10 667 | 20 006 |
| 20 006 | 12 551 | 20 006 |
| 20 006 | 17 276 | 20 006 |
| 20 006 | 20 006 | 20 006 |

The fix (in this branch): the FSM records the index of the last command it applied (also carried
in its snapshots), `/status` reports it as `appliedIndex`, and Raft's value is kept as
`raftAppliedIndex`. The Verify now times both; in run 10 Raft's index matched the leader 0.1 s
after the pod was Ready (FSM at 18 110 of 20 006) and the FSM matched at 0.9 s.

## Pod RAM

About 19 to 21 MiB RSS per pod (cgroup `memory.current` 19 to 20 MiB) after the load and failover,
with a 1-row-per-key table. The pods have a 256 MiB limit; the real storage node will be larger
(more tables, the outbox), but Raft plus boltdb plus SQLite is not a memory concern for the
five-node cluster.

## Plan impact

- **M2: pin v1.8.0.** The spike passed on v1.8.0, so no fallback to v1.7.3. `raft-boltdb/v2` is
  pinned at v2.4.2; note that it pulls `raft-boltdb` v1 and `boltdb/bolt` in as indirect
  dependencies (for its v1-to-v2 migration helper). **Change to the plan:** `internal/raftstore`
  cannot use `raft.NewTCPTransport`; it must carry the DNS `StreamLayer` above with
  `NewNetworkTransport`. The v3.1 phrase "stable advertised DNS" is otherwise achievable as written.
- **M3: confirmed.** The FSM deletes its SQLite file on open; on restart the old leader logged
  `restored from snapshot: ... last-index=24011` and replayed the log to the leader's applied
  index (50 006 in run 2), then served the latest value. Nothing durable lives in the SQLite file.
- **M4: confirmed that the demo settings snapshot.** With threshold 1024 the 50 000-write load
  produced snapshots at indexes 19 822, 24 011 and 30 836 across the pods. Not exercised:
  InstallSnapshot to a follower that is more than `TrailingLogs` behind (the old pod was never
  that far behind). That belongs in the S2 snapshot tests.
- **M7 and v3.1 section 10: the per-pod guard is not enough.** `BootstrapCluster` refuses only when
  *this* pod has state (`ErrCantBootstrap`). A wiped pod would accept a direct bootstrap and form
  a second one-node cluster. The spike's Job checks every peer first, which caught the re-run;
  the real system needs the manifest ConfigMap and admission gate exactly as M7 says.
- **M5, S4.1 and readiness: use the FSM's applied index.** Anything that waits for "applied up to
  index N" (strong reads after a barrier, the fast read fence, replica readiness, the
  completed-apply lag metric) must read the FSM's own index, never `raft.AppliedIndex()`. The
  `internal/fsm` package in PR #6 already writes `applied_index` in the same SQLite transaction
  as the command, which is the right source; `internal/raftstore` must wait on that.
- **Invariant 5.** `Apply` errors other than `ErrNotLeader` (for example `ErrLeadershipLost`) are
  returned as "outcome unknown" (HTTP 503), not as a rejection; `internal/raftstore` should keep
  that distinction.

## Limitations

- One host; the three "nodes" are kind containers sharing a kernel and disk, so there is no real
  network partition or disk failure here.
- Leader death is a SIGTERM with the process exiting at once; no `kill -9` mid-write, no
  partition (that is chaos work in later goals).
- Election time is limited by `kubectl exec` polling resolution.
- The spike's FSM keeps its applied index in memory (updated after the SQLite commit), not in the
  same transaction as the write; the real FSM must persist it with the write.
- Reads are local FSM reads (no strong reads through the log, M5) and `LocalID` is the pod name
  (no incarnation UUID yet, v3.1 section 10).
