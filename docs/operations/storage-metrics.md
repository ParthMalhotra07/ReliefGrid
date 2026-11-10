# Storage and coordinator metrics contract

What the storage nodes and the coordinator export for Prometheus (A4.2's
storage panels, S5.2's load report). Owner: S. Changing a name, type, label
or unit here is a contract change: say so in the PR title.

## Scrape

| | storagenode (StatefulSet `storage-<shard>`) | coordinator (Deployment) |
|---|---|---|
| Listener | `:9090`, container port named `metrics` | `:9090`, container port named `metrics` |
| Path | `GET /metrics` (Prometheus text format; nothing else is served on this port) | same |
| Auth | none: read-only, no operator token, no write routes on this port | same |
| Discovery | pod annotation `prometheus.io/scrape: "true"` | same |
| NetworkPolicy | ingress to 9090 only from pods in the release namespace labelled `reliefgrid.io/metrics-scraper: "true"`; the data (8080), mgmt (8081) and Raft (7000) rules are unchanged | ingress to 9090 from the same pods; 8080 unchanged |

The mgmt port keeps its operator-only policy; Prometheus never needs the
operator token. A scrape never proposes a Raft command and never touches the
apply path; metrics are observations only and never feed an FSM decision.

## Labels

Every storage family carries `shard` (`0`..`shards-1`, from the ring config)
and `node` (the pod name, e.g. `storage-1-0`). Other labels have fixed value
sets, listed per family. Never request, resource, operation or attempt IDs,
peers' addresses, paths or error text. Prometheus adds `instance`, `job`,
`component` and `application` from discovery.

## Storage node

Histograms use these buckets (seconds): 0.001, 0.0025, 0.005, 0.01, 0.025,
0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10 (snapshot families add 30 and 60).

| Family | Type | Extra labels | Unit | Meaning |
|---|---|---|---|---|
| `storage_command_latency_seconds` | histogram | `command` = `CreateEntity` `CompareAndSet` `TransitionRequest` `RetryWorkflow` `AgeRequest` `TransitionResource` `TryClaim` `CloseAttempt` `MarkOutboxPublished`; `outcome` = `ok` `error` | s | One mutation on the replica that received it: propose, majority commit and FSM apply (plan §25 "majority/apply cost"). `error` is any non-result (not leader, timeout, unknown outcome); a domain rejection is a result, so `ok`. |
| `storage_read_latency_seconds` | histogram | `consistency` = `strong` `linearizable` `stale` | s | One read served by this replica, including its fence. |
| `storage_raft_commit_seconds` | histogram | | s | Leader only: log append to commit (hashicorp/raft `raft.commitTime`). |
| `storage_raft_fsm_apply_seconds` | histogram | | s | FSM apply of one entry (`raft.fsm.apply`). |
| `storage_raft_snapshot_create_seconds` | histogram | | s | Snapshot taken (`raft.snapshot.create`); `_count` is the number of snapshots. |
| `storage_raft_snapshot_persist_seconds` | histogram | | s | Snapshot written to the PVC (`raft.snapshot.persist`). |
| `storage_raft_snapshot_restore_seconds` | histogram | | s | Snapshot restored into the FSM (`raft.fsm.restore`); `_count` counts restores (replay at start, installs). |
| `storage_raft_install_snapshot_seconds` | histogram | | s | Leader only: snapshot sent to a lagging follower (`raft.replication.installSnapshot`, peer label dropped). |
| `storage_raft_compact_logs_seconds` | histogram | | s | Log compaction after a snapshot (`raft.compactLogs`). |
| `storage_raft_transitions_total` | counter | `to` = `leader` `candidate` `follower` | | Raft state changes of this replica; `to="leader"` counts elections won (failovers). |
| `storage_raft_heartbeat_timeouts_total` | counter | | | Follower lost the leader's heartbeat. |
| `storage_raft_leader` | gauge | | 0/1 | 1 while this replica is leader. |
| `storage_raft_term` | gauge | | | Current term. |
| `storage_raft_commit_index` | gauge | | index | Commit index as this replica knows it. |
| `storage_raft_applied_index` | gauge | | index | FSM's completed index (`/mgmt/status` `appliedIndex`). |
| `storage_raft_last_log_index` | gauge | | index | Last index in the local log. |
| `storage_raft_last_snapshot_index` | gauge | | index | Index of the newest local snapshot (0: none). |
| `storage_replication_lag_entries` | gauge | | entries | `leaderCommitIndex - appliedIndex` on this replica, as it last learned the leader's commit (0 on the leader once applied). |
| `storage_raft_log_bytes` | gauge | | bytes | Size of `raft.db` (log + stable store) on the PVC. |
| `storage_snapshot_bytes` | gauge | | bytes | Size of the snapshot directory on the PVC. |
| `relief_outbox_pending` | gauge | | events | Exact count of unpublished outbox rows in this replica's FSM (`COUNT(*) WHERE published = 0`), not a capped batch. |
| `relief_outbox_oldest_age_seconds` | gauge | | s | Scrape time minus the oldest unpublished row's proposer timestamp; 0 when none are pending. |
| `relief_outbox_observation_success` | gauge | | 0/1 | 1 if the observation above succeeded at this scrape. |
| `relief_outbox_observation_timestamp_seconds` | gauge | | Unix s | When that observation was taken. |
| `relief_outbox_observed_applied_index` | gauge | | index | The FSM's `meta.applied_index`, read by the same SQLite statement (one read snapshot) as the count and oldest row; the count and age above describe exactly this index. |

Outbox semantics: if the stats query fails or times out (1 s),
`relief_outbox_pending`, `relief_outbox_oldest_age_seconds` and
`relief_outbox_observed_applied_index` are **absent** from that scrape and
`relief_outbox_observation_success` is 0. A failed observation is never a
fresh zero. The pending count, oldest age and observed index come from one
SQLite statement, so one read snapshot: an apply committing during the scrape
is in all three or in none. `storage_raft_applied_index` is read separately
and may differ by a few entries; use the observed index to pair a count with
its log position. The read is a local stale read, never a Raft write. Every
replica reports its own FSM; a
follower can lag, so the shard's backlog is the leader's:

```promql
max by (shard) (relief_outbox_pending
  and on (shard, node) (storage_raft_leader == 1)
  and on (shard, node) (relief_outbox_observation_success == 1))
```

The age uses the proposer's wall clock (in the row) against the replica's
wall clock (at scrape); on one host or a synced cluster that is accurate to
clock skew.

Also exported: the standard `go_*` and `process_*` collectors (CPU seconds,
resident memory, open FDs).

## Coordinator

| Family | Type | Labels | Unit | Meaning |
|---|---|---|---|---|
| `coordinator_node_state` | gauge | `shard`, `node`, `state` = `UP` `SUSPECT` `DOWN` | 0/1 | 1 for each registered member's current heartbeat state, 0 for the other two. A member that leaves the registry loses its series. |
| `coordinator_forward_total` | counter | `shard`, `outcome` = `answered` `all_down` `exhausted` `misrouted` | | One routed call: answered by a node (any result or rejection), failed fast because every member is DOWN, budget ran out, or every member answered as another server. This is #113's `route_refresh_total`. |
| `coordinator_leader_hints_total` | counter | `shard` | | NOT_LEADER hints followed (leader discovery). |
| `coordinator_forward_attempts` | histogram | `shard` | attempts | Nodes dialled per routed call (buckets 1, 2, 3, 4, 6, 8, 12, 16). |

Plus `go_*` and `process_*`.

## Cardinality

Storage: about 582 series per replica. `storage_command_latency_seconds`
dominates (9 commands x 2 outcomes x 16 series per histogram = 288), so about
5000 for nine replicas in total. Coordinator: about 250 for three shards of
three. Each target stays well under a `sample_limit` of 5000.
