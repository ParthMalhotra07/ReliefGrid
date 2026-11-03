# Merkle audit (S4.5)

The Merkle audit checks that the three replicas of a shard hold the same
FSM content at the same command index (v3.1 §15, §26). It only reads.
Matching roots support a tested convergence claim, not a proof of every
possible execution.

## What is hashed

| Table | Order | Notes |
|---|---|---|
| `records` | `key` | every column, tombstones (`deleted = 1`) included; `body` as stored bytes |
| `operation_results` | `operation_id` | includes `payload_hash` and `applied_index` |
| `attempt_closures` | `resource_key, attempt_id` | |
| `outbox` | `seq` | includes `published` |
| `meta` | — | not hashed: `schemaVersion` and `appliedIndex` are reported beside the root |

- **Explicit columns:** the columns are listed in `internal/fsm/merkle_read.go`.
  A test compares them with `PRAGMA table_info`, so a column added later fails
  the build until the audit covers it. An unexpected `meta` key is an error.
- **Encoding:** each value is a type tag (NULL, integer, text or blob), then a
  fixed 8 bytes for an integer or a length plus the bytes for text and blobs.
  Every value is self-delimiting, so different rows never encode to the same
  bytes.
- **Hashes:** SHA-256, with a distinct first byte for leaves, inner nodes,
  buckets and the empty tree.
- **Shape:** each table has 256 fixed buckets, chosen by the first byte of the
  SHA-256 of the row's primary key. A table's root is a tree over its bucket
  hashes, and the shard root is a tree over the four table roots.
- **Why buckets:** a changed, missing or extra row changes exactly one bucket.
  That's what lets a mismatch be narrowed down to one row.

## Same index, without blocking applies

- **One read transaction:** a digest is read in one read transaction on the
  FSM's read-only pool. Its first statement reads `meta.applied_index`, which
  fixes the view (WAL mode) for every table read after it.
- **Exact index:** `GET /mgmt/merkle?at=N` answers 409 `INDEX_MISMATCH` with
  the replica's own `appliedIndex` unless that read is exactly at command
  index N.
- **No effect on Apply:** Apply keeps committing on the write connection
  throughout, and nothing in the audit runs inside Apply or writes. A snapshot
  Restore waits for the digest to finish, at most 30 s.
- **Command index, not Raft index:** comparisons are always at the FSM's
  command index (`appliedIndex`). Raft's index also counts term no-ops,
  barriers and configuration entries, which change no records. The audit
  reports both indexes and notes when the Raft indexes differ.

## Running it

1. **Quiesce first.** Stop the API writers and matcher admission, let the
   relay drain, and let workflows settle.
2. **Run the audit** with one management endpoint per replica, from a pod
   labelled `reliefgrid.io/storage-operator=true`. Each shard's NetworkPolicy
   admits the management port (8081) only from operator pods, so running it
   inside a storage pod times out. `scripts/verify-S4.5.sh --kind` runs it as
   a Job built from the bootstrap Job's pod spec, the same way
   `scripts/storage-replace.sh` runs storagectl:

   ```sh
   OPERATOR_TOKEN=... storagectl audit \
     --addr storage-shard-0-0.storage-shard-0:8081 \
     --addr storage-shard-0-1.storage-shard-0:8081 \
     --addr storage-shard-0-2.storage-shard-0:8081
   ```

3. **What it does:** it takes the highest applied index the replicas report,
   waits up to `--settle` (30 s) for all of them to be exactly there, and asks
   each for its digest at that index. A replica that has moved past it makes
   the audit retarget, up to `--attempts` (5) times. The audit never asks for
   a digest without an exact index.

| Exit | Verdict | Meaning |
|---|---|---|
| 0 | `equal` | every root is equal at `appliedIndex` |
| 1 | `divergent` | `tables` lists the differing buckets; `rows` gives each differing key, its kind (`content`, `missing`, `extra`, or `conflict` when no majority agrees) and the pods that disagree |
| 2 | `incomplete` | a replica failed, or the replicas are not one shard on one ring and schema: no verdict |
| 3 | `not-quiescent` | the replicas did not hold one index: no verdict; quiesce and rerun |

- **One pod at a time:** `storagectl merkle [--at N] [--table T --bucket B]`
  prints a single pod's digest, or one bucket's leaves. Leaving out `--at`
  gives the digest at whatever index the read sees. That's for looking at one
  pod, never for comparing.

## When replicas differ

Repair is through Raft, never by copying rows. The FSM is derived state (M3):
restart the divergent pod and it rebuilds its database from its snapshot and
log. If its disk is suspect, use `storagectl replace`. Then rerun the audit.
`internal/raftstore` tests this: a follower whose FSM file was changed outside
Raft gets the leader's root back after a rebuild.

## Cost (measured)

`BenchmarkMerkle` takes a full digest of a file database (`internal/fsm`,
Intel Core Ultra 7 258V, go1.27.2):

| Rows | Full digest | Per row |
|---|---|---|
| 5,000 | 17.1 ms | 3.4 µs |
| 50,000 | 179 ms | 3.6 µs |

- **About 3.5 µs per row**, mostly the SQLite read (modernc, pure Go). Each
  `scripts/verify-S4.5.sh` run measures it again and records the result in
  `docs/evidence/S4.5/`.
- **Other costs:** a digest holds one of the 4 read connections. Fetching one
  bucket's rows rescans that table, and only differing buckets are fetched.
  The response is about 66 KB per pod.

## Verify

- **Go part:** `scripts/verify-S4.5.sh` runs every S4.5 test suite and the
  benchmark, and writes `docs/evidence/S4.5/verify-<time>.txt`.
- **Kind part:** `scripts/verify-S4.5.sh --kind` runs against a deployed
  cluster (`make demo-reset`) and never recreates it. It runs a seeded
  `invcheck load` (`SEED`, one per cluster), then `invcheck verify`. It
  quiesces by scaling every Deployment except the coordinator to 0, then runs
  `invcheck check`. On any exit, including a failure, Ctrl-C or SIGTERM, a
  trap scales those Deployments back to their replicas in their Helm release. Last, it runs `storagectl audit` for each shard. It passes
  only if every shard is `equal` with three pods at one `appliedIndex` and one
  root, past the index read before the load. It writes
  `docs/evidence/S4.5/kind-<time>.txt`.

## Limits

- Equal roots show equal content at one index. They don't show that the
  reads in between were linearizable (v3.1 §14 covers that).
- The audit needs a quiescent shard. Under continuous writes it reports
  `not-quiescent`, not a verdict.
- On an in-memory FSM (unit tests only) reads share the write connection, so
  a digest there holds Apply off while it runs. Pods always use a file.
- Snapshot images aren't yet checked with `integrity_check`, and the snapshot
  header doesn't carry the ring version yet (issue #25, a separate PR).
