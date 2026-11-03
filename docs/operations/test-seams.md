# Test seams for SCH01, SCH12 and SCH13 (S4.6b, #88)

Deterministic boundaries that let the adversarial schedule harness (A2.3) stop the storage
layer at a point it can prove, kill or restart a replica there, and check what comes back.
They are test-only:

- **M8** (`internal/fsm/memstore`): methods on `*memstore.Store`. memstore is in no binary.
- **raftstore**: unexported hooks, nil unless a test sets `Config.seams`. Every production
  call site is a nil check; the log store is wrapped only when seams are configured.
- **kind**: the `seams` build tag compiles in `(*raftstore.Node).ArmSeam` and the
  `/mgmt/test/seams` routes. Only `reliefgrid/storagenode:seams` (`make storage-image-seams`)
  has them; the default image is byte-identical to a build without the tag, and
  `TestSeamsCompiledOutOfDefaultBinary` checks that neither marker is in it.
- **Checks**: `make check-seams` runs go vet, golangci-lint and the race tests for the seams and
  kind builds, plus the symbol check above. CI runs it in the `seams` job, only for PRs that
  touch the seam packages (`scripts/ci-changes.sh`), so `make check` stays fast. Run it
  before you push seam changes.

Hooks receive only positions and IDs. They never change what FSM apply computes (invariant
2): the apply hold only delays, and only the snapshot streams can be failed, through the error
paths a crash already takes.

Every boundary is one-shot: **armed → reached → released | aborted | expired**. A harness waits
for *reached* and fails if it never comes (`testseam.ErrNotReached`). A reached hold that is
neither released nor aborted expires after its timeout and lets the node go on, so a forgotten
seam cannot wedge a replica. Arming lives in memory: a restarted process is always disarmed.

## Kinds

| Kind | Schedule | Where it stops | Point reported |
| --- | --- | --- | --- |
| `log-barrier` | SCH01 | the first listed command, before apply, until every listed command is appended to the leader's log **and committed** while none has applied | `indexes` (opId → log index), `commitIndex`, `appliedIndex` |
| `snapshot-persist` | SCH12 | halfway through writing the snapshot image (Persist) | `imageIndex`, `bytes`, `size` |
| `restore-transfer` | SCH13a | halfway through receiving InstallSnapshot, before Raft has the snapshot | `raftIndex`, `bytes`, `size` |
| `restore-stage` | SCH13b | halfway through staging a fully received snapshot into the FSM | `imageIndex`, `bytes`, `size` |

## SCH13 is two schedules

raft v1.8.0 installs a snapshot in two steps. It streams the RPC into its snapshot store
(raft.go:1909), renames it into place on `sink.Close()` (:1928), and only then calls
`FSM.Restore` (:1936). So "crash during restore" is two different crashes:

- **SCH13a, during transfer:** the incomplete `<id>.tmp` is removed at the next start and the
  leader sends the snapshot again.
- **SCH13b, during stage:** the snapshot is already complete on the PVC. At the next start
  the node restores **its own copy** before talking to the leader. The leader still resends
  the same snapshot once (its `nextIndex` for the follower only moves on a successful install,
  replication.go:370), which installs the same index.

In both cases the follower ends with the leader's rows at the same index.

Proposed fixture change for `tests/workflow/fixtures.go` (A-owned):

```go
// ── SCH13a ──
{
	Tag:    "SCH13a_RestartDuringSnapshotTransfer",
	Name:   "Restart during snapshot transfer",
	Source: "restart during snapshot creation/restore",
	InitialState: /* as SCH13 */,
	Steps: []Step{
		{Actor: "Follower", Kind: StepBarrier, Details: "isolated past the leader's trailing logs; leader snapshots and compacts"},
		{Actor: "Follower", Kind: StepBarrier, Details: "begins receiving InstallSnapshot from the leader"},
		{Actor: "Follower", Kind: StepFault, Details: "killed halfway through the transfer; snapshot incomplete (<id>.tmp)"},
		{Actor: "Follower", Kind: StepBarrier, Details: "restarts; removes the incomplete snapshot"},
		{Actor: "Follower", Kind: StepBarrier, Details: "leader sends the snapshot again; it installs; remaining log entries applied"},
	},
	FinalState: /* as SCH13 */,
	Protects: "§16/I2: a partial transfer is never installed; the leader resends; follower matches the leader's committed state",
},
// ── SCH13b ──
{
	Tag:    "SCH13b_RestartDuringSnapshotStage",
	Name:   "Restart during snapshot restore into the FSM",
	Source: "restart during snapshot creation/restore",
	InitialState: /* as SCH13 */,
	Steps: []Step{
		{Actor: "Follower", Kind: StepBarrier, Details: "isolated past the leader's trailing logs; leader snapshots and compacts"},
		{Actor: "Follower", Kind: StepBarrier, Details: "receives the whole snapshot (stored); begins restoring it into the FSM"},
		{Actor: "Follower", Kind: StepFault, Details: "killed halfway through staging the image; FSM directory holds a partial database"},
		{Actor: "Follower", Kind: StepBarrier, Details: "restarts; wipes the FSM directory; restores its own complete snapshot"},
		{Actor: "Follower", Kind: StepBarrier, Details: "remaining log entries applied (the leader may resend the same snapshot)"},
	},
	FinalState: /* as SCH13 */,
	Protects: "§16/I2: a partially staged image is never opened; the stored snapshot restores; follower matches the leader",
},
```

## M8 API (`*memstore.Store`)

```go
s, _ := memstore.New(ctx, memstore.WithShards(3), memstore.WithCheckpointDir(t.TempDir()))

// SCH01: propose A, wait for it in the log, then B; reached = both appended, none applied.
b, _ := s.ArmLogBarrier(shard, 10*time.Second, opA, opB)
go casA(); b.WaitAppended(ctx, opA)
go casB(); p, err := b.Hold().WaitReached(ctx) // p.Indexes, p.CommitIndex, p.AppliedIndex
b.Hold().Release()                             // A applies first

// SCH12: one complete checkpoint, more writes, then a checkpoint that dies halfway.
s.Checkpoint(ctx, shard)
h, _ := s.ArmCheckpointPause(shard, 10*time.Second)
go s.Checkpoint(ctx, shard)
h.WaitReached(ctx)
s.Crash(shard)                   // aborts the paused checkpoint; calls fail with ErrShardDown
info, _ := s.Restart(ctx, shard) // info.RestoredIndex = the complete checkpoint; log replayed

// SCH13b: a restart that dies halfway through staging its checkpoint.
h, _ = s.ArmRestorePause(shard, 10*time.Second)
s.Crash(shard); go s.Restart(ctx, shard)
h.WaitReached(ctx); s.Crash(shard)
s.Restart(ctx, shard)

s.Dump(ctx, shard) // rows, to compare before and after
```

In M8 an appended entry counts as committed, and a crashed shard reads as a failed shard
(scans are partial, never empty). Commands that were appended but not applied before a crash
return an unknown outcome and are applied on restart. M8 has no leader, so SCH13a has no M8
form. M8 models checkpoints with the same FSM image code; it does not exercise Raft's
snapshot store (the raftstore tests and the kind run do).

## kind API (seams image only)

On each storage pod's mgmt port (8081), with the operator token (`Authorization: Bearer
<storage-operator-token>`). Reach it with `kubectl port-forward pod/<pod> :8081`: a
port-forward is not cut by the Raft isolation label.

```
GET  /mgmt/status                        … "seams": true  (absent on the default image)
POST /mgmt/test/seams                    {"kind", "operationIds"?, "timeoutMs"} → 201 {"id","kind","state"}
GET  /mgmt/test/seams/{id}               {"id","kind","state","point"?,"appended"?}
POST /mgmt/test/seams/{id}/release       → status
POST /mgmt/test/seams/{id}/abort         → status
```

`timeoutMs` is required, at most 120000. `operationIds` (two or more, distinct, the first is
held) is for `log-barrier` only. One boundary of each kind can be armed at a time per process
(400 otherwise). `appended` shows a log barrier's operations in the log so far, so the runner
can send B only after A is appended.

### Procedures (`tests/seams/kind_test.go`, `make verify-S4.6b`)

- **SCH01:** arm `log-barrier` on the shard leader with `[opA, opB]` and `timeoutMs` 2500.
  Send A through the coordinator. Poll until `appended[opA]` is set, send B, poll until
  `reached`, then release. The storage node's `REQUEST_TIMEOUT_MS` is 3000, so the hold must
  end before then; a slower runner sees `expired` and fails rather than an unknown outcome.
- **SCH12:** write to the shard, `POST /mgmt/snapshot` on the leader (the complete earlier
  snapshot), write more, arm `snapshot-persist` on the leader, and trigger a snapshot. Once it
  is reached, SIGKILL the leader. Check that `restoredIndex` equals the earlier snapshot, that
  the log reports `removed incomplete snapshot left by a crash`, that the rows equal the new
  leader's, and that the next snapshot succeeds.
- **SCH13a/b:** label a follower `reliefgrid.io/chaos-isolated=true` (S2.3; if the cut does
  not take within 8 s, clear the pod's conntrack entries on every kind node). Write more than
  `TrailingLogs` (512 in the demo profile) plus a margin to the shard and snapshot the leader.
  Arm `restore-transfer` or `restore-stage` on the still-running follower, then remove the
  label. The leader's InstallSnapshot reaches the pause; SIGKILL the follower. Then check as
  described above.

**Kill:** `docker exec <node> crictl ps -q --name storagenode --label
io.kubernetes.pod.name=<pod>`, then `crictl inspect` for the PID, then `kill -9`. The kubelet
restarts the container in the same pod, so both the PVC and the emptyDir FSM directory
survive, and the FSM directory is wiped on start. Fallback, reported as such:
`kubectl delete pod --force --grace-period=0`, which replaces the pod and its emptyDir.
