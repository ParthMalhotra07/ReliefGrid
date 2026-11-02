package memstore

import (
	"context"
	"errors"
	"time"

	"github.com/ParthMalhotra07/ReliefGrid/internal/testseam"
)

// Test seams for the adversarial schedules (S4.6, #88). M8 has no Raft: a
// shard's "log" is the sequence of entries handed to its FSM, its
// "snapshot" a checkpoint file (VACUUM INTO plus a header) in the directory
// given by WithCheckpointDir, and a crash drops the shard's FSM so that
// Restart rebuilds it from the last complete checkpoint and the retained log.

// ErrShardDown is returned for a shard between Crash and a completed Restart.
var ErrShardDown = errors.New("memstore: shard down")

// WithCheckpointDir enables Checkpoint, Crash and Restart, keeping
// checkpoint files in dir and every shard's log since its last checkpoint.
func WithCheckpointDir(dir string) Option { return func(c *config) {} }

// CheckpointInfo describes a complete checkpoint.
type CheckpointInfo struct{ AppliedIndex uint64 }

// RestartInfo is what Restart rebuilt: the checkpoint it restored (0: none)
// and the applied index after replaying the log.
type RestartInfo struct{ RestoredIndex, AppliedIndex uint64 }

// ArmLogBarrier holds the first of opIDs before it applies on shard until
// every listed command is in the shard's log (SCH01).
func (s *Store) ArmLogBarrier(shard int, timeout time.Duration, opIDs ...string) (*testseam.Barrier, error) {
	return nil, errors.New("memstore: ArmLogBarrier not implemented")
}

// Checkpoint writes a complete checkpoint of shard.
func (s *Store) Checkpoint(ctx context.Context, shard int) (CheckpointInfo, error) {
	return CheckpointInfo{}, errors.New("memstore: Checkpoint not implemented")
}

// ArmCheckpointPause pauses the next Checkpoint of shard halfway through
// writing its image (SCH12).
func (s *Store) ArmCheckpointPause(shard int, timeout time.Duration) (*testseam.Hold, error) {
	return nil, errors.New("memstore: ArmCheckpointPause not implemented")
}

// ArmRestorePause pauses the next Restart of shard halfway through staging
// its checkpoint (SCH13b).
func (s *Store) ArmRestorePause(shard int, timeout time.Duration) (*testseam.Hold, error) {
	return nil, errors.New("memstore: ArmRestorePause not implemented")
}

// Crash kills shard: its FSM is dropped, a checkpoint or restart paused at a
// seam stops where it is (leaving its partial file, as a dying process
// would), and calls fail with ErrShardDown until Restart.
func (s *Store) Crash(shard int) error { return errors.New("memstore: Crash not implemented") }

// Restart rebuilds a crashed shard from its last complete checkpoint and
// replays the log after it.
func (s *Store) Restart(ctx context.Context, shard int) (RestartInfo, error) {
	return RestartInfo{}, errors.New("memstore: Restart not implemented")
}

// Dump returns shard's rows (fsm.FSM.Dump).
func (s *Store) Dump(ctx context.Context, shard int) (map[string][]string, error) {
	return nil, errors.New("memstore: Dump not implemented")
}
