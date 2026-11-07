package raftstore

import (
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
)

// StorageInfo is what this replica keeps on its PVC (S5.2 metrics, #113).
type StorageInfo struct {
	LogBytes          int64  // raft.db: the log and stable store
	SnapshotBytes     int64  // the snapshot directory, every retained snapshot
	LastSnapshotIndex uint64 // Raft index of the newest snapshot; 0 if none
}

// StorageInfo stats the PVC. It is an observation only: no lock Raft or the
// FSM takes, and no command.
func (n *Node) StorageInfo() (StorageInfo, error) {
	var info StorageInfo
	st, err := os.Stat(filepath.Join(n.cfg.DataDir, "raft", "raft.db"))
	if err != nil {
		return StorageInfo{}, fmt.Errorf("raftstore: storage info: %w", err)
	}
	info.LogBytes = st.Size()
	snapDir := filepath.Join(n.cfg.DataDir, "snapshots")
	err = filepath.WalkDir(snapDir, func(_ string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.Type().IsRegular() {
			fi, err := d.Info()
			if err != nil {
				return err
			}
			info.SnapshotBytes += fi.Size()
		}
		return nil
	})
	if err != nil && !os.IsNotExist(err) {
		return StorageInfo{}, fmt.Errorf("raftstore: storage info: %w", err)
	}
	metas, err := n.snaps.List()
	if err != nil {
		return StorageInfo{}, fmt.Errorf("raftstore: storage info: %w", err)
	}
	if len(metas) > 0 {
		info.LastSnapshotIndex = metas[0].Index // newest first
	}
	return info, nil
}
