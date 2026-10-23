package raftstore

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/google/uuid"
	"github.com/hashicorp/raft"
)

const identityFile = "identity.json"

var (
	// ErrIdentityMismatch: the PVC's identity belongs to another pod, group
	// or shard.
	ErrIdentityMismatch = errors.New("raftstore: identity on disk does not match this node")
	// ErrIdentityMissing: Raft state exists without an identity file. The disk
	// is inconsistent; a new incarnation must not adopt an old member's log.
	ErrIdentityMissing = errors.New("raftstore: raft state exists but identity file is missing")
)

// Identity is a replica's persistent identity (v3.1 §10). The incarnation is
// a UUID created once, when the PVC is new, and kept with the Raft state on
// every ordinary restart; a wiped disk gets a new one.
type Identity struct {
	PodName     string `json:"podName"`
	Incarnation string `json:"incarnation"`
	GroupID     string `json:"groupId"`
	Shard       int    `json:"shard"`
}

// ServerID is the Raft LocalID: podName/incarnation.
func (id Identity) ServerID() raft.ServerID {
	return raft.ServerID(id.PodName + "/" + id.Incarnation)
}

// loadOrCreateIdentity reads dir/identity.json and checks it against want
// (whose Incarnation is ignored). If the file is absent it creates one with
// a new incarnation, but only when the disk has no Raft state.
func loadOrCreateIdentity(dir string, want Identity, hasRaftState bool) (Identity, error) {
	path := filepath.Join(dir, identityFile)
	b, err := os.ReadFile(path)
	switch {
	case err == nil:
		var id Identity
		if err := json.Unmarshal(b, &id); err != nil {
			return Identity{}, fmt.Errorf("raftstore: decode %s: %w", path, err)
		}
		if _, err := uuid.Parse(id.Incarnation); err != nil {
			return Identity{}, fmt.Errorf("raftstore: %s: incarnation %q: %w", path, id.Incarnation, err)
		}
		if id.PodName != want.PodName || id.GroupID != want.GroupID || id.Shard != want.Shard {
			return Identity{}, fmt.Errorf("%w: disk has %s/%s shard %d, node is %s/%s shard %d", ErrIdentityMismatch,
				id.PodName, id.GroupID, id.Shard, want.PodName, want.GroupID, want.Shard)
		}
		return id, nil
	case !errors.Is(err, os.ErrNotExist):
		return Identity{}, fmt.Errorf("raftstore: read %s: %w", path, err)
	case hasRaftState:
		return Identity{}, fmt.Errorf("%w (%s)", ErrIdentityMissing, path)
	}

	id := want
	id.Incarnation = uuid.NewString()
	b, err = json.Marshal(id)
	if err != nil {
		return Identity{}, fmt.Errorf("raftstore: encode identity: %w", err)
	}
	if err := writeFileSync(dir, identityFile, b); err != nil {
		return Identity{}, err
	}
	return id, nil
}

// writeFileSync writes dir/name through a temporary file, fsyncs it, renames
// it into place and fsyncs dir, so a crash leaves either no file or the
// whole file.
func writeFileSync(dir, name string, b []byte) error {
	tmp := filepath.Join(dir, name+".tmp")
	f, err := os.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o600)
	if err != nil {
		return fmt.Errorf("raftstore: create %s: %w", tmp, err)
	}
	_, werr := f.Write(b)
	serr := f.Sync()
	cerr := f.Close()
	if err := errors.Join(werr, serr, cerr); err != nil {
		_ = os.Remove(tmp)
		return fmt.Errorf("raftstore: write %s: %w", tmp, err)
	}
	if err := os.Rename(tmp, filepath.Join(dir, name)); err != nil {
		return fmt.Errorf("raftstore: rename %s: %w", tmp, err)
	}
	return syncDir(dir)
}

func syncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return fmt.Errorf("raftstore: open dir %s: %w", dir, err)
	}
	serr := d.Sync()
	cerr := d.Close()
	if err := errors.Join(serr, cerr); err != nil {
		return fmt.Errorf("raftstore: sync dir %s: %w", dir, err)
	}
	return nil
}
