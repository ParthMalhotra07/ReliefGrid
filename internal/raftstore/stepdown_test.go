package raftstore

import (
	"errors"
	"testing"

	"github.com/hashicorp/raft"
)

// StepDown (S1.4) moves leadership through Raft: the old leader becomes a
// follower and another voter leads. A follower refuses with ErrNotLeader.
func TestStepDown(t *testing.T) {
	c := newBootstrappedCluster(t, testProfile)
	i, leader := c.waitLeader()
	if _, f := c.follower(i); !errors.Is(f.StepDown(testCtx(t)), ErrNotLeader) {
		t.Fatalf("follower StepDown: want ErrNotLeader")
	}
	if err := leader.StepDown(testCtx(t)); err != nil {
		t.Fatalf("StepDown: %v", err)
	}
	if st := leader.Status(); st.State == raft.Leader.String() {
		t.Fatalf("old leader still %s after StepDown", st.State)
	}
	c.waitLeader() // another voter takes over
}
