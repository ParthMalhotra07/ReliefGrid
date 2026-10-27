package admission

import (
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ParthMalhotra07/ReliefGrid/internal/raftstore"
)

var self = raftstore.Identity{PodName: "storage-shard-1-2", Incarnation: "11111111-1111-4111-8111-111111111111", GroupID: "shard-1", Shard: 1}

const (
	otherInc = "22222222-2222-4222-8222-222222222222"
	peer0    = "storage-shard-1-0/aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"
	peer1    = "storage-shard-1-1/bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb"
)

func data(phase, peerIDs, admitted, removed string) map[string]string {
	d := map[string]string{"groupId": "shard-1", "shard": "1", "phase": phase}
	if peerIDs != "" {
		d["peerIds"] = peerIDs
	}
	if admitted != "" {
		d["admitted"] = admitted
	}
	if removed != "" {
		d["removed"] = removed
	}
	return d
}

// M7 (plan Part 1, v3.1 §10): an empty disk opens its Raft transport only
// for a fresh install, as a recorded voter, or once storagectl replace has
// written admitted[<podName>] = <incarnation>. A disk with Raft state is an
// ordinary restart unless replace removed its ServerID.
func TestDecide(t *testing.T) {
	sid := string(self.ServerID())
	for _, tc := range []struct {
		name     string
		data     map[string]string
		hasState bool
		want     bool
		why      string // a fragment of the reason
	}{
		{"fresh install", data("PENDING", "", "", ""), false, true, "fresh install"},
		{"recorded voter during bootstrap", data("BOOTSTRAPPING", `["`+peer0+`","`+peer1+`","`+sid+`"]`, "", ""), false, true, "voter"},
		{"unrecorded disk during bootstrap", data("BOOTSTRAPPING", `["`+peer0+`","`+peer1+`","storage-shard-1-2/`+otherInc+`"]`, "", ""), false, false, "not admitted"},
		{"recorded voter that has not received the log yet", data("BOOTSTRAPPED", `["`+peer0+`","`+peer1+`","`+sid+`"]`, "", ""), false, true, "voter"},
		{"wiped disk of a bootstrapped group", data("BOOTSTRAPPED", `["`+peer0+`","`+peer1+`","storage-shard-1-2/`+otherInc+`"]`, "", ""), false, false, "not admitted"},
		{"admitted by replace", data("BOOTSTRAPPED", `["`+peer0+`","`+peer1+`","storage-shard-1-2/`+otherInc+`"]`, `{"storage-shard-1-2":"`+self.Incarnation+`"}`, ""), false, true, "admitted"},
		{"another incarnation admitted", data("BOOTSTRAPPED", `["`+peer0+`","`+peer1+`"]`, `{"storage-shard-1-2":"`+otherInc+`"}`, ""), false, false, "not admitted"},
		{"another pod admitted", data("BOOTSTRAPPED", `["`+peer0+`","`+peer1+`"]`, `{"storage-shard-1-1":"`+self.Incarnation+`"}`, ""), false, false, "not admitted"},
		{"ordinary restart", data("BOOTSTRAPPED", `["`+peer0+`","`+peer1+`","`+sid+`"]`, "", ""), true, true, "restart"},
		{"restart of a removed member", data("BOOTSTRAPPED", `["`+peer0+`","`+peer1+`","storage-shard-1-2/`+otherInc+`"]`, "", `["`+sid+`"]`), true, false, "removed"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			m, err := Parse(tc.data)
			if err != nil {
				t.Fatal(err)
			}
			got, why := Decide(m, self, tc.hasState)
			if got != tc.want || !strings.Contains(why, tc.why) {
				t.Fatalf("Decide = %v (%q), want %v (%q)", got, why, tc.want, tc.why)
			}
		})
	}
}

// A malformed manifest is an error, never an admission.
func TestParseRejectsMalformed(t *testing.T) {
	for _, d := range []map[string]string{
		data("PENDING", "not json", "", ""),
		data("BOOTSTRAPPED", "", `["not", "a map"]`, ""),
		data("BOOTSTRAPPED", "", "", "{}"),
		data("SOMETHING", "", "", ""),
	} {
		if _, err := Parse(d); err == nil {
			t.Fatalf("Parse(%v) accepted it", d)
		}
	}
}

type source struct {
	reads atomic.Int32
	get   func(n int32) (map[string]string, error)
}

func (s *source) Get(context.Context) (map[string]string, error) { return s.get(s.reads.Add(1)) }

// Wait polls the manifest until the disk is admitted. An empty disk fails
// closed: an unreadable manifest is not an admission. A disk with Raft state
// does not depend on the API server for an ordinary restart.
func TestWait(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	notYet := data("BOOTSTRAPPED", `["`+peer0+`","`+peer1+`"]`, "", "")
	admitted := data("BOOTSTRAPPED", `["`+peer0+`","`+peer1+`"]`, `{"storage-shard-1-2":"`+self.Incarnation+`"}`, "")

	s := &source{get: func(n int32) (map[string]string, error) {
		switch {
		case n == 1:
			return nil, errors.New("api server unreachable")
		case n < 4:
			return notYet, nil
		}
		return admitted, nil
	}}
	var whys []string
	if err := Wait(ctx, s, self, false, time.Millisecond, func(why string) { whys = append(whys, why) }); err != nil {
		t.Fatalf("Wait: %v", err)
	}
	if s.reads.Load() != 4 || len(whys) != 3 {
		t.Fatalf("Wait read %d times and reported %q, want 4 reads and 3 reasons", s.reads.Load(), whys)
	}

	never := &source{get: func(int32) (map[string]string, error) { return nil, errors.New("api server unreachable") }}
	short, stop := context.WithTimeout(ctx, 50*time.Millisecond)
	defer stop()
	if err := Wait(short, never, self, false, time.Millisecond, nil); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("empty disk, manifest unreadable: %v, want the deadline", err)
	}
	if err := Wait(ctx, never, self, true, time.Millisecond, nil); err != nil {
		t.Fatalf("restart with Raft state, manifest unreadable: %v, want admitted", err)
	}

	removed := &source{get: func(int32) (map[string]string, error) {
		return data("BOOTSTRAPPED", `["`+peer0+`","`+peer1+`"]`, "", `["`+string(self.ServerID())+`"]`), nil
	}}
	short2, stop2 := context.WithTimeout(ctx, 50*time.Millisecond)
	defer stop2()
	if err := Wait(short2, removed, self, true, time.Millisecond, nil); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("restart of a removed member: %v, want it held until the deadline", err)
	}
}
