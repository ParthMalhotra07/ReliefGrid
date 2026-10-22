package main

import (
	"encoding/json"
	"errors"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/hashicorp/raft"
)

type node struct {
	cfg  config
	raft *raft.Raft
	fsm  *fsm
}

func (n *node) routes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })
	mux.HandleFunc("GET /status", n.handleStatus)
	mux.HandleFunc("GET /value", n.handleValue)
	mux.HandleFunc("POST /cas", n.handleCAS)
	mux.HandleFunc("POST /bootstrap", n.handleBootstrap)
	return mux
}

type serverJSON struct {
	ID       string `json:"id"`
	Address  string `json:"address"`
	Suffrage string `json:"suffrage"`
}

type statusJSON struct {
	ID           string       `json:"id"`
	Advertise    string       `json:"advertise"`
	State        string       `json:"state"`
	Leader       string       `json:"leader"`
	LeaderID     string       `json:"leaderId"`
	Term         uint64       `json:"term"`
	AppliedIndex uint64       `json:"appliedIndex"`
	CommitIndex  uint64       `json:"commitIndex"`
	LastIndex    uint64       `json:"lastIndex"`
	Servers      []serverJSON `json:"servers"`
	Memory       memoryJSON   `json:"memory"`
}

type memoryJSON struct {
	RSSBytes    uint64 `json:"rssBytes"`
	CgroupBytes uint64 `json:"cgroupBytes"`
}

func (n *node) handleStatus(w http.ResponseWriter, _ *http.Request) {
	leader, leaderID := n.raft.LeaderWithID()
	s := statusJSON{
		ID:           n.cfg.pod,
		Advertise:    n.cfg.raftAddr(n.cfg.pod),
		State:        n.raft.State().String(),
		Leader:       string(leader),
		LeaderID:     string(leaderID),
		Term:         n.raft.CurrentTerm(),
		AppliedIndex: n.raft.AppliedIndex(),
		CommitIndex:  n.raft.CommitIndex(),
		LastIndex:    n.raft.LastIndex(),
		Servers:      []serverJSON{},
		Memory:       readMemory(),
	}
	if fut := n.raft.GetConfiguration(); fut.Error() == nil {
		for _, srv := range fut.Configuration().Servers {
			s.Servers = append(s.Servers, serverJSON{ID: string(srv.ID), Address: string(srv.Address), Suffrage: srv.Suffrage.String()})
		}
	}
	writeJSON(w, http.StatusOK, s)
}

type valueJSON struct {
	Key     string `json:"key"`
	Value   string `json:"value"`
	Version int64  `json:"version"`
}

func (n *node) handleValue(w http.ResponseWriter, req *http.Request) {
	key := req.URL.Query().Get("key")
	value, version, err := n.fsm.Get(key)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, valueJSON{Key: key, Value: value, Version: version})
}

// handleCAS proposes the command through Raft; the version check happens in Apply.
//
//	200 applied, 409 version conflict, 421 not the leader (never appended),
//	503 outcome unknown (e.g. leadership lost after appending; retry or re-read).
func (n *node) handleCAS(w http.ResponseWriter, req *http.Request) {
	var cmd casCommand
	if err := json.NewDecoder(req.Body).Decode(&cmd); err != nil {
		writeError(w, http.StatusBadRequest, "decode body: "+err.Error())
		return
	}
	if n.raft.State() != raft.Leader {
		n.notLeader(w)
		return
	}
	data, err := json.Marshal(cmd)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	fut := n.raft.Apply(data, 5*time.Second)
	if err := fut.Error(); err != nil {
		if errors.Is(err, raft.ErrNotLeader) {
			n.notLeader(w)
			return
		}
		writeError(w, http.StatusServiceUnavailable, "outcome unknown: "+err.Error())
		return
	}
	res, ok := fut.Response().(casResult)
	switch {
	case !ok:
		writeError(w, http.StatusInternalServerError, "unexpected apply response")
	case res.Err != "":
		writeError(w, http.StatusInternalServerError, res.Err)
	case res.Conflict:
		writeJSON(w, http.StatusConflict, valueJSON{Key: cmd.Key, Value: res.Value, Version: res.Version})
	default:
		writeJSON(w, http.StatusOK, valueJSON{Key: cmd.Key, Value: res.Value, Version: res.Version})
	}
}

func (n *node) notLeader(w http.ResponseWriter) {
	leader, _ := n.raft.LeaderWithID()
	writeJSON(w, http.StatusMisdirectedRequest, map[string]string{"error": "not leader", "leader": string(leader)})
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, code int, msg string) {
	writeJSON(w, code, map[string]string{"error": msg})
}

// readMemory reports the process RSS and the container's cgroup v2 usage; zero when unavailable.
func readMemory() memoryJSON {
	var m memoryJSON
	if b, err := os.ReadFile("/proc/self/status"); err == nil {
		for _, line := range strings.Split(string(b), "\n") {
			if f := strings.Fields(line); len(f) >= 2 && f[0] == "VmRSS:" {
				kb, _ := strconv.ParseUint(f[1], 10, 64)
				m.RSSBytes = kb * 1024
			}
		}
	}
	if b, err := os.ReadFile("/sys/fs/cgroup/memory.current"); err == nil {
		m.CgroupBytes, _ = strconv.ParseUint(strings.TrimSpace(string(b)), 10, 64)
	}
	return m
}
