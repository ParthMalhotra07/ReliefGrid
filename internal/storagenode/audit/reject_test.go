package audit_test

import (
	"encoding/json"
	"testing"

	"github.com/ParthMalhotra07/ReliefGrid/internal/fsm/merkle"
	"github.com/ParthMalhotra07/ReliefGrid/internal/storagenode/audit"
	"github.com/ParthMalhotra07/ReliefGrid/internal/storagenode/audit/audittest"
)

// wantIncomplete fails t unless r is incomplete (exit 2) with a reason.
func wantIncomplete(t *testing.T, r audit.Report) {
	t.Helper()
	if r.Verdict != audit.Incomplete || r.Verdict.ExitCode() != 2 || r.Error == "" {
		t.Fatalf("report %s (%q), want incomplete with a reason", r.Verdict, r.Error)
	}
}

// editDigest returns an Edit that replaces every digest answer's digest by
// f of it.
func editDigest(f func(d merkle.Digest) any) func(string, map[string]any) {
	return func(kind string, resp map[string]any) {
		if kind == "digest" {
			resp["digest"] = f(resp["digest"].(merkle.Digest))
		}
	}
}

// asJSON is v as generic JSON, so a test can cut it shorter than its Go
// type allows.
func asJSON(t *testing.T, v any) map[string]any {
	t.Helper()
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	var m map[string]any
	if err := json.Unmarshal(b, &m); err != nil {
		t.Fatal(err)
	}
	return m
}

// A digest answer that is missing, empty, truncated or not self-consistent
// is no observation. Every pod sends the same malformed answer, so a
// comparison that skipped validation would find no difference and say
// equal; it must say incomplete.
func TestAuditRejectsMalformedDigest(t *testing.T) {
	for name, edit := range map[string]func(t *testing.T) func(string, map[string]any){
		"missing digest": func(*testing.T) func(string, map[string]any) {
			return func(kind string, resp map[string]any) {
				if kind == "digest" {
					delete(resp, "digest")
				}
			}
		},
		"empty digest": func(*testing.T) func(string, map[string]any) {
			return editDigest(func(merkle.Digest) any { return merkle.Digest{} })
		},
		"no tables, root of nothing": func(*testing.T) func(string, map[string]any) {
			return editDigest(func(merkle.Digest) any { return merkle.NewDigest() })
		},
		"a table left out": func(*testing.T) func(string, map[string]any) {
			return editDigest(func(d merkle.Digest) any { return merkle.NewDigest(d.Tables[:3]...) })
		},
		"tables out of order": func(*testing.T) func(string, map[string]any) {
			return editDigest(func(d merkle.Digest) any {
				return merkle.NewDigest(d.Tables[1], d.Tables[0], d.Tables[2], d.Tables[3])
			})
		},
		"a table renamed": func(*testing.T) func(string, map[string]any) {
			return editDigest(func(d merkle.Digest) any {
				d.Tables[3].Name = "outbox_old"
				return merkle.NewDigest(d.Tables...)
			})
		},
		"a table twice": func(*testing.T) func(string, map[string]any) {
			return editDigest(func(d merkle.Digest) any {
				return merkle.NewDigest(d.Tables[0], d.Tables[1], d.Tables[2], d.Tables[3], d.Tables[3])
			})
		},
		"bucket list truncated": func(t *testing.T) func(string, map[string]any) {
			return editDigest(func(d merkle.Digest) any {
				m := asJSON(t, d)
				tb := m["tables"].([]any)[1].(map[string]any)
				tb["buckets"] = tb["buckets"].([]any)[:200]
				return m
			})
		},
		"table root not over its buckets": func(*testing.T) func(string, map[string]any) {
			return editDigest(func(d merkle.Digest) any {
				d.Tables[0].Buckets[5] = d.Tables[0].Root // a hash no bucket has
				return d
			})
		},
		"shard root not over table roots": func(*testing.T) func(string, map[string]any) {
			return editDigest(func(d merkle.Digest) any {
				d.Root = d.Tables[0].Root
				return d
			})
		},
		"rows in a table with no buckets": func(*testing.T) func(string, map[string]any) {
			return editDigest(func(d merkle.Digest) any {
				d.Tables[1].Rows = 5
				return d
			})
		},
		"no schema version": func(*testing.T) func(string, map[string]any) {
			return func(kind string, resp map[string]any) {
				if kind == "digest" {
					resp["summary"].(map[string]any)["schemaVersion"] = ""
				}
			}
		},
		"digest ring version not the status's": func(*testing.T) func(string, map[string]any) {
			return func(kind string, resp map[string]any) {
				if kind == "digest" {
					resp["ringVersion"] = 2
				}
			}
		},
		"digest without a pod name": func(*testing.T) func(string, map[string]any) {
			return func(kind string, resp map[string]any) {
				if kind == "digest" {
					delete(resp, "podName")
				}
			}
		},
	} {
		t.Run(name, func(t *testing.T) {
			c := audittest.NewCluster(t, 50, "pod-0", "pod-1", "pod-2")
			for _, p := range c.Pods {
				p.Edit = edit(t)
			}
			wantIncomplete(t, run(t, c))
		})
	}
}

// Each digest must come from the replica its status named: a digest naming
// another pod is not that replica's observation.
func TestAuditRejectsDigestFromAnotherPod(t *testing.T) {
	c := audittest.NewCluster(t, 50, "pod-0", "pod-1", "pod-2")
	c.Pods[1].Edit = func(kind string, resp map[string]any) {
		if kind == "digest" {
			resp["podName"] = "pod-9"
		}
	}
	wantIncomplete(t, run(t, c))
}

// A status without a pod name gives the replica no identity to count it by.
func TestAuditRejectsStatusWithoutPodName(t *testing.T) {
	c := audittest.NewCluster(t, 50, "pod-0", "pod-1", "pod-2")
	c.Pods[1].Edit = func(kind string, resp map[string]any) {
		if kind == "status" {
			resp["podName"] = ""
		}
	}
	r := run(t, c)
	wantIncomplete(t, r)
	if q := c.Queries(); len(q) != 0 {
		t.Fatalf("asked for digests %v", q)
	}
}

// The #99 review case: pod-2 diverges, and the audit is pointed at pod-0
// twice instead of pod-2. Counting pod-0 twice would say equal.
func TestAuditRejectsDuplicateEndpoint(t *testing.T) {
	c := audittest.NewCluster(t, 50, "pod-0", "pod-1", "pod-2")
	c.Pods[2].Rows[7][2] = `{"n":"corrupt"}`
	statuses := 0
	c.OnStatus = func(*audittest.Cluster, *audittest.Pod) { statuses++ }
	addrs := c.Addrs()
	r := audit.Run(t.Context(), audit.Config{Addrs: []string{addrs[0], addrs[1], addrs[0]}, Token: audittest.Token})
	wantIncomplete(t, r)
	c.Lock()
	defer c.Unlock()
	if statuses != 0 {
		t.Fatalf("a repeated endpoint list was not refused up front: %d status requests", statuses)
	}
}

// An address that answers as one pod and later as another is not one
// replica throughout the audit.
func TestAuditRejectsPodNameChange(t *testing.T) {
	c := audittest.NewCluster(t, 50, "pod-0", "pod-1", "pod-2")
	seen := 0
	c.Pods[1].Edit = func(kind string, resp map[string]any) {
		if kind == "status" {
			if seen++; seen > 1 {
				resp["podName"] = "pod-9"
			}
		}
	}
	wantIncomplete(t, run(t, c))
}

// One replica reached through two addresses (two Services, a pod IP and a
// DNS name) reports one pod name twice: it is still one replica.
func TestAuditRejectsDuplicatePodName(t *testing.T) {
	c := audittest.NewCluster(t, 50, "pod-0", "pod-1", "pod-0")
	r := run(t, c)
	wantIncomplete(t, r)
	if q := c.Queries(); len(q) != 0 {
		t.Fatalf("asked for digests %v", q)
	}
}

// Auditing exactly two distinct replicas stays supported.
func TestAuditTwoReplicas(t *testing.T) {
	c := audittest.NewCluster(t, 50, "pod-0", "pod-1")
	if r := run(t, c); r.Verdict != audit.Equal {
		t.Fatalf("report %s (%q), want equal", r.Verdict, r.Error)
	}
}

// A bucket answer is checked to be the bucket asked for, from the pod
// asked, at the index compared; otherwise its rows are not that replica's.
func TestAuditRejectsMismatchedBucketAnswer(t *testing.T) {
	for name, edit := range map[string]func(resp map[string]any){
		"another pod":    func(resp map[string]any) { resp["podName"] = "pod-9" },
		"another bucket": func(resp map[string]any) { resp["bucket"] = resp["bucket"].(int) + 1 },
		"another table":  func(resp map[string]any) { resp["table"] = "outbox" },
		"another index":  func(resp map[string]any) { resp["appliedIndex"] = resp["appliedIndex"].(uint64) + 1 },
	} {
		t.Run(name, func(t *testing.T) {
			c := audittest.NewCluster(t, 50, "pod-0", "pod-1", "pod-2")
			c.Pods[2].Rows[7][2] = `{"n":"corrupt"}`
			c.Pods[0].Edit = func(kind string, resp map[string]any) {
				if kind == "bucket" {
					edit(resp)
				}
			}
			r := run(t, c)
			wantIncomplete(t, r)
			if r.Rows != nil {
				t.Fatalf("rows reported from a mismatched answer: %+v", r.Rows)
			}
		})
	}
}
