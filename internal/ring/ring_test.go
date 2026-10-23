package ring_test

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"testing"

	"github.com/cespare/xxhash/v2"

	"github.com/ParthMalhotra07/ReliefGrid/internal/ring"
	"github.com/ParthMalhotra07/ReliefGrid/internal/store"
)

const (
	ringFile       = "../../contracts/ring.yaml" // the canonical ring; no copy
	goldenChecksum = "sha256:670e1eca690b168c83fd910b2bdf1e2fbf81f4cd8de91994d8d5ac2989de6360"
	sampleSize     = 10_000
)

func load(t *testing.T) *ring.Ring {
	t.Helper()
	r, err := ring.Load(ringFile)
	if err != nil {
		t.Fatalf("Load(%s): %v", ringFile, err)
	}
	return r
}

// sampleKeys is a fixed 10k sample: half requests, half resources, with
// UUID-shaped IDs derived from SHA-256 so they look like real IDs.
func sampleKeys() []string {
	keys := make([]string, 0, sampleSize)
	for i := range sampleSize {
		sum := sha256.Sum256(fmt.Appendf(nil, "sample-%d", i))
		h := hex.EncodeToString(sum[:16])
		id := h[0:8] + "-" + h[8:12] + "-" + h[12:16] + "-" + h[16:20] + "-" + h[20:32]
		if i%2 == 0 {
			keys = append(keys, store.RequestKey(id))
		} else {
			keys = append(keys, store.ResourceKey(id))
		}
	}
	return keys
}

// referenceOwner is an independent linear scan over the token map: the first
// token clockwise from xxhash64(key), wrapping past the largest token.
func referenceOwner(tokens []ring.Token, key string) (shard int, wrapped bool) {
	h := xxhash.Sum64String(key)
	for _, tk := range tokens {
		if tk.Hash >= h {
			return tk.Shard, false
		}
	}
	return tokens[0].Shard, true
}

func owner(t *testing.T, r *ring.Ring, key string) int {
	t.Helper()
	s, err := r.Owner(key)
	if err != nil {
		t.Fatalf("Owner(%q): %v", key, err)
	}
	return s
}

func TestLoadPublishedRing(t *testing.T) {
	r := load(t)
	if r.Version() != 1 {
		t.Errorf("Version = %d, want 1", r.Version())
	}
	if r.Shards() != 3 {
		t.Errorf("Shards = %d, want 3", r.Shards())
	}
	if n := len(r.Tokens()); n != 3*128 {
		t.Errorf("len(Tokens) = %d, want %d", n, 3*128)
	}
	if r.Checksum() != goldenChecksum {
		t.Errorf("Checksum = %q, want %q", r.Checksum(), goldenChecksum)
	}
}

func TestTokensFollowSpec(t *testing.T) {
	tokens := load(t).Tokens()
	seen := make(map[[2]int]bool, len(tokens))
	for i, tk := range tokens {
		if want := xxhash.Sum64String(fmt.Sprintf("shard-%d-vnode-%d", tk.Shard, tk.VNode)); tk.Hash != want {
			t.Errorf("token %d (shard %d vnode %d): hash %016x, want %016x", i, tk.Shard, tk.VNode, tk.Hash, want)
		}
		if tk.Shard < 0 || tk.Shard >= 3 || tk.VNode < 0 || tk.VNode >= 128 {
			t.Errorf("token %d out of range: %+v", i, tk)
		}
		id := [2]int{tk.Shard, tk.VNode}
		if seen[id] {
			t.Errorf("duplicate token %+v", tk)
		}
		seen[id] = true
		if i > 0 {
			p := tokens[i-1]
			if p.Hash > tk.Hash || (p.Hash == tk.Hash && (p.Shard > tk.Shard || (p.Shard == tk.Shard && p.VNode >= tk.VNode))) {
				t.Errorf("tokens %d,%d not in (Hash, Shard, VNode) order: %+v, %+v", i-1, i, p, tk)
			}
		}
	}
}

func TestOwnerMatchesReferenceScan(t *testing.T) {
	r := load(t)
	tokens := r.Tokens()
	wraps := 0
	for _, k := range sampleKeys() {
		want, wrapped := referenceOwner(tokens, k)
		if wrapped {
			wraps++
		}
		if got := owner(t, r, k); got != want {
			t.Fatalf("Owner(%q) = %d, want %d (wrapped=%v)", k, got, want, wrapped)
		}
	}
	// Keys hashing past the largest token must wrap to the first; make sure
	// the sample actually exercised that path.
	if wraps == 0 {
		t.Fatal("sample never wrapped past the largest token")
	}
	t.Logf("%d of %d keys wrapped", wraps, sampleSize)
}

// TestGoldenOwners pins owners computed from the spec by a separate program,
// so a change of hash library, token naming or tie-break shows up here.
func TestGoldenOwners(t *testing.T) {
	r := load(t)
	for key, want := range map[string]int{
		"req:r-0001": 2,
		"res:r-0001": 2,
		"req:00000000-0000-0000-0000-000000000000": 1,
		"res:water-42": 0,
		"req:a":        1,
		"res:a":        2,
		"kv:spike":     2,
	} {
		if got := owner(t, r, key); got != want {
			t.Errorf("Owner(%q) = %d, want %d", key, got, want)
		}
	}
}

// dumpOwners is the canonical text a process reports for the sample: the
// ring identity followed by one owner per key.
func dumpOwners(t *testing.T, r *ring.Ring) string {
	t.Helper()
	var b strings.Builder
	fmt.Fprintf(&b, "version=%d checksum=%s\n", r.Version(), r.Checksum())
	for _, k := range sampleKeys() {
		fmt.Fprintf(&b, "%s %d\n", k, owner(t, r, k))
	}
	return b.String()
}

func TestSameOwnerAcrossReloads(t *testing.T) {
	first := load(t)
	second := load(t)
	if a, b := dumpOwners(t, first), dumpOwners(t, second); a != b {
		t.Fatal("two loads of the same file route the sample differently")
	}

	// Same values, different layout: reordered fields, comments, quoting.
	third, err := ring.Load("testdata/ring-reformatted.yaml")
	if err != nil {
		t.Fatalf("Load(reformatted): %v", err)
	}
	if a, b := dumpOwners(t, first), dumpOwners(t, third); a != b {
		t.Fatal("reformatted file routes the sample differently")
	}

	// New from the struct is the same ring as the file.
	fourth, err := ring.New(ring.Config{RingVersion: 1, Hash: "xxhash64", Shards: 3, VNodesPerShard: 128, Checksum: goldenChecksum})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if a, b := dumpOwners(t, first), dumpOwners(t, fourth); a != b {
		t.Fatal("New(Config) routes the sample differently from Load")
	}
}

func TestTokensReturnsACopy(t *testing.T) {
	r := load(t)
	before := dumpOwners(t, r)
	tokens := r.Tokens()
	for i := range tokens {
		tokens[i].Shard = 0
		tokens[i].Hash = 0
	}
	if r.Checksum() != goldenChecksum {
		t.Errorf("Checksum changed after mutating Tokens(): %q", r.Checksum())
	}
	if dumpOwners(t, r) != before {
		t.Error("mutating Tokens() changed routing")
	}
}

const helperEnv = "RING_HELPER_DUMP_OWNERS"

// TestHelperDumpOwners is not a test on its own: TestSameOwnerAcrossRestarts
// re-runs the test binary with helperEnv set, and this prints the dump from
// a fresh process (new map seeds, new heap, nothing shared).
func TestHelperDumpOwners(t *testing.T) {
	if os.Getenv(helperEnv) != "1" {
		t.Skip("helper process only")
	}
	fmt.Print("BEGIN-DUMP\n" + dumpOwners(t, load(t)) + "END-DUMP\n")
}

func TestSameOwnerAcrossRestarts(t *testing.T) {
	want := dumpOwners(t, load(t))
	for run := range 2 {
		cmd := exec.CommandContext(t.Context(), os.Args[0], "-test.run=^TestHelperDumpOwners$", "-test.count=1", "-test.v")
		cmd.Env = append(os.Environ(), helperEnv+"=1")
		out, err := cmd.Output()
		if err != nil {
			t.Fatalf("restart %d: %v\n%s", run, err, out)
		}
		got, ok := between(out, "BEGIN-DUMP\n", "END-DUMP\n")
		if !ok {
			t.Fatalf("restart %d: no dump in output:\n%s", run, out)
		}
		if got != want {
			t.Fatalf("restart %d routes the sample differently from this process", run)
		}
	}
}

func between(out []byte, begin, end string) (string, bool) {
	_, rest, ok := bytes.Cut(out, []byte(begin))
	if !ok {
		return "", false
	}
	body, _, ok := bytes.Cut(rest, []byte(end))
	return string(body), ok
}

func TestEveryShardOwnsKeysOn10kSample(t *testing.T) {
	r := load(t)
	counts := make([]int, r.Shards())
	for _, k := range sampleKeys() {
		counts[owner(t, r, k)]++
	}
	t.Logf("10k sample: %v", counts)
	for s, n := range counts {
		if n == 0 {
			t.Errorf("shard %d owns no keys", s)
		}
		// Consistent hashing is only roughly even; this floor catches a ring
		// that ignores vnodes or a shard, not ordinary imbalance.
		if n < sampleSize/5 {
			t.Errorf("shard %d owns %d of %d keys, under 20%%", s, n, sampleSize)
		}
	}
}

func TestKeyPrefixes(t *testing.T) {
	r := load(t)
	tokens := r.Tokens()

	// The prefix is part of the hashed key: req:{id} and res:{id} are routed
	// independently, exactly as the reference scan over the full key says.
	for _, id := range []string{"a", "r-0001", "water-42"} {
		for _, k := range []string{store.RequestKey(id), store.ResourceKey(id)} {
			want, _ := referenceOwner(tokens, k)
			if got := owner(t, r, k); got != want {
				t.Errorf("Owner(%q) = %d, want %d", k, got, want)
			}
		}
	}
	if owner(t, r, "req:a") == owner(t, r, "res:a") {
		t.Error(`req:a and res:a share an owner; the golden vectors say they differ, so the prefix was not hashed`)
	}

	// No normalisation: a different spelling is a different key.
	for _, k := range []string{"REQ:a", "req:a ", "req: a"} {
		want, _ := referenceOwner(tokens, k)
		if got := owner(t, r, k); got != want {
			t.Errorf("Owner(%q) = %d, want %d (hashed as given)", k, got, want)
		}
	}

	// Generic KV keys (store.KindKV) still route.
	if _, err := r.Owner("kv:spike"); err != nil {
		t.Errorf("Owner(kv:spike): %v", err)
	}

	for _, k := range []string{"", store.RequestKey(""), store.ResourceKey("")} {
		if _, err := r.Owner(k); !errors.Is(err, ring.ErrInvalidKey) {
			t.Errorf("Owner(%q) err = %v, want ErrInvalidKey", k, err)
		}
	}
}

func TestParseRejectsBadConfig(t *testing.T) {
	good := map[string]string{
		"ringVersion":    "1",
		"hash":           "xxhash64",
		"shards":         "3",
		"vnodesPerShard": "128",
		"checksum":       goldenChecksum,
	}
	render := func(change map[string]string, extra string) []byte {
		var b strings.Builder
		for _, k := range []string{"ringVersion", "hash", "shards", "vnodesPerShard", "checksum"} {
			v := good[k]
			if c, ok := change[k]; ok {
				if c == "-" {
					continue
				}
				v = c
			}
			fmt.Fprintf(&b, "%s: %s\n", k, v)
		}
		b.WriteString(extra)
		return []byte(b.String())
	}

	if _, err := ring.Parse(render(nil, "")); err != nil {
		t.Fatalf("baseline config rejected: %v", err)
	}

	for _, tc := range []struct {
		name     string
		change   map[string]string
		extra    string
		mismatch bool // must be ErrChecksumMismatch specifically
	}{
		{name: "wrong checksum", change: map[string]string{"checksum": "sha256:" + strings.Repeat("0", 64)}, mismatch: true},
		{name: "ringVersion changed, checksum kept", change: map[string]string{"ringVersion": "2"}, mismatch: true},
		{name: "vnodes changed, checksum kept", change: map[string]string{"vnodesPerShard": "64"}, mismatch: true},
		{name: "shards changed, checksum kept", change: map[string]string{"shards": "4"}, mismatch: true},
		{name: "missing checksum", change: map[string]string{"checksum": "-"}},
		{name: "checksum without sha256 prefix", change: map[string]string{"checksum": strings.TrimPrefix(goldenChecksum, "sha256:")}},
		{name: "unknown field", extra: "replicas: 3\n"},
		{name: "other hash", change: map[string]string{"hash": "murmur3"}},
		{name: "zero ringVersion", change: map[string]string{"ringVersion": "0"}},
		{name: "zero shards", change: map[string]string{"shards": "0"}},
		{name: "negative shards", change: map[string]string{"shards": "-1"}},
		{name: "zero vnodes", change: map[string]string{"vnodesPerShard": "0"}},
		{name: "not yaml", extra: "::: [\n"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r, err := ring.Parse(render(tc.change, tc.extra))
			if err == nil {
				t.Fatalf("accepted; checksum %q", r.Checksum())
			}
			if tc.mismatch && !errors.Is(err, ring.ErrChecksumMismatch) {
				t.Fatalf("err = %v, want ErrChecksumMismatch", err)
			}
		})
	}
}

func TestLoadMissingFile(t *testing.T) {
	if _, err := ring.Load("testdata/no-such-ring.yaml"); err == nil {
		t.Fatal("Load of a missing file succeeded")
	}
}
