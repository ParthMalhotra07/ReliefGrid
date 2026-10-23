package ring

import (
	"slices"
	"strconv"
	"testing"
)

// TestChecksumCoversFullTokenMap changes one thing at a time — a header field,
// or one token's hash, shard or vnode at the start, middle and end of the
// map, or a dropped or swapped token — and requires the checksum to change.
func TestChecksumCoversFullTokenMap(t *testing.T) {
	cfg := Config{RingVersion: 1, Hash: "xxhash64", Shards: 3, VNodesPerShard: 128}
	r, err := New(Config{RingVersion: 1, Hash: "xxhash64", Shards: 3, VNodesPerShard: 128,
		Checksum: "sha256:670e1eca690b168c83fd910b2bdf1e2fbf81f4cd8de91994d8d5ac2989de6360"})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	base := r.Tokens()
	want := checksum(cfg, base)
	if want != r.Checksum() {
		t.Fatalf("checksum(cfg, Tokens()) = %q, Ring.Checksum() = %q", want, r.Checksum())
	}

	seen := map[string]string{want: "base"}
	check := func(name string, c Config, tokens []Token) {
		t.Helper()
		got := checksum(c, tokens)
		if prev, dup := seen[got]; dup {
			t.Errorf("%s: checksum %q same as %s", name, got, prev)
			return
		}
		seen[got] = name
	}

	header := []struct {
		name string
		edit func(*Config)
	}{
		{"ringVersion", func(c *Config) { c.RingVersion++ }},
		{"hash", func(c *Config) { c.Hash = "xxhash64-other" }},
		{"shards", func(c *Config) { c.Shards++ }},
		{"vnodes", func(c *Config) { c.VNodesPerShard++ }},
	}
	for _, h := range header {
		c := cfg
		h.edit(&c)
		check("header "+h.name, c, base)
	}

	for _, i := range []int{0, len(base) / 2, len(base) - 1} {
		edits := []struct {
			name string
			edit func(*Token)
		}{
			{"hash+1", func(tk *Token) { tk.Hash++ }},
			{"shard", func(tk *Token) { tk.Shard = (tk.Shard + 1) % 3 }},
			{"vnode", func(tk *Token) { tk.VNode += 1000 }},
		}
		for _, e := range edits {
			tokens := slices.Clone(base)
			e.edit(&tokens[i])
			check("token "+strconv.Itoa(i)+" "+e.name, cfg, tokens)
		}
		check("drop token "+strconv.Itoa(i), cfg, slices.Delete(slices.Clone(base), i, i+1))
	}

	// Same multiset of shards, different placement: swap the shards of the
	// first two tokens that belong to different shards.
	tokens := slices.Clone(base)
	for j := 1; j < len(tokens); j++ {
		if tokens[j].Shard != tokens[0].Shard {
			tokens[0].Shard, tokens[j].Shard = tokens[j].Shard, tokens[0].Shard
			check("swap shards of tokens 0 and "+strconv.Itoa(j), cfg, tokens)
			break
		}
	}

	// The published checksum field itself is not an input.
	c := cfg
	c.Checksum = "sha256:anything"
	if got := checksum(c, base); got != want {
		t.Errorf("checksum depends on cfg.Checksum: %q vs %q", got, want)
	}
}
