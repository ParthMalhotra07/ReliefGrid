package ring

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"slices"
	"sort"
	"strings"

	"github.com/cespare/xxhash/v2"
	"go.yaml.in/yaml/v3"

	"github.com/ParthMalhotra07/ReliefGrid/internal/store"
)

// HashXXHash64 is the only supported key hash.
const HashXXHash64 = "xxhash64"

// maxTokens bounds shards × vnodes so a bad config cannot allocate without limit.
const maxTokens = 1 << 20

// Config is the published ring (ring.yaml). Every field feeds the checksum,
// so a coordinator restart either loads exactly this ring or refuses to start.
type Config struct {
	RingVersion    uint64 `yaml:"ringVersion"`
	Hash           string `yaml:"hash"` // must be "xxhash64"
	Shards         int    `yaml:"shards"`
	VNodesPerShard int    `yaml:"vnodesPerShard"`
	Checksum       string `yaml:"checksum"` // "sha256:<hex>" of the full token map
}

// Token is one virtual node: Hash = xxhash64("shard-{Shard}-vnode-{VNode}").
// The ring is the tokens sorted by (Hash, Shard, VNode); a key belongs to the
// first token whose Hash >= xxhash64(key), wrapping to the first token.
type Token struct {
	Hash  uint64
	Shard int
	VNode int
}

// Ring is an immutable, loaded ring. It is safe for concurrent use.
type Ring struct {
	cfg    Config  // Checksum is the verified value
	tokens []Token // ring order
	hashes []uint64
}

var (
	// ErrChecksumMismatch means the published checksum does not match the
	// token map computed from the config.
	ErrChecksumMismatch = errors.New("ring: checksum mismatch")
	// ErrInvalidKey means the key cannot be routed: empty, or a req:/res:
	// prefix with an empty ID.
	ErrInvalidKey = errors.New("ring: invalid key")
)

const checksumPrefix = "sha256:"

// New builds the ring from cfg and verifies cfg.Checksum.
func New(cfg Config) (*Ring, error) {
	if err := validate(cfg); err != nil {
		return nil, err
	}
	tokens := buildTokens(cfg.Shards, cfg.VNodesPerShard)
	if got := checksum(cfg, tokens); got != cfg.Checksum {
		return nil, fmt.Errorf("%w: published %s, computed %s", ErrChecksumMismatch, cfg.Checksum, got)
	}
	hashes := make([]uint64, len(tokens))
	for i, tk := range tokens {
		hashes[i] = tk.Hash
	}
	return &Ring{cfg: cfg, tokens: tokens, hashes: hashes}, nil
}

// Build makes the ring for version, shards and vnodes with its checksum
// computed: the same tokens New would build from a published config with
// those numbers. It is for candidates (/admin/ring/simulate) and tools; the
// coordinator routes only by a ring loaded from the published ring.yaml.
func Build(version uint64, shards, vnodes int) (*Ring, error) {
	cfg := Config{RingVersion: version, Hash: HashXXHash64, Shards: shards, VNodesPerShard: vnodes,
		Checksum: checksumPrefix + strings.Repeat("0", 2*sha256.Size)}
	if err := validate(cfg); err != nil {
		return nil, err
	}
	cfg.Checksum = checksum(cfg, buildTokens(shards, vnodes))
	return New(cfg)
}

func validate(cfg Config) error {
	switch {
	case cfg.RingVersion == 0:
		return errors.New("ring: ringVersion must be at least 1")
	case cfg.Hash != HashXXHash64:
		return fmt.Errorf("ring: hash %q, want %q", cfg.Hash, HashXXHash64)
	case cfg.Shards < 1:
		return fmt.Errorf("ring: %d shards", cfg.Shards)
	case cfg.VNodesPerShard < 1:
		return fmt.Errorf("ring: %d vnodes per shard", cfg.VNodesPerShard)
	case cfg.Shards > maxTokens/cfg.VNodesPerShard:
		return fmt.Errorf("ring: %d shards × %d vnodes exceeds %d tokens", cfg.Shards, cfg.VNodesPerShard, maxTokens)
	}
	sum, ok := strings.CutPrefix(cfg.Checksum, checksumPrefix)
	if !ok || len(sum) != 2*sha256.Size {
		return fmt.Errorf("ring: checksum %q is not %s<64 hex digits>", cfg.Checksum, checksumPrefix)
	}
	if _, err := hex.DecodeString(sum); err != nil || strings.ToLower(sum) != sum {
		return fmt.Errorf("ring: checksum %q is not lower-case hex", cfg.Checksum)
	}
	return nil
}

func buildTokens(shards, vnodes int) []Token {
	tokens := make([]Token, 0, shards*vnodes)
	for s := range shards {
		for v := range vnodes {
			tokens = append(tokens, Token{Hash: xxhash.Sum64String(fmt.Sprintf("shard-%d-vnode-%d", s, v)), Shard: s, VNode: v})
		}
	}
	slices.SortFunc(tokens, func(a, b Token) int {
		if a.Hash != b.Hash {
			if a.Hash < b.Hash {
				return -1
			}
			return 1
		}
		if a.Shard != b.Shard {
			return a.Shard - b.Shard
		}
		return a.VNode - b.VNode
	})
	return tokens
}

// Parse decodes ring.yaml strictly (unknown fields rejected) and calls New.
func Parse(data []byte) (*Ring, error) {
	dec := yaml.NewDecoder(bytes.NewReader(data))
	dec.KnownFields(true)
	var cfg Config
	if err := dec.Decode(&cfg); err != nil {
		if errors.Is(err, io.EOF) {
			return nil, errors.New("ring: empty config")
		}
		return nil, fmt.Errorf("ring: decode: %w", err)
	}
	var extra any
	if err := dec.Decode(&extra); !errors.Is(err, io.EOF) {
		return nil, errors.New("ring: config has more than one YAML document")
	}
	return New(cfg)
}

// Load reads and parses the ring file at path.
func Load(path string) (*Ring, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("ring: %w", err)
	}
	r, err := Parse(data)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", path, err)
	}
	return r, nil
}

// Owner returns the shard that owns key. The full key, prefix included, is
// hashed byte for byte with no normalisation. Any non-empty key routes:
// req:/res: are the canonical domain prefixes, and generic KV keys (§12) are
// routed the same way.
func (r *Ring) Owner(key string) (int, error) {
	if key == "" || key == store.RequestKeyPrefix || key == store.ResourceKeyPrefix {
		return 0, fmt.Errorf("%w: %q", ErrInvalidKey, key)
	}
	h := xxhash.Sum64String(key)
	i := sort.Search(len(r.hashes), func(i int) bool { return r.hashes[i] >= h })
	if i == len(r.hashes) {
		i = 0
	}
	return r.tokens[i].Shard, nil
}

func (r *Ring) Version() uint64  { return r.cfg.RingVersion }
func (r *Ring) Shards() int      { return r.cfg.Shards }
func (r *Ring) Checksum() string { return r.cfg.Checksum }

// VNodesPerShard is the number of tokens each shard owns.
func (r *Ring) VNodesPerShard() int { return r.cfg.VNodesPerShard }

// Tokens returns a copy of the sorted token map.
func (r *Ring) Tokens() []Token { return slices.Clone(r.tokens) }

// checksum is "sha256:<hex>" over a canonical text encoding of the config
// header (version, hash, shards, vnodes) and every token in ring order.
// cfg.Checksum is not an input. Writes to a hash.Hash never fail.
func checksum(cfg Config, tokens []Token) string {
	h := sha256.New()
	_, _ = fmt.Fprintf(h, "reliefgrid-ring/v1\nringVersion=%d\nhash=%s\nshards=%d\nvnodesPerShard=%d\n",
		cfg.RingVersion, cfg.Hash, cfg.Shards, cfg.VNodesPerShard)
	for _, tk := range tokens {
		_, _ = fmt.Fprintf(h, "%016x %d %d\n", tk.Hash, tk.Shard, tk.VNode)
	}
	return checksumPrefix + hex.EncodeToString(h.Sum(nil))
}
