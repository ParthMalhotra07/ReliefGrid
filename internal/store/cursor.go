package store

import (
	"bytes"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"sort"
)

// ScanCursor is the decoded ScanQuery.Cursor. Callers treat the encoded
// string as opaque; only Store implementations build and split it.
type ScanCursor struct {
	// RingVersion is the ring the positions were read under.
	RingVersion uint64
	// Query is ScanQueryHash of the query that made the cursor; a page with
	// other filters or another limit may not continue it.
	Query string
	// After maps each shard still to be read to the last key it returned;
	// "" reads it from its first key. A shard that is absent is exhausted.
	After map[int]string
}

// cursorVersion is the wire format of an encoded cursor.
const cursorVersion = 1

// wireCursor is the encoded form. Keys are bytes so that any key, valid
// UTF-8 or not, survives JSON; shards are sorted so encoding is canonical.
type wireCursor struct {
	V     int         `json:"v"`
	Ring  uint64      `json:"r"`
	Query string      `json:"q"`
	After []wireAfter `json:"a"`
}

type wireAfter struct {
	Shard int    `json:"s"`
	Key   []byte `json:"k"`
}

// EncodeScanCursor returns c as a URL-safe string; an empty After gives "".
func EncodeScanCursor(c ScanCursor) string {
	if len(c.After) == 0 {
		return ""
	}
	w := wireCursor{V: cursorVersion, Ring: c.RingVersion, Query: c.Query, After: []wireAfter{}}
	for s, k := range c.After {
		w.After = append(w.After, wireAfter{Shard: s, Key: []byte(k)})
	}
	sort.Slice(w.After, func(i, j int) bool { return w.After[i].Shard < w.After[j].Shard })
	raw, _ := json.Marshal(w) // cannot fail: no maps, channels or floats
	return base64.RawURLEncoding.EncodeToString(raw)
}

var errBadCursor = errors.New("store: malformed scan cursor")

// DecodeScanCursor parses s. It rejects anything EncodeScanCursor would not
// produce, including an empty shard set, negative shards and ring version 0.
func DecodeScanCursor(s string) (ScanCursor, error) {
	raw, err := base64.RawURLEncoding.Strict().DecodeString(s)
	if err != nil || s == "" {
		return ScanCursor{}, errBadCursor
	}
	var w wireCursor
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&w); err != nil {
		return ScanCursor{}, fmt.Errorf("%w: %w", errBadCursor, err)
	}
	if w.V != cursorVersion || w.Ring == 0 || len(w.After) == 0 {
		return ScanCursor{}, fmt.Errorf("%w: version %d, ring %d, %d shards", errBadCursor, w.V, w.Ring, len(w.After))
	}
	c := ScanCursor{RingVersion: w.Ring, Query: w.Query, After: map[int]string{}}
	for _, a := range w.After {
		if _, dup := c.After[a.Shard]; dup || a.Shard < 0 {
			return ScanCursor{}, fmt.Errorf("%w: shard %d", errBadCursor, a.Shard)
		}
		c.After[a.Shard] = string(a.Key)
	}
	if EncodeScanCursor(c) != s { // trailing bytes, unsorted shards, or another spelling
		return ScanCursor{}, fmt.Errorf("%w: not canonical", errBadCursor)
	}
	return c, nil
}

// ScanQueryHash is a short hash of the parts of q that decide which records
// a page holds: kind, type, statuses, geohash prefixes and limit. The order
// of statuses and of prefixes does not matter; Cursor is not included.
func ScanQueryHash(q ScanQuery) string {
	canon := struct {
		Kind     Kind         `json:"kind"`
		Type     ResourceType `json:"type"`
		Statuses []string     `json:"statuses"`
		Prefixes []string     `json:"prefixes"`
		Limit    int          `json:"limit"`
	}{q.Kind, q.Type, sorted(q.Statuses), sorted(q.GeohashPrefixes), q.Limit}
	raw, _ := json.Marshal(canon)
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:8])
}

func sorted(s []string) []string {
	out := slices.Clone(s)
	slices.Sort(out)
	if out == nil {
		out = []string{}
	}
	return out
}
