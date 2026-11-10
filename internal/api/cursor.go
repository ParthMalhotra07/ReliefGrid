package api

import (
	"bytes"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/ParthMalhotra07/ReliefGrid/internal/store"
	"github.com/google/uuid"
)

// Resume an opaque Store page and skip only the already emitted part of that
// page. Advancing to NextCursor before consuming every merged row loses data.
// No FSM records or historical ownership proofs are cached in this token.
type publicCursor struct {
	Version int    `json:"v"`
	Query   string `json:"q"`
	Scan    string `json:"s"`
	After   string `json:"a"`
}

func queryFingerprint(q listQuery) string {
	raw, _ := json.Marshal(struct {
		Scan   string
		Near   *store.Location
		Radius float64
	}{store.ScanQueryHash(q.scan), q.near, q.radius})
	return fmt.Sprintf("%x", sha256.Sum256(raw))
}

func encodePublicCursor(q listQuery, scan, after string) string {
	raw, _ := json.Marshal(publicCursor{1, queryFingerprint(q), scan, after})
	return base64.RawURLEncoding.EncodeToString(raw)
}

func decodePublicCursor(q *listQuery, encoded string) error {
	if len(encoded) > 16384 {
		return invalid("cursor", "cursor exceeds maximum length")
	}
	raw, err := base64.RawURLEncoding.Strict().DecodeString(encoded)
	if err != nil {
		return invalid("cursor", "malformed continuation")
	}
	var cursor publicCursor
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&cursor); err != nil || cursor.Version != 1 || cursor.Query != queryFingerprint(*q) || (cursor.Scan == "" && cursor.After == "") {
		return invalid("cursor", "continuation belongs to another query or is malformed")
	}
	if cursor.After != "" {
		prefix := store.RequestKeyPrefix
		if q.scan.Kind == store.KindResource {
			prefix = store.ResourceKeyPrefix
		}
		if !strings.HasPrefix(cursor.After, prefix) {
			return invalid("cursor", "wrong continuation kind")
		}
		if _, err := uuid.Parse(strings.TrimPrefix(cursor.After, prefix)); err != nil {
			return invalid("cursor", "malformed continuation key")
		}
	}
	if cursor.Scan != "" {
		inner, err := store.DecodeScanCursor(cursor.Scan)
		if err != nil || inner.Query != store.ScanQueryHash(q.scan) {
			return invalid("cursor", "malformed storage continuation")
		}
	}
	if encodePublicCursor(*q, cursor.Scan, cursor.After) != encoded {
		return invalid("cursor", "noncanonical continuation")
	}
	q.scan.Cursor, q.after = cursor.Scan, cursor.After
	return nil
}
