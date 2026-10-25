package internalapi

import (
	"errors"
	"fmt"
	"strconv"
)

// ErrBadETag: an If-Match value is not one strong entity tag holding a
// decimal entity version.
var ErrBadETag = errors.New("internalapi: bad entity tag")

// FormatETag renders an entity version as a strong entity tag, e.g. "7".
// It is the only encoding of versions in ETag and If-Match headers.
func FormatETag(version uint64) string {
	return `"` + strconv.FormatUint(version, 10) + `"`
}

// ParseETag parses one entity tag written by FormatETag. Weak tags (W/"7"),
// lists, "*", whitespace, signs and zero-padded numbers are rejected, so
// every version has exactly one tag.
func ParseETag(tag string) (uint64, error) {
	if len(tag) < 3 || tag[0] != '"' || tag[len(tag)-1] != '"' {
		return 0, fmt.Errorf("%w: %q", ErrBadETag, tag)
	}
	digits := tag[1 : len(tag)-1]
	for _, c := range digits {
		if c < '0' || c > '9' {
			return 0, fmt.Errorf("%w: %q", ErrBadETag, tag)
		}
	}
	if len(digits) > 1 && digits[0] == '0' {
		return 0, fmt.Errorf("%w: %q has a leading zero", ErrBadETag, tag)
	}
	v, err := strconv.ParseUint(digits, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("%w: %w", ErrBadETag, err)
	}
	return v, nil
}
