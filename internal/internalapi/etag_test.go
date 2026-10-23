package internalapi

import (
	"errors"
	"math"
	"testing"
)

func TestETagRoundTrip(t *testing.T) {
	for _, v := range []uint64{0, 1, 7, 4182, math.MaxUint64} {
		got, err := ParseETag(FormatETag(v))
		if err != nil || got != v {
			t.Errorf("ParseETag(FormatETag(%d)) = %d, %v", v, got, err)
		}
	}
	if got := FormatETag(7); got != `"7"` {
		t.Errorf("FormatETag(7) = %s; want \"7\"", got)
	}
}

func TestParseETagRejects(t *testing.T) {
	for _, tag := range []string{
		``, `7`, `"`, `""`, `"07"`, `"-1"`, `"+7"`, `"7 "`, ` "7"`, `"7`, `7"`,
		`W/"7"`, `*`, `"7", "8"`, `"0x7"`, `"18446744073709551616"`,
	} {
		if v, err := ParseETag(tag); !errors.Is(err, ErrBadETag) {
			t.Errorf("ParseETag(%q) = %d, %v; want ErrBadETag", tag, v, err)
		}
	}
}
