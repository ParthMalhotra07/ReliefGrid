package coordinator

import (
	"testing"
	"time"
)

// jitter spreads a backoff over [d/2, d] so concurrent retries to one shard
// do not move in lockstep.
func TestJitterStaysWithinTheBackoff(t *testing.T) {
	d := 200 * time.Millisecond
	seen := map[time.Duration]bool{}
	for range 1000 {
		j := jitter(d)
		if j < d/2 || j > d {
			t.Fatalf("jitter(%s) = %s, want within [%s, %s]", d, j, d/2, d)
		}
		seen[j] = true
	}
	if len(seen) < 10 {
		t.Fatalf("jitter(%s) took %d distinct values in 1000 draws; want a spread", d, len(seen))
	}
}
