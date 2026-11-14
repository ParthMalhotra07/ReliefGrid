"""Per-shard post-grace read coverage for restart-coordinator, by Monte Carlo of the actual
reader mechanism (loadtest/ledger/driver.go readLoop):

- 16 update keys (WORKERS=4 x KEYS=4); each key's shard drawn from the ring's measured shares
  (contracts/ring.yaml: 0.3568, 0.3005, 0.3427), since the key prefix changes every run;
- reader j of R reads keys j, j+R, j+2R, ... (mod 16) in turn; at the fault it is at a uniformly
  random point of its own cycle;
- each reader makes N_j ~ Poisson(rate * window) reads in the post-grace window, rate and window
  from the retained runs (12 Nov: 20 reads, 2 readers, 30.44 s; 14 Nov: 18 reads, 2 readers,
  30.11 s): rate = 38 / (2 * 60.55) reads/s per reader; window = 30.11 s (the shorter);
- a run fails coverage when any shard gets fewer than MIN reads.
"""
import math
import random
import sys

SHARES = [0.3568, 0.3005, 0.3427]
KEYS, MIN = 16, 3
RATE = 38 / (2 * (30.44 + 30.11))  # reads per second per reader
WINDOW = 30.11


def poisson(lam, rng):
    # Knuth; lam is small (about 9.5)
    L, k, p = math.exp(-lam), 0, 1.0
    while True:
        p *= rng.random()
        if p <= L:
            return k
        k += 1


def trial(R, rng, rate=RATE, window=WINDOW):
    shard = rng.choices(range(3), weights=SHARES, k=KEYS)
    counts = [0, 0, 0]
    for j in range(R):
        cycle = [(j + i * R) % KEYS for i in range(KEYS // math.gcd(R, KEYS))]
        start = rng.randrange(len(cycle))
        for i in range(poisson(rate * window, rng)):
            counts[shard[cycle[(start + i) % len(cycle)]]] += 1
    return counts


def p_fail(R, n, seed=1, **kw):
    rng = random.Random(seed)
    fails = sum(min(trial(R, rng, **kw)) < MIN for _ in range(n))
    return fails / n


if __name__ == '__main__':
    n = int(sys.argv[1]) if len(sys.argv) > 1 else 200000
    print(f'rate {RATE:.4f} reads/s per reader; mean reads per reader in the window {RATE * WINDOW:.2f}')
    # The model at R = 2 against what was observed.
    rng = random.Random(7)
    samples = [trial(2, rng) for _ in range(n)]
    totals = sorted(sum(c) for c in samples)
    print(f'R=2: mean total {sum(totals) / n:.1f} (observed 20, 18); '
          f'P(min shard < 3) = {sum(min(c) < 3 for c in samples) / n:.3f}; '
          f'P(min shard <= 3) = {sum(min(c) <= 3 for c in samples) / n:.3f}')
    floor = 3 * (1 - SHARES[1]) ** KEYS  # roughly: some shard owns none of the 16 keys
    print(f'floor (a shard owns no key; no reader count helps): '
          f'{sum((1 - s) ** KEYS for s in SHARES):.4f}')
    for R in range(2, 17):
        print(f'R={R:2d}: P(any shard < {MIN}) = {p_fail(R, n):.4f}')
    # Sensitivity: a slower reader (the slower run's rate) and a 10% shorter window.
    slow = 18 / (2 * 30.11)
    for R in (6, 8, 10, 12):
        print(f'R={R:2d} sensitivity: rate {slow:.3f}, window 27.1 s -> '
              f'P = {p_fail(R, n, seed=3, rate=slow, window=27.1):.4f}')
