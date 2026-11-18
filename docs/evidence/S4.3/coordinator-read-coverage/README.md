# restart-coordinator: readers for per-shard read coverage after the grace

`chaos/judge` requires at least `MinCoverage` (3) reads on every shard in the window after the
grace (`readsInsideFault`). For `restart-coordinator` with two readers, the retained runs give:

| Run (evidence) | Readers | Window after the grace | Shard 0 | Shard 1 | Shard 2 | Total |
|---|---|---|---|---|---|---|
| 12 Nov gate (auth off) | 2 | 30.44 s | 6 | 11 | 3 | 20 |
| 14 Nov auth run (#181) | 2 | 30.11 s | 9 | **2** | 7 | 18 |

## Why the counts vary

Readers do not sample randomly. While the coordinator is down, each read waits on it. The pooled
rate is 38 reads / (2 readers × 60.55 s) = **0.314 reads/s per reader**, about 9.45 reads per
reader in a 30.11 s window. Reader *j* of *R* reads the 16 update keys (4 writers × 4 keys)
*j, j+R, j+2R, …* in turn (`loadtest/ledger/driver.go`, `readLoop`). A shard's reads therefore
depend on how many of the 16 keys it owns, which changes every run because the key prefix does,
and on where each reader is in its cycle when the fault starts.

## Model

`model.py` is a Monte Carlo of that mechanism:
- each key's shard is drawn from the ring's measured shares: 0.3568 / 0.3005 / 0.3427,
  from 200 000 random keys through `ring.Owner` on `contracts/ring.yaml`;
- each reader starts at a random point of its cycle;
- each reader's read count is Poisson with mean 0.314 × 30.11.

200 000 trials per reader count, seeded; full results in `output.txt`. At 2 readers the model
gives a mean total of 18.9 (observed: 20 and 18) and P(some shard < 3) = 20.7%; one of the two
runs failed and the other passed at exactly 3.

| Readers | P(some shard < 3 reads) | Sensitivity: rate 0.299/s, window 27.1 s |
|---|---|---|
| 2 | 20.7% | |
| 4 | 3.5% | |
| 6 | 1.04% | 1.64% |
| 8 | 0.81% | 1.09% |
| **10** | **0.55%** | **0.58%** |
| 12 | 0.54% | 0.55% |

**The floor.** A shard owns none of the 16 keys with probability
Σ(1 − sᵢ)¹⁶ = 0.6432¹⁶ + 0.6995¹⁶ + 0.6573¹⁶ ≈ 0.00086 + 0.00329 + 0.00121 = **0.54%**. No reader
count changes that case, which also fails the existing "every shard acknowledges writes again"
check.

**Ten readers** are the fewest that keep the chance below 1% under the slower rate and the
shorter window, and they reach the floor. The Monte Carlo's standard error near 0.55% is about
±0.02 percentage points. Only `restart-coordinator` changes; the other experiments keep two
readers, and the per-shard rule stays at 3.

Reproduce with `python3 model.py 200000` (about 45 s).
