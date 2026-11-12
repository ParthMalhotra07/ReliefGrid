| | Phase 1 (strong default, 7dd1940) | Phase 2 (linearizable default, 514bb33) |
|---|---|---|
| Commit | `7dd19404d6df` | `514bb33e1ffd` |
| Saturation point (3 step runs, 10→80 req/s; median, min–max) | 50 req/s | above 80 req/s (no step broke a threshold in 3 of 3 runs) |
| First step broken / by (per run) | 60 by writeP99, 60 by writeP99, 60 by writeP99 | none, none, none |
| First component at its CPU limit (step) | storage-coordinator @ 20; storage-coordinator @ 20; storage-coordinator @ 20 | storage-coordinator @ 20; storage-coordinator @ 20; storage-coordinator @ 20 |
| Step-run error rate | 0.00095 (0.000736–0.001211) | 0.001093 (0.000617–0.00121) |
| Step-run refused publishes (broker nacks) | 6324 (5968–6381) | 8 (6–33) |
| Step-run `matching.wakeups` peak (limit 10000) | 10016 (10014–10020) | 159 (127–195) |

**Raft entries vs FSM commands** (all 3 shards; non-command = barriers, no-ops, configuration)

| | Phase 1 (strong default, 7dd1940) | Phase 2 (linearizable default, 514bb33) |
|---|---|---|
| Step run: Raft entries / FSM commands per run | 342242 / 72046 | 132108 / 132079 |
| Step run: non-command entries per run (share of the log) | 270196 (79.0%) | 29 (0.0%) |
| Step run: storage reads by mode per run | linearizable 0, stale 0, strong 270149 | linearizable 555572, stale 0, strong 0 |
| Baseline run: Raft entries / FSM commands per run | 207539 / 41267 | 82473 / 82462 |
| Baseline run: non-command entries per run (share of the log) | 166272 (80.1%) | 11 (0.0%) |
| Baseline run: storage reads by mode per run | linearizable 0, stale 0, strong 166206 | linearizable 348212, stale 0, strong 0 |
| Idle: non-command entries/s per shard (samples) | 6.23 (6.21–6.23) (1) | 0 (6) |
| Idle: FSM commands/s per shard | 0 | 0 |

**Baseline at 25 req/s** (median of 3 runs, min–max)

| | Phase 1 (strong default, 7dd1940) | Phase 2 (linearizable default, 514bb33) |
|---|---|---|
| write p50 (ms) | 135.98 (133.51–136.53) | 31.91 (31.02–32.37) |
| write p95 (ms) | 425.86 (420.57–495.16) | 75.12 (69.73–79.11) |
| write p99 (ms) | 684 (665.75–690.92) | 93.57 (85.16–99.46) |
| read p50 (ms) | 41.11 (40.19–41.17) | 2.49 (2.35–2.68) |
| read p95 (ms) | 67.87 (67.01–68.9) | 30.83 (26.74–39.78) |
| read p99 (ms) | 79.94 (79.12–80.48) | 56.43 (51.45–60) |
| list p50 (ms) | 130.41 (128.95–134.2) | 18.29 (17.5–21.12) |
| list p95 (ms) | 234.05 (233.21–238.01) | 71.5 (66.41–72.22) |
| list p99 (ms) | 291.22 (280.82–305.65) | 90.31 (85.66–91.3) |
| error rate | 0.001 (0.0005–0.00125) | 0.000167 (8.3e-05–0.00025) |
| coordinator CPU (millicores) | 119.9 (119.8–120.1) | 217.1 (213.2–218.4) |
| refused publishes | 0 | 0 |
| `matching.wakeups` peak | 232 (216–232) | 29 (28–31) |

**Per step** (median of the 3 step runs; Phase 1 (strong default, 7dd1940) → Phase 2 (linearizable default, 514bb33))

| req/s | write p50/p95/p99 (ms) | read p50/p95/p99 (ms) | list p50/p95/p99 (ms) | errors | coordinator mCPU | refused publishes | wakeups peak | pods at limit |
|---|---|---|---|---|---|---|---|---|
| 10 | 8.6/12.76/28.27 → 7.9/11.59/21.3 | 3.66/5.43/11.07 → 2.53/5.07/12.9 | 7.17/9.77/16.15 → 4.13/5.88/7.49 | 0 → 0 | 55.4 → 50.8 | 0 → 0 | 2 → 3 | none → none |
| 20 | 19.88/146.16/188.02 → 8.04/57.2/70.86 | 10.36/50.51/64.82 → 2.41/35.35/52.74 | 15.54/140.53/185.91 → 5.53/56.15/72.42 | 0.000462 → 0 | 144.6 → 157.1 | 0 → 0 | 69 → 10 | storage-coordinator → storage-coordinator |
| 30 | 137.92/550.45/718.6 → 26.42/68.57/85.32 | 43.12/67.55/81.67 → 4.51/51.66/54.05 | 129.12/214.82/250.47 → 25.66/69.39/86.8 | 0.000609 → 0.000304 | 123.2 → 225.1 | 1 → 2 | 322 → 21 | none → storage-coordinator,storage-shard-1-0 |
| 40 | 148.95/636.79/780.43 → 50.37/89.94/113.2 | 46.17/73.54/89.68 → 4.42/28.63/45.53 | 141.73/254.05/301.7 → 33.56/83.65/106.49 | 0.000228 → 0 | 139.2 → 241.8 | 0 → 2 | 978 → 35 | none → storage-coordinator |
| 50 | 162.67/720.84/970.23 → 55.51/101.9/146.25 | 49.53/78.42/95.09 → 6.39/32.68/51.87 | 154.5/281.22/346.86 → 41.01/99.63/130.58 | 0.001639 → 0.000364 | 153.1 → 246 | 0 → 1 | 2331 → 47 | none → storage-coordinator |
| 60 | 176.98/805.13/1115.09 → 67.81/139.95/230.38 | 53.6/84.86/103.94 → 8.71/41.91/59.42 | 166.51/295.12/359.56 → 54.17/122.45/160.92 | 0.000607 → 0.001366 | 168.5 → 245.9 | -1 → -1 | 5101 → 80 | none → storage-coordinator |
| 70 | 186.08/848.63/1207.95 → 75.89/187.16/317.78 | 55.98/89.12/109.3 → 12.1/52.57/71.66 | 179.15/320.31/393.64 → 68.14/159.78/217.97 | 0.001302 → 0.000911 | 187.9 → 245.3 | 0 → 0 | 8485 → 99 | none → storage-coordinator |
| 80 | 182.34/860.59/1180.37 → 88.68/232.12/388.5 | 54.05/89.22/110.58 → 14.95/55.73/76.52 | 176.45/314/381.56 → 80.73/185.66/246.8 | 0.001024 → 0.002389 | 182.2 → 245.6 | 5557 → 8 | 10016 → 159 | none → storage-coordinator |
