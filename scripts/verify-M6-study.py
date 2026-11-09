#!/usr/bin/env python3
"""Verify retained S4.1 study data and its proposed M6 decision record.

This replays historical evidence; it does not run a new benchmark or certify
the present cluster. Emit JSON so the report can reproduce its calculations.
"""
import json
from pathlib import Path
import re
from statistics import median

ROOT = Path(__file__).resolve().parents[1]
BENCH = ROOT / 'docs/evidence/S4.1/bench-20261031T083641Z.txt'
SAFETY = ROOT / 'docs/evidence/S4.1/fence-seeds-20261031T082917Z.txt'
ADR = ROOT / 'docs/adr/006-read-fence-study.md'


def verify():
    text = BENCH.read_text()
    groups = {(mode, load): [] for mode in ('barrier', 'fence')
              for load in ('serial', 'parallel')}
    for line in text.replace('\\t', ' ').splitlines():
        match = re.fullmatch(
            r'BenchmarkStrongRead/(barrier|fence)/(serial|parallel)-\d+\s+'
            r'(\d+)\s+(\d+) ns/op\s+([0-9.]+) entries/op', line)
        if match:
            mode, load, count, elapsed, entries = match.groups()
            if int(count) != 2000:
                raise ValueError('unexpected iteration count: ' + line)
            expected = 1 if mode == 'barrier' else 0
            if float(entries) != expected:
                raise ValueError('unexpected log-entry measurement: ' + line)
            groups[mode, load].append(int(elapsed))
    if any(len(samples) != 5 for samples in groups.values()):
        raise ValueError('expected five samples for each mode/load combination')
    if 'code dirty: no' not in text or 'commit:     5ad15ac' not in text:
        raise ValueError('missing historical benchmark source metadata')
    safety = SAFETY.read_text()
    if 'result:     PASSED (21/21 runs)' not in safety:
        raise ValueError('historical safety campaign is not complete')
    for seed in range(1, 21):
        if not re.search(rf'^seed {seed}: PASS ', safety, re.MULTILINE):
            raise ValueError(f'missing successful historical seed {seed}')
    if 'routes and config: PASS' not in safety:
        raise ValueError('missing historical route/config check')
    if not ADR.is_file():
        raise ValueError('missing M6 proposal: docs/adr/006-read-fence-study.md')
    summary = {
        'scope': 'historical three-node loopback microbenchmark; proposed M6 selection',
        'benchmark_commit': '5ad15ac',
        'benchmark_path': str(BENCH.relative_to(ROOT)),
        'historical_safety_path': str(SAFETY.relative_to(ROOT)),
        'samples_per_combination': 5,
        'iterations_per_sample': 2000,
        'measurements': {},
    }
    for load in ('serial', 'parallel'):
        baseline, fence = groups['barrier', load], groups['fence', load]
        summary['measurements'][load] = {
            'barrier_ns_per_op': {'median': median(baseline), 'min': min(baseline), 'max': max(baseline)},
            'fence_ns_per_op': {'median': median(fence), 'min': min(fence), 'max': max(fence)},
            'ratio_of_medians': round(median(baseline) / median(fence), 3),
            'barrier_entries_per_op': 1,
            'fence_entries_per_op': 0,
        }
    print(json.dumps(summary, indent=2))


if __name__ == '__main__':
    try:
        verify()
    except (OSError, ValueError) as error:
        raise SystemExit('M6 STUDY VERIFY FAIL: ' + str(error)) from error
