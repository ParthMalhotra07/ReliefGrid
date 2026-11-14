#!/usr/bin/env python3
"""Per-step figures for a step run whose report was never built (audit failed),
from k6's raw points, cgroup samples and background samples, using run.sh's
windows: t0 = first "vus" point, step i holds [i*(R+H)+R, (i+1)*(R+H)).
Request time = k6 sample time minus its duration (the report counts requests
by start). Not a harness report: unvalidated, exploratory.
  explore_raw.py <run dir> <START> <STEP> <N> <R> <H> <limits.json> > out.json"""
import datetime, gzip, json, sys
d, a, s, n, r, h = sys.argv[1], *map(int, sys.argv[2:7])
_lim = json.load(open(sys.argv[7]))
def comp(pod):
    return pod if pod.startswith('storage-shard-') or pod.endswith('-0') and 'rabbitmq' in pod else pod.rsplit('-', 2)[0]
limits_by = {comp(k): v for k, v in _lim.items()}
class _L(dict):
    def __contains__(self, k): return comp(k) in limits_by
    def __getitem__(self, k): return limits_by[comp(k)]
limits = _L()
def ts(x): return datetime.datetime.fromisoformat(x).timestamp()
def pct(v, q):
    if not v: return None
    v = sorted(v); p = q * (len(v) - 1); lo = int(p); hi = min(lo + 1, len(v) - 1)
    return round(v[lo] + (v[hi] - v[lo]) * (p - lo), 2)
t0 = None
steps = [{'targetRps': a + i * s, 'lo': i * (r + h) + r, 'hi': (i + 1) * (r + h)} for i in range(n)]
for st in steps: st.update(dur={'write': [], 'read': [], 'list': []}, reqs=0, failed=0, dropped=0)
def which(t):
    for st in steps:
        if st['lo'] <= t < st['hi']: return st
with gzip.open(f'{d}/raw/k6-points.jsonl.gz', 'rt') as f:
    for line in f:
        if '"Point"' not in line: continue
        p = json.loads(line); m = p['metric']; dt = p['data']
        if t0 is None and m == 'vus': t0 = ts(dt['time'])
        if t0 is None or m not in ('http_req_duration', 'http_req_failed', 'dropped_iterations'): continue
        t = ts(dt['time']) - t0
        if m == 'http_req_duration':
            st = which(t - dt['value'] / 1000)
            if st and dt['tags'].get('class') in st['dur']:
                st['dur'][dt['tags']['class']].append(dt['value']); st['reqs'] += 1
        elif m == 'http_req_failed':
            st = which(t)
            if st: st['failed'] += dt['value']
        else:
            st = which(t)
            if st: st['dropped'] += dt['value']
# cgroup: per pod millicores per step
cg = {}
for line in open(f'{d}/raw/cgroup.jsonl'):
    x = json.loads(line); cg.setdefault(x['pod'], []).append((x['tMs'] / 1000 - t0, x['usageUsec']))
bg = [json.loads(l) for l in open(f'{d}/raw/background.jsonl')]
out = []
for st in steps:
    row = {'targetRps': st['targetRps'], 'achievedRps': round(st['reqs'] / (st['hi'] - st['lo']), 2),
           'errorRate': round(st['failed'] / st['reqs'], 6) if st['reqs'] else None, 'droppedIterations': int(st['dropped'])}
    for c, v in st['dur'].items(): row[c] = {'n': len(v), 'p50': pct(v, .5), 'p95': pct(v, .95), 'p99': pct(v, .99)}
    cpu = {}
    for pod, smp in cg.items():
        w = sorted(x for x in smp if st['lo'] <= x[0] <= st['hi'])
        if len(w) >= 2: cpu[pod] = round((w[-1][1] - w[0][1]) / 1000 / (w[-1][0] - w[0][0]), 1)
    row['coordinatorMillicores'] = next((v for k, v in cpu.items() if k.startswith('storage-coordinator')), None)
    row['nearLimit'] = sorted([{'pod': k, 'm': v, 'pct': round(100 * v / limits[k], 1)} for k, v in cpu.items() if k in limits and v >= 0.9 * limits[k]], key=lambda z: -z['pct'])
    w = [b for b in bg if st['lo'] <= b['tMs'] / 1000 - t0 < st['hi']]
    row['queuePeak'] = {q: max((b['queues'][q]['ready'] + b['queues'][q]['unacked'] for b in w), default=None) for q in ('matching.requests', 'matching.wakeups', 'retry.5s')}
    row['nacks'] = (w[-1]['brokerReceivedConfirm'] - w[-1]['brokerConfirmed']) - (w[0]['brokerReceivedConfirm'] - w[0]['brokerConfirmed']) if len(w) >= 2 else None
    out.append(row)
json.dump({'t0': t0, 'steps': out}, sys.stdout, indent=1)
