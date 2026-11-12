#!/usr/bin/env python3
"""S5.2 Phase 1 vs Phase 2 comparison (stdlib only, no cluster).

  compare.py <config.json>  ->  writes <out>.json and <out>.md

config.json: {"out": "<path without extension>",
              "phases": {"<label>": {"commit": "...", "steps": [run dirs], "baselines": [run dirs],
                                       "extras": {"<runId>": "<extras.json>"}, "idle": ["<extras.json>", ...]}}}
Run dirs are committed report directories (summary.json, facts.json, raw/requests.csv.gz).
Per-step p50/p95/p99 are recomputed from raw/requests.csv.gz with the report's
definition (requests that started in the step window, ramp excluded; linear
interpolation between the closest ranks); each recomputed p99 must equal the
report's own steps[].<class>P99Ms, or the script fails.
"""
import csv
import datetime
import gzip
import json
import statistics
import sys

CLASSES = ('write', 'read', 'list')


def pct(sorted_v, q):
    if not sorted_v:
        return None
    if len(sorted_v) == 1:
        return sorted_v[0]
    pos = q * (len(sorted_v) - 1)
    lo = int(pos)
    hi = min(lo + 1, len(sorted_v) - 1)
    return sorted_v[lo] + (sorted_v[hi] - sorted_v[lo]) * (pos - lo)


def r2(x):
    return None if x is None else round(x, 2)


def ms(iso):
    return datetime.datetime.fromisoformat(iso).timestamp() * 1000


def load(d):
    s = json.load(open(f'{d}/summary.json'))
    f = json.load(open(f'{d}/facts.json'))
    return s, f


def step_percentiles(d, s, f):
    start = ms(f['startedAt'])
    rows = []
    with gzip.open(f'{d}/raw/requests.csv.gz', 'rt') as fh:
        for r in csv.DictReader(fh):
            rows.append((float(r['t_ms']), r['class'], float(r['duration_ms'])))
    out = []
    for i, st in enumerate(f['steps']):
        lo, hi = ms(st['from']) - start, ms(st['to']) - start
        per = {}
        for c in CLASSES:
            v = sorted(dur for t, cl, dur in rows if cl == c and lo <= t < hi)
            per[c] = {'n': len(v), 'p50Ms': r2(pct(v, .50)), 'p95Ms': r2(pct(v, .95)), 'p99Ms': r2(pct(v, .99))}
        rep = s['steps'][i]
        for c, key in (('write', 'writeP99Ms'), ('read', 'readP99Ms'), ('list', 'listP99Ms')):
            if per[c]['p99Ms'] is not None and abs(per[c]['p99Ms'] - rep[key]) > 0.011:
                raise SystemExit(f'{d} step {st["targetRps"]} {c}: recomputed p99 {per[c]["p99Ms"]} != report {rep[key]}')
        out.append({'targetRps': rep['targetRps'], 'achievedRps': rep['achievedRps'], 'errorRate': rep['errorRate'],
                    'droppedIterations': rep['droppedIterations'], 'classes': per,
                    'podsAtLimit': sorted(p['name'].rsplit('-', 2)[0] if p['name'].count('-') >= 2 and not p['name'].startswith('storage-shard') else p['name'] for p in rep['podsAtLimit']),
                    'refusedPublishes': rep['background']['publishesNotConfirmed'],
                    'wakeupsPeak': rep['background']['queuePeak'].get('matching.wakeups')})
    return out


def component(pod):
    # storage-coordinator-6c449fd88-hfd55 -> storage-coordinator; reliefgrid-api-x-y -> reliefgrid-api
    parts = pod.split('-')
    if pod.startswith('storage-shard-'):
        return 'storage-shard'
    if len(parts) >= 3:
        return '-'.join(parts[:-2])
    return pod


def med(vals, nd=2):
    vals = [v for v in vals if v is not None]
    return None if not vals else round(statistics.median(vals), nd)


def spread(vals, nd=2):
    vals = [v for v in vals if v is not None]
    if not vals:
        return None
    return {'median': round(statistics.median(vals), nd), 'min': round(min(vals), nd), 'max': round(max(vals), nd), 'n': len(vals)}


def phase(cfg):
    res = {'commit': cfg['commit'], 'steps': [], 'baselines': []}
    for d in cfg['steps']:
        s, f = load(d)
        sat = s.get('saturation') or {}
        x = json.load(open(cfg['extras'][s['runId']]))
        # First step with any pod at its CPU limit, from the report's per-step
        # podsAtLimit (the same source as saturation.firstAtLimit, which is
        # absent when no step breaks a threshold).
        fal = next(({'targetRps': st['targetRps'], 'pods': [p['name'] for p in st['podsAtLimit']]}
                    for st in s['steps'] if st['podsAtLimit']), {})
        if sat.get('firstAtLimit') and sat['firstAtLimit'] != fal:
            raise SystemExit(f"{d}: firstAtLimit {sat['firstAtLimit']} != per-step {fal}")
        top = s['steps'][-1]['targetRps']
        cpu = {c['targetRps']: c['millicores'] for c in x['coordinatorCpu']}
        res['steps'].append({'runId': s['runId'], 'saturationRps': sat.get('rps'), 'aboveRange': not sat, 'topStepRps': top,
                             'brokenAt': sat.get('brokenAt'), 'brokenBy': sat.get('brokenBy'),
                             'firstAtLimit': {'targetRps': fal.get('targetRps'), 'components': sorted({component(p) for p in fal.get('pods', [])})},
                             'errorRate': s['results']['errors']['rate'],
                             'refusedPublishesRun': s['background']['run']['publishesNotConfirmed'],
                             'wakeupsPeakRun': s['background']['run']['queuePeak'].get('matching.wakeups'),
                             'steps': step_percentiles(d, s, f), 'coordinatorCpu': cpu,
                             'raft': x['load']})
    for d in cfg['baselines']:
        s, f = load(d)
        x = json.load(open(cfg['extras'][s['runId']]))
        res['baselines'].append({'runId': s['runId'], 'rate': s['load'].get('rate') or f['load'].get('rate'),
                                 'classes': {c: {k: s['results']['classes'][c][k] for k in ('p50Ms', 'p95Ms', 'p99Ms')} for c in CLASSES},
                                 'errorRate': s['results']['errors']['rate'],
                                 'achievedRps': s['results']['throughputRps'],
                                 'refusedPublishesRun': s['background']['run']['publishesNotConfirmed'],
                                 'wakeupsPeakRun': s['background']['run']['queuePeak'].get('matching.wakeups'),
                                 'coordinatorCpu': x['coordinatorCpu'][0]['millicores'], 'raft': x['load']})
    res['idle'] = [json.load(open(p))['idle'] for p in cfg.get('idle', [])]
    return res


def agg(p):
    a = {}
    a['saturationRps'] = spread([r['saturationRps'] for r in p['steps']])
    a['aboveRange'] = [r['aboveRange'] for r in p['steps']]
    a['topStepRps'] = max((r['topStepRps'] for r in p['steps']), default=None)
    a['brokenAt'] = [r['brokenAt'] for r in p['steps']]
    a['brokenBy'] = [r['brokenBy'] for r in p['steps']]
    a['firstAtLimit'] = [r['firstAtLimit'] for r in p['steps']]
    targets = [st['targetRps'] for st in p['steps'][0]['steps']] if p['steps'] else []
    a['perStep'] = []
    for i, t in enumerate(targets):
        row = {'targetRps': t}
        for c in CLASSES:
            for q in ('p50Ms', 'p95Ms', 'p99Ms'):
                row[f'{c}.{q}'] = med([r['steps'][i]['classes'][c][q] for r in p['steps']])
        row['errorRate'] = med([r['steps'][i]['errorRate'] for r in p['steps']], 6)
        row['achievedRps'] = med([r['steps'][i]['achievedRps'] for r in p['steps']])
        row['coordinatorMillicores'] = med([r['coordinatorCpu'].get(t) for r in p['steps']])
        row['refusedPublishes'] = med([r['steps'][i]['refusedPublishes'] for r in p['steps']])
        row['wakeupsPeak'] = med([r['steps'][i]['wakeupsPeak'] for r in p['steps']])
        row['podsAtLimit'] = [r['steps'][i]['podsAtLimit'] for r in p['steps']]
        a['perStep'].append(row)
    a['baseline'] = {}
    for c in CLASSES:
        for q in ('p50Ms', 'p95Ms', 'p99Ms'):
            a['baseline'][f'{c}.{q}'] = spread([b['classes'][c][q] for b in p['baselines']])
    a['baseline']['errorRate'] = spread([b['errorRate'] for b in p['baselines']], 6)
    a['baseline']['coordinatorMillicores'] = spread([b['coordinatorCpu'] for b in p['baselines']])
    a['baseline']['refusedPublishes'] = spread([b['refusedPublishesRun'] for b in p['baselines']])
    a['baseline']['wakeupsPeak'] = spread([b['wakeupsPeakRun'] for b in p['baselines']])
    a['stepRun'] = {'errorRate': spread([r['errorRate'] for r in p['steps']], 6),
                    'refusedPublishes': spread([r['refusedPublishesRun'] for r in p['steps']]),
                    'wakeupsPeak': spread([r['wakeupsPeakRun'] for r in p['steps']])}

    def raft_share(runs):
        ent = sum(sh['raftEntries'] for r in runs for sh in r['raft'])
        cmd = sum(sh['commands'] for r in runs for sh in r['raft'])
        reads = {}
        for r in runs:
            for sh in r['raft']:
                for k, v in sh['reads'].items():
                    reads[k] = reads.get(k, 0) + v
        n = len(runs) or 1
        return {'raftEntriesPerRun': round(ent / n), 'commandsPerRun': round(cmd / n), 'nonCommandPerRun': round((ent - cmd) / n),
                'nonCommandShare': None if not ent else round((ent - cmd) / ent, 4), 'readsPerRun': {k: round(v / n) for k, v in sorted(reads.items())}}
    a['raftStep'] = raft_share(p['steps'])
    a['raftBaseline'] = raft_share(p['baselines'])
    a['raftStepPerShard'] = [[{'shard': sh['shard'], 'nonCommand': sh['nonCommand'], 'commands': sh['commands']} for sh in r['raft']] for r in p['steps']]
    idle = [sh for sample in p['idle'] for sh in sample]
    a['idle'] = None if not idle else {
        'samples': len(p['idle']),
        'nonCommandPerSPerShard': spread([sh['nonCommandPerS'] for sh in idle]),
        'commandsPerSPerShard': spread([sh['commandsPerS'] for sh in idle]),
        'perShard': [[{k: sh[k] for k in ('shard', 'seconds', 'raftEntries', 'commands', 'nonCommand', 'nonCommandPerS', 'reads')} for sh in sample] for sample in p['idle']]}
    return a


def fmt(x):
    if x is None:
        return '–'
    if isinstance(x, dict):
        if x.get('n', 1) > 1 and x['min'] != x['max']:
            return f"{fmt(x['median'])} ({fmt(x['min'])}–{fmt(x['max'])})"
        return fmt(x['median'])
    if isinstance(x, float):
        return f'{x:g}'
    return str(x)


def markdown(labels, A, P):
    a, b = labels
    L = []
    w = L.append
    w(f'| | {a} | {b} |')
    w('|---|---|---|')
    w(f"| Commit | `{P[a]['commit'][:12]}` | `{P[b]['commit'][:12]}` |")
    def satv(x):
        n_above = sum(x['aboveRange'])
        if n_above == len(x['aboveRange']):
            return f"above {x['topStepRps']} req/s (no step broke a threshold in {n_above} of {n_above} runs)"
        s = f"{fmt(x['saturationRps'])} req/s"
        return s + (f"; {n_above} run(s) above {x['topStepRps']}" if n_above else '')
    def brk(x):
        return ', '.join('none' if above else f"{at} by {','.join(by)}" for at, by, above in zip(x['brokenAt'], x['brokenBy'], x['aboveRange']))
    w(f"| Saturation point (3 step runs, 10→80 req/s; median, min–max) | {satv(A[a])} | {satv(A[b])} |")
    w(f"| First step broken / by (per run) | {brk(A[a])} | {brk(A[b])} |")
    def fal(lst):
        return '; '.join((','.join(x['components']) or 'none') + ' @ ' + str(x['targetRps']) for x in lst)
    w(f"| First component at its CPU limit (step) | {fal(A[a]['firstAtLimit'])} | {fal(A[b]['firstAtLimit'])} |")
    w(f"| Step-run error rate | {fmt(A[a]['stepRun']['errorRate'])} | {fmt(A[b]['stepRun']['errorRate'])} |")
    w(f"| Step-run refused publishes (broker nacks) | {fmt(A[a]['stepRun']['refusedPublishes'])} | {fmt(A[b]['stepRun']['refusedPublishes'])} |")
    w(f"| Step-run `matching.wakeups` peak (limit 10000) | {fmt(A[a]['stepRun']['wakeupsPeak'])} | {fmt(A[b]['stepRun']['wakeupsPeak'])} |")
    w('')
    w('**Raft entries vs FSM commands** (all 3 shards; non-command = barriers, no-ops, configuration)')
    w('')
    w(f'| | {a} | {b} |')
    w('|---|---|---|')
    for key, name in (('raftStep', 'Step run'), ('raftBaseline', 'Baseline run')):
        x, y = A[a][key], A[b][key]
        w(f"| {name}: Raft entries / FSM commands per run | {x['raftEntriesPerRun']} / {x['commandsPerRun']} | {y['raftEntriesPerRun']} / {y['commandsPerRun']} |")
        w(f"| {name}: non-command entries per run (share of the log) | {x['nonCommandPerRun']} ({x['nonCommandShare']:.1%}) | {y['nonCommandPerRun']} ({y['nonCommandShare']:.1%}) |")
        w(f"| {name}: storage reads by mode per run | {', '.join(f'{k} {v}' for k, v in x['readsPerRun'].items())} | {', '.join(f'{k} {v}' for k, v in y['readsPerRun'].items())} |")
    ia, ib = A[a]['idle'], A[b]['idle']
    w(f"| Idle: non-command entries/s per shard (samples) | {fmt(ia and ia['nonCommandPerSPerShard'])} ({ia and ia['samples']}) | {fmt(ib and ib['nonCommandPerSPerShard'])} ({ib and ib['samples']}) |")
    w(f"| Idle: FSM commands/s per shard | {fmt(ia and ia['commandsPerSPerShard'])} | {fmt(ib and ib['commandsPerSPerShard'])} |")
    w('')
    w('**Baseline at 25 req/s** (median of 3 runs, min–max)')
    w('')
    w(f'| | {a} | {b} |')
    w('|---|---|---|')
    for c in CLASSES:
        for q in ('p50Ms', 'p95Ms', 'p99Ms'):
            w(f"| {c} {q[:3]} (ms) | {fmt(A[a]['baseline'][f'{c}.{q}'])} | {fmt(A[b]['baseline'][f'{c}.{q}'])} |")
    for k, name in (('errorRate', 'error rate'), ('coordinatorMillicores', 'coordinator CPU (millicores)'),
                    ('refusedPublishes', 'refused publishes'), ('wakeupsPeak', '`matching.wakeups` peak')):
        w(f"| {name} | {fmt(A[a]['baseline'][k])} | {fmt(A[b]['baseline'][k])} |")
    w('')
    w(f'**Per step** (median of the 3 step runs; {a} → {b})')
    w('')
    w('| req/s | write p50/p95/p99 (ms) | read p50/p95/p99 (ms) | list p50/p95/p99 (ms) | errors | coordinator mCPU | refused publishes | wakeups peak | pods at limit |')
    w('|---|---|---|---|---|---|---|---|---|')
    for ra, rb in zip(A[a]['perStep'], A[b]['perStep']):
        def trip(r, c):
            return '/'.join(fmt(r[f'{c}.{q}']) for q in ('p50Ms', 'p95Ms', 'p99Ms'))
        def lim(r):
            s = sorted({p for run in r['podsAtLimit'] for p in run})
            return ','.join(s) or 'none'
        w(f"| {ra['targetRps']} | {trip(ra, 'write')} → {trip(rb, 'write')} | {trip(ra, 'read')} → {trip(rb, 'read')} | {trip(ra, 'list')} → {trip(rb, 'list')} | "
          f"{fmt(ra['errorRate'])} → {fmt(rb['errorRate'])} | {fmt(ra['coordinatorMillicores'])} → {fmt(rb['coordinatorMillicores'])} | "
          f"{fmt(ra['refusedPublishes'])} → {fmt(rb['refusedPublishes'])} | {fmt(ra['wakeupsPeak'])} → {fmt(rb['wakeupsPeak'])} | {lim(ra)} → {lim(rb)} |")
    return '\n'.join(L) + '\n'


def main(path):
    cfg = json.load(open(path))
    labels = list(cfg['phases'])
    P = {k: phase(v) for k, v in cfg['phases'].items()}
    A = {k: agg(v) for k, v in P.items()}
    json.dump({'phases': P, 'aggregate': A}, open(cfg['out'] + '.json', 'w'), indent=1)
    open(cfg['out'] + '.md', 'w').write(markdown(labels, A, P))
    print(open(cfg['out'] + '.md').read())


if __name__ == '__main__':
    main(sys.argv[1])
