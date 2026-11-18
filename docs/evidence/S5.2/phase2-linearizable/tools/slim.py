#!/usr/bin/env python3
"""Slim one copied run dir like Phase 1's committed copy (01feb5b): k6 points, datasets,
statuses, cgroup samples and metrics scrapes stay off-repo. summary.json marks
them committed=false with bytes, sha256 and where; SHA256SUMS keeps listing
them and gets summary.json's new sum.   slim.py <copied-run-dir> <where-prefix>"""
import hashlib, json, os, sys
dst, where = sys.argv[1], sys.argv[2]
OFF = ('dataset.json', 'raw/k6-points.jsonl.gz', 'raw/statuses.jsonl', 'raw/cgroup.jsonl', 'raw/metrics-start/', 'raw/metrics-end/')
sp = os.path.join(dst, 'summary.json')
s = json.load(open(sp))
def sha(p):
    return hashlib.sha256(open(p, 'rb').read()).hexdigest()
for r in s['raw']:
    if r.get('committed') is True and r['path'].startswith(OFF):
        p = os.path.join(dst, r['path'])
        r.update(committed=False, bytes=os.path.getsize(p), sha256=sha(p), where=f"{where}/{r['path']} (author's machine)")
        os.remove(p)
for d in ('raw/metrics-start', 'raw/metrics-end'):
    if os.path.isdir(os.path.join(dst, d)) and not os.listdir(os.path.join(dst, d)):
        os.rmdir(os.path.join(dst, d))
json.dump(s, open(sp, 'w'), indent=2); open(sp, 'a').write('\n')
lines = open(os.path.join(dst, 'SHA256SUMS')).read().splitlines()
out = [f"{sha(sp)}  summary.json" if l.endswith('  summary.json') else l for l in lines]
assert out != lines
open(os.path.join(dst, 'SHA256SUMS'), 'w').write('\n'.join(out) + '\n')
