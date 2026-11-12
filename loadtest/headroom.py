#!/usr/bin/env python3
"""Queue headroom for the S5.2 audit.

    rabbitmqctl -q list_queues name arguments effective_policy_definition --formatter json |
        python3 -I loadtest/headroom.py limits            > raw/queue-limits.json
    python3 -I loadtest/headroom.py check --limits raw/queue-limits.json --pct 80 < sample

limits: each named queue's max-length, the lower of its x-max-length
argument and its effective policy's max-length (a queue with neither is
unbounded and left out). Exit 1 if no queue has a limit.

check: one loadtest/background.py sample on stdin. Exit 0 when every limited
queue's ready+unacked is at most pct% of its limit; otherwise exit 1 and
print one line per queue without headroom. A sample with "error", or a
limited queue missing from it, is never headroom.
"""
import argparse
import json
import sys


def arguments(raw):
    # rabbitmqctl's JSON gives arguments as [name, type, value] triples.
    if isinstance(raw, dict):
        return raw
    return {a[0]: a[-1] for a in raw if isinstance(a, list) and a}


def limits(queues):
    out = {}
    for q in queues:
        name = q.get('name', '')
        if not name or name.startswith('amq.gen-'):
            continue
        policy = q.get('effective_policy_definition') or {}
        found = [v for v in (arguments(q.get('arguments') or []).get('x-max-length'),
                             policy.get('max-length') if isinstance(policy, dict) else None)
                 if isinstance(v, int) and v > 0]
        if found:
            out[name] = min(found)
    return dict(sorted(out.items()))


def check(sample, lim, pct):
    if 'error' in sample or not isinstance(sample.get('queues'), dict):
        return ['no sample: ' + str(sample.get('error', 'no queues'))]
    over = []
    for name, limit in sorted(lim.items()):
        d = sample['queues'].get(name)
        if d is None:
            over.append(f'{name} not sampled')
            continue
        depth, allowed = d['ready'] + d['unacked'], limit * pct // 100
        if depth > allowed:
            over.append(f'{name} {depth} > {allowed} ({pct}% of {limit})')
    return over


def main():
    p = argparse.ArgumentParser()
    sub = p.add_subparsers(dest='cmd', required=True)
    sub.add_parser('limits')
    c = sub.add_parser('check')
    c.add_argument('--limits', required=True)
    c.add_argument('--pct', type=int, required=True)
    a = p.parse_args()
    if a.cmd == 'limits':
        out = limits(json.load(sys.stdin))
        if not out:
            print('headroom: no queue has a max-length', file=sys.stderr)
            return 1
        print(json.dumps(out, separators=(',', ':')))
        return 0
    if not 1 <= a.pct <= 100:
        print('headroom: --pct must be 1..100', file=sys.stderr)
        return 2
    with open(a.limits) as f:
        lim = json.load(f)
    over = check(json.loads(sys.stdin.read() or '{}'), lim, a.pct)
    for line in over:
        print(line)
    return 1 if over else 0


if __name__ == '__main__':
    sys.exit(main())
