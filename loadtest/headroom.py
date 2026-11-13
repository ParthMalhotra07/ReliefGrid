#!/usr/bin/env python3
"""Queue headroom for the S5.2 audit.

    rabbitmqctl -q list_queues name arguments effective_policy_definition --formatter json |
        python3 -I loadtest/headroom.py limits            > raw/queue-limits.json
    python3 -I loadtest/headroom.py routes --queues q.json --bindings b.json --exchanges e.json \
        > raw/queue-routes.json
    rabbitmqctl -q list_queues name messages messages_ready messages_unacknowledged --formatter json |
        python3 -I loadtest/headroom.py check --limits raw/queue-limits.json --routes raw/queue-routes.json --pct 80
    ... | python3 -I loadtest/headroom.py depths --routes raw/queue-routes.json

limits: each named queue's max-length, the lower of its x-max-length
argument and its effective policy's max-length (a queue with neither is
unbounded and left out). Exit 1 if no queue has a limit.

routes: which queue each retry tier returns its messages to. A retry tier is
a queue with a message TTL (x-message-ttl or a policy message-ttl) and a
dead-letter exchange; its expired messages are dead-lettered to that
exchange with its x-dead-letter-routing-key, and the bindings that key
matches (direct, topic or fanout; exchange-to-exchange followed) name the
queues they return to. From rabbitmqctl's list_queues (name arguments
effective_policy_definition), list_bindings (source_name source_kind
destination_name destination_kind routing_key) and list_exchanges (name
type), as JSON. Exit 1 if a tier routes nowhere.

check: one rabbitmqctl list_queues sample (name messages messages_ready
messages_unacknowledged) on stdin. A queue's depth is every message it holds
(messages: ready, unacked and any held for at-least-once dead-lettering,
which shows as messages with nothing ready) plus every message in the retry
tiers that return to it. Exit 0 when every limited queue's depth is at most
pct% of its limit; otherwise exit 1 and print one line per queue without
headroom. Counting ready+unacked alone passed the exploratory 10->160 run's
audit with 10001 expired retries parked in retry.5s behind a full
matching.requests; paused, they returned and the drain was refused. A sample
with "error", a limited queue missing from it, or a total below
ready+unacked, is never headroom.

depths: the same sample, per queue: messages, ready, unacked, returning and
depth (what check compared), for raw/headroom.json.
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


def topic_match(pattern, key):
    """AMQP topic matching: words split on '.', '*' one word, '#' zero or more."""
    def m(p, k):
        if not p:
            return not k
        if p[0] == '#':
            return any(m(p[1:], k[i:]) for i in range(len(k) + 1))
        return bool(k) and (p[0] == '*' or p[0] == k[0]) and m(p[1:], k[1:])
    return m(pattern.split('.'), key.split('.') if key else [])


def destinations(exchange, key, bindings, types, seen=()):
    if exchange in seen:
        return set()
    kind = types.get(exchange)
    out = set()
    for b in bindings:
        if b.get('source_name') != exchange:
            continue
        rk = b.get('routing_key', '')
        if kind == 'fanout' or (kind == 'topic' and topic_match(rk, key)) or (kind not in ('fanout', 'topic') and rk == key):
            if b.get('destination_kind') == 'exchange':
                out |= destinations(b['destination_name'], key, bindings, types, seen + (exchange,))
            else:
                out.add(b['destination_name'])
    return out


def routes(queues, bindings, exchanges):
    types = {e['name']: e.get('type') for e in exchanges}
    out, err = {}, []
    for q in queues:
        name = q.get('name', '')
        if not name or name.startswith('amq.gen-'):
            continue
        args = arguments(q.get('arguments') or [])
        policy = q.get('effective_policy_definition') or {}
        policy = policy if isinstance(policy, dict) else {}
        ttl = args.get('x-message-ttl', policy.get('message-ttl'))
        dlx = args.get('x-dead-letter-exchange', policy.get('dead-letter-exchange'))
        if ttl is None or not dlx:
            continue
        key = args.get('x-dead-letter-routing-key', policy.get('dead-letter-routing-key'))
        if key is None:
            err.append(f'{name}: a retry tier without a dead-letter routing key')
            continue
        to = sorted(destinations(dlx, key, bindings, types) - {name})
        if not to:
            err.append(f'{name}: dead-letters to {dlx} with {key!r}, which routes to no queue')
            continue
        out[name] = to
    return dict(sorted(out.items())), err


def table(sample):
    """name -> (messages, ready, unacked) from a list_queues sample."""
    if not isinstance(sample, list):
        return None
    out = {}
    for q in sample:
        name = q.get('name', '')
        if name and not name.startswith('amq.gen-'):
            out[name] = (q.get('messages'), q.get('messages_ready'), q.get('messages_unacknowledged'))
    return out


def count(v):
    """An exact non-negative integer count: never a bool, float, string or null."""
    return type(v) is int and v >= 0


def depth_of(t, rts):
    """Per sampled queue: its messages plus those of the tiers returning to it.
    An unsampled tier or a malformed count makes returning and depth null: an
    unknown backlog is never counted as zero."""
    back = {}
    for tier, targets in rts.items():
        for q in targets:
            back.setdefault(q, []).append(tier)
    out = {}
    for name, (m, r, u) in t.items():
        tiers = sorted(back.get(name, []))
        known = all(x in t and count(t[x][0]) for x in tiers)
        returning = sum(t[x][0] for x in tiers) if known else None
        out[name] = {'messages': m, 'ready': r, 'unacked': u, 'returning': returning,
                     'depth': m + returning if count(m) and returning is not None else None, 'tiers': tiers}
    return out


def check(sample, lim, rts, pct):
    t = table(sample)
    if t is None:
        return ['no sample: ' + str(sample.get('error', 'not a queue list') if isinstance(sample, dict) else 'not a queue list')]
    # Every queue the arithmetic uses: each limited queue, each retry tier
    # (bounded or not) and each queue a tier returns to. All must be sampled
    # with exact non-negative counts and messages >= ready+unacked before any
    # depth is computed; otherwise the check fails, never passes.
    over = []
    for name in sorted(set(lim) | {x for tg in rts.values() for x in tg} | set(rts)):
        if name not in t:
            over.append(f'{name} not sampled')
            continue
        m, r, u = t[name]
        if not (count(m) and count(r) and count(u)):
            over.append(f'{name}: counts missing or malformed (messages {m!r}, ready {r!r}, unacked {u!r})')
        elif m < r + u:
            over.append(f'{name}: messages {m} < ready+unacked {r + u}')
    if over:
        return over
    d = depth_of(t, rts)
    for name, limit in sorted(lim.items()):
        q = d[name]
        allowed = limit * pct // 100
        if q['depth'] > allowed:
            held = [x for x in q['tiers'] if t[x][0]]
            why = f"; {q['messages']} queued + {q['returning']} returning from {','.join(held)}" if q['returning'] else ''
            over.append(f"{name} {q['depth']} > {allowed} ({pct}% of {limit}{why})")
    return over


def main():
    p = argparse.ArgumentParser()
    sub = p.add_subparsers(dest='cmd', required=True)
    sub.add_parser('limits')
    rt = sub.add_parser('routes')
    for k in ('--queues', '--bindings', '--exchanges'):
        rt.add_argument(k, required=True)
    c = sub.add_parser('check')
    c.add_argument('--limits', required=True)
    c.add_argument('--routes', required=True)
    c.add_argument('--pct', type=int, required=True)
    dp = sub.add_parser('depths')
    dp.add_argument('--routes', required=True)
    a = p.parse_args()
    if a.cmd == 'routes':
        docs = []
        for f in (a.queues, a.bindings, a.exchanges):
            with open(f) as fh:
                docs.append(json.load(fh))
        out, err = routes(*docs)
        for e in err:
            print('headroom: ' + e, file=sys.stderr)
        if err:
            return 1
        print(json.dumps(out, separators=(',', ':')))
        return 0
    if a.cmd == 'depths':
        with open(a.routes) as f:
            rts = json.load(f)
        t = table(json.loads(sys.stdin.read() or 'null'))
        if t is None:
            print('headroom: not a queue list', file=sys.stderr)
            return 1
        d = {k: {f: v[f] for f in ('messages', 'ready', 'unacked', 'returning', 'depth')} for k, v in depth_of(t, rts).items()}
        print(json.dumps(dict(sorted(d.items())), separators=(',', ':')))
        return 0
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
    with open(a.routes) as f:
        rts = json.load(f)
    over = check(json.loads(sys.stdin.read() or '{}'), lim, rts, a.pct)
    for line in over:
        print(line)
    return 1 if over else 0


if __name__ == '__main__':
    sys.exit(main())
