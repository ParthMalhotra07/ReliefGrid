#!/usr/bin/env python3
"""loadtest/headroom.py: queue limits from the broker and the headroom check."""
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

ROOT = Path(__file__).resolve().parents[1]
HEADROOM = ROOT / 'loadtest' / 'headroom.py'

# rabbitmqctl -q list_queues name arguments effective_policy_definition --formatter json
QUEUES = [
    {"name": "matching.wakeups", "arguments": [["x-max-length", "long", 10000], ["x-queue-type", "longstr", "quorum"]],
     "effective_policy_definition": {"max-length": 10000, "overflow": "reject-publish"}},
    {"name": "matching.requests", "arguments": [["x-max-length", "long", 10000]],
     "effective_policy_definition": {"max-length": 6000}},
    {"name": "retry.5s", "arguments": [], "effective_policy_definition": {"max-length": 4000}},
    {"name": "matching.dead", "arguments": [["x-max-length", "long", 10000]], "effective_policy_definition": []},
    {"name": "amq.gen-Ro6HeA9eTGeHCQapN54Log", "arguments": [], "effective_policy_definition": []},
]


def run(*args, stdin):
    return subprocess.run([sys.executable, '-I', str(HEADROOM), *args], input=stdin,
                          capture_output=True, text=True, timeout=10)


class Limits(unittest.TestCase):
    def test_the_lower_of_argument_and_policy(self):
        r = run('limits', stdin=json.dumps(QUEUES))
        self.assertEqual(r.returncode, 0, r.stderr)
        self.assertEqual(json.loads(r.stdout), {'matching.dead': 10000, 'matching.requests': 6000,
                                                'matching.wakeups': 10000, 'retry.5s': 4000})

    def test_no_limited_queue_is_an_error(self):
        r = run('limits', stdin=json.dumps(QUEUES[-1:]))
        self.assertEqual(r.returncode, 1)


# The deployed topology (deploy/helm/reliefgrid/files/rabbitmq-definitions.json)
# in rabbitmqctl's JSON shapes:
#   list_queues name arguments effective_policy_definition --formatter json
#   list_bindings source_name source_kind destination_name destination_kind routing_key --formatter json
#   list_exchanges name type --formatter json
DEFS = json.loads((ROOT / 'deploy' / 'helm' / 'reliefgrid' / 'files' / 'rabbitmq-definitions.json').read_text())
TOPOLOGY = {
    'queues': [{'name': q['name'], 'arguments': [[k, 'x', v] for k, v in q.get('arguments', {}).items()],
                'effective_policy_definition': []} for q in DEFS['queues']],
    'bindings': [{'source_name': b['source'], 'source_kind': 'exchange', 'destination_name': b['destination'],
                  'destination_kind': b['destination_type'], 'routing_key': b['routing_key']} for b in DEFS['bindings']],
    'exchanges': [{'name': e['name'], 'type': e['type']} for e in DEFS['exchanges']],
}
RETURNS = {'retry.120s': ['matching.requests'], 'retry.30s': ['matching.requests'], 'retry.5s': ['matching.requests'],
           'wakeup.retry.5s': ['matching.wakeups']}
LIMITS = {q: 10000 for q in ('matching.dead', 'matching.requests', 'matching.wakeups', 'retry.120s', 'retry.30s',
                             'retry.5s', 'wakeup.retry.5s')}


def depths(**queues):
    """rabbitmqctl -q list_queues name messages messages_ready messages_unacknowledged --formatter json"""
    return [{'name': n, 'messages': m, 'messages_ready': r, 'messages_unacknowledged': u} for n, (m, r, u) in queues.items()]


def quiet(**over):
    q = {n: (0, 0, 0) for n in LIMITS}
    q.update(over)
    return depths(**q)


class Routes(unittest.TestCase):
    def files(self, topology):
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        out = []
        for k in ('queues', 'bindings', 'exchanges'):
            f = Path(tmp.name) / f'{k}.json'
            f.write_text(json.dumps(topology[k]))
            out += [f'--{k}', str(f)]
        return out

    def test_each_retry_tier_returns_to_the_queue_it_retries(self):
        # A delay tier (a queue with a message TTL) dead-letters its messages
        # back through an exchange; the binding that routing key matches names
        # the queue they return to. matching.requests and matching.wakeups
        # dead-letter only rejected messages (no TTL): not a retry tier.
        r = run('routes', *self.files(TOPOLOGY), stdin='')
        self.assertEqual(r.returncode, 0, r.stderr)
        self.assertEqual(json.loads(r.stdout), RETURNS)

    def test_a_tier_that_routes_nowhere_is_an_error(self):
        t = dict(TOPOLOGY, bindings=[b for b in TOPOLOGY['bindings'] if b['routing_key'] != 'request.retry'])
        r = run('routes', *self.files(t), stdin='')
        self.assertEqual(r.returncode, 1)
        self.assertIn('retry.5s', r.stderr)


class Check(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        d = Path(self.tmp.name)
        self.limits, self.routes = d / 'queue-limits.json', d / 'queue-routes.json'
        self.limits.write_text(json.dumps(LIMITS))
        self.routes.write_text(json.dumps(RETURNS))

    def tearDown(self):
        self.tmp.cleanup()

    def check(self, sample):
        return run('check', '--limits', str(self.limits), '--routes', str(self.routes), '--pct', '80',
                   stdin=json.dumps(sample))

    def test_quiet_broker_passes(self):
        r = self.check(quiet(**{'matching.requests': (120, 104, 16), 'retry.30s': (40, 3, 0)}))
        self.assertEqual(r.returncode, 0, r.stdout + r.stderr)

    def test_exploratory_parked_retries_are_refused(self):
        # S5.2 exploratory 10->160 run (2026-11-12), at the audit's pause:
        # matching.requests 7820 ready + 16 unacked, so ready+unacked passed
        # 80% of 10000. But retry.5s held 10001 messages with 0 ready: expired
        # retries parked by at-least-once dead-lettering behind the full
        # matching.requests. Paused, they flowed back and the drain's
        # publishes were refused (100 nacks). Every message counts, and the
        # tiers' messages count toward the queue they return to.
        sample = quiet(**{'matching.requests': (7836, 7820, 16), 'matching.wakeups': (2298, 2282, 16),
                          'retry.5s': (10001, 0, 0), 'retry.30s': (2378, 126, 0), 'retry.120s': (2823, 226, 0)})
        r = self.check(sample)
        self.assertEqual(r.returncode, 1, r.stdout)
        self.assertIn('retry.5s 10001 > 8000', r.stdout)
        # 7836 queued + 10001 + 2378 + 2823 returning from the retry tiers
        self.assertIn('matching.requests 23038 > 8000 (80% of 10000; 7836 queued + 15202 returning from retry.120s,retry.30s,retry.5s)', r.stdout)
        self.assertNotIn('matching.wakeups', r.stdout)

    def test_returning_retries_alone_can_exhaust_the_target(self):
        r = self.check(quiet(**{'matching.requests': (5000, 5000, 0), 'retry.30s': (3001, 0, 0)}))
        self.assertEqual(r.returncode, 1)
        self.assertIn('matching.requests 8001 > 8000 (80% of 10000; 5000 queued + 3001 returning from retry.30s)', r.stdout)
        self.assertNotIn('\nretry.30s', '\n' + r.stdout)

    def test_at_the_threshold_passes(self):
        r = self.check(quiet(**{'matching.wakeups': (7000, 6984, 16), 'wakeup.retry.5s': (1000, 0, 0)}))
        self.assertEqual(r.returncode, 0, r.stdout + r.stderr)
        r = self.check(quiet(**{'matching.wakeups': (7001, 6985, 16), 'wakeup.retry.5s': (1000, 0, 0)}))
        self.assertEqual(r.returncode, 1)
        self.assertIn('matching.wakeups 8001 > 8000', r.stdout)

    def test_messages_never_below_ready_plus_unacked(self):
        # A total below ready+unacked is a torn sample: never headroom.
        r = self.check(quiet(**{'matching.requests': (10, 20, 0)}))
        self.assertEqual(r.returncode, 1)
        self.assertIn('matching.requests: messages 10 < ready+unacked 20', r.stdout)

    def test_a_missing_queue_or_a_gap_is_not_headroom(self):
        r = self.check([q for q in quiet() if q['name'] != 'retry.5s'])
        self.assertEqual(r.returncode, 1)
        self.assertIn('retry.5s not sampled', r.stdout)
        r = self.check({'error': 'rabbitmqctl: timeout'})
        self.assertEqual(r.returncode, 1)
        self.assertIn('no sample', r.stdout)

    def test_depths_records_what_was_checked(self):
        sample = quiet(**{'matching.requests': (7836, 7820, 16), 'retry.5s': (10001, 0, 0)})
        r = run('depths', '--routes', str(self.routes), stdin=json.dumps(sample))
        self.assertEqual(r.returncode, 0, r.stderr)
        d = json.loads(r.stdout)
        self.assertEqual(d['matching.requests'], {'messages': 7836, 'ready': 7820, 'unacked': 16,
                                                  'returning': 10001, 'depth': 17837})
        self.assertEqual(d['retry.5s'], {'messages': 10001, 'ready': 0, 'unacked': 0, 'returning': 0, 'depth': 10001})



MISSING = object()
# Every malformed count a sample can carry: absent, null, negative, a float,
# a boolean (an int subclass in Python), a numeric string and a word.
MALFORMED = [MISSING, None, -1000, 1.5, True, False, '12', 'many']


class UnboundedTier(unittest.TestCase):
    """#170 review: a retry tier with no max-length of its own is not in the
    limits, but its messages still return to a limited queue. Its counts must
    be exact non-negative integers like any other; an unknown backlog is never
    counted as zero."""

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        d = Path(self.tmp.name)
        self.limits, self.routes = d / 'queue-limits.json', d / 'queue-routes.json'
        self.limits.write_text(json.dumps({'matching.requests': 10000}))
        self.routes.write_text(json.dumps({'retry.unbounded': ['matching.requests']}))

    def tearDown(self):
        self.tmp.cleanup()

    @staticmethod
    def row(name, counts):
        out = {'name': name}
        for k, v in zip(('messages', 'messages_ready', 'messages_unacknowledged'), counts):
            if v is not MISSING:
                out[k] = v
        return out

    def check(self, tier, requests=(7999, 7999, 0)):
        sample = [self.row('matching.requests', requests), self.row('retry.unbounded', tier)]
        return run('check', '--limits', str(self.limits), '--routes', str(self.routes), '--pct', '80',
                   stdin=json.dumps(sample))

    def test_valid_unbounded_tier_counts_toward_its_target(self):
        r = self.check((1, 0, 0))
        self.assertEqual(r.returncode, 0, r.stdout + r.stderr)
        r = self.check((2, 0, 0))
        self.assertEqual(r.returncode, 1, r.stdout)
        self.assertIn('matching.requests 8001 > 8000 (80% of 10000; 7999 queued + 2 returning from retry.unbounded)', r.stdout)

    def test_malformed_unbounded_tier_total_fails(self):
        for v in MALFORMED:
            with self.subTest(messages=v):
                r = self.check((v, 0, 0))
                self.assertEqual(r.returncode, 1, r.stdout + r.stderr)
                self.assertIn('retry.unbounded: counts missing or malformed', r.stdout)

    def test_malformed_unbounded_tier_ready_or_unacked_fails(self):
        for v in MALFORMED:
            for tier in ((0, v, 0), (0, 0, v)):
                with self.subTest(tier=tier):
                    r = self.check(tier)
                    self.assertEqual(r.returncode, 1, r.stdout + r.stderr)
                    self.assertIn('retry.unbounded: counts missing or malformed', r.stdout)

    def test_torn_unbounded_tier_fails(self):
        r = self.check((0, 5, 0))
        self.assertEqual(r.returncode, 1)
        self.assertIn('retry.unbounded: messages 0 < ready+unacked 5', r.stdout)

    def test_malformed_limited_queue_fails(self):
        for v in MALFORMED:
            with self.subTest(messages=v):
                r = self.check((0, 0, 0), requests=(v, 0, 0))
                self.assertEqual(r.returncode, 1, r.stdout + r.stderr)
                self.assertIn('matching.requests: counts missing or malformed', r.stdout)

    def test_depths_never_count_an_unknown_tier_as_zero(self):
        for v in MALFORMED:
            with self.subTest(messages=v):
                row = {'name': 'retry.unbounded', 'messages_ready': 0, 'messages_unacknowledged': 0}
                if v is not MISSING:
                    row['messages'] = v
                r = run('depths', '--routes', str(self.routes), stdin=json.dumps(depths(**{'matching.requests': (7999, 7999, 0)}) + [row]))
                self.assertEqual(r.returncode, 0, r.stderr)
                d = json.loads(r.stdout)['matching.requests']
                self.assertIsNone(d['returning'])
                self.assertIsNone(d['depth'])

if __name__ == '__main__':
    unittest.main(verbosity=2)
