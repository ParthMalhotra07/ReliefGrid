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


class Check(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.limits = Path(self.tmp.name) / 'queue-limits.json'
        self.limits.write_text(json.dumps({'matching.wakeups': 10000, 'retry.5s': 4000}))

    def tearDown(self):
        self.tmp.cleanup()

    def check(self, sample):
        return run('check', '--limits', str(self.limits), '--pct', '80', stdin=json.dumps(sample))

    def test_at_the_threshold_passes(self):
        r = self.check({'queues': {'matching.wakeups': {'ready': 7984, 'unacked': 16},
                                   'retry.5s': {'ready': 3200, 'unacked': 0}}})
        self.assertEqual(r.returncode, 0, r.stdout + r.stderr)

    def test_over_the_threshold_names_the_queue(self):
        r = self.check({'queues': {'matching.wakeups': {'ready': 7985, 'unacked': 16},
                                   'retry.5s': {'ready': 10, 'unacked': 0}}})
        self.assertEqual(r.returncode, 1)
        self.assertIn('matching.wakeups 8001 > 8000', r.stdout)
        self.assertNotIn('retry.5s', r.stdout)

    def test_a_missing_queue_or_a_gap_is_not_headroom(self):
        r = self.check({'queues': {'matching.wakeups': {'ready': 0, 'unacked': 0}}})
        self.assertEqual(r.returncode, 1)
        self.assertIn('retry.5s not sampled', r.stdout)
        r = self.check({'tMs': 1, 'phase': 'settle', 'error': 'broker: down'})
        self.assertEqual(r.returncode, 1)
        self.assertIn('no sample', r.stdout)


if __name__ == '__main__':
    unittest.main(verbosity=2)
