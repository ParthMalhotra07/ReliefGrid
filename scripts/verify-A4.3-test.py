import importlib.util
from pathlib import Path
import unittest

spec = importlib.util.spec_from_file_location('acceptance', Path(__file__).with_name('verify-A4.3.py'))
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)


class ScalingEvidenceTest(unittest.TestCase):
    def sample(self, n, desired, ready, depth, done=False, unacked=0):
        return dict(time=n, desired=desired, ready=ready, queueReady=depth,
                    unacked=unacked, loadFinished=done, restarts=0)

    def test_accepts_observed_ready_up_and_drained_down(self):
        module.acceptance([self.sample(1, 1, 1, 0), self.sample(2, 4, 3, 100),
                           self.sample(3, 1, 1, 0, True)])

    def test_empty_and_idle_windows_fail(self):
        for samples in [[], [self.sample(n, 1, 1, 0, True) for n in range(3)]]:
            with self.subTest(samples=samples), self.assertRaises(AssertionError):
                module.acceptance(samples)

    def test_desired_without_ready_up_fails(self):
        with self.assertRaises(AssertionError):
            module.acceptance([self.sample(1, 1, 1, 0), self.sample(2, 8, 1, 100),
                               self.sample(3, 1, 1, 0, True)])

    def test_undrained_or_restarting_final_pod_fails(self):
        for tail in [self.sample(3, 1, 1, 0, True, unacked=1),
                     dict(self.sample(3, 1, 1, 0, True), restarts=1),
                     self.sample(3, 1, 1, 0, False)]:
            with self.subTest(tail=tail), self.assertRaises(AssertionError):
                module.acceptance([self.sample(1, 1, 1, 0), self.sample(2, 4, 4, 100), tail])

    def test_missing_queue_observations_never_mean_empty(self):
        for queue in [{}, {'messages_ready': 0}, {'messages_ready': 0, 'messages_unacknowledged': None},
                      {'messages_ready': -1, 'messages_unacknowledged': 0}]:
            with self.subTest(queue=queue), self.assertRaises((AssertionError, KeyError, TypeError)):
                module.queue_counts(queue)
        self.assertEqual(module.queue_counts({'messages_ready': 2, 'messages_unacknowledged': 3}), (2, 3))


if __name__ == '__main__':
    unittest.main()
