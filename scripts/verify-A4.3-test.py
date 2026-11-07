import importlib.util
from pathlib import Path
import unittest
import copy
import io
import json
import tempfile
from types import SimpleNamespace
from unittest.mock import patch
import contextlib
import os

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

    def test_mid_load_scale_down_is_not_accepted(self):
        samples = [self.sample(1, 1, 1, 0), self.sample(2, 5, 5, 50),
                   self.sample(3, 1, 1, 20), self.sample(4, 1, 1, 0, True)]
        with self.assertRaises(AssertionError):
            module.no_mid_load_downscale(samples)

    def test_missing_or_killed_termination_is_not_clean(self):
        pod = dict(metadata=dict(uid='pod1', name='matcher1', deletionTimestamp='2026-11-06T08:00:30Z', deletionGracePeriodSeconds=30,firstDeletionTimestamp='2026-11-06T08:00:30Z',firstDeletionGracePeriodSeconds=30),
                   spec=dict(terminationGracePeriodSeconds=30), status=dict(containerStatuses=[]))
        with self.assertRaises(AssertionError):
            module.clean_termination(pod)
        pod['status']['containerStatuses'] = [dict(name='matcher', state=dict(terminated=dict(exitCode=137, finishedAt='2026-11-06T08:00:31Z')))]
        with self.assertRaises(AssertionError):
            module.clean_termination(pod)

    def test_clean_exit_after_grace_is_rejected(self):
        pod=dict(metadata=dict(uid='p',name='matcher',deletionTimestamp='2026-11-06T08:00:30Z',firstDeletionTimestamp='2026-11-06T08:00:30Z',firstDeletionGracePeriodSeconds=30),
                 status=dict(containerStatuses=[dict(name='matcher',state=dict(terminated=dict(exitCode=0,finishedAt='2026-11-06T08:00:31Z')))]))
        with self.assertRaises(AssertionError): module.clean_termination(pod)

    def test_terminal_history_does_not_hide_unfinished_fixture_work(self):
        from urllib.parse import parse_qs, urlsplit
        active = ['PENDING', 'WAITING', 'MATCHING', 'MATCHED', 'IN_PROGRESS', 'RETRYING', 'CANCELLING', 'FULFILLING']
        def page(url, bad=None, partial=False):
            status = parse_qs(urlsplit(url).query).get('status', [None])[0]
            return dict(items=[dict(status='CANCELLED')] if status is None else ([dict(status=status)] if status == bad else []),
                        partial=partial, failedShards=[1] if partial else [],
                        positions=[dict(shard=i, hasMore=status is None) for i in range(3)])
        module.assert_no_active_requests(page, 'http://api')
        for status in active:
            with self.subTest(status=status), self.assertRaises(AssertionError):
                module.assert_no_active_requests(lambda url: page(url, bad=status), 'http://api')
        with self.assertRaises(AssertionError):
            module.assert_no_active_requests(lambda url: page(url, partial=True), 'http://api')

    def test_real_tail_can_downscale_after_bulk(self):
        samples=[dict(self.sample(1,1,1,0),phase='baseline'),dict(self.sample(2,5,5,50),phase='bulk'),
                 dict(self.sample(3,1,1,1),phase='tail')]
        module.no_mid_load_downscale(samples)


    def watched(self, finished, initial=True):
        pod=dict(metadata=dict(uid='p',name='matcher',labels={'app.kubernetes.io/component':'matcher'}),
                 spec=dict(terminationGracePeriodSeconds=30),status=dict(containerStatuses=[]))
        events=[]
        if initial:
            pod['metadata'].update(deletionTimestamp='2026-11-06T08:00:30Z',deletionGracePeriodSeconds=30)
            events.append(dict(type='MODIFIED',object=copy.deepcopy(pod)))
        pod['metadata'].update(deletionTimestamp='2026-11-06T08:00:40Z',deletionGracePeriodSeconds=0)
        pod['status']['containerStatuses']=[dict(name='matcher',state=dict(terminated=dict(exitCode=0,finishedAt=finished)))]
        events.append(dict(type='DELETED',object=pod))
        watch=module.PodWatch([],Path('.'))
        watch.process=SimpleNamespace(stdout=io.StringIO(''.join(json.dumps(e)+'\n' for e in events)))
        watch._read()
        self.assertEqual(watch.errors,[])
        return watch.deleted['p']

    def test_final_zero_grace_rewrite_cannot_hide_late_clean_exit(self):
        with self.assertRaises(AssertionError):
            module.clean_termination(self.watched('2026-11-06T08:00:35Z'))

    def test_early_clean_exit_uses_first_observed_deadline(self):
        proof=module.clean_termination(self.watched('2026-11-06T08:00:25Z'))
        self.assertEqual(proof['deadline'],'2026-11-06T08:00:30Z')

    def test_missing_positive_grace_observation_cannot_pass(self):
        with self.assertRaises(AssertionError):
            module.clean_termination(self.watched('2026-11-06T08:00:25Z',initial=False))

    def test_k6_pin_has_one_owner(self):
        import re
        text=(Path(__file__).resolve().parents[1]/'tools.mk').read_text()
        for name in ('K6_VERSION','K6_SHA256'):
            self.assertEqual(len(re.findall(r'^'+name+r'\s*:?=',text,re.M)),1,name+' is duplicated')


    def test_post_round_audit_restores_pause_and_environment_after_failure(self):
        commands=[]
        def run(command, **kwargs):
            commands.append(command)
            if 'scaledobject' in command and 'get' in command:
                return json.dumps(dict(metadata=dict(annotations={})))
            if 'deployment' in command and 'get' in command:
                return json.dumps(dict(spec=dict(replicas=0)))
            if 'get' in command:
                return json.dumps(dict(items=[]))
            return ''
        @contextlib.contextmanager
        def forward(*args):
            yield 'http://127.0.0.1:1234'
        class FailedDemo:
            def __init__(self,*args): pass
            def audit_without_admission(self): raise RuntimeError('strict audit failed')
        before=dict(os.environ)
        with tempfile.TemporaryDirectory() as directory, patch('subprocess.check_output',side_effect=run):
            with self.assertRaisesRegex(RuntimeError,'strict audit failed'):
                module.post_round_audit(['kubectl','--context','kind-reliefgrid','-n','reliefgrid'],forward,
                    'kind-reliefgrid',Path(directory),demo_type=FailedDemo)
        self.assertEqual(dict(os.environ),before)
        updates=[json.loads(c[-1]) for c in commands if 'patch' in c]
        self.assertEqual(updates[0]['metadata']['annotations']['autoscaling.keda.sh/paused-replicas'],'0')
        self.assertIsNone(updates[-1]['metadata']['annotations']['autoscaling.keda.sh/paused-replicas'])



if __name__ == '__main__':
    unittest.main()
