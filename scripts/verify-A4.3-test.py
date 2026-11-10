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




class OrchestrationOrderTest(unittest.TestCase):
    def exercise(self, failure=None):
        """Run actual main; fake only external I/O, measured verdicts and time waits."""
        import base64
        trace=[]
        state=SimpleNamespace(polls=0,resources=0)
        class Process:
            def __init__(self, command, **kwargs):
                self.load='run' in command
                if self.load:
                    trace.append('load-start')
                    for i in range(32):
                        for prefix,value in [('A43_CREATED ',dict(requestId='R'+str(i),initialVersion='v1')),
                                             ('A43_HANDOFF ',dict(requestId='R'+str(i),version='v2',status='MATCHED',reciprocalVerified=True))]:
                            kwargs['stdout'].write(json.dumps(dict(msg=prefix+json.dumps(value)))+'\n')
                    kwargs['stdout'].flush()
                else:
                    kwargs['stdout'].write('Forwarding from 127.0.0.1:12345 -> 80\n')
                    kwargs['stdout'].flush()
            def poll(self):
                if not self.load:return None
                state.polls+=1
                return None if state.polls==1 else 0
            def wait(self, **kwargs):
                if self.load:trace.append('load-finished')
                return 0
            def terminate(self):pass
        class Watch:
            def __init__(self,*args):pass
            def start(self):pass
            def stop(self):trace.append('watch-stop')
            def finish(self,uids):
                trace.append('termination-checks')
                delta=500 if failure=='termination' else 100
                at=(module.dt.datetime.now(module.dt.timezone.utc)+module.dt.timedelta(seconds=delta)).isoformat()
                return [dict(component=c,exits=[dict(finishedAt=at)]) for c in ('api','matcher')]
        def command(args,**kwargs):
            if args[0]=='git':return 'synthetic-test-head' if 'rev-parse' in args else ''
            if args[0]=='make':return 'v2.3.0'
            if 'secret' in args:return json.dumps(dict(data={'amqp-url':base64.b64encode(b'amqp://test:test@broker/').decode()}))
            if 'scaledobject' in args:return json.dumps(dict(metadata=dict(uid='scaled')))
            if 'hpa' in args:return json.dumps(dict(items=[dict(spec=dict(scaleTargetRef=dict(name='reliefgrid-'+c)),metadata=dict(ownerReferences=[dict(uid='scaled',controller=True)])) for c in ('api','matcher')]))
            if 'deploy' in args:
                up=state.polls==1
                return json.dumps(dict(items=[dict(metadata=dict(name='reliefgrid-'+c),spec=dict(replicas=n),status=dict(readyReplicas=n)) for c,n in [('api',3 if up else 2),('matcher',3 if up else 1)]]))
            if 'pods' in args:return json.dumps(dict(items=[dict(metadata=dict(uid=c),status=dict(containerStatuses=[dict(restartCount=0)])) for c in ('api','matcher')]))
            raise AssertionError('unexpected external command '+repr(args))
        class Response(io.StringIO):
            def __enter__(self):return self
            def __exit__(self,*args):self.close()
        def urlopen(request,**kwargs):
            path=request.full_url
            if '/api/queues/' in path:
                result=dict(messages_ready=5 if 'matching.requests' in path and state.polls==1 else 0,messages_unacknowledged=0)
            elif '?' in path:result=dict(items=[],partial=False,failedShards=[],positions=[dict(shard=i,hasMore=False) for i in range(3)])
            elif '/resources' in path and request.method=='POST':
                state.resources+=1;result=dict(resourceId='Res'+str(state.resources))
            elif '/resources/' in path:result=dict(status='OFFLINE',activeClaim=None)
            elif request.method=='POST':result=dict(requestId='read-fixture')
            else:result=dict(status='CANCELLED',assignedResourceId=None)
            return Response(json.dumps(result))
        def verdict(samples):
            trace.append('scaling-verdict')
            if failure=='scaling':raise AssertionError('invalid measured round')
        def audit(*args):
            trace.append('audit')
            self.assertIn('load-finished',trace[:-1], 'audit ran before load completed')
            self.assertIn('scaling-verdict',trace[:-1], 'audit ran before measured scaling verdict')
            self.assertIn('termination-checks',trace[:-1], 'audit ran before termination checks')
            return dict(ok=True,verdict='clean',report=dict(quiescent=True))
        with tempfile.TemporaryDirectory() as directory, contextlib.ExitStack() as stack:
            stack.enter_context(patch.dict(os.environ,{'A43_EVIDENCE_DIR':str(Path(directory)/'run')}))
            for name,value in [('subprocess.check_output',command),('subprocess.Popen',Process),('urllib.request.urlopen',urlopen),
                               ('time.sleep',lambda _:None),('os.chdir',lambda _:None),('PodWatch',Watch),
                               ('acceptance',verdict),('no_mid_load_downscale',lambda _:None),('post_round_audit',audit)]:
                stack.enter_context(patch.object(module,name,value) if '.' not in name else patch(name,new=value))
            stack.enter_context(contextlib.redirect_stdout(io.StringIO()))
            if failure:
                with self.assertRaises(AssertionError):module.main()
                self.assertNotIn('audit',trace,'audit ran before a failed measured verdict')
            else:
                module.main()
                self.assertEqual(trace.count('audit'),1)
                for prerequisite in ('load-finished','scaling-verdict','termination-checks'):
                    self.assertLess(trace.index(prerequisite),trace.index('audit'))
        return trace
    def test_audit_once_after_load_and_measured_termination_checks(self):
        self.exercise()
    def test_invalid_measured_round_cannot_start_audit(self):
        self.exercise('scaling')
    def test_invalid_measured_termination_cannot_start_audit(self):
        self.exercise('termination')

if __name__ == '__main__':
    unittest.main()
