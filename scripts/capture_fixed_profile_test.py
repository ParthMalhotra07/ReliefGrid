"""Offline regressions for a read-only final fixed-profile observation."""
import copy
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch
import subprocess

import capture_fixed_profile as capture


def fixture():
    snapshot = dict(deployments=[], replicasets=[], pods=[], hpas=[], scaledobjects=[],
                    values=dict(app=dict(enabled=True, autoscaling=dict(enabled=False),
                                         api=dict(replicas=2), matcher=dict(replicas=2)),
                                storage=dict(enabled=False)))
    for component in ('api', 'matcher'):
        name = 'reliefgrid-' + component
        labels = {'app.kubernetes.io/component': component, 'app.kubernetes.io/instance': 'reliefgrid'}
        containers = [dict(name=component, image='reliefgrid/' + component + ':acceptance')]
        meta = dict(name=name, uid=name, generation=2, labels=labels,
                    annotations={'deployment.kubernetes.io/revision': '3'})
        deploy = dict(metadata=meta, spec=dict(replicas=2, template=dict(spec=dict(containers=containers))),
                      status=dict(observedGeneration=2, replicas=2, updatedReplicas=2,
                                  readyReplicas=2, availableReplicas=2, unavailableReplicas=0))
        rs = copy.deepcopy(deploy)
        rs['metadata'].update(name=name + '-rs', uid=name + '-rs', ownerReferences=[dict(uid=name, kind='Deployment', controller=True)])
        rs['status'].pop('updatedReplicas')
        snapshot['deployments'].append(deploy)
        snapshot['replicasets'].append(rs)
        for index in range(2):
            snapshot['pods'].append(dict(
                metadata=dict(name=f'{name}-{index}', uid=f'{name}-{index}', labels=labels,
                              ownerReferences=[dict(uid=name + '-rs', kind='ReplicaSet', controller=True)]),
                spec=dict(containers=containers), status=dict(phase='Running',
                    conditions=[dict(type='Ready', status='True'), dict(type='ContainersReady', status='True')],
                    containerStatuses=[dict(name=component, ready=True, restartCount=0,
                                            imageID='containerd://sha256:' + 'a' * 64, state=dict(running=dict(startedAt='2026-11-10T00:00:00Z')))])))
    return snapshot


class ValidationTests(unittest.TestCase):
    def test_complete_fixed_observation(self):
        result = capture.validate(fixture(), 'reliefgrid')
        self.assertEqual(result['fixedReplicas'], {'api': 2, 'matcher': 2})
        self.assertEqual(len(result['pods']), 4)

    def test_rejects_incomplete_or_moving_controller_state(self):
        for field, value in [('observedGeneration', 1), ('replicas', 3), ('updatedReplicas', 1),
                             ('readyReplicas', 1), ('availableReplicas', 1), ('unavailableReplicas', 1)]:
            with self.subTest(field=field):
                data = fixture()
                data['deployments'][0]['status'][field] = value
                with self.assertRaises(capture.InvalidObservation):
                    capture.validate(data, 'reliefgrid')

    def test_fail_closed_on_values_controllers_ownership_images_and_readiness(self):
        changes = [
            lambda d: d['values']['storage'].update(enabled=True),
            lambda d: d['values']['app']['autoscaling'].update(enabled=True),
            lambda d: d['values']['app']['api'].update(replicas=1),
            lambda d: d['hpas'].append(dict(spec=dict(scaleTargetRef=dict(kind='Deployment', name='reliefgrid-api')))),
            lambda d: d['scaledobjects'].append(dict(spec=dict(scaleTargetRef=dict(name='reliefgrid-matcher')))),
            lambda d: d['pods'][0]['metadata'].update(deletionTimestamp='now'),
            lambda d: d['pods'][0]['metadata']['ownerReferences'][0].update(uid='old-rs'),
            lambda d: d['pods'][0]['status']['containerStatuses'][0].update(imageID=''),
            lambda d: d['pods'][0]['status']['containerStatuses'][0].update(imageID='mutable:tag'),
            lambda d: d['pods'][0]['status']['containerStatuses'][0].update(imageID='containerd://sha256:' + 'b' * 64),
            lambda d: d['pods'][0]['status']['containerStatuses'][0].update(ready=False),
            lambda d: d['pods'][0]['status']['conditions'][0].update(status='False'),
            lambda d: d['pods'][0]['spec'].update(containers=[dict(name='api', image='wrong:image')]),
            lambda d: d['replicasets'][0]['metadata']['annotations'].update({'deployment.kubernetes.io/revision': '2'}),
            lambda d: d['replicasets'][0]['status'].update(readyReplicas=1),
            lambda d: d['pods'].append(copy.deepcopy(d['pods'][0])),
            lambda d: d['deployments'].pop(),
        ]
        for index, change in enumerate(changes):
            with self.subTest(index=index):
                data = fixture()
                change(data)
                with self.assertRaises(capture.InvalidObservation):
                    capture.validate(data, 'reliefgrid')

    def test_unrelated_autoscaler_is_allowed(self):
        data = fixture()
        data['hpas'] = [dict(spec=dict(scaleTargetRef=dict(kind='Deployment', name='other-app')))]
        capture.validate(data, 'reliefgrid')

    def test_projection_omits_credentials_and_arbitrary_annotations(self):
        data = fixture()
        for pod in data['pods']:
            pod['spec']['containers'][0]['env'] = [dict(name='TOKEN', value='PRIVATE-MARKER')]
            pod['metadata']['annotations'] = {'secret': 'PRIVATE-MARKER'}
            pod['status']['message'] = 'PRIVATE-MARKER'
        data['values']['credentials'] = 'PRIVATE-MARKER'
        projected = capture.sanitize(data)
        self.assertNotIn('PRIVATE-MARKER', json.dumps(projected))
        capture.validate(projected, 'reliefgrid')

    def test_capture_reads_twice_and_never_mutates_cluster(self):
        calls = []
        def read(command):
            calls.append(command)
            if command[0] == 'helm':
                return fixture()['values']
            resource = command[command.index('get') + 1]
            mapping = {'deployments': 'deployments', 'replicasets': 'replicasets', 'pods': 'pods',
                       'hpa': 'hpas', 'scaledobjects.keda.sh': 'scaledobjects'}
            return dict(items=fixture()[mapping[resource]])
        first = capture.collect('kind-reliefgrid', 'reliefgrid', 'reliefgrid', read)
        second = capture.collect('kind-reliefgrid', 'reliefgrid', 'reliefgrid', read)
        capture.stable(first, second, 'reliefgrid')
        self.assertEqual(len(calls), 12)
        self.assertTrue(all('get' in command for command in calls))
        self.assertTrue(all(any('--kube-context=' in x or '--context=' in x for x in command) for command in calls))
        changed = copy.deepcopy(second)
        changed['pods'][0]['metadata']['uid'] = 'replaced'
        with self.assertRaises(capture.InvalidObservation):
            capture.stable(first, changed, 'reliefgrid')

    def test_command_failure_cannot_be_an_empty_controller_list(self):
        def fail(_):
            raise capture.InvalidObservation('command failed')
        with self.assertRaises(capture.InvalidObservation):
            capture.collect('kind-reliefgrid', 'reliefgrid', 'reliefgrid', fail)

    def test_tools_snapshot_requires_completed_owned_job_and_digest(self):
        job = dict(metadata=dict(uid='job-uid'), status=dict(succeeded=1))
        pod = copy.deepcopy(fixture()['pods'][0])
        pod['metadata']['ownerReferences'] = [dict(kind='Job', uid='job-uid', controller=True)]
        pod['status']['phase'] = 'Succeeded'
        status = pod['status']['containerStatuses'][0]
        status.update(state=dict(terminated=dict(exitCode=0)))
        result = capture.tools_observation(job, [pod])
        self.assertTrue(result['containers'][0]['imageID'])
        self.assertEqual(result['job']['metadata']['uid'], 'job-uid')
        self.assertEqual(result['job']['status']['succeeded'], 1)
        self.assertEqual(result['pod']['status']['containerStatuses'][0]['state']['terminated']['exitCode'], 0)
        for status in (dict(succeeded=0), dict(succeeded=1, failed=1), dict(succeeded=1, active=1)):
            with self.assertRaises(capture.InvalidObservation):
                capture.tools_observation(dict(metadata=dict(uid='job-uid'), status=status), [pod])
        for change in (lambda p: p['metadata']['ownerReferences'][0].update(uid='wrong'),
                       lambda p: p['status']['containerStatuses'][0].update(imageID=''),
                       lambda p: p['status']['containerStatuses'][0]['state']['terminated'].update(exitCode=1)):
            invalid = copy.deepcopy(pod)
            change(invalid)
            with self.assertRaises(capture.InvalidObservation):
                capture.tools_observation(job, [invalid])

    def test_output_refuses_existing_artifact_directory(self):
        with tempfile.TemporaryDirectory() as tmp:
            with self.assertRaises(FileExistsError):
                capture.new_output(Path(tmp))

    def test_command_errors_and_timeout_do_not_expose_stderr(self):
        for error in (subprocess.CalledProcessError(1, ['kubectl'], stderr='PRIVATE-MARKER'),
                      subprocess.TimeoutExpired(['kubectl'], 25, stderr='PRIVATE-MARKER')):
            with patch.object(capture.subprocess, 'run', side_effect=error):
                with self.assertRaises(capture.InvalidObservation) as raised:
                    capture.command_json(['kubectl', 'get', 'pods'])
                self.assertNotIn('PRIVATE-MARKER', str(raised.exception))

    def test_cli_persists_only_sanitized_observation_and_fails_on_source_change(self):
        job = dict(metadata=dict(uid='job-uid'), status=dict(succeeded=1))
        pod = copy.deepcopy(fixture()['pods'][0])
        pod['metadata']['ownerReferences'] = [dict(kind='Job', uid='job-uid', controller=True)]
        pod['status']['phase'] = 'Succeeded'
        pod['status']['containerStatuses'][0]['state'] = dict(terminated=dict(exitCode=0))
        pod['spec']['containers'][0]['env'] = [dict(name='TOKEN', value='PRIVATE-MARKER')]
        pod['metadata']['annotations'] = {'secret': 'PRIVATE-MARKER'}
        source = dict(collectorCommit='a' * 40, collectorTree='b' * 40, collectorDirty=False,
                      runtimeBuildBinding='NOT_ESTABLISHED')
        for changed in (False, True):
            with self.subTest(source_changed=changed), tempfile.TemporaryDirectory() as tmp:
                output = Path(tmp) / 'capture'
                args = ['capture', '--context=kind-reliefgrid', '--namespace=reliefgrid',
                        '--release=reliefgrid', '--tools-job=audit', '--output=' + str(output)]
                second_source = dict(source, collectorDirty=True) if changed else source
                with patch.object(capture.sys, 'argv', args), \
                     patch.object(capture, 'source_observation', side_effect=[source, second_source]), \
                     patch.object(capture, 'collect', side_effect=[capture.sanitize(fixture()), capture.sanitize(fixture())]) as collect, \
                     patch.object(capture, 'command_json', side_effect=[job, dict(items=[pod])]):
                    self.assertEqual(capture.main(), int(changed))
                    self.assertEqual(collect.call_count, 2)
                saved = json.loads((output / 'fixed-profile.json').read_text())
                self.assertNotIn('PRIVATE-MARKER', json.dumps(saved))
                self.assertIs(saved['ok'], not changed)
                self.assertEqual(saved['source']['runtimeBuildBinding'], 'NOT_ESTABLISHED')
                self.assertEqual(len(saved['tools']['containers']), 1)
                self.assertEqual(saved['tools']['pod']['status']['containerStatuses'][0]['state']['terminated']['exitCode'], 0)


    def test_profile_rejection_guards_identify_the_actual_failure(self):
        def obsolete_rs(data):
            rs = copy.deepcopy(data['replicasets'][0])
            rs['metadata'].update(name='api-obsolete', uid='obsolete-rs')
            rs['metadata']['annotations']['deployment.kubernetes.io/revision'] = '2'
            rs['spec']['replicas'] = rs['status']['replicas'] = 1
            data['replicasets'].append(rs)
        cases = [
            ('deleting deployment', lambda d: d['deployments'][0]['metadata'].update(deletionTimestamp='now'), 'controller identity missing or deleting'),
            ('missing controller uid', lambda d: d['deployments'][0]['metadata'].pop('uid'), 'controller identity missing or deleting'),
            ('bad generation', lambda d: d['deployments'][0]['metadata'].update(generation='2'), 'controller generation is not observed'),
            ('disabled app', lambda d: d['values']['app'].update(enabled=False), 'fixed app profile is not configured'),
            ('extra deployment', lambda d: d['deployments'].append(dict(metadata=dict(name='extra'), spec={}, status={})), 'expected exactly two selected Deployments'),
            ('ambiguous deployment', lambda d: d['deployments'][1]['metadata']['labels'].update({'app.kubernetes.io/component': 'api'}), 'missing or ambiguous app Deployment'),
            ('inactive current rs', lambda d: (d['replicasets'][0]['spec'].update(replicas=0), d['replicasets'][0]['status'].update(replicas=0)), 'expected one active ReplicaSet'),
            ('scaled obsolete rs', obsolete_rs, 'expected one active ReplicaSet'),
            ('rs images', lambda d: d['replicasets'][0]['spec']['template']['spec'].update(containers=[dict(name='api', image='obsolete')]), 'ReplicaSet images differ'),
            ('missing app pod', lambda d: d['pods'].pop(0), 'expected exactly two app pods'),
            ('missing pod uid', lambda d: d['pods'][0]['metadata'].pop('uid'), 'pod missing, duplicated or terminating'),
            ('duplicate pod uid', lambda d: d['pods'][0]['metadata'].update(uid='reliefgrid-api-1'), 'pod missing, duplicated or terminating'),
            ('wrong pod release', lambda d: d['pods'][0]['metadata'].update(labels={'app.kubernetes.io/component': 'api', 'app.kubernetes.io/instance': 'other'}), 'pod is not owned by current app ReplicaSet'),
            ('pod phase', lambda d: d['pods'][0]['status'].update(phase='Pending'), 'pod is not running and Ready'),
            ('containers ready', lambda d: d['pods'][0]['status']['conditions'][1].update(status='False'), 'pod is not running and Ready'),
            ('missing statuses', lambda d: d['pods'][0]['status'].update(containerStatuses=[]), 'container status is incomplete'),
            ('status names', lambda d: d['pods'][0]['status']['containerStatuses'][0].update(name='wrong'), 'container status is incomplete'),
            ('not running', lambda d: d['pods'][0]['status']['containerStatuses'][0].update(state=dict(waiting={})), 'container is unready or lacks observed image identity'),
            ('extra selected pod', lambda d: d['pods'].append(dict(metadata=dict(uid='other', labels={'app.kubernetes.io/component': 'relay'}), spec={}, status={})), 'unexpected selected app pods'),
        ]
        for name, change, message in cases:
            with self.subTest(name=name):
                data = fixture()
                change(data)
                with self.assertRaisesRegex(capture.InvalidObservation, message):
                    capture.validate(data, 'reliefgrid')

    def test_controller_rejection_guards_apply_to_replica_sets_too(self):
        for field, value, message in [('observedGeneration', 1, 'generation is not observed'),
                                      ('replicas', 1, 'two available replicas'),
                                      ('readyReplicas', 1, 'two available replicas'),
                                      ('availableReplicas', 1, 'two available replicas'),
                                      ('unavailableReplicas', 1, 'unavailable replicas')]:
            data = fixture()
            data['replicasets'][0]['status'][field] = value
            with self.subTest(field=field), self.assertRaisesRegex(capture.InvalidObservation, message):
                capture.validate(data, 'reliefgrid')
        data = fixture()
        data['replicasets'][0]['metadata']['deletionTimestamp'] = 'now'
        with self.assertRaisesRegex(capture.InvalidObservation, 'controller identity missing or deleting'):
            capture.validate(data, 'reliefgrid')
        for group in ('deployments', 'replicasets'):
            data = fixture()
            data[group][0]['spec']['replicas'] = 3
            with self.subTest(group=group), self.assertRaisesRegex(capture.InvalidObservation, 'two available replicas'):
                capture.validate(data, 'reliefgrid')

    def test_invalid_names_fail_before_source_read_or_capture_creation(self):
        for flag in ('namespace', 'release', 'tools-job'):
            with tempfile.TemporaryDirectory() as temporary:
                output = Path(temporary) / 'capture'
                values = dict(namespace='reliefgrid', release='reliefgrid', **{'tools-job': 'audit'})
                values[flag] = 'INVALID'
                argv = ['capture', '--context=kind-reliefgrid', '--output=' + str(output)] + ['--' + key + '=' + value for key, value in values.items()]
                with patch.object(capture.sys, 'argv', argv), patch.object(capture, 'source_observation', side_effect=AssertionError('source must not be read')) as source:
                    with self.assertRaisesRegex(capture.InvalidObservation, 'invalid Kubernetes name'):
                        capture.main()
                    source.assert_not_called()
                self.assertFalse(output.exists())

    def test_tools_rejection_guards_identify_the_actual_failure(self):
        job = dict(metadata=dict(uid='job-uid'), status=dict(succeeded=1))
        pod = copy.deepcopy(fixture()['pods'][0])
        pod['metadata']['ownerReferences'] = [dict(kind='Job', uid='job-uid', controller=True)]
        pod['status']['phase'] = 'Succeeded'
        pod['status']['containerStatuses'][0]['state'] = dict(terminated=dict(exitCode=0))
        cases = [
            ('missing job uid', lambda j, p: j['metadata'].pop('uid'), 'tools Job did not complete'),
            ('deleting job', lambda j, p: j['metadata'].update(deletionTimestamp='now'), 'failed/active attempts or is deleting'),
            ('no pods', lambda j, p: p.clear(), 'exactly one retained pod'),
            ('multiple pods', lambda j, p: p.append(copy.deepcopy(p[0])), 'exactly one retained pod'),
            ('deleting pod', lambda j, p: p[0]['metadata'].update(deletionTimestamp='now'), 'not owned by retained Job'),
            ('pod phase', lambda j, p: p[0]['status'].update(phase='Running'), 'tools container observation is incomplete'),
            ('no statuses', lambda j, p: p[0]['status'].update(containerStatuses=[]), 'tools container observation is incomplete'),
            ('extra status', lambda j, p: p[0]['status']['containerStatuses'].append(copy.deepcopy(p[0]['status']['containerStatuses'][0])), 'tools container observation is incomplete'),
            ('wrong status name', lambda j, p: p[0]['status']['containerStatuses'][0].update(name='wrong'), 'tools container names do not match'),
        ]
        for name, change, message in cases:
            j, pods = copy.deepcopy(job), [copy.deepcopy(pod)]
            change(j, pods)
            with self.subTest(name=name), self.assertRaisesRegex(capture.InvalidObservation, message):
                capture.tools_observation(j, pods)

    def test_malformed_shapes_persist_sanitized_fail_artifacts(self):
        for field in ('app', 'storage', 'pod-status', 'tools-status'):
            with self.subTest(field=field), tempfile.TemporaryDirectory() as temporary:
                output = Path(temporary) / 'capture'
                args = ['capture', '--context=kind-reliefgrid', '--namespace=reliefgrid',
                        '--release=reliefgrid', '--tools-job=audit', '--output=' + str(output)]
                malformed = fixture()
                if field in ('app', 'storage'): malformed['values'][field] = None
                if field == 'pod-status': malformed['pods'][0]['status'] = None
                source = dict(collectorCommit='a' * 40, collectorTree='b' * 40, collectorDirty=False)
                job = dict(metadata=dict(uid='job-uid'), status=None)
                def read_profile(*_):
                    return capture.sanitize(malformed)
                with patch.object(capture.sys, 'argv', args), patch.object(capture, 'source_observation', return_value=source), \
                     patch.object(capture, 'collect', side_effect=read_profile), patch.object(capture, 'command_json', side_effect=[job, dict(items=[])]):
                    self.assertEqual(capture.main(), 1)
                result = json.loads((output / 'fixed-profile.json').read_bytes())
                self.assertIs(result['ok'], False)
                self.assertEqual(result['error'], 'malformed observation')


if __name__ == '__main__':
    unittest.main()
