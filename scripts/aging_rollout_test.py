import copy
import hashlib
import json
import unittest
from pathlib import Path
from types import SimpleNamespace
import tempfile
from unittest.mock import patch
import sys

import aging_rollout as aging


def fixture():
    source = dict(commit='a' * 40, tree='b' * 40, dirty=False)
    records = []
    for role in ('coordinator', 'storagenode'):
        records.append(dict(schemaVersion=1, kind='reliefgrid-build-provenance', ok=True, source=source,
            contextArchiveSHA256='c' * 64, images=[dict(component=role, image='reliefgrid/' + role + ':fixture',
            imageId='sha256:' + ('d' if role == 'coordinator' else 'e') * 64, repoDigests=[],
            labels={'org.opencontainers.image.revision': source['commit'], 'io.reliefgrid.source-tree': source['tree'],
                    'io.reliefgrid.context-sha256': 'c' * 64})]))
    approval = dict(schemaVersion=1, sourceCommit=source['commit'], namespace='reliefgrid', context='kind-reliefgrid',
                    capability='AgeRequest', approvedBy=dict(A='ParthMalhotra07', S='coDEmaVEriCK06'), interval='1m',
                    canonicalBuildRecordSHA256=[aging.record_hash(record) for record in records])
    snapshot = dict(controllers=[], pods=[], statuses={})
    def controller(name, kind, count, component, shard=None):
        labels = {'app.kubernetes.io/component': component}
        if shard is not None: labels['reliefgrid.io/shard'] = str(shard)
        status = dict(observedGeneration=2, replicas=count, readyReplicas=count, updatedReplicas=count,
                      availableReplicas=count, currentRevision='r1', updateRevision='r1')
        snapshot['controllers'].append(dict(kind=kind, metadata=dict(name=name, uid=name, generation=2, labels=labels),
                                            spec=dict(replicas=count), status=status))
    controller('storage-coordinator', 'Deployment', 1, 'coordinator')
    for shard in range(3): controller('storage-shard-' + str(shard), 'StatefulSet', 3, 'storagenode', shard)
    for controller in snapshot['controllers']:
        component = controller['metadata']['labels']['app.kubernetes.io/component']
        for index in range(controller['spec']['replicas']):
            name = controller['metadata']['name'] + '-' + str(index)
            owner_kind, owner_uid = controller['kind'], controller['metadata']['uid']
            if component == 'coordinator': owner_kind, owner_uid = 'ReplicaSet', 'coordinator-rs'
            pod = dict(metadata=dict(name=name, uid=name, labels=copy.deepcopy(controller['metadata']['labels']),
                                    ownerReferences=[dict(kind=owner_kind, uid=owner_uid, controller=True)]),
                       spec=dict(containers=[dict(name=component, image='reliefgrid/' + component + ':fixture')]),
                       status=dict(phase='Running', conditions=[dict(type='Ready', status='True')],
                                   containerStatuses=[dict(name=component, ready=True, imageID='containerd://sha256:' + ('d' if component == 'coordinator' else 'e') * 64,
                                                          state=dict(running={}))]))
            if component == 'storagenode':
                pod['metadata']['labels']['controller-revision-hash'] = 'r1'
                shard = int(pod['metadata']['labels']['reliefgrid.io/shard'])
                ids = [f'storage-shard-{shard}-{n}/inc' for n in range(3)]
                snapshot['statuses'][name] = dict(podName=name, shard=shard, groupId='shard-' + str(shard),
                    serverId=ids[index], leaderId=ids[0], term=2, state='Leader' if index == 0 else 'Follower',
                    admitted=True, caughtUp=True, hasState=True,
                    configuration=dict(committed=True, servers=[dict(id=member, suffrage='Voter') for member in ids]))
            snapshot['pods'].append(pod)
    snapshot['replicasets'] = [dict(metadata=dict(uid='coordinator-rs', ownerReferences=[dict(kind='Deployment', uid='storage-coordinator', controller=True)]),
                                   spec=dict(replicas=1), status=dict(replicas=1))]
    snapshot['pods'].append(dict(metadata=dict(name='matcher-0', uid='matcher-0', labels={'app.kubernetes.io/component': 'matcher'}),
                                spec=dict(containers=[dict(name='matcher', image='matcher:fixture', env=[dict(name='AGING_INTERVAL', value='0s')])]), status={}))
    return approval, records, snapshot


class RolloutTests(unittest.TestCase):
    def test_complete_cohort_and_disabled_producer(self):
        approval, records, snapshot = fixture()
        result = aging.validate(approval, records, snapshot)
        self.assertTrue(result['ok'])
        self.assertEqual(result['replicasVerified'], 9)
        self.assertEqual(result['enablement'], 'NOT_PERFORMED')

    def test_missing_duplicate_old_or_unready_replica_never_passes(self):
        changes = [lambda s: s['pods'].pop(1), lambda s: s['pods'].append(copy.deepcopy(s['pods'][1])),
                   lambda s: s['pods'][1]['status']['containerStatuses'][0].update(imageID='sha256:' + 'f' * 64),
                   lambda s: s['pods'][1]['status']['conditions'][0].update(status='False'),
                   lambda s: s['pods'][1]['metadata'].update(deletionTimestamp='now'),
                   lambda s: s['controllers'][1]['status'].update(currentRevision='old'),
                   lambda s: s['controllers'][0]['status'].update(observedGeneration=1),
                   lambda s: s['pods'][1]['metadata']['ownerReferences'][0].update(uid='other'),
                   lambda s: s['pods'][-1]['spec']['containers'][0]['env'][0].update(value='1m')]
        for index, change in enumerate(changes):
            with self.subTest(index=index):
                approval, records, snapshot = fixture()
                change(snapshot)
                with self.assertRaises(aging.RolloutError): aging.validate(approval, records, snapshot)

    def test_raft_views_need_exact_voters_admission_and_one_agreed_leader(self):
        for change in (lambda r: r.update(admitted=False), lambda r: r.update(caughtUp=False),
                       lambda r: r.update(leaderId='unknown'), lambda r: r.update(term=3),
                       lambda r: r.update(state='Candidate'), lambda r: r['configuration'].update(committed=False),
                       lambda r: r['configuration']['servers'].pop()):
            approval, records, snapshot = fixture()
            change(snapshot['statuses']['storage-shard-0-1'])
            with self.assertRaises(aging.RolloutError): aging.validate(approval, records, snapshot)

    def test_owner_approval_and_build_record_integrity_required(self):
        for mode in ('approval', 'hash', 'source', 'labels'):
            approval, records, snapshot = fixture()
            if mode == 'approval': approval['approvedBy'].pop('S')
            if mode == 'hash': approval['canonicalBuildRecordSHA256'][0] = '0' * 64
            if mode == 'source': records[0]['source'] = dict(records[0]['source'], dirty=True)
            if mode == 'labels': records[0]['images'][0]['labels']['org.opencontainers.image.revision'] = '0' * 40
            if mode in ('source', 'labels'):
                approval['canonicalBuildRecordSHA256'] = [aging.record_hash(record) for record in records]
            with self.subTest(mode=mode), self.assertRaises(aging.RolloutError): aging.validate(approval, records, snapshot)

    def test_two_passes_reject_pod_replacement_but_allow_advancing_indexes(self):
        approval, records, first = fixture()
        second = copy.deepcopy(first)
        second['statuses']['storage-shard-0-0']['appliedIndex'] = 123
        aging.stable(approval, records, first, second)
        second['pods'][1]['metadata']['uid'] = 'replacement'
        with self.assertRaises(aging.RolloutError): aging.stable(approval, records, first, second)

    def test_collect_uses_only_get_and_readonly_storage_status(self):
        _, _, snapshot = fixture()
        calls = []
        def read(command):
            calls.append(command)
            if 'exec' in command: return snapshot['statuses'][command[command.index('exec') + 1]]
            resource = command[command.index('get') + 1]
            items = snapshot['pods'] if resource == 'pods' else snapshot['replicasets'] if resource == 'replicasets' else [c for c in snapshot['controllers'] if c['kind'] == ('Deployment' if resource == 'deployments' else 'StatefulSet')]
            return dict(items=items)
        result = aging.collect('kind-reliefgrid', 'reliefgrid', read)
        self.assertEqual(len(result['statuses']), 9)
        self.assertEqual(sum('exec' in c for c in calls), 9)
        self.assertTrue(all('get' in c or c[-4:] == ['/storagectl', 'status', '--timeout', '3s'] for c in calls))

    def test_sanitized_artifacts_can_be_rejudged_and_have_no_unrelated_secrets(self):
        approval, records, snapshot = fixture()
        snapshot['pods'][0]['spec']['containers'][0]['env'] = [dict(name='TOKEN', value='PRIVATE-MARKER')]
        snapshot['pods'][0]['metadata']['annotations'] = {'private': 'PRIVATE-MARKER'}
        projected = aging.sanitize(snapshot)
        self.assertNotIn('PRIVATE-MARKER', json.dumps(projected))
        self.assertTrue(aging.validate(approval, records, projected)['ok'])
        snapshot['statuses']['storage-shard-0-1']['podName'] = 'storage-shard-0-2'
        with self.assertRaises(aging.RolloutError): aging.validate(approval, records, snapshot)

    def test_cli_verifies_lineage_and_writes_rejudgable_sanitized_result(self):
        approval, records, snapshot = fixture()
        with tempfile.TemporaryDirectory() as temporary:
            directory = Path(temporary)
            approved = directory / 'approval.json'
            approved.write_text(json.dumps(approval))
            argv = ['check', '--approval', str(approved), '--context', 'kind-reliefgrid', '--namespace', 'reliefgrid', '--output', str(directory / 'result.json')]
            for index, record in enumerate(records):
                path = directory / f'build-{index}.json'
                path.write_text(json.dumps(record))
                argv += ['--build-record', str(path)]
            with patch.object(sys, 'argv', argv), patch.object(aging, 'collect', side_effect=[snapshot, snapshot]), \
                 patch.object(aging.subprocess, 'run', return_value=SimpleNamespace(stdout='b' * 40 + '\n')) as git:
                self.assertEqual(aging.main(), 0)
                self.assertEqual(git.call_args_list[0].args[0][-3:], ['--is-ancestor', aging.MINIMUM_SEAM, 'a' * 40])
            result = json.loads((directory / 'result.json').read_bytes())
            self.assertTrue(result['ok'])
            self.assertTrue(aging.validate(approval, records, result['first'])['ok'])


if __name__ == '__main__': unittest.main()
