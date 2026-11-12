#!/usr/bin/env python3
"""Read-only coordinator/nine-replica aging prerequisite observation.

Capability approval is supplied by owners, not inferred from Pod readiness.
This neither enables aging nor proves functional aging or routing acceptance.
"""
import argparse
import hashlib
import json
from pathlib import Path
import re
import subprocess

MINIMUM_SEAM = 'e50685db3fb4a9b683c5e7f3688583dd545e570e'


class RolloutError(Exception):
    pass


def require(condition, message):
    if not condition: raise RolloutError(message)


def record_hash(record):
    return hashlib.sha256(json.dumps(record, sort_keys=True, separators=(',', ':')).encode()).hexdigest()


def digest(value):
    match = re.search(r'(sha256:[0-9a-f]{64})$', value or '')
    require(match, 'missing immutable runtime image identity')
    return match[1]


def run_json(command):
    try:
        return json.loads(subprocess.run(command, capture_output=True, text=True, check=True, timeout=20).stdout)
    except (OSError, subprocess.SubprocessError, ValueError) as error:
        raise RolloutError('read command failed; no readiness inferred') from error


def metadata(item):
    meta = item.get('metadata', {})
    return dict(**{k: meta[k] for k in ('name', 'uid', 'generation', 'deletionTimestamp') if k in meta},
                labels={k: v for k, v in meta.get('labels', {}).items() if k in
                    ('app.kubernetes.io/component', 'reliefgrid.io/shard', 'controller-revision-hash')},
                ownerReferences=[{k: v for k, v in owner.items() if k in ('kind', 'uid', 'controller')}
                                 for owner in meta.get('ownerReferences', [])])


def sanitize(snapshot):
    output = dict(controllers=[], replicasets=[], pods=[], statuses={})
    for group in ('controllers', 'replicasets'):
        for item in snapshot[group]:
            output[group].append(dict(kind=item.get('kind'), metadata=metadata(item), spec=dict(replicas=item.get('spec', {}).get('replicas')),
                status={k: v for k, v in item.get('status', {}).items() if k in
                    ('observedGeneration', 'replicas', 'readyReplicas', 'updatedReplicas', 'availableReplicas', 'currentRevision', 'updateRevision')}))
    for pod in snapshot['pods']:
        containers = []
        for container in pod.get('spec', {}).get('containers', []):
            containers.append(dict(name=container.get('name'), image=container.get('image'),
                env=[dict(name='AGING_INTERVAL', value=item.get('value')) for item in container.get('env', []) if item.get('name') == 'AGING_INTERVAL']))
        status = pod.get('status', {})
        output['pods'].append(dict(metadata=metadata(pod), spec=dict(containers=containers), status=dict(phase=status.get('phase'),
            conditions=[dict(type=item.get('type'), status=item.get('status')) for item in status.get('conditions', []) if item.get('type') == 'Ready'],
            containerStatuses=[dict(name=item.get('name'), ready=item.get('ready'), imageID=item.get('imageID'),
                                    running=item.get('running') is True or 'running' in item.get('state', {})) for item in status.get('containerStatuses', [])])))
    for name, status in snapshot['statuses'].items():
        projected = {k: v for k, v in status.items() if k in ('podName', 'shard', 'groupId', 'serverId', 'leaderId', 'term',
                      'state', 'admitted', 'caughtUp', 'hasState', 'appliedIndex', 'leaderCommitIndex')}
        config = status.get('configuration', {})
        projected['configuration'] = dict(committed=config.get('committed'), servers=[{k: v for k, v in item.items() if k in ('id', 'suffrage')}
                                                                                    for item in config.get('servers', [])])
        output['statuses'][name] = projected
    return output


def collect(context, namespace, read=run_json):
    kc = ['kubectl', '--context=' + context, '--namespace=' + namespace, '--request-timeout=10s']
    snapshot = dict(controllers=[], replicasets=[], pods=[], statuses={})
    for resource, component, kind in (('deployments', 'coordinator', 'Deployment'), ('statefulsets', 'storagenode', 'StatefulSet')):
        items = read(kc + ['get', resource, '-l', 'app.kubernetes.io/component=' + component, '-o', 'json'])['items']
        for item in items: item['kind'] = kind
        snapshot['controllers'].extend(items)
    snapshot['replicasets'] = read(kc + ['get', 'replicasets', '-l', 'app.kubernetes.io/component=coordinator', '-o', 'json'])['items']
    snapshot['pods'] = read(kc + ['get', 'pods', '-l', 'app.kubernetes.io/component in (coordinator,storagenode,matcher)', '-o', 'json'])['items']
    for pod in snapshot['pods']:
        if pod.get('metadata', {}).get('labels', {}).get('app.kubernetes.io/component') == 'storagenode':
            name = pod['metadata']['name']
            require(re.fullmatch('[a-z0-9][a-z0-9.-]*', name), 'invalid observed pod name')
            snapshot['statuses'][name] = read(kc + ['exec', name, '-c', 'storagenode', '--', '/storagectl', 'status', '--timeout', '3s'])
    return snapshot


def owner(item, kind, uid):
    refs = [r for r in item['metadata']['ownerReferences'] if r.get('controller') is True]
    return len(refs) == 1 and refs[0].get('kind') == kind and refs[0].get('uid') == uid


def validate(approval, records, snapshot):
    require(approval.get('schemaVersion') == 1 and approval.get('capability') == 'AgeRequest', 'AgeRequest approval is required')
    require(all(isinstance(approval.get('approvedBy', {}).get(role), str) and approval['approvedBy'][role].strip() for role in ('A', 'S')),
            'both owner approval declarations are required')
    require(re.fullmatch(r'[1-9][0-9]*(ms|s|m|h)', approval.get('interval', '')), 'use an approved canonical interval of at least 1ms')
    require(approval.get('canonicalBuildRecordSHA256') == [record_hash(r) for r in records], 'approved build record hashes differ')
    expected, source = {}, None
    for record in records:
        require(record.get('kind') == 'reliefgrid-build-provenance' and record.get('schemaVersion') == 1 and record.get('ok') is True,
                'successful strict build records required')
        require(record['source'].get('dirty') is False and record['source']['commit'] == approval['sourceCommit'], 'wrong or dirty build source')
        require(source is None or source == record['source'], 'build source trees differ')
        source = record['source']
        for image in record['images']:
            role = image['component']
            if role not in ('coordinator', 'storagenode'): continue
            require(role not in expected, 'duplicate approved storage image')
            require(image['labels'].get('org.opencontainers.image.revision') == source['commit']
                    and image['labels'].get('io.reliefgrid.source-tree') == source['tree']
                    and image['labels'].get('io.reliefgrid.context-sha256') == record['contextArchiveSHA256'], 'image/source labels differ')
            expected[role] = image
    require(set(expected) == {'coordinator', 'storagenode'}, 'coordinator and storage build records are required')
    data = sanitize(snapshot)
    require(len(data['controllers']) == 4, 'expected coordinator and three StatefulSets')
    storage_pods, observed, seen = [], [], set()
    for controller in data['controllers']:
        meta, status, spec = controller['metadata'], controller['status'], controller['spec']
        role = meta['labels'].get('app.kubernetes.io/component')
        require(role in expected and not meta.get('deletionTimestamp'), 'unexpected or deleting controller')
        count = 1 if role == 'coordinator' else 3
        require(type(meta.get('generation')) is int and status.get('observedGeneration') == meta['generation'], 'stale controller generation')
        require(all(type(value) is int and value == count for value in (spec['replicas'], status.get('replicas'), status.get('readyReplicas'), status.get('updatedReplicas'))),
                'controller rollout incomplete')
        if role == 'coordinator':
            require(controller['kind'] == 'Deployment' and status.get('availableReplicas') == 1, 'coordinator is not available')
            candidates = [p for p in data['pods'] if p['metadata']['labels'].get('app.kubernetes.io/component') == role]
            active_rs = [rs for rs in data['replicasets'] if owner(rs, 'Deployment', meta['uid']) and rs['spec']['replicas'] == 1 and rs['status'].get('replicas') == 1]
            require(len(active_rs) == 1, 'missing current coordinator ReplicaSet')
            owner_kind, owner_uid = 'ReplicaSet', active_rs[0]['metadata']['uid']
        else:
            require(controller['kind'] == 'StatefulSet' and status.get('currentRevision') and status['currentRevision'] == status.get('updateRevision'), 'storage revision rollout incomplete')
            shard = meta['labels'].get('reliefgrid.io/shard')
            require(shard in ('0', '1', '2'), 'unexpected shard')
            candidates = [p for p in data['pods'] if p['metadata']['labels'].get('app.kubernetes.io/component') == role and p['metadata']['labels'].get('reliefgrid.io/shard') == shard]
            owner_kind, owner_uid = 'StatefulSet', meta['uid']
        require(len(candidates) == count, 'missing/extra/deleting rollout pods')
        for pod in candidates:
            m, s = pod['metadata'], pod['status']
            require(m.get('uid') and m['uid'] not in seen and not m.get('deletionTimestamp') and owner(pod, owner_kind, owner_uid), 'duplicate, deleting or foreign-owned pod')
            seen.add(m['uid'])
            require(s['phase'] == 'Running' and any(c['status'] == 'True' for c in s['conditions']), 'pod is not Ready')
            require(len(pod['spec']['containers']) == 1 and len(s['containerStatuses']) == 1, 'unexpected or missing container')
            wanted, runtime = pod['spec']['containers'][0], s['containerStatuses'][0]
            image = expected[role]
            require(wanted['name'] == runtime['name'] == role and runtime['ready'] is True and runtime['running'], 'container is not running/Ready')
            require(wanted['image'] in [image['image'], *image.get('repoDigests', [])] and digest(runtime['imageID']) in
                    {digest(i) for i in [image['imageId'], *image.get('repoDigests', [])]}, 'deployed image is not in the approved build record')
            if role == 'storagenode':
                require(m['labels'].get('controller-revision-hash') == status['updateRevision'], 'pod has old storage revision')
                storage_pods.append(pod)
            observed.append(dict(pod=m['name'], uid=m['uid'], role=role, imageID=runtime['imageID']))
    require(len(storage_pods) == 9 and len(observed) == 10, 'coordinator plus all nine replicas are required')
    require(set(data['statuses']) == {p['metadata']['name'] for p in storage_pods}, 'storage status coverage incomplete')
    for name, status in data['statuses'].items():
        require(status.get('podName') == name and status.get('serverId', '').startswith(name + '/'), 'status identity differs from the observed pod')
    groups = []
    for shard in range(3):
        members = [data['statuses'][p['metadata']['name']] for p in storage_pods if p['metadata']['labels']['reliefgrid.io/shard'] == str(shard)]
        require(len(members) == 3 and len({r['serverId'] for r in members}) == 3, 'shard membership incomplete')
        ids = {r['serverId'] for r in members}
        leaders, terms = {r['leaderId'] for r in members}, {r['term'] for r in members}
        require(len(leaders) == len(terms) == 1 and next(iter(leaders)) in ids and type(next(iter(terms))) is int and next(iter(terms)) > 0,
                'replicas do not agree on a current leader/term')
        require(sum(r['state'] == 'Leader' for r in members) == 1, 'shard has no unique leader')
        for r in members:
            require(r['podName'] in data['statuses'] and r['shard'] == shard and r['groupId'] == 'shard-' + str(shard)
                    and r['admitted'] is True and r['caughtUp'] is True and r['hasState'] is True and r['state'] in ('Leader', 'Follower'), 'replica is unadmitted, behind or in wrong group')
            require(r['state'] != 'Leader' or r['serverId'] == r['leaderId'], 'leader does not identify itself')
            config = r['configuration']
            require(config['committed'] is True and len(config['servers']) == 3 and {s.get('id') for s in config['servers']} == ids
                    and all(s.get('suffrage') == 'Voter' for s in config['servers']), 'committed voter configuration differs')
        groups.append(dict(shard=shard, leaderId=next(iter(leaders)), term=next(iter(terms))))
    producers = [p for p in data['pods'] if p['metadata']['labels'].get('app.kubernetes.io/component') == 'matcher']
    require(producers, 'no deployed matcher config observed')
    for pod in producers:
        env = [item for c in pod['spec']['containers'] for item in c['env'] if item['name'] == 'AGING_INTERVAL']
        require(len(env) == 1 and env[0].get('value') == '0s', 'producer aging must remain explicitly disabled during prerequisite verification')
    return dict(ok=True, schemaVersion=1, kind='reliefgrid-aging-rollout', source=source, replicasVerified=9, enablement='NOT_PERFORMED',
                observations=sorted(observed, key=lambda p: p['pod']), groups=groups,
                policyInterval=approval['interval'], disabledProducers=sorted([dict(name=p['metadata']['name'], uid=p['metadata']['uid']) for p in producers], key=lambda p: p['name']),
                scope='supplied owner capability approval plus deployed-image/readiness/Raft observation; not functional aging or routing acceptance')


def stable(approval, records, first, second):
    before, after = validate(approval, records, first), validate(approval, records, second)
    require(before == after, 'pod identities, images or leader view changed during observation')
    return after


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--build-record', type=Path, action='append', required=True)
    parser.add_argument('--record-hashes', action='store_true')
    parser.add_argument('--approval', type=Path)
    parser.add_argument('--context')
    parser.add_argument('--namespace')
    parser.add_argument('--output', type=Path)
    parser.add_argument('--repo', type=Path, default=Path(__file__).resolve().parents[1])
    args = parser.parse_args()
    result = dict(ok=False)
    try:
        records = [json.loads(p.read_bytes()) for p in args.build_record]
        if args.record_hashes:
            print(json.dumps([record_hash(r) for r in records]))
            return 0
        require(args.approval and args.context and args.namespace and args.output, 'approval/context/namespace/output are required')
        require(not args.output.exists(), 'output already exists')
        approval = json.loads(args.approval.read_bytes())
        require((approval['context'], approval['namespace']) == (args.context, args.namespace), 'approval targets another cluster/namespace')
        commit = approval['sourceCommit']
        require(re.fullmatch('[0-9a-f]{40}', commit), 'source commit is malformed')
        subprocess.run(['git', '-C', str(args.repo), 'merge-base', '--is-ancestor', MINIMUM_SEAM, commit], check=True, capture_output=True, timeout=15)
        tree = subprocess.run(['git', '-C', str(args.repo), 'rev-parse', commit + '^{tree}'], text=True, check=True, capture_output=True, timeout=15).stdout.strip()
        require(all(r['source']['tree'] == tree for r in records), 'recorded tree does not match Git commit')
        first, second = collect(args.context, args.namespace), collect(args.context, args.namespace)
        result = stable(approval, records, first, second)
        result.update(context=args.context, namespace=args.namespace, minimumSeamAncestor=MINIMUM_SEAM,
                      first=sanitize(first), second=sanitize(second), canonicalApprovalSHA256=record_hash(approval))
    except (RolloutError, OSError, ValueError, KeyError, TypeError, AttributeError, subprocess.SubprocessError) as error:
        result['error'] = str(error) if isinstance(error, RolloutError) else 'source, input or read verification failed'
    if args.output and not args.output.exists():
        with args.output.open('x') as stream: stream.write(json.dumps(result, indent=2, sort_keys=True) + '\n')
    print('AGING ROLLOUT: ' + ('PASS (no enablement)' if result['ok'] else 'FAIL'))
    return 0 if result['ok'] else 1


if __name__ == '__main__': raise SystemExit(main())
