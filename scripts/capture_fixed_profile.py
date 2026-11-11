#!/usr/bin/env python3
"""Read-only, fail-closed evidence of a restored fixed API2/matcher2 profile.

This observation is not a gate verdict, a restoration action or a build binding.
Only allowlisted fields are persisted; never retain raw Helm/Pod responses.
"""
import argparse
import datetime as dt
import json
from pathlib import Path
import re
import subprocess
import sys


class InvalidObservation(Exception):
    pass


def require(condition, message):
    if not condition:
        raise InvalidObservation(message)


def command_json(command):
    try:
        result = subprocess.run(command, capture_output=True, text=True, timeout=25, check=True)
        return json.loads(result.stdout)
    except (OSError, subprocess.SubprocessError, ValueError) as error:
        # stderr/command output may contain credentials or full API objects.
        raise InvalidObservation('read command failed: ' + command[0]) from error


def metadata(obj):
    meta = obj.get('metadata', {})
    result = {key: meta[key] for key in ('name', 'uid', 'generation', 'deletionTimestamp') if key in meta}
    result['labels'] = {key: value for key, value in meta.get('labels', {}).items()
                        if key in ('app.kubernetes.io/instance', 'app.kubernetes.io/component')}
    result['ownerReferences'] = [{key: owner[key] for key in ('kind', 'uid', 'controller') if key in owner}
                                 for owner in meta.get('ownerReferences', [])]
    result['annotations'] = {key: value for key, value in meta.get('annotations', {}).items()
                             if key == 'deployment.kubernetes.io/revision'}
    return result


def containers(spec):
    return [{key: container[key] for key in ('name', 'image') if key in container}
            for container in spec.get('containers', [])]


def pod_record(pod):
    status = pod.get('status', {})
    statuses = []
    for container in status.get('containerStatuses', []):
        record = {key: container[key] for key in ('name', 'ready', 'restartCount', 'imageID') if key in container}
        state = container.get('state', {})
        record['state'] = {key: {field: value for field, value in state[key].items()
                                if field in ('exitCode', 'startedAt', 'finishedAt')}
                           for key in ('running', 'terminated') if key in state}
        statuses.append(record)
    return dict(metadata=metadata(pod), spec=dict(containers=containers(pod.get('spec', {}))),
                status=dict(phase=status.get('phase'), containerStatuses=statuses,
                            conditions=[{key: item[key] for key in ('type', 'status') if key in item}
                                        for item in status.get('conditions', [])]))


def sanitize(snapshot):
    result = dict(deployments=[], replicasets=[], pods=[pod_record(p) for p in snapshot['pods']])
    for group in ('deployments', 'replicasets'):
        for item in snapshot[group]:
            spec, status = item.get('spec', {}), item.get('status', {})
            result[group].append(dict(metadata=metadata(item), spec=dict(replicas=spec.get('replicas'),
                template=dict(spec=dict(containers=containers(spec.get('template', {}).get('spec', {}))))),
                status={key: status[key] for key in ('observedGeneration', 'replicas', 'updatedReplicas',
                        'readyReplicas', 'availableReplicas', 'unavailableReplicas') if key in status}))
    for group in ('hpas', 'scaledobjects'):
        result[group] = [dict(spec=dict(scaleTargetRef={key: value for key, value in
            item.get('spec', {}).get('scaleTargetRef', {}).items() if key in ('apiVersion', 'kind', 'name')}))
            for item in snapshot[group]]
    values = snapshot.get('values', {})
    app = values.get('app', {})
    result['values'] = dict(storage=dict(enabled=values.get('storage', {}).get('enabled')),
        app=dict(enabled=app.get('enabled'), autoscaling=dict(enabled=app.get('autoscaling', {}).get('enabled')),
                 api=dict(replicas=app.get('api', {}).get('replicas')),
                 matcher=dict(replicas=app.get('matcher', {}).get('replicas'))))
    return result


def collect(context, namespace, release, read=command_json):
    kubectl = ['kubectl', '--context=' + context, '--namespace=' + namespace, '--request-timeout=15s', 'get']
    selector = 'app.kubernetes.io/instance=' + release + ',app.kubernetes.io/component in (api,matcher)'
    result = {}
    for resource, key in (('deployments', 'deployments'), ('replicasets', 'replicasets'), ('pods', 'pods')):
        result[key] = read(kubectl + [resource, '-l', selector, '-o', 'json'])['items']
    # Controllers must be listed namespace-wide: a foreign-labelled controller
    # can still target our Deployment. CRD/RBAC/read failures never mean "none".
    for resource, key in (('hpa', 'hpas'), ('scaledobjects.keda.sh', 'scaledobjects')):
        result[key] = read(kubectl + [resource, '-o', 'json'])['items']
    result['values'] = read(['helm', '--kube-context=' + context, '--namespace=' + namespace,
                            'get', 'values', release, '--all', '-o', 'json'])
    return sanitize(result)


def owner_matches(item, kind, uid):
    owners = [owner for owner in item['metadata'].get('ownerReferences', []) if owner.get('controller') is True]
    return len(owners) == 1 and owners[0].get('kind') == kind and owners[0].get('uid') == uid


def integer(value):
    return type(value) is int


def image_identity(value):
    return isinstance(value, str) and re.search(r'(?:^|[@/])sha256:[0-9a-f]{64}$', value) is not None


def healthy_controller(item, deployment=False):
    meta, spec, status = item['metadata'], item['spec'], item['status']
    require(meta.get('uid') and not meta.get('deletionTimestamp'), 'controller identity missing or deleting')
    require(integer(meta.get('generation')) and status.get('observedGeneration') == meta['generation'],
            'controller generation is not observed')
    for value in (spec.get('replicas'), status.get('replicas'), status.get('readyReplicas'), status.get('availableReplicas')):
        require(integer(value) and value == 2, 'controller has not reached two available replicas')
    require(status.get('unavailableReplicas', 0) == 0, 'controller has unavailable replicas')
    if deployment:
        require(status.get('updatedReplicas') == 2, 'Deployment update is incomplete')


def validate(snapshot, release):
    data = sanitize(snapshot)
    values, app = data['values'], data['values']['app']
    require(values['storage']['enabled'] is False, 'application release must not own storage')
    require(app['enabled'] is True and app['autoscaling']['enabled'] is False, 'fixed app profile is not configured')
    require(all(type(app[c]['replicas']) is int and app[c]['replicas'] == 2 for c in ('api', 'matcher')),
            'Helm fixed replica defaults are not two')
    require(len(data['deployments']) == 2, 'expected exactly two selected Deployments')
    selected, seen = [], set()
    for component in ('api', 'matcher'):
        deployments = [d for d in data['deployments'] if d['metadata']['labels'].get('app.kubernetes.io/component') == component
                       and d['metadata']['labels'].get('app.kubernetes.io/instance') == release]
        require(len(deployments) == 1, 'missing or ambiguous app Deployment')
        deploy = deployments[0]
        healthy_controller(deploy, deployment=True)
        name, uid = deploy['metadata']['name'], deploy['metadata']['uid']
        for group in ('hpas', 'scaledobjects'):
            for controller in data[group]:
                target = controller['spec']['scaleTargetRef']
                require(not (target.get('name') == name and target.get('kind', 'Deployment') == 'Deployment'),
                        'autoscaling controller still targets a fixed Deployment')
        active = [rs for rs in data['replicasets'] if owner_matches(rs, 'Deployment', uid)
                  and (rs['spec'].get('replicas', 0) or rs['status'].get('replicas', 0))]
        require(len(active) == 1, 'expected one active ReplicaSet per Deployment')
        rs = active[0]
        healthy_controller(rs)
        revision = deploy['metadata']['annotations'].get('deployment.kubernetes.io/revision')
        require(revision and rs['metadata']['annotations'].get('deployment.kubernetes.io/revision') == revision,
                'active ReplicaSet is not current Deployment revision')
        images = deploy['spec']['template']['spec']['containers']
        require(images and images == rs['spec']['template']['spec']['containers'], 'ReplicaSet images differ from Deployment')
        pods = [pod for pod in data['pods'] if pod['metadata']['labels'].get('app.kubernetes.io/component') == component]
        require(len(pods) == 2, 'expected exactly two app pods without leftover terminating pods')
        identities = []
        for pod in pods:
            meta, status = pod['metadata'], pod['status']
            require(meta.get('uid') and meta['uid'] not in seen and not meta.get('deletionTimestamp'), 'pod missing, duplicated or terminating')
            seen.add(meta['uid'])
            require(meta['labels'].get('app.kubernetes.io/instance') == release and owner_matches(pod, 'ReplicaSet', rs['metadata']['uid']),
                    'pod is not owned by current app ReplicaSet')
            require(pod['spec']['containers'] == images, 'pod image differs from current Deployment')
            conditions = {c['type']: c.get('status') for c in status['conditions']}
            require(status['phase'] == 'Running' and conditions.get('Ready') == 'True' and conditions.get('ContainersReady') == 'True',
                    'pod is not running and Ready')
            statuses = status['containerStatuses']
            require(len(statuses) == len(images) and {c.get('name') for c in statuses} == {c.get('name') for c in images},
                    'container status is incomplete')
            require(all(c.get('ready') is True and 'running' in c['state'] and image_identity(c.get('imageID')) for c in statuses),
                    'container is unready or lacks observed image identity')
            identities.append({c['name']: c['imageID'] for c in statuses})
            selected.append(pod)
        require(identities[0] == identities[1], 'replicas of one component run different image identities')
    require(len(data['pods']) == 4, 'unexpected selected app pods')
    return dict(fixedReplicas=dict(api=2, matcher=2), deployments=data['deployments'],
                replicasets=data['replicasets'], pods=sorted(selected, key=lambda p: p['metadata']['uid']),
                hpas=data['hpas'], scaledobjects=data['scaledobjects'], values=data['values'])


def stable(first, second, release):
    before, after = validate(first, release), validate(second, release)
    # Lists have no API ordering guarantee. Normalize by canonical object bytes.
    normalized = lambda d: {key: sorted(json.dumps(item, sort_keys=True) for item in value)
                            if isinstance(value, list) else value for key, value in d.items()}
    require(normalized(before) == normalized(after), 'state changed across the two read passes')
    return after


def tools_observation(job, pods):
    require(job.get('metadata', {}).get('uid') and job.get('status', {}).get('succeeded') == 1,
            'tools Job did not complete successfully')
    require(not job['metadata'].get('deletionTimestamp') and job['status'].get('failed', 0) == 0
            and job['status'].get('active', 0) == 0, 'tools Job has failed/active attempts or is deleting')
    require(len(pods) == 1, 'tools Job needs exactly one retained pod; retries require separate evidence review')
    pod = pod_record(pods[0])
    require(owner_matches(pod, 'Job', job['metadata']['uid']) and not pod['metadata'].get('deletionTimestamp'),
            'tools pod is not owned by retained Job')
    statuses = pod['status']['containerStatuses']
    require(pod['status']['phase'] == 'Succeeded' and statuses and len(statuses) == len(pod['spec']['containers']),
            'tools container observation is incomplete')
    require({c.get('name') for c in statuses} == {c.get('name') for c in pod['spec']['containers']},
            'tools container names do not match observed statuses')
    require(all(image_identity(c.get('imageID')) and c['state'].get('terminated', {}).get('exitCode') == 0 for c in statuses),
            'tools container did not exit cleanly with an observed image ID')
    return dict(job=dict(metadata={key: job['metadata'][key] for key in ('name', 'uid') if key in job['metadata']},
                         status={key: job['status'].get(key, 0) for key in ('succeeded', 'failed', 'active')}),
                pod=pod, containers=[dict(**image, imageID=status['imageID'])
                for image in pod['spec']['containers'] for status in statuses if image['name'] == status['name']])


def new_output(path):
    path.mkdir(parents=True, exist_ok=False, mode=0o700)


def source_observation(source):
    def git(*args):
        try:
            return subprocess.run(['git', '-C', str(source), *args], text=True, capture_output=True,
                                  check=True, timeout=15).stdout.strip()
        except (OSError, subprocess.SubprocessError) as error:
            raise InvalidObservation('source identity could not be recorded') from error
    return dict(collectorCommit=git('rev-parse', 'HEAD'), collectorTree=git('rev-parse', 'HEAD^{tree}'),
                collectorDirty=bool(git('status', '--porcelain', '--untracked-files=all')),
                runtimeBuildBinding='NOT_ESTABLISHED: observed image IDs do not prove source/build contents')


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for flag in ('context', 'namespace', 'release', 'tools-job', 'output'):
        parser.add_argument('--' + flag, required=True)
    parser.add_argument('--source', type=Path, default=Path(__file__).resolve().parents[1])
    args = parser.parse_args()
    for name in ('namespace', 'release', 'tools_job'):
        require(re.fullmatch(r'[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?', getattr(args, name)), 'invalid Kubernetes name')
    source = source_observation(args.source)
    output = Path(args.output)
    new_output(output)
    result = dict(ok=False, scope='fixed-profile observation only; not a gate or build-source certification',
                  source=source, context=args.context, namespace=args.namespace, release=args.release, toolsJob=args.tools_job)
    try:
        result['startedAt'] = dt.datetime.now(dt.timezone.utc).isoformat()
        first = collect(args.context, args.namespace, args.release)
        result['first'] = first
        kc = ['kubectl', '--context=' + args.context, '--namespace=' + args.namespace, '--request-timeout=15s', 'get']
        job = command_json(kc + ['job', args.tools_job, '-o', 'json'])
        pods = command_json(kc + ['pods', '-l', 'job-name=' + args.tools_job, '-o', 'json'])['items']
        result['tools'] = tools_observation(job, pods)
        second = collect(args.context, args.namespace, args.release)
        result['second'] = second
        result['fixedProfile'] = stable(first, second, args.release)
        require(source == source_observation(args.source), 'collector source changed during capture')
        result['ok'] = True
    except (InvalidObservation, KeyError, TypeError, ValueError, AttributeError) as error:
        # Only messages generated here may reach persisted evidence.
        result['error'] = str(error) if isinstance(error, InvalidObservation) else 'malformed observation'
    result['finishedAt'] = dt.datetime.now(dt.timezone.utc).isoformat()
    (output / 'fixed-profile.json').write_text(json.dumps(result, indent=2, sort_keys=True) + '\n')
    print('fixed-profile observation: ' + ('PASS' if result['ok'] else 'FAIL'))
    return 0 if result['ok'] else 1


if __name__ == '__main__':
    try:
        sys.exit(main())
    except (InvalidObservation, OSError) as failure:
        print('fixed-profile capture could not start: ' + type(failure).__name__, file=sys.stderr)
        sys.exit(1)
