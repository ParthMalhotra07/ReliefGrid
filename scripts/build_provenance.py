#!/usr/bin/env python3
"""Build from a clean immutable Git archive; retain a trusted-builder record.

This is not a signed attestation or proof of a later runtime deployment.
"""
import argparse
import datetime as dt
import hashlib
import json
from pathlib import Path
import re
import subprocess
import tarfile
import tempfile


class ProvenanceError(Exception):
    pass


COMPONENTS = {name: ('cmd/' + name + '/Dockerfile', '/' + name)
              for name in ('api', 'matcher', 'coordinator', 'storagenode')}
COMPONENTS.update(ui=('ui/Dockerfile', '/ui'), tools=('cmd/invcheck/Dockerfile', '/invcheck'))


def require(value, message):
    if not value:
        raise ProvenanceError(message)


def execute(command):
    try:
        result = subprocess.run(command, text=True, capture_output=True, check=True, timeout=1200)
        return result.stdout.strip()
    except (OSError, subprocess.SubprocessError) as error:
        # Never persist arbitrary build stdout/stderr; contexts may contain data.
        raise ProvenanceError('builder command failed: ' + command[0]) from error


def git(source, *args):
    try:
        return subprocess.run(['git', '-C', str(source), *args], text=True, capture_output=True,
                              check=True, timeout=60).stdout.strip()
    except (OSError, subprocess.SubprocessError) as error:
        raise ProvenanceError('Git source read failed') from error


def identity(source):
    require(Path(git(source, 'rev-parse', '--show-toplevel')).resolve() == source, 'source must be the repository root')
    require(not git(source, 'status', '--porcelain', '--untracked-files=all'), 'strict build requires a clean source checkout')
    require(not git(source, 'ls-files', '--stage').startswith('160000 ') and
            '\n160000 ' not in git(source, 'ls-files', '--stage'), 'submodules require an explicit materialized build recipe')
    return dict(commit=git(source, 'rev-parse', 'HEAD'), tree=git(source, 'rev-parse', 'HEAD^{tree}'), dirty=False)


def sha(path):
    digest = hashlib.sha256()
    with path.open('rb') as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b''):
            digest.update(block)
    return digest.hexdigest()


def build_images(source, output, tag, components, run=execute):
    source, output = Path(source).resolve(), Path(output).resolve()
    require(re.fullmatch(r'[A-Za-z0-9_][A-Za-z0-9_.-]{0,127}', tag), 'invalid image tag')
    require(components and len(set(components)) == len(components) and all(c in COMPONENTS for c in components), 'invalid component selection')
    require(not output.is_relative_to(source) and not output.exists(), 'evidence must be a new directory outside the source checkout')
    initial = identity(source)
    output.mkdir(parents=True, mode=0o700)
    with tempfile.TemporaryDirectory(prefix='reliefgrid-build-') as temporary:
        work = Path(temporary)
        archive, context = work / 'source.tar', work / 'context'
        context.mkdir()
        git(source, 'archive', '--format=tar', '--output=' + str(archive), initial['commit'])
        with tarfile.open(archive) as exported:
            exported.extractall(context, filter='data')
        context_sha = sha(archive)
        result = dict(schemaVersion=1, ok=False, kind='reliefgrid-build-provenance', source=initial,
                      contextArchiveSHA256=context_sha, contextMethod='git archive HEAD; tracked files only',
                      builder=run(['docker', 'version', '--format', '{{.Client.Version}}/{{.Server.Version}}']),
                      inputs={}, images=[], startedAt=dt.datetime.now(dt.timezone.utc).isoformat(),
                      trust='unsigned record from the local trusted builder; not runtime or semantic acceptance')
        labels = {'org.opencontainers.image.revision': initial['commit'], 'io.reliefgrid.source-tree': initial['tree'],
                  'io.reliefgrid.context-sha256': context_sha}
        for name in components:
            dockerfile, entrypoint = COMPONENTS[name]
            for path in ('go.mod', 'go.sum', 'tools.mk', '.dockerignore', dockerfile, dockerfile + '.dockerignore'):
                candidate = context / path
                if candidate.is_file():
                    result['inputs'][path] = sha(candidate)
            require((context / dockerfile).is_file(), 'selected Dockerfile is absent')
            image, iidfile = 'reliefgrid/' + name + ':' + tag, work / ('iid-' + name)
            command = ['docker', 'build', '--progress=plain', '--provenance=false', '--iidfile', str(iidfile),
                       '-f', str(context / dockerfile), '-t', image]
            for key, value in labels.items():
                command.extend(['--label', key + '=' + value])
            command.append(str(context))
            run(command)
            require(iidfile.is_file(), 'builder did not produce an immutable image ID')
            iid = iidfile.read_text().strip()
            require(re.fullmatch(r'sha256:[0-9a-f]{64}', iid), 'builder returned a nonimmutable image ID')
            try:
                records = json.loads(run(['docker', 'image', 'inspect', iid]))
                require(len(records) == 1, 'ambiguous image inspection')
                record, config = records[0], records[0]['Config']
                require(record['Id'] == iid and config['User'] == '65532:65532' and config['Entrypoint'] == [entrypoint],
                        'image identity or nonroot entrypoint differs from recipe')
                require(all(config.get('Labels', {}).get(key) == value for key, value in labels.items()), 'source labels do not match the build context')
                digests = record.get('RepoDigests') or []
                require(all(re.fullmatch(r'[^\s]+@sha256:[0-9a-f]{64}', d) for d in digests), 'invalid repository digest')
                image_record = dict(component=name, image=image, imageId=iid, repoDigests=digests,
                                    user=config['User'], entrypoint=config['Entrypoint'],
                                    architecture=record['Architecture'], os=record['Os'], labels=labels)
            except (KeyError, TypeError, ValueError) as error:
                raise ProvenanceError('malformed image inspection') from error
            result['images'].append(image_record)
            require(identity(source) == initial, 'source changed during the build')
        require(identity(source) == initial, 'source changed before recording completion')
        result.update(ok=True, finishedAt=dt.datetime.now(dt.timezone.utc).isoformat())
        # Publish the success manifest last. Failed/partial builds never get one.
        for record in result['images']:
            (output / ('image-' + record['component'] + '.json')).write_text(json.dumps(record, indent=2) + '\n')
        (output / 'build-provenance.json').write_text(json.dumps(result, indent=2, sort_keys=True) + '\n')
        return result


def digest_id(value):
    match = re.search(r'(sha256:[0-9a-f]{64})$', value or '')
    require(match is not None, 'runtime image lacks an immutable SHA-256 identity')
    return match[1]


def bind_runtime(records, capture):
    """Compare immutable observations, never infer source from a mutable tag."""
    require(records and capture.get('ok') is True, 'build records and a successful capture are required')
    images, source = {}, None
    for record in records:
        require(record.get('schemaVersion') == 1 and record.get('kind') == 'reliefgrid-build-provenance'
                and record.get('ok') is True and record['source']['dirty'] is False, 'invalid trusted-builder record')
        require(source is None or source == record['source'], 'build records use different source trees')
        source = record['source']
        require(re.fullmatch('[0-9a-f]{40}', source['commit']) and re.fullmatch('[0-9a-f]{40}', source['tree'])
                and re.fullmatch('[0-9a-f]{64}', record['contextArchiveSHA256']), 'source identity is malformed')
        for image in record['images']:
            component = image['component']
            require(component in COMPONENTS and component not in images, 'duplicate or unknown built component')
            labels = image['labels']
            require(labels.get('org.opencontainers.image.revision') == source['commit']
                    and labels.get('io.reliefgrid.source-tree') == source['tree']
                    and labels.get('io.reliefgrid.context-sha256') == record['contextArchiveSHA256'], 'image/source labels disagree')
            images[component] = image
    require(capture['source'].get('collectorCommit') == source['commit']
            and capture['source'].get('collectorTree') == source['tree']
            and capture['source'].get('collectorDirty') is False, 'collector and builds must share the same clean baseline')
    observed, counts, seen = [], dict(api=0, matcher=0, tools=0), set()
    for pod in capture['fixedProfile']['pods']:
        uid = pod['metadata'].get('uid')
        require(uid and uid not in seen, 'runtime pod identity is missing or duplicated')
        seen.add(uid)
        role = pod['metadata']['labels']['app.kubernetes.io/component']
        require(role in ('api', 'matcher') and len(pod['spec']['containers']) == 1
                and len(pod['status']['containerStatuses']) == 1, 'unexpected runtime app container')
        wanted, status = pod['spec']['containers'][0], pod['status']['containerStatuses'][0]
        require(wanted['name'] == status['name'], 'runtime container names disagree')
        observed.append((role, wanted['image'], status['imageID']))
        counts[role] += 1
    for tool in capture['tools']['containers']:
        observed.append(('tools', tool['image'], tool['imageID']))
        counts['tools'] += 1
    require(counts == dict(api=2, matcher=2, tools=1), 'binding needs both app pairs and the tools image')
    bound = []
    for role, reference, runtime_id in observed:
        require(role in images, 'runtime component has no build record')
        image = images[role]
        require(reference in [image['image'], *image.get('repoDigests', [])], 'runtime requested image differs from build record')
        known = [image['imageId'], *image.get('repoDigests', [])]
        require(digest_id(runtime_id) in {digest_id(value) for value in known}, 'runtime digest is not established by the build record')
        bound.append(dict(component=role, requestedImage=reference, runtimeImageID=runtime_id))
    return dict(schemaVersion=1, kind='reliefgrid-runtime-build-binding', ok=True, source=source, observations=bound,
                trust='comparison against supplied unsigned trusted-builder records; not a signed attestation or gate verdict')


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest='mode', required=True)
    building = commands.add_parser('build')
    building.add_argument('--source', type=Path, default=Path(__file__).resolve().parents[1])
    building.add_argument('--output', type=Path, required=True)
    building.add_argument('--tag', required=True)
    building.add_argument('--component', action='append', choices=COMPONENTS)
    binding = commands.add_parser('bind')
    binding.add_argument('--build-record', type=Path, action='append', required=True)
    binding.add_argument('--capture', type=Path, required=True)
    binding.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    try:
        if args.mode == 'build':
            build_images(args.source, args.output, args.tag, args.component or ['api', 'matcher', 'ui'])
        else:
            payloads = [path.read_bytes() for path in args.build_record]
            captured = args.capture.read_bytes()
            result = bind_runtime([json.loads(data) for data in payloads], json.loads(captured))
            result['buildRecordSHA256'] = [hashlib.sha256(data).hexdigest() for data in payloads]
            result['captureSHA256'] = hashlib.sha256(captured).hexdigest()
            with args.output.open('x') as stream:
                stream.write(json.dumps(result, indent=2, sort_keys=True) + '\n')
    except (ProvenanceError, OSError, tarfile.TarError, ValueError, KeyError, TypeError) as error:
        print('STRICT BUILD: FAIL - ' + (str(error) if isinstance(error, ProvenanceError) else type(error).__name__))
        return 1
    print(('STRICT BUILD' if args.mode == 'build' else 'RUNTIME BINDING') + ': PASS (trusted records; not a gate verdict)')
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
