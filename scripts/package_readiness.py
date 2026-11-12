#!/usr/bin/env python3
"""Check final-package inventory, source identity and evidence consistency.

Summary envelopes/signoffs are supplied by the owners. This does not execute or
independently certify live gates, benchmarks, publication or media playback.
"""
import argparse
from collections import Counter
import hashlib
import json
from pathlib import Path, PurePosixPath
import re
import subprocess

LOCAL_CHECKS = ('make-check', 'gen-check', 'contract-validation', 'doctor', 'clean-clone', 'recording-playback')
ENHANCED_CHECKS = ('ui', 'dashboards', 'trace', 'chaos', 'autoscaling', 'replacement', 'ci-smoke', 'api-refinements', 'm6')
ROLES = {'source', 'report', 'recording', 'offline-kit', 'evidence', 'checks', 'gate', 'performance', 'offline',
         'build-provenance', 'fixed-profile', 'runtime-binding'}
SMALL_ROLES = {'checks', 'gate', 'performance', 'offline', 'build-provenance', 'fixed-profile', 'runtime-binding'}


class ReadinessError(Exception):
    pass


def require(condition, message):
    if not condition: raise ReadinessError(message)


def sha(path):
    digest = hashlib.sha256()
    with path.open('rb') as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b''): digest.update(block)
    return digest.hexdigest()


def content_state(stat):
    # Reads may update atime. Content/identity changes, including same-size
    # rewrites and replacements, still change this signature.
    return (stat.st_dev, stat.st_ino, stat.st_mode, stat.st_size, stat.st_mtime_ns, stat.st_ctime_ns)


def immutable(value):
    match = re.search(r'(sha256:[0-9a-f]{64})$', value or '')
    require(match, 'immutable image identity is missing')
    return match[1]


def inventory(root, manifest_path):
    files = set()
    for path in root.rglob('*'):
        require(not path.is_symlink(), 'package contains a symlink')
        if path.is_file() and (manifest_path is None or path.resolve() != Path(manifest_path).resolve()):
            files.add(path.relative_to(root).as_posix())
    return files


def validate(root, manifest, source, manifest_path=None):
    root = Path(root).resolve()
    require(manifest.get('schemaVersion') == 1 and source.get('dirty') is False, 'manifest version or clean checkout requirement failed')
    commit, tree = source['commit'], source['tree']
    require(re.fullmatch('[0-9a-f]{40}', commit) and re.fullmatch('[0-9a-f]{40}', tree), 'source identity is malformed')
    require(manifest.get('sourceCommit') == commit and manifest.get('sourceTree') == tree and source.get('tagCommit') == commit,
            'package/tag/source baseline mismatch')
    require(isinstance(manifest.get('releaseTag'), str) and manifest['releaseTag'], 'release tag is missing')
    require(manifest.get('unresolved') == [] and manifest.get('reportFormatConfirmed') is True and manifest.get('publicVisibilityConfirmed') is True,
            'unresolved work or report/publication confirmation remains')
    require(re.fullmatch(r'https://github\.com/[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+', manifest.get('repositoryURL', '')), 'repository link is malformed')
    require(all(isinstance(manifest.get('ownerSignoff', {}).get(role), str) and manifest['ownerSignoff'][role].strip() for role in ('A', 'S')),
            'both owner signoff declarations are required')
    entries, payloads, states = {}, {}, {}
    by_role = {role: [] for role in ROLES}
    for item in manifest['artifacts']:
        name, role = item['path'], item['role']
        path = PurePosixPath(name)
        require(isinstance(name, str) and name and not path.is_absolute() and str(path) == name and '..' not in path.parts
                and '\\' not in name and ':' not in name and role in ROLES and name not in entries, 'invalid, duplicate or escaping artifact path/role')
        actual = root / name
        require(actual.resolve().is_relative_to(root) and actual.is_file() and not any((root / Path(*path.parts[:i])).is_symlink() for i in range(1, len(path.parts) + 1)),
                'artifact is absent or a symlink')
        before = actual.stat()
        require(before.st_size > 0, 'empty artifact')
        require(re.fullmatch('[0-9a-f]{64}', item['sha256']), 'artifact hash is malformed')
        if role in SMALL_ROLES:
            require(before.st_size <= 16 * 1024 * 1024, 'summary is too large')
            data = actual.read_bytes()
            require(hashlib.sha256(data).hexdigest() == item['sha256'], 'artifact checksum differs')
            payloads[name] = json.loads(data)
        else:
            require(sha(actual) == item['sha256'], 'artifact checksum differs')
        require(content_state(actual.stat()) == content_state(before), 'artifact changed while reading')
        entries[name], states[name] = item, content_state(before)
        by_role[role].append(name)
    require(all(by_role.values()), 'required artifact category is missing')
    require(inventory(root, manifest_path) == set(entries), 'package contains missing or unlisted files')
    source_files = source.get('sourceFiles', {})
    require(all(p in source_files for p in ('README.md', 'contracts/public.yaml', 'contracts/internal.yaml', 'contracts/ring.yaml',
                                         'docs/adr/004-attempt-closure.md', 'docs/adr/005-outbox-handoff.md'))
            and any(p.startswith('contracts/events/') for p in source_files), 'source contract inventory is incomplete')
    require(set(by_role['source']) == set(source_files), 'source-file inventory differs from the baseline')
    for name, expected in source_files.items(): require(entries[name]['sha256'] == expected, 'packaged source file differs from Git baseline')

    def single(role):
        require(len(by_role[role]) == 1, 'ambiguous summary role: ' + role)
        return payloads[by_role[role][0]]

    def summary(role):
        record = single(role)
        require(record.get('sourceCommit') == commit and record.get('status') == 'PASS', 'stale or incomplete summary: ' + role)
        references = record.get('rawArtifacts', [])
        require(references and len(set(references)) == len(references) and all(p in by_role['evidence'] for p in references),
                'summary lacks retained raw evidence: ' + role)
        return record

    checks = summary('checks')
    require(all(checks.get('checks', {}).get(name) == 'PASS' for name in LOCAL_CHECKS), 'local verification receipt is incomplete')
    gate = summary('gate')
    require(all(gate.get('checks', {}).get(name) == 'PASS' for name in ENHANCED_CHECKS), 'enhanced-gate declaration is incomplete')
    course = gate.get('historicalCourseGate', {})
    require(re.fullmatch('[0-9a-f]{40}', course.get('sourceCommit', '')), 'historical course-gate source is missing')
    demos = course.get('demos', [])
    require(len(demos) == 18 and {(d['round'], d['demo']) for d in demos} == {(r, d) for r in (1, 2) for d in range(1, 10)}
            and all(d.get('status') == 'PASS' for d in demos), 'course-gate declaration is not 18 distinct passes')
    performance = summary('performance')
    require(performance.get('pendingMeasurements') is False and performance.get('acknowledgedWriteAudit') == 'PASS', 'performance/ledger declaration remains pending')
    offline = summary('offline')
    demos = offline.get('demos', [])
    require(len(demos) == 9 and {d['demo'] for d in demos} == set(range(1, 10)) and all(d.get('status') == 'PASS' for d in demos), 'offline declaration is not nine distinct passes')
    isolation = offline.get('isolation', {})
    nodes = isolation.get('nodes', [])
    require(isolation.get('runnerBlocked') is True and len(nodes) == 5 and len({n['name'] for n in nodes}) == 5
            and all(n.get('externalBlocked') is True and n.get('name') for n in nodes), 'offline isolation declaration lacks runner/five-node coverage')
    require(len(by_role['offline-kit']) == 1 and offline.get('archiveSHA256') == entries[by_role['offline-kit'][0]]['sha256'], 'offline evidence describes another archive')

    images = {}
    for name in by_role['build-provenance']:
        record = payloads[name]
        require(record.get('schemaVersion') == 1 and record.get('kind') == 'reliefgrid-build-provenance' and record.get('ok') is True
                and record.get('source') == dict(commit=commit, tree=tree, dirty=False), 'build record is unbound, dirty or stale')
        require(re.fullmatch('[0-9a-f]{64}', record['contextArchiveSHA256']), 'build context hash is missing')
        for image in record['images']:
            role = image['component']
            require(role not in images, 'duplicate image build record')
            require(image['labels'].get('org.opencontainers.image.revision') == commit and image['labels'].get('io.reliefgrid.source-tree') == tree
                    and image['labels'].get('io.reliefgrid.context-sha256') == record['contextArchiveSHA256'], 'image/source labels differ')
            immutable(image['imageId'])
            images[role] = image
    require(set(images) == {'api', 'matcher', 'ui', 'coordinator', 'storagenode', 'tools'}, 'complete six-image build inventory is required')
    fixed, binding = single('fixed-profile'), single('runtime-binding')
    require(fixed.get('ok') is True and fixed['source'].get('collectorCommit') == commit and fixed['source'].get('collectorTree') == tree
            and fixed['source'].get('collectorDirty') is False and fixed['fixedProfile'].get('fixedReplicas') == dict(api=2, matcher=2), 'fixed-profile observation is stale or incomplete')
    require(binding.get('ok') is True and binding.get('kind') == 'reliefgrid-runtime-build-binding' and binding.get('schemaVersion') == 1
            and binding.get('source') == dict(commit=commit, tree=tree, dirty=False), 'runtime binding is stale or missing')
    require(binding.get('captureSHA256') == entries[by_role['fixed-profile'][0]]['sha256']
            and sorted(binding.get('buildRecordSHA256', [])) == sorted(entries[p]['sha256'] for p in by_role['build-provenance']), 'runtime binding references different evidence')
    observed, seen = [], set()
    for pod in fixed['fixedProfile']['pods']:
        role, status, spec = pod['metadata']['labels']['app.kubernetes.io/component'], pod['status'], pod['spec']
        require(role in ('api', 'matcher') and pod['metadata'].get('uid') and pod['metadata']['uid'] not in seen
                and not pod['metadata'].get('deletionTimestamp'), 'fixed-profile pod identity is incomplete')
        seen.add(pod['metadata']['uid'])
        require(status.get('phase') == 'Running' and any(c.get('type') == 'Ready' and c.get('status') == 'True' for c in status.get('conditions', []))
                and len(spec['containers']) == len(status['containerStatuses']) == 1 and status['containerStatuses'][0].get('ready') is True, 'fixed-profile pod is not Ready')
        observed.append(dict(component=role, requestedImage=spec['containers'][0]['image'], runtimeImageID=status['containerStatuses'][0]['imageID']))
    tools = fixed['tools']
    require(tools['job']['status'].get('succeeded') == 1 and tools['job']['status'].get('failed', 0) == 0 and tools['job']['status'].get('active', 0) == 0
            and tools['pod']['status'].get('phase') == 'Succeeded' and tools['pod']['status']['containerStatuses']
            and all(s.get('state', {}).get('terminated', {}).get('exitCode') == 0 for s in tools['pod']['status']['containerStatuses']), 'tools completion evidence is incomplete')
    for tool in tools['containers']: observed.append(dict(component='tools', requestedImage=tool['image'], runtimeImageID=tool['imageID']))
    require(Counter(o['component'] for o in observed) == Counter(api=2, matcher=2, tools=1), 'runtime observation coverage is incomplete')
    require(sorted(json.dumps(o, sort_keys=True) for o in observed) == sorted(json.dumps(o, sort_keys=True) for o in binding['observations']), 'binding observations differ from capture')
    for item in observed:
        image = images[item['component']]
        require(item['requestedImage'] in [image['image'], *image.get('repoDigests', [])] and immutable(item['runtimeImageID']) in
                {immutable(i) for i in [image['imageId'], *image.get('repoDigests', [])]}, 'runtime image is not bound to the recorded build')
    for name, state in states.items(): require(content_state((root / name).stat()) == state, 'artifact changed during verification')
    require(inventory(root, manifest_path) == set(entries), 'package inventory changed during verification')
    return dict(ok=True, schemaVersion=1, kind='reliefgrid-package-readiness', sourceCommit=commit, sourceTree=tree,
                releaseTag=manifest['releaseTag'], artifactCount=len(entries),
                scope='mechanical inventory/source/evidence consistency only; owner declarations, semantic results, publication and playback are not independently certified')


def source_identity(repo, tag):
    def git(*args):
        return subprocess.run(['git', '-C', str(repo), *args], capture_output=True, check=True, timeout=30).stdout
    git('check-ref-format', 'refs/tags/' + tag)
    commit = git('rev-parse', 'HEAD').decode().strip()
    files = ['README.md', 'contracts/public.yaml', 'contracts/internal.yaml', 'contracts/ring.yaml']
    files += [p for p in git('ls-tree', '-r', '--name-only', commit, '--', 'contracts/events').decode().splitlines() if p.endswith('.json')]
    files += [p for p in git('ls-tree', '-r', '--name-only', commit, '--', 'docs/adr').decode().splitlines() if p.endswith('.md')]
    return dict(commit=commit, tree=git('rev-parse', 'HEAD^{tree}').decode().strip(), dirty=bool(git('status', '--porcelain', '--untracked-files=all').strip()),
                tagCommit=git('rev-parse', 'refs/tags/' + tag + '^{commit}').decode().strip(),
                sourceFiles={p: hashlib.sha256(git('show', commit + ':' + p)).hexdigest() for p in files})


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--package-root', type=Path, required=True)
    parser.add_argument('--manifest', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--repo', type=Path, default=Path(__file__).resolve().parents[1])
    args = parser.parse_args()
    result = dict(ok=False)
    try:
        require(not args.output.resolve().is_relative_to(args.package_root.resolve()) and not args.output.resolve().is_relative_to(args.repo.resolve())
                and not args.output.exists(), 'output must be new and outside the package and checkout')
        manifest_bytes = args.manifest.read_bytes()
        manifest = json.loads(manifest_bytes)
        before = source_identity(args.repo, manifest['releaseTag'])
        result = validate(args.package_root, manifest, before, args.manifest)
        require(before == source_identity(args.repo, manifest['releaseTag']), 'source changed during package verification')
        require(args.manifest.read_bytes() == manifest_bytes, 'manifest changed during verification')
        result['manifestSHA256'] = hashlib.sha256(manifest_bytes).hexdigest()
    except (ReadinessError, OSError, ValueError, KeyError, TypeError, subprocess.SubprocessError) as error:
        result = dict(ok=False, error=str(error) if isinstance(error, ReadinessError) else 'package/source input verification failed')
    if not args.output.exists() and not args.output.resolve().is_relative_to(args.package_root.resolve()) and not args.output.resolve().is_relative_to(args.repo.resolve()):
        with args.output.open('x') as stream: stream.write(json.dumps(result, indent=2, sort_keys=True) + '\n')
    print('PACKAGE CONSISTENCY: ' + ('PASS (owner review still required)' if result['ok'] else 'FAIL'))
    return 0 if result['ok'] else 1


if __name__ == '__main__': raise SystemExit(main())
