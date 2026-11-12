import copy
import hashlib
import json
from pathlib import Path
import tempfile
import sys
import unittest
from unittest.mock import patch

import package_readiness as package


class PackageTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.commit, self.tree = 'a' * 40, 'b' * 40
        self.manifest = dict(schemaVersion=1, sourceCommit=self.commit, sourceTree=self.tree, releaseTag='v1.0.0',
            repositoryURL='https://github.com/owner/fixture', publicVisibilityConfirmed=True,
            reportFormatConfirmed=True, ownerSignoff=dict(A='owner-a', S='owner-s'), unresolved=[], artifacts=[])
        self.source = dict(commit=self.commit, tree=self.tree, dirty=False, tagCommit=self.commit, sourceFiles={})
        for name in ('README.md', 'contracts/public.yaml', 'contracts/internal.yaml', 'contracts/ring.yaml', 'contracts/events/request.json',
                     'docs/adr/004-attempt-closure.md', 'docs/adr/005-outbox-handoff.md', 'docs/adr/006-read-fence-study.md'):
            self.add(name, 'source', 'fixture source')
            self.source['sourceFiles'][name] = self.manifest['artifacts'][-1]['sha256']
        self.add('report.md', 'report', 'Historical evidence remains qualified; pending at its original date.\n')
        self.add('recording.mp4', 'recording', 'fixture recording bytes, not a real recording')
        self.add('kit.tar', 'offline-kit', 'fixture kit bytes')
        self.add('raw/run.txt', 'evidence', 'retained command output')
        image_labels = {'org.opencontainers.image.revision': self.commit, 'io.reliefgrid.source-tree': self.tree, 'io.reliefgrid.context-sha256': 'c' * 64}
        image_ids = {role: 'sha256:' + format(index, '064x') for index, role in enumerate(('api', 'matcher', 'ui', 'coordinator', 'storagenode', 'tools'), 1)}
        self.add('build.json', 'build-provenance', dict(schemaVersion=1, kind='reliefgrid-build-provenance', ok=True,
            source=dict(commit=self.commit, tree=self.tree, dirty=False), contextArchiveSHA256='c' * 64,
            images=[dict(component=c, image=c + ':fixture', imageId=image_ids[c], repoDigests=[], labels=image_labels) for c in image_ids]))
        pods, observations = [], []
        for role in ('api', 'matcher'):
            for index in range(2):
                pods.append(dict(metadata=dict(uid=role + str(index), labels={'app.kubernetes.io/component': role}),
                    spec=dict(containers=[dict(name=role, image=role + ':fixture')]),
                    status=dict(phase='Running', containerStatuses=[dict(name=role, ready=True, imageID=image_ids[role])], conditions=[dict(type='Ready', status='True')])))
                observations.append(dict(component=role, requestedImage=role + ':fixture', runtimeImageID=image_ids[role]))
        observations.append(dict(component='tools', requestedImage='tools:fixture', runtimeImageID=image_ids['tools']))
        self.add('fixed.json', 'fixed-profile', dict(ok=True, source=dict(collectorCommit=self.commit, collectorTree=self.tree, collectorDirty=False),
            fixedProfile=dict(fixedReplicas=dict(api=2, matcher=2), pods=pods),
            tools=dict(containers=[dict(image='tools:fixture', imageID=image_ids['tools'])], job=dict(status=dict(succeeded=1, failed=0, active=0)),
                       pod=dict(status=dict(phase='Succeeded', containerStatuses=[dict(state=dict(terminated=dict(exitCode=0)))])))))
        hashes = {a['path']: a['sha256'] for a in self.manifest['artifacts']}
        self.add('binding.json', 'runtime-binding', dict(schemaVersion=1, kind='reliefgrid-runtime-build-binding', ok=True,
            source=dict(commit=self.commit, tree=self.tree, dirty=False), buildRecordSHA256=[hashes['build.json']], captureSHA256=hashes['fixed.json'],
            observations=observations))
        self.add('checks.json', 'checks', dict(sourceCommit=self.commit, status='PASS', rawArtifacts=['raw/run.txt'],
            checks={name: 'PASS' for name in package.LOCAL_CHECKS}))
        self.add('gate.json', 'gate', dict(sourceCommit=self.commit, status='PASS', rawArtifacts=['raw/run.txt'],
            checks={name: 'PASS' for name in package.ENHANCED_CHECKS},
            historicalCourseGate=dict(sourceCommit='c' * 40, demos=[dict(round=r, demo=d, status='PASS') for r in (1, 2) for d in range(1, 10)])))
        self.add('performance.json', 'performance', dict(sourceCommit=self.commit, status='PASS', pendingMeasurements=False, acknowledgedWriteAudit='PASS', rawArtifacts=['raw/run.txt']))
        self.add('offline.json', 'offline', dict(sourceCommit=self.commit, status='PASS', rawArtifacts=['raw/run.txt'],
            archiveSHA256=hashes['kit.tar'], demos=[dict(demo=d, status='PASS') for d in range(1, 10)],
            isolation=dict(runnerBlocked=True, nodes=[dict(name='node-' + str(n), externalBlocked=True) for n in range(5)])))

    def add(self, name, role, value):
        path = self.root / name
        path.parent.mkdir(parents=True, exist_ok=True)
        data = json.dumps(value).encode() if isinstance(value, dict) else value.encode()
        path.write_bytes(data)
        self.manifest['artifacts'].append(dict(path=name, role=role, sha256=hashlib.sha256(data).hexdigest()))

    def replace(self, name, edit):
        value = json.loads((self.root / name).read_bytes())
        edit(value)
        data = json.dumps(value).encode()
        (self.root / name).write_bytes(data)
        next(a for a in self.manifest['artifacts'] if a['path'] == name)['sha256'] = hashlib.sha256(data).hexdigest()

    def test_complete_consistent_declared_package(self):
        result = package.validate(self.root, self.manifest, self.source)
        self.assertTrue(result['ok'])
        self.assertIn('mechanical', result['scope'])

    def test_missing_unlisted_changed_or_escaped_artifact_is_rejected(self):
        original_report = (self.root / 'report.md').read_bytes()
        for mode in ('missing', 'unlisted', 'changed', 'escape', 'duplicate'):
            with self.subTest(mode=mode):
                root = self.root / mode
                # Independent fixture instance avoids destructive restore/reset.
                manifest = copy.deepcopy(self.manifest)
                if mode == 'missing': manifest['artifacts'] = [a for a in manifest['artifacts'] if a['role'] != 'report']
                if mode == 'unlisted': (self.root / 'extra').write_text('unlisted')
                if mode == 'changed': (self.root / 'report.md').write_text('tampered')
                if mode == 'escape': manifest['artifacts'][0]['path'] = '../outside'
                if mode == 'duplicate': manifest['artifacts'].append(manifest['artifacts'][0])
                with self.assertRaises(package.ReadinessError): package.validate(self.root, manifest, self.source)
                (self.root / 'extra').unlink(missing_ok=True)
                (self.root / 'report.md').write_bytes(original_report)

    def test_stale_pending_or_incomplete_summaries_do_not_pass(self):
        for name, change in (
            ('gate.json', lambda r: r['historicalCourseGate']['demos'].pop()),
            ('gate.json', lambda r: r['checks'].update(autoscaling='PENDING')),
            ('performance.json', lambda r: r.update(pendingMeasurements=True)),
            ('performance.json', lambda r: r.update(sourceCommit='c' * 40)),
            ('offline.json', lambda r: r['isolation']['nodes'].pop()),
            ('offline.json', lambda r: r['demos'][0].update(status='FAIL')),
            ('checks.json', lambda r: r.update(rawArtifacts=[])),
        ):
            with self.subTest(name=name):
                original = (self.root / name).read_bytes()
                entry = next(a for a in self.manifest['artifacts'] if a['path'] == name)
                old_hash = entry['sha256']
                self.replace(name, change)
                with self.assertRaises(package.ReadinessError): package.validate(self.root, self.manifest, self.source)
                (self.root / name).write_bytes(original)
                entry['sha256'] = old_hash

    def test_unresolved_signoff_source_and_binding_are_required(self):
        for edit in (lambda m: m.update(unresolved=['auth rollout']), lambda m: m.update(reportFormatConfirmed=False),
                     lambda m: m['ownerSignoff'].pop('S'), lambda m: m.update(publicVisibilityConfirmed=False),
                     lambda m: m.update(sourceCommit='c' * 40)):
            manifest = copy.deepcopy(self.manifest)
            edit(manifest)
            with self.assertRaises(package.ReadinessError): package.validate(self.root, manifest, self.source)
        source = dict(self.source, dirty=True)
        with self.assertRaises(package.ReadinessError): package.validate(self.root, self.manifest, source)
        self.replace('binding.json', lambda r: r.update(captureSHA256='0' * 64))
        with self.assertRaises(package.ReadinessError): package.validate(self.root, self.manifest, self.source)

    def test_symlinks_and_qualification_preservation(self):
        self.assertTrue(package.validate(self.root, self.manifest, self.source)['ok'])
        (self.root / 'link').symlink_to(self.root / 'report.md')
        with self.assertRaises(package.ReadinessError): package.validate(self.root, self.manifest, self.source)

    def test_reading_an_old_artifact_does_not_count_as_a_content_change(self):
        import os
        report = self.root / 'report.md'
        os.utime(report, ns=(1, report.stat().st_mtime_ns))
        self.assertTrue(package.validate(self.root, self.manifest, self.source)['ok'])

    def test_real_git_source_and_tag_identity(self):
        import subprocess
        with tempfile.TemporaryDirectory() as temporary:
            repo = Path(temporary)
            def git(*args):
                return subprocess.run(['git', '-C', str(repo), *args], capture_output=True, text=True, check=True).stdout.strip()
            git('init', '-q')
            git('config', 'user.name', 'Fixture')
            git('config', 'user.email', 'fixture@example.invalid')
            for name in self.source['sourceFiles']:
                target = repo / name
                target.parent.mkdir(parents=True, exist_ok=True)
                target.write_text('fixture source')
            git('add', '.')
            git('commit', '-qm', 'fixture')
            git('tag', 'v1.0.0')
            identity = package.source_identity(repo, 'v1.0.0')
            self.assertEqual(identity['commit'], identity['tagCommit'])
            self.assertFalse(identity['dirty'])
            self.assertEqual(identity['sourceFiles'], self.source['sourceFiles'])
            (repo / 'README.md').write_text('changed')
            self.assertTrue(package.source_identity(repo, 'v1.0.0')['dirty'])

    def test_json_mutation_cannot_satisfy_binding_or_source_hashes(self):
        for name, change in (('build.json', lambda r: r.update(ok=False)),
                             ('fixed.json', lambda r: r['source'].update(collectorDirty=True)),
                             ('binding.json', lambda r: r['observations'].pop()),
                             ('offline.json', lambda r: r.update(archiveSHA256='0' * 64))):
            original = (self.root / name).read_bytes()
            entry = next(a for a in self.manifest['artifacts'] if a['path'] == name)
            old_hash = entry['sha256']
            self.replace(name, change)
            with self.assertRaises(package.ReadinessError): package.validate(self.root, self.manifest, self.source)
            (self.root / name).write_bytes(original)
            entry['sha256'] = old_hash

    def test_cli_records_manifest_hash_and_never_writes_inside_source_or_package(self):
        manifest = self.root / 'manifest.json'
        manifest.write_text(json.dumps(self.manifest))
        with tempfile.TemporaryDirectory() as external:
            external = Path(external)
            repo = external / 'checkout'
            repo.mkdir()
            for destination, expected in ((external / 'result.json', 0), (self.root / 'result.json', 1), (repo / 'result.json', 1)):
                argv = ['check', '--package-root', str(self.root), '--manifest', str(manifest), '--repo', str(repo), '--output', str(destination)]
                with patch.object(package, 'source_identity', return_value=self.source), patch.object(sys, 'argv', argv):
                    self.assertEqual(package.main(), expected)
                if expected:
                    self.assertFalse(destination.exists())
                else:
                    result = json.loads(destination.read_bytes())
                    self.assertTrue(result['ok'])
                    self.assertEqual(result['manifestSHA256'], hashlib.sha256(manifest.read_bytes()).hexdigest())


if __name__ == '__main__': unittest.main()
