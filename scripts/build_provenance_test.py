import copy
import json
from pathlib import Path
import subprocess
import tempfile
import unittest

import build_provenance as build


class ProvenanceTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name) / 'repo'
        self.root.mkdir()
        self.git('init', '-q')
        self.git('config', 'user.name', 'Fixture')
        self.git('config', 'user.email', 'fixture@example.invalid')
        for name, value in {'go.mod': 'module fixture\n', 'go.sum': '', 'tools.mk': 'GO_VERSION := 1.27.2\n',
                            '.gitignore': '*.ignored\n', '.dockerignore': '*.ignored\n', 'cmd/api/Dockerfile': 'FROM fixture@sha256:' + 'a' * 64 + '\n'}.items():
            path = self.root / name
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(value)
        self.git('add', '.')
        self.git('commit', '-qm', 'fixture')

    def git(self, *args):
        return subprocess.run(['git', '-C', str(self.root), *args], capture_output=True, text=True, check=True).stdout.strip()

    def image(self, command):
        if command[1:3] == ['image', 'inspect']:
            return json.dumps([dict(Id='sha256:' + 'b' * 64, RepoDigests=['fixture/api@sha256:' + 'c' * 64],
                 Architecture='amd64', Os='linux', Config=dict(User='65532:65532', Entrypoint=['/api'], Labels=self.labels))])
        if command[1] == 'version':
            return 'fixture-docker-version'
        if command[1] == 'build':
            Path(command[command.index('--iidfile') + 1]).write_text('sha256:' + 'b' * 64)
            self.labels = dict(command[i + 1].split('=', 1) for i, value in enumerate(command) if value == '--label')
            context = Path(command[-1])
            self.assertNotEqual(context, self.root)
            self.assertEqual((context / 'go.mod').read_text(), 'module fixture\n')
            self.assertFalse((context / '.git').exists())
            self.assertFalse((context / 'secret.ignored').exists())
            return ''
        self.fail('unexpected command: ' + str(command[:3]))

    def test_snapshot_build_binds_source_context_and_immutable_image(self):
        (self.root / 'secret.ignored').write_text('PRIVATE-MARKER')
        output = Path(self.tmp.name) / 'evidence'
        result = build.build_images(self.root, output, 'fixture', ['api'], self.image)
        self.assertTrue(result['ok'])
        self.assertEqual(result['source']['commit'], self.git('rev-parse', 'HEAD'))
        self.assertFalse(result['source']['dirty'])
        self.assertEqual(len(result['contextArchiveSHA256']), 64)
        self.assertEqual(result['images'][0]['imageId'], 'sha256:' + 'b' * 64)
        self.assertEqual(result['images'][0]['repoDigests'], ['fixture/api@sha256:' + 'c' * 64])
        self.assertIn('cmd/api/Dockerfile', result['inputs'])
        self.assertTrue((output / 'image-api.json').exists())

    def test_dirty_untracked_or_mid_build_source_changes_never_pass(self):
        for mode in ('tracked', 'untracked', 'during'):
            with self.subTest(mode=mode):
                if mode == 'tracked':
                    (self.root / 'go.mod').write_text('changed')
                if mode == 'untracked':
                    (self.root / 'untracked').write_text('not captured')
                def execute(command):
                    if mode == 'during' and command[1] == 'build':
                        (self.root / 'go.mod').write_text('changed during build')
                    return self.image(command)
                with self.assertRaises(build.ProvenanceError):
                    build.build_images(self.root, Path(self.tmp.name) / mode, 'fixture', ['api'], execute)
                (self.root / 'go.mod').write_text('module fixture\n')
                (self.root / 'untracked').unlink(missing_ok=True)

    def test_invalid_image_failure_or_missing_component_leaves_no_success_manifest(self):
        for mode in ('identity', 'user', 'build', 'component', 'labels'):
            def execute(command):
                if mode == 'build' and command[1] == 'build':
                    raise build.ProvenanceError('fixture build failed')
                result = self.image(command)
                if command[1:3] == ['image', 'inspect']:
                    record = json.loads(result)
                    if mode == 'identity': record[0]['Id'] = 'mutable-tag'
                    if mode == 'user': record[0]['Config']['User'] = 'root'
                    if mode == 'labels': record[0]['Config']['Labels'] = {}
                    return json.dumps(record)
                return result
            output = Path(self.tmp.name) / mode
            with self.subTest(mode=mode), self.assertRaises(build.ProvenanceError):
                build.build_images(self.root, output, 'fixture', ['ui' if mode == 'component' else 'api'], execute)
            self.assertFalse((output / 'build-provenance.json').exists())

    def test_does_not_overwrite_or_put_evidence_inside_source(self):
        existing = Path(self.tmp.name) / 'existing'
        existing.mkdir()
        for output in (existing, self.root / 'output'):
            with self.assertRaises(build.ProvenanceError):
                build.build_images(self.root, output, 'fixture', ['api'], self.image)

    def test_build_uses_iid_not_a_racy_tag_inspection(self):
        def execute(command):
            if command[1] == 'build':
                Path(command[command.index('--iidfile') + 1]).write_text('sha256:' + 'b' * 64)
                self.assertIn('--label', command)
            if command[1:3] == ['image', 'inspect']:
                self.assertEqual(command[-1], 'sha256:' + 'b' * 64)
            return self.image(command)
        result = build.build_images(self.root, Path(self.tmp.name) / 'iid', 'fixture', ['api'], execute)
        self.assertTrue(result['ok'])

    def test_binding_needs_exact_baseline_and_every_runtime_digest(self):
        record = build.build_images(self.root, Path(self.tmp.name) / 'binding', 'fixture', ['api'], self.image)
        api = record['images'][0]
        record['images'] = []
        for role in ('api', 'matcher', 'tools'):
            image = copy.deepcopy(api)
            image.update(component=role, image='reliefgrid/' + role + ':fixture')
            record['images'].append(image)
        capture = dict(ok=True, source=dict(collectorCommit=record['source']['commit'], collectorTree=record['source']['tree'], collectorDirty=False),
                       fixedProfile=dict(pods=[]), tools=dict(containers=[dict(image='reliefgrid/tools:fixture', imageID='containerd://sha256:' + 'b' * 64)]))
        for role in ('api', 'matcher'):
            for index in range(2):
                capture['fixedProfile']['pods'].append(dict(metadata=dict(uid=role + str(index), labels={'app.kubernetes.io/component': role}),
                    spec=dict(containers=[dict(name=role, image='reliefgrid/' + role + ':fixture')]),
                    status=dict(containerStatuses=[dict(name=role, imageID='containerd://sha256:' + 'b' * 64)])))
        result = build.bind_runtime([record], capture)
        self.assertEqual(len(result['observations']), 5)
        mutations = [lambda c: c['source'].update(collectorDirty=True),
                     lambda c: c['source'].update(collectorCommit='f' * 40),
                     lambda c: c['fixedProfile']['pods'].pop(),
                     lambda c: c['fixedProfile']['pods'][1]['metadata'].update(uid='api0'),
                     lambda c: c['tools']['containers'][0].update(imageID='containerd://sha256:' + 'e' * 64),
                     lambda c: c['fixedProfile']['pods'][0]['spec']['containers'][0].update(image='different:tag')]
        for change in mutations:
            altered = copy.deepcopy(capture)
            change(altered)
            with self.assertRaises(build.ProvenanceError): build.bind_runtime([record], altered)
        altered = copy.deepcopy(record)
        altered['images'][0]['labels']['org.opencontainers.image.revision'] = 'e' * 40
        with self.assertRaises(build.ProvenanceError): build.bind_runtime([altered], capture)


if __name__ == '__main__': unittest.main()
