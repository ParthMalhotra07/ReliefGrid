"""Exercise real entry points with isolated files and fake external commands."""
import http.server
import os
from pathlib import Path
import shutil
import subprocess
import tempfile
import threading
import unittest

SOURCE = Path(__file__).resolve().parent


class EvidenceIsolation(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        scripts = self.root / 'scripts'
        scripts.mkdir()
        for name in ('app-images.sh', 'app-deploy.sh', 'e2e-smoke.py', 'app_evidence.py'):
            if (SOURCE / name).exists():
                shutil.copyfile(SOURCE / name, scripts / name)
        self.saved = self.root / 'docs/evidence/A1.5'
        self.saved.mkdir(parents=True)
        self.sentinels = {name: b'original accepted evidence\n' for name in
                          ('image-api.json', 'image-matcher.json', 'image-ui.json', 'image-tag.txt', 'nodes.json')}
        for name, data in self.sentinels.items():
            (self.saved / name).write_bytes(data)
        self.bin = self.root / 'bin'
        self.bin.mkdir()
        self.env = dict(os.environ, PATH=str(self.bin) + ':' + os.environ['PATH'],
                        XDG_CACHE_HOME=str(self.root / 'cache'), APP_IMAGE_TAG='isolation-fixture')
        self.env.pop('A15_EVIDENCE_DIR', None)
        self.command('docker', """#!/usr/bin/env python3
import json, sys
if sys.argv[1:3] == ['image', 'inspect']:
    component = sys.argv[3].split('/')[1].split(':')[0]
    print(json.dumps(dict(Id='sha256:synthetic-fixture', Architecture='amd64',
          Config=dict(User='65532:65532', Entrypoint=['/' + component]))))
""")
        self.command('kubectl', '#!/usr/bin/env python3\nimport json\nprint(json.dumps(dict(items=[])))\n')

    def command(self, name, source):
        path = self.bin / name
        path.write_text(source)
        path.chmod(0o755)

    def execute(self, name, env=None):
        tool = 'python3' if name.endswith('.py') else 'bash'
        return subprocess.run([tool, str(self.root / 'scripts' / name)], cwd=self.root,
                              env=env or self.env, text=True, capture_output=True, timeout=15)

    def unchanged(self):
        for name, original in self.sentinels.items():
            self.assertEqual((self.saved / name).read_bytes(), original,
                             'default run overwrote saved A1.5 evidence: ' + name)
        self.assertEqual(set(path.name for path in self.saved.iterdir()), set(self.sentinels))

    def test_images_default_is_unique_and_preserves_saved_evidence(self):
        for _ in range(2):
            result = self.execute('app-images.sh')
            self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
            self.unchanged()
        runs = list((self.root / 'cache/reliefgrid/evidence').iterdir())
        self.assertEqual(len(runs), 2)
        for run in runs:
            self.assertTrue((run / 'image-api.json').is_file())

    def test_deploy_failure_retains_run_evidence(self):
        result = self.execute('app-deploy.sh')
        self.assertNotEqual(result.returncode, 0, 'invalid placement must fail')
        self.unchanged()
        runs = list((self.root / 'cache/reliefgrid/evidence').iterdir())
        self.assertEqual(len(runs), 1)
        self.assertTrue((runs[0] / 'nodes.json').is_file())

    def test_smoke_failure_does_not_touch_saved_evidence(self):
        class Rejected(http.server.BaseHTTPRequestHandler):
            def do_GET(self):
                self.send_error(501)
            def log_message(self, *_):
                pass
        with http.server.HTTPServer(('127.0.0.1', 0), Rejected) as server:
            worker = threading.Thread(target=server.serve_forever, daemon=True)
            worker.start()
            try:
                result = self.execute('e2e-smoke.py', dict(self.env,
                    E2E_BASE_URL='http://127.0.0.1:' + str(server.server_port), E2E_RUN_ID='isolation'))
            finally:
                server.shutdown()
                worker.join(timeout=2)
        self.assertNotEqual(result.returncode, 0, 'unexpected HTTP status must fail')
        self.unchanged()
        scenarios = list((self.root / 'cache/reliefgrid/evidence').glob('*/e2e-isolation/scenario.json'))
        self.assertEqual(len(scenarios), 1, result.stdout + result.stderr)

    def test_explicit_directory_is_propagated(self):
        selected = self.root / 'explicit chosen run'
        result = self.execute('app-images.sh', dict(self.env, A15_EVIDENCE_DIR=str(selected)))
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertTrue((selected / 'image-tag.txt').is_file())
        self.assertFalse((self.root / 'cache').exists())
        self.unchanged()


if __name__ == '__main__':
    unittest.main()
