#!/usr/bin/env python3
"""Disposable namespace: old chart upgrade, PVC restart, fresh monitoring install."""
import argparse, base64, contextlib, hashlib, json, os, re, shutil, subprocess, tempfile, time, urllib.parse, uuid
from pathlib import Path
ROOT = Path(__file__).resolve().parents[1]

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--context', default='kind-reliefgrid')
    parser.add_argument('--out', type=Path, required=True)
    args = parser.parse_args()
    assert args.context.startswith('kind-')
    args.out.mkdir(parents=True, exist_ok=False)
    ns = 'reliefgrid-monitor-' + uuid.uuid4().hex[:8]
    kc = ['kubectl', '--context', args.context, '-n', ns, '--request-timeout=10s']

    def run(command, **kwargs):
        try:
            return subprocess.check_output(command, cwd=ROOT, text=True, timeout=kwargs.pop('timeout', 60), **kwargs)
        except subprocess.CalledProcessError as error:
            if command[0] == 'go':
                (args.out / 'failed-go.txt').write_text(error.output or '')
                print(error.output or '', flush=True)
            raise

    def kube(*command):
        return run(kc + list(command))

    def helm(chart):
        return run(['helm', '--kube-context', args.context, 'upgrade', '--install', 'reliefgrid', str(chart), '-n', ns, '--set', 'storage.enabled=false', '--set', 'app.enabled=false', '--set', 'traefik.enabled=false', '--wait', '--timeout', '4m'], timeout=260)

    def secret():
        return json.loads(kube('get', 'secret', 'reliefgrid-rabbitmq', '-o', 'json'))['data']

    def pod():
        return json.loads(kube('get', 'pod', 'reliefgrid-rabbitmq-0', '-o', 'json'))

    @contextlib.contextmanager
    def forward(port):
        path = args.out / ('forward-' + str(port) + '.txt')
        with path.open('w') as log:
            proc = subprocess.Popen(kc + ['port-forward', '--address=127.0.0.1', 'svc/reliefgrid-rabbitmq', ':' + str(port)], stdout=log, stderr=subprocess.STDOUT)
            try:
                end = time.monotonic() + 20
                while time.monotonic() < end:
                    match = re.search('Forwarding from 127\\.0\\.0\\.1:(\\d+) ->', path.read_text())
                    if match:
                        yield match[1]
                        return
                    assert proc.poll() is None
                    time.sleep(0.05)
                raise TimeoutError('monitor forward missing')
            finally:
                proc.terminate()
                proc.wait(timeout=5)

    def env(data, amqp_port, http_port=None):
        decode = lambda key: base64.b64decode(data[key]).decode()
        address = lambda scheme, user, password, port: scheme + '://' + urllib.parse.quote(user, safe='') + ':' + urllib.parse.quote(password, safe='') + '@127.0.0.1:' + port + '/'
        values = dict(os.environ, MONITOR_APP_AMQP=address('amqp', decode('username'), decode('password'), amqp_port))
        if http_port:
            values.update(MONITOR_READ_AMQP=address('amqp', decode('ui-monitor-username'), decode('ui-monitor-password'), amqp_port), MONITOR_HTTP=address('http', decode('ui-monitor-username'), decode('ui-monitor-password'), http_port))
        return values

    def probe(seed=False):
        with forward(5672) as amqp_port:
            if seed:
                return run(['go', 'test', '-race', '-tags', 'monitor', '-count=1', '-run', '^TestMonitorSeed$', './scripts'], env=env(secret(), amqp_port), timeout=45)
            with forward(15672) as http_port:
                return run(['go', 'test', '-race', '-tags', 'monitor', '-count=1', '-run', '^TestUIBrokerMonitorLive$', './scripts'], env=env(secret(), amqp_port, http_port), timeout=60)
    run(['bash', 'scripts/app-chart-deps.sh'])
    result = dict(result='FAIL', namespace=ns, context=args.context, stages=[])
    kube('create', 'namespace', ns)
    try:
        with tempfile.TemporaryDirectory(prefix='reliefgrid-monitor-baseline-') as directory:
            baseline = Path(directory) / 'chart'
            shutil.copytree(ROOT / 'deploy/helm/reliefgrid', baseline)
            for name in ['rabbitmq-secret.yaml', 'rabbitmq-statefulset.yaml', 'app.yaml']:
                (baseline / 'templates' / name).write_text(run(['git', 'show', '0fa3a63cbddb283a8ddb5f73b100a3afa967202f:deploy/helm/reliefgrid/templates/' + name]))
            (args.out / 'baseline.txt').write_text(helm(baseline))
            probe(seed=True)
            old_uid = pod()['metadata']['uid']
            (args.out / 'upgrade.txt').write_text(helm(ROOT / 'deploy/helm/reliefgrid'))
            assert pod()['metadata']['uid'] != old_uid, 'migration did not roll the broker'
            data = secret()
            identity = hashlib.sha256(data['ui-monitor-password'].encode()).hexdigest()
            (args.out / 'upgrade-probe.txt').write_text(probe())
            result['stages'].append('existing PVC upgrade: stats readable, mutations/AMQP denied, application message survived')
            uid = pod()['metadata']['uid']
            helm(ROOT / 'deploy/helm/reliefgrid')
            assert pod()['metadata']['uid'] == uid and hashlib.sha256(secret()['ui-monitor-password'].encode()).hexdigest() == identity, 'no-op upgrade changed identity or restarted broker'
            probe(seed=True)
            kube('delete', 'pod', 'reliefgrid-rabbitmq-0', '--wait=true')
            kube('wait', '--for=condition=Ready', 'pod/reliefgrid-rabbitmq-0', '--timeout=120s')
            assert hashlib.sha256(secret()['ui-monitor-password'].encode()).hexdigest() == identity
            (args.out / 'restart-probe.txt').write_text(probe())
            result['stages'].append('same PVC restart and no-op upgrade: credential stable and message survived')
            run(['helm', '--kube-context', args.context, 'uninstall', 'reliefgrid', '-n', ns, '--wait'], timeout=90)
            kube('delete', 'pvc', '--all', '--wait=true')
            (args.out / 'fresh.txt').write_text(helm(ROOT / 'deploy/helm/reliefgrid'))
            probe(seed=True)
            (args.out / 'fresh-probe.txt').write_text(probe())
            result['stages'].append('fresh PVC: read-only account works, denied operations remain denied')
            result['result'] = 'PASS'
            print('UI MONITOR LIVE: PASS', flush=True)
    finally:
        (args.out / 'result.json').write_text(json.dumps(result, indent=2) + '\n')
        subprocess.run(kc + ['delete', 'namespace', ns, '--ignore-not-found', '--wait=false'], check=True, text=True, timeout=20)
if __name__ == '__main__':
    main()
