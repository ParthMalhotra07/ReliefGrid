#!/usr/bin/env python3
"""Bounded kind-only A4.3 load acceptance; no manual replica changes."""
import base64
import contextlib
import datetime as dt
import json
import os
from pathlib import Path
import re
import subprocess
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid

ROOT = Path(__file__).resolve().parents[1]


def queue_counts(queue):
    values = tuple(queue[name] for name in ('messages_ready', 'messages_unacknowledged'))
    if not all(type(value) is int and value >= 0 for value in values):
        raise AssertionError('missing or invalid broker queue observation')
    return values


def acceptance(samples):
    if len(samples) < 3:
        raise AssertionError('missing scaling observations')
    base = samples[0]
    if base['desired'] != 1 or base['ready'] != 1:
        raise AssertionError('matcher must start at one Ready replica')
    up = [s for s in samples if s['ready'] > 1 and s['queueReady'] > 0]
    if not up:
        raise AssertionError('no Ready scale-up observed alongside queue load')
    final = samples[-1]
    if not final['loadFinished'] or final['desired'] != 1 or final['ready'] != 1:
        raise AssertionError('matcher did not settle back to one Ready replica')
    if final['queueReady'] or final['unacked']:
        raise AssertionError('queue did not drain')
    if not any(s['time'] < final['time'] for s in up):
        raise AssertionError('scale-down must follow scale-up')
    if any(s['desired'] < 1 or s['desired'] > 8 or s['restarts'] for s in samples):
        raise AssertionError('replica bound or restart invariant failed')


def main():
    os.chdir(ROOT)
    context = 'kind-' + os.environ.get('CLUSTER', 'reliefgrid')
    stamp = dt.datetime.now(dt.timezone.utc).strftime('%Y%m%dT%H%M%SZ')
    out = Path(os.environ.get('A43_EVIDENCE_DIR', str(ROOT / 'docs/evidence/A4.3' / stamp)))
    out.mkdir(parents=True, exist_ok=False)
    kc = ['kubectl', '--context', context, '--request-timeout=10s', '-n', 'reliefgrid']

    def run(args, timeout=20):
        return subprocess.check_output(args, text=True, timeout=timeout)

    def kube(*args):
        return json.loads(run(kc + list(args) + ['-o', 'json']))

    def save(name, value):
        (out / name).write_text(json.dumps(value, indent=2) + '\n')

    @contextlib.contextmanager
    def forward(service, port):
        with (out / ('forward-' + service + '.log')).open('w+') as log:
            p = subprocess.Popen(kc + ['port-forward', 'svc/' + service, ':' + str(port), '--address=127.0.0.1'],
                                 stdout=log, stderr=subprocess.STDOUT)
            try:
                deadline = time.monotonic() + 20
                while time.monotonic() < deadline:
                    log.seek(0)
                    match = re.search(r'Forwarding from 127\.0\.0\.1:(\d+)', log.read())
                    if match:
                        yield 'http://127.0.0.1:' + match[1]
                        return
                    if p.poll() is not None:
                        raise RuntimeError('port-forward exited: ' + service)
                    time.sleep(.2)
                raise TimeoutError('port-forward did not become ready: ' + service)
            finally:
                p.terminate()
                p.wait(timeout=10)

    secret = kube('get', 'secret', 'reliefgrid-rabbitmq')['data']
    amqp = urllib.parse.urlsplit(base64.b64decode(secret['amqp-url']).decode())
    user, password = urllib.parse.unquote(amqp.username), urllib.parse.unquote(amqp.password)
    auth = 'Basic ' + base64.b64encode((user + ':' + password).encode()).decode()

    def http(url, method='GET', body=None, broker=False, key=None):
        headers = {'Content-Type': 'application/json'}
        if broker:
            headers['Authorization'] = auth
        if key:
            headers['Idempotency-Key'] = key
        req = urllib.request.Request(url, method=method, headers=headers,
                                     data=None if body is None else json.dumps(body).encode())
        with urllib.request.urlopen(req, timeout=15) as response:
            return json.load(response)

    scaled = kube('get', 'scaledobject', 'reliefgrid-matcher')
    assert scaled['spec']['minReplicaCount'] == 1 and scaled['spec']['maxReplicaCount'] == 8
    hpas = kube('get', 'hpa')['items']
    targets = [h['spec']['scaleTargetRef']['name'] for h in hpas]
    assert sorted(targets) == ['reliefgrid-api', 'reliefgrid-matcher'], targets
    matcher_hpa = next(h for h in hpas if h['spec']['scaleTargetRef']['name'] == 'reliefgrid-matcher')
    assert any(o['uid'] == scaled['metadata']['uid'] and o.get('controller') for o in matcher_hpa['metadata']['ownerReferences'])
    save('controllers.json', {'scaledObject': scaled, 'hpas': hpas})
    save('source.json', {'head': run(['git', 'rev-parse', 'HEAD']).strip(),
                         'changes': run(['git', 'status', '--short']), 'context': context})
    samples = []
    report = {'result': 'FAIL', 'workload': '20000 valid duplicate hints for one terminal request',
              'manualReplicaChanges': False, 'samples': samples}
    k6 = None
    fixture = None
    with forward('reliefgrid-rabbitmq', 15672) as broker, forward('reliefgrid-api', 8080) as api:
        queue = lambda name: http(broker + '/api/queues/%2F/' + name, broker=True)
        dead_before = queue('matching.dead')['messages']

        def sample(load_finished=False):
            q = queue('matching.requests')
            ready_messages, unacked_messages = queue_counts(q)
            d = kube('get', 'deploy', 'reliefgrid-matcher')
            pods = kube('get', 'pods', '-l', 'app.kubernetes.io/component=matcher')['items']
            s = dict(time=dt.datetime.now(dt.timezone.utc).isoformat(), desired=d['spec']['replicas'],
                     ready=d.get('status', {}).get('readyReplicas', 0), queueReady=ready_messages,
                     unacked=unacked_messages, loadFinished=load_finished,
                     restarts=sum(c['restartCount'] for p in pods for c in p.get('status', {}).get('containerStatuses', [])))
            samples.append(s)
            save('report.json', report)
            print(json.dumps(s), flush=True)
            return s

        try:
            # Observe the real controller's idle decision; do not scale manually.
            deadline = time.monotonic() + 150
            while True:
                s = sample()
                if s['desired'] == s['ready'] == 1 and s['queueReady'] == s['unacked'] == 0:
                    samples.clear()
                    samples.append(s)
                    break
                if time.monotonic() >= deadline:
                    raise TimeoutError('baseline queue/minimum did not settle')
                time.sleep(3)
            body = dict(requestType='SHELTER', details='Synthetic A4.3 bounded redelivery fixture',
                        peopleAffected=1, urgency='HIGH', location=dict(lat=-77, lng=-130))
            created = http(api + '/api/v1/requests', 'POST', body, key=str(uuid.uuid4()))
            fixture = created['requestId']
            http(api + '/api/v1/requests/' + fixture + '/status', 'PUT', {'status': 'CANCELLED'})
            deadline = time.monotonic() + 90
            while http(api + '/api/v1/requests/' + fixture)['status'] != 'CANCELLED':
                if time.monotonic() >= deadline:
                    raise TimeoutError('fixture cancellation did not commit')
                time.sleep(1)
            event = dict(schemaVersion=1, eventId=str(uuid.uuid4()), entityId=fixture, entityVersion=1,
                         eventType='request.created', occurredAt=dt.datetime.now(dt.timezone.utc).isoformat(),
                         traceparent='00-' + uuid.uuid4().hex + '-' + uuid.uuid4().hex[:16] + '-00',
                         payload=dict(body, requestId=fixture, status='PENDING'))
            # Retain synthetic input, never broker credentials or Secret values.
            save('fixture.json', {'requestId': fixture, 'event': event})
            version = run(['make', '-s', 'print-K6_VERSION']).strip()
            cache = Path(os.environ.get('XDG_CACHE_HOME', str(Path.home() / '.cache'))) / 'reliefgrid-autoscaling'
            binary = cache / ('k6-v' + version + '-linux-amd64/k6')
            env = dict(os.environ, BROKER_URL=broker, BROKER_USER=user, BROKER_PASSWORD=password,
                       REQUEST_EVENT=json.dumps(event))
            with (out / 'k6.log').open('w') as log:
                k6 = subprocess.Popen([str(binary), 'run', '--no-usage-report', '--summary-export',
                                       str(out / 'k6-summary.json'), 'scripts/autoscaling-load.js'],
                                      env=env, stdout=log, stderr=subprocess.STDOUT)
                deadline = time.monotonic() + 480
                stable = 0
                while time.monotonic() < deadline:
                    done = k6.poll() is not None
                    s = sample(done)
                    stable = stable + 1 if done and s['desired'] == s['ready'] == 1 and s['queueReady'] == s['unacked'] == 0 else 0
                    if stable >= 3:
                        break
                    time.sleep(3)
                else:
                    raise TimeoutError('bounded scale-up/drain/scale-down did not finish')
                assert k6.wait(timeout=10) == 0, 'k6 load/check threshold failed; inspect k6.log'
            acceptance(samples)
            summary = json.loads((out / 'k6-summary.json').read_text())
            assert summary['metrics']['iterations']['count'] == 20000, 'incomplete load window'
            assert queue('matching.dead')['messages'] == dead_before, 'duplicate hints were rejected'
            assert http(api + '/api/v1/requests/' + fixture)['status'] == 'CANCELLED'
            api_hpa = kube('get', 'hpa', 'reliefgrid-api')
            assert any(m.get('type') == 'Resource' and m.get('resource', {}).get('name') == 'cpu' and type(m.get('resource', {}).get('current', {}).get('averageUtilization')) is int for m in api_hpa['status'].get('currentMetrics', [])), 'API CPU metrics unavailable'
            save('final-hpas.json', kube('get', 'hpa'))
            report['result'] = 'PASS'
            report['peakReadyMatchers'] = max(s['ready'] for s in samples)
            report['peakQueueReady'] = max(s['queueReady'] for s in samples)
        finally:
            if k6 is not None and k6.poll() is None:
                k6.terminate()
                k6.wait(timeout=15)
            save('report.json', report)
    print('A4.3 LIVE VERIFY: ' + report['result'] + ' evidence=' + str(out))


if __name__ == '__main__':
    main()
