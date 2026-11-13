#!/usr/bin/env python3
"""Real-cluster A2.4 assertions; fault hooks exist only in a test worker image."""
import argparse, datetime as dt, json, os, pathlib, subprocess, time, urllib.error, urllib.parse, urllib.request, uuid

from internal_auth import Sender, mount_app_sender

class Verify:

    def __init__(self, out, probe):
        self.out = out
        out.mkdir(parents=True, exist_ok=True)
        self.probe = probe
        self.seed = str(uuid.uuid4())
        self.context = 'kind-reliefgrid'
        self.ns = 'reliefgrid'
        self.release = 'reliefgrid'
        self.coord = os.environ['DEMO_COORD_URL'].rstrip('/')
        self.sender = Sender(self.coord)
        self.api = os.environ.get('API_URL', 'http://127.0.0.1:8080/api/v1').rstrip('/')
        self.saved = self.replicas()
        self.cases = []
        for endpoint in (self.coord, self.api):
            assert urllib.parse.urlsplit(endpoint).hostname in ('127.0.0.1', 'localhost', '::1'), endpoint
        self.save('run', {'seed': self.seed, 'context': self.context, 'initialReplicas': self.saved})

    def save(self, name, data):
        p = self.out / (name + '.json')
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text(json.dumps(data, indent=2) + '\n')

    def k(self, *args, stdin=None, check=True, timeout=130):
        p = subprocess.run(['kubectl', '--context', self.context, '--request-timeout=10s', '-n', self.ns, *args], input=stdin, text=True, capture_output=True, timeout=timeout)
        if check and p.returncode:
            raise RuntimeError(f'kubectl {args[:2]}: {p.stderr[:1500]}')
        return p.stdout

    def call(self, base, method, path, body=None, key=None, expected=200):
        end = time.monotonic() + 20
        raw = None if body is None else json.dumps(body, sort_keys=True, separators=(',', ':')).encode()
        headers = {'Content-Type': 'application/json', 'Connection': 'close'}
        if key:
            headers['Idempotency-Key'] = key
        opener = Sender(self.coord) if base == self.coord else urllib.request.build_opener(urllib.request.ProxyHandler({}))
        while time.monotonic() < end:
            request = urllib.request.Request(base + path, raw, headers, method=method)
            try:
                with opener.open(request, timeout=min(5, max(0.1, end - time.monotonic()))) as response:
                    code, data = (response.status, response.read())
            except urllib.error.HTTPError as e:
                code, data = (e.code, e.read())
            except (urllib.error.URLError, TimeoutError, ConnectionError):
                time.sleep(0.2)
                continue
            if code == expected:
                return json.loads(data)
            if code not in (502, 503, 504):
                raise AssertionError(f'{method} {path}: {code} {data[:400]!r}')
            time.sleep(0.2)
        raise TimeoutError(f'{method} {path}: fixed HTTP retry deadline expired')

    def record(self, prefix, entity):
        return self.call(self.coord, 'GET', f'/internal/data/{prefix}:{entity}?consistency=strong')['record']

    def replicas(self):
        return json.loads(self.k('get', 'deployment', self.release + '-matcher', '-o', 'json'))['spec']['replicas']

    def scale(self, n):
        self.k('scale', 'deployment/' + self.release + '-matcher', '--replicas=' + str(n))
        self.k('rollout', 'status', 'deployment/' + self.release + '-matcher', '--timeout=90s')
        if n == 0:
            end = time.monotonic() + 45
            while time.monotonic() < end:
                pods = json.loads(self.k('get', 'pods', '-l', 'app.kubernetes.io/instance=reliefgrid,app.kubernetes.io/component=matcher', '-o', 'json'))['items']
                if not pods:
                    return
                time.sleep(0.2)
            raise TimeoutError('matcher pods did not stop')

    def wait(self, qid, status, seconds=90):
        end = time.monotonic() + seconds
        last = None
        while time.monotonic() < end:
            last = self.record('req', qid)
            if last['request']['status'] == status:
                return last
            time.sleep(0.2)
        self.save('first-failure', {'request': last, 'expected': status})
        raise TimeoutError(f'{qid}: did not reach {status} within {seconds}s')

    def owns(self, rec, resource):
        q = rec['request']
        r = resource['resource']
        a = q.get('currentAttempt')
        assert a and q['assignedResourceId'] == r['id'] and (r['status'] == 'ASSIGNED')
        assert r['activeClaim'] == dict(requestId=q['id'], attemptId=a['attemptId'], fingerprint=a['fingerprint']), (q, r)

    def host_probe(self, *args):
        p = subprocess.run([self.probe, *args], env=dict(os.environ, COORDINATOR_URL=self.coord), text=True, capture_output=True, timeout=40)
        assert p.returncode == 0, p.stderr
        return json.loads(p.stdout)

    def crash_case(self, stage, index):
        name = 'a24-crash-' + str(index)
        started = time.monotonic()
        self.scale(0)
        pod = {'apiVersion': 'v1', 'kind': 'Pod', 'metadata': {'name': name, 'labels': {'reliefgrid.io/coordinator-client': 'true', 'app.kubernetes.io/component': 'recovery-test'}}, 'spec': {'restartPolicy': 'Never', 'automountServiceAccountToken': False, 'securityContext': {'runAsNonRoot': True, 'runAsUser': 65532, 'runAsGroup': 65532}, 'containers': [{'name': 'test-matcher', 'image': 'reliefgrid/reconcile-crash:a24', 'imagePullPolicy': 'Never', 'args': ['--stage', stage, '--run', self.seed, '--latitude', str(-72 + index * 9)], 'env': [{'name': 'COORDINATOR_URL', 'value': 'http://storage-coordinator:8080'}], 'resources': {'requests': {'cpu': '100m', 'memory': '128Mi'}, 'limits': {'memory': '512Mi'}}}]}}
        mount_app_sender(pod)
        self.k('create', '-f', '-', stdin=json.dumps(pod))
        checkpoint = None
        try:
            end = time.monotonic() + 60
            while time.monotonic() < end:
                logs = self.k('logs', name, check=False)
                for line in logs.splitlines():
                    try:
                        entry = json.loads(line)
                    except json.JSONDecodeError:
                        continue
                    if entry.get('phase') == 'READY':
                        checkpoint = entry
                if checkpoint:
                    break
                state = json.loads(self.k('get', 'pod', name, '-o', 'json'))
                if state.get('status', {}).get('phase') in ('Failed', 'Succeeded'):
                    raise AssertionError(f'{stage}: worker exited before boundary: {logs[-1500:]}')
                time.sleep(0.2)
            assert checkpoint, f'{stage}: committed boundary not reached'
            state = json.loads(self.k('get', 'pod', name, '-o', 'json'))
            checkpoint['podUID'] = state['metadata']['uid']
            self.save(stage + '/checkpoint', checkpoint)
            (self.out / stage / 'worker.log').write_text(logs)
            self.k('delete', 'pod', name, '--force', '--grace-period=0', '--wait=true')
        finally:
            self.k('delete', 'pod', name, '--ignore-not-found', '--wait=false', check=False)
            self.scale(self.saved)
        target = 'CANCELLED' if stage.startswith('cancel-') else 'FULFILLED' if stage.startswith('fulfil-') else 'MATCHED'
        req = self.wait(checkpoint['requestId'], target)
        resource = self.record('res', checkpoint['resourceId'])
        self.save(stage + '/request', req)
        self.save(stage + '/resource', resource)
        if target == 'MATCHED':
            self.owns(req, resource)
        else:
            assert not resource['resource'].get('activeClaim'), resource
            assert resource['resource']['status'] == ('CONSUMED' if target == 'FULFILLED' else 'AVAILABLE'), resource
        if stage.startswith(('cancel-', 'fulfil-', 'retry-')) or stage == 'old-claim':
            aid = checkpoint['attemptId']
            closure = self.call(self.coord, 'GET', f"/internal/resources/{checkpoint['resourceId']}/closures/{aid}?consistency=strong")['closure']
            assert closure['requestId'] == checkpoint['requestId'] and closure['attemptId'] == aid
            self.save(stage + '/closure', closure)
            proof = self.host_probe('--probe', str(self.out / stage / 'checkpoint.json'))
            assert proof['lateOutcome'] == 'CLOSED' and proof['currentOwnershipUnchanged']
            self.save(stage + '/late-claim', proof)
        item = {'stage': stage, 'result': 'PASS', 'terminal': target, 'elapsedSeconds': time.monotonic() - started}
        self.cases.append(item)
        self.save('cases', self.cases)
        print('PASS crash/restart ' + stage + ' -> ' + target, flush=True)

    def key(self, name):
        return str(uuid.uuid5(uuid.UUID(self.seed), name))

    def create(self, collection, name, body):
        self.save(name + '/input', {'key': self.key(name), 'body': body})
        result = self.call(self.api, 'POST', '/' + collection, body, self.key(name), expected=201)
        return result['resourceId' if collection == 'resources' else 'requestId']

    def resource_body(self, name, lat, reusable):
        now = dt.datetime.now(dt.timezone.utc)
        return {'resourceType': 'TRANSPORTATION', 'name': 'Synthetic ' + name, 'location': {'lat': lat, 'lng': 0}, 'windowStart': (now - dt.timedelta(minutes=1)).isoformat(timespec='seconds').replace('+00:00', 'Z'), 'windowEnd': (now + dt.timedelta(hours=2)).isoformat(timespec='seconds').replace('+00:00', 'Z'), 'reusable': reusable}

    def request_body(self, lat):
        return {'requestType': 'TRANSPORTATION', 'details': 'Synthetic reconciliation verification', 'peopleAffected': 1, 'urgency': 'NORMAL', 'location': {'lat': lat, 'lng': 0}}

    def public_cleanup(self, name, target, lat, reusable):
        rid = self.create('resources', name + '-resource', self.resource_body(name, lat, reusable))
        qid = self.create('requests', name + '-request', self.request_body(lat))
        self.wait(qid, 'MATCHED', 30)
        intermediate = self.call(self.api, 'PUT', '/requests/' + qid + '/status', {'status': target})
        assert intermediate['status'] in (target, 'CANCELLING' if target == 'CANCELLED' else 'FULFILLING')
        self.save(name + '/intermediate', intermediate)
        rec = self.wait(qid, target, 30)
        resource = self.record('res', rid)
        assert not resource['resource'].get('activeClaim')
        assert resource['resource']['status'] == ('AVAILABLE' if reusable else 'CONSUMED')
        self.save(name + '/terminal', rec)
        self.save(name + '/resource', resource)
        print('PASS public ' + name + ' -> ' + target + ' without manual match', flush=True)

    def resource_wakeup(self):
        qid = self.create('requests', 'wake-request', self.request_body(56))
        self.wait(qid, 'WAITING', 30)
        future = self.host_probe('--defer-request', qid)
        self.save('resource-wakeup/future', future)
        assert future['nextAttemptAt'] > time.time() * 1000 + 90000
        rid = self.create('resources', 'wake-resource', self.resource_body('wake-resource', 56, True))
        started = time.monotonic()
        rec = self.wait(qid, 'MATCHED', 30)
        resource = self.record('res', rid)
        self.owns(rec, resource)
        assert time.time() * 1000 < future['nextAttemptAt']
        self.save('resource-wakeup/matched', rec)
        self.save('resource-wakeup/timing', {'elapsedSeconds': time.monotonic() - started, 'originalDue': future['nextAttemptAt']})
        print('PASS resource registration wakes 120-second future waiter before due time', flush=True)

    def scan_requests(self):
        cursor = ''
        expected = {0, 1, 2}
        rows = []
        seen = set()
        for _ in range(100):
            path = '/internal/scan?' + urllib.parse.urlencode({'kind': 'request', 'limit': 100, 'consistency': 'strong', **({'cursor': cursor} if cursor else {})})
            page = self.call(self.coord, 'GET', path)
            assert page['partial'] is False and (not page.get('failedShards')) and (page['consistency'] == 'strong'), page
            positions = page['positions']
            assert len(positions) == len(expected) and {p['shard'] for p in positions} == expected, page
            rows += page['records']
            nxt = page.get('nextCursor', '')
            more = {p['shard'] for p in positions if p.get('hasMore')}
            assert bool(nxt) == bool(more)
            if not nxt:
                return rows
            assert nxt not in seen
            seen.add(nxt)
            cursor = nxt
            expected = more
        raise AssertionError('request scan budget exhausted')

    def audit(self):
        rows = self.scan_requests()
        bad = []
        now = time.time() * 1000
        for r in rows:
            q = r['request']
            status = q['status']
            due = q.get('nextAttemptAt', 0)
            if status in ('PENDING', 'MATCHING', 'RETRYING', 'CANCELLING', 'FULFILLING') and now - r['updatedAt'] > 60000:
                bad.append(r)
            if status == 'WAITING' and due and (now - due > 60000):
                bad.append(r)
        self.save('age-audit', {'thresholdSeconds': 60, 'requests': len(rows), 'overdue': bad})
        assert not bad, 'workflow exceeds bounded age threshold'
        end = time.monotonic() + 30
        while time.monotonic() < end:
            pages = [self.call(self.coord, 'GET', f'/internal/outbox?shard={s}&limit=100&consistency=strong') for s in range(3)]
            if all((not p['events'] for p in pages)):
                break
            time.sleep(0.2)
        assert all((not p['events'] for p in pages)), 'outbox not drained'
        name = 'a24-final-invcheck'
        pod = {'metadata': {'labels': {'reliefgrid.io/coordinator-client': 'true'}}, 'spec': {'restartPolicy': 'Never', 'automountServiceAccountToken': False, 'containers': [{'name': 'check', 'image': 'reliefgrid/tools:dev', 'imagePullPolicy': 'Never', 'args': ['check', '--coordinator', 'http://storage-coordinator:8080', '--timeout', '90s', '--max-backlog', '0', '--json'], 'resources': {'requests': {'cpu': '100m', 'memory': '128Mi'}, 'limits': {'memory': '512Mi'}}}]}}
        mount_app_sender(pod)
        job = {'apiVersion': 'batch/v1', 'kind': 'Job', 'metadata': {'name': name}, 'spec': {'backoffLimit': 0, 'activeDeadlineSeconds': 110, 'template': pod}}
        self.k('delete', 'job', name, '--ignore-not-found', '--wait=true')
        self.k('create', '-f', '-', stdin=json.dumps(job))
        try:
            self.k('wait', '--for=condition=complete', 'job/' + name, '--timeout=115s')
            report = json.loads(self.k('logs', 'job/' + name))
            self.save('invcheck', report)
            assert report['ok'] is True and report['verdict'] == 'clean' and (report['report']['quiescent'] is True), report
        finally:
            self.k('delete', 'job', name, '--ignore-not-found', '--wait=false', check=False)
        print('PASS complete age audit, exact closures/current claims and strict zero-backlog invcheck', flush=True)

    def run(self):
        stages = ['pending', 'intent', 'claim', 'confirm', 'cancel-intent', 'cancel-close', 'cancel-terminal', 'fulfil-intent', 'fulfil-close', 'fulfil-terminal', 'retry-intent', 'retry-close', 'old-claim']
        try:
            self.public_cleanup('public-cancel', 'CANCELLED', 68, True)
            self.public_cleanup('public-fulfil', 'FULFILLED', 62, False)
            self.resource_wakeup()
            for i, stage in enumerate(stages):
                self.crash_case(stage, i)
            self.audit()
            self.save('result', {'result': 'PASS', 'crashCases': len(self.cases), 'publicCleanupCases': 2, 'resourceWakeup': True})
            print('A2.4 CLUSTER VERIFY: ALL PASS', flush=True)
        finally:
            if self.replicas() != self.saved:
                self.scale(self.saved)
if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--out', type=pathlib.Path, required=True)
    parser.add_argument('--probe', required=True)
    args = parser.parse_args()
    Verify(args.out, args.probe).run()
