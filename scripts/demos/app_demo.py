#!/usr/bin/env python3
"""Public-API application demonstrations; all claims are checked by strong reads.

Invoked by demo-2/3/5/8/9.sh after the shared Kubernetes preflight. HTTP retry
inputs and every observed outcome are retained, including failed attempts.
"""
import argparse
import base64
import concurrent.futures
import datetime as dt
import hashlib
import http.client
import json
import os
import pathlib
import socket
import subprocess
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid


def require_complete(page):
    assert page['partial'] is False and page['failedShards'] == [], page
    assert page['consistency'] == 'strong', page
    assert len(page['positions']) == 3 and {p['shard'] for p in page['positions']} == {0, 1, 2}, page
    assert all(p.get('hasMore') is False for p in page['positions']), 'bounded listing is truncated'


def require_reciprocal(request, resource):
    assert request['status'] == 'MATCHED' and request['assignedResourceId'] == resource['id'], (request, resource)
    assert resource['status'] == 'ASSIGNED' and resource.get('activeClaim'), resource
    assert resource['activeClaim']['requestId'] == request['requestId'] and resource['activeClaim']['attemptId'], resource


def queue_counts(queue):
    counts = (queue.get('messages_ready'), queue.get('messages_unacknowledged'))
    return counts if all(isinstance(value, int) and value >= 0 for value in counts) else None


def require_recovered_interruptions(attempts):
    failed = [row for row in attempts if row['status'] in (0, 502, 503, 504)]
    assert failed, 'no interrupted call was observed'
    for row in failed:
        assert any(other['at'] >= row['at'] and other['status'] == (201 if row['method'] == 'POST' else 200)
                   and all(other.get(field) == row.get(field) for field in ('method', 'path', 'key', 'payloadSHA256'))
                   for other in attempts), 'interrupted call has no successful identical-input retry'
    return len(failed)


class Client:
    def __init__(self, base, out, budget=120, auth=None):
        url = urllib.parse.urlsplit(base)
        if url.scheme not in ('http', 'https') or url.hostname not in ('127.0.0.1', 'localhost', '::1') or url.username or url.password or url.query or url.fragment:
            raise ValueError('demo HTTP endpoints must be credential-free loopback URLs')
        self.base, self.out, self.auth = base.rstrip('/'), out, auth
        self.deadline = time.monotonic() + budget
        self.lock = threading.Lock()

    def call(self, method, path, expected, payload=None, key=None, deadline=None):
        end = min(self.deadline, deadline) if deadline is not None else self.deadline
        body = None if payload is None else json.dumps(payload, sort_keys=True, separators=(',', ':')).encode()
        headers = {'Content-Type': 'application/json', 'Connection': 'close'}
        if key:
            headers['Idempotency-Key'] = key
        if self.auth:
            headers['Authorization'] = self.auth
        opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
        while time.monotonic() < end:
            stamp = time.time()
            request = urllib.request.Request(self.base + path, body, headers, method=method)
            try:
                with opener.open(request, timeout=min(5, max(.1, end - time.monotonic()))) as response:
                    status, raw, response_headers = response.status, response.read(), dict(response.headers.items())
            except urllib.error.HTTPError as error:
                with error:
                    status, raw, response_headers = error.code, error.read(), dict(error.headers.items())
            except (urllib.error.URLError, TimeoutError, ConnectionError, http.client.HTTPException) as error:
                status, raw, response_headers = 0, str(error).encode(), {}
            response_headers = {k.lower(): v for k, v in response_headers.items()}
            row = dict(at=stamp, method=method, path=path, status=status, key=key,
                       payloadSHA256=hashlib.sha256(body or b'').hexdigest(),
                       pod=response_headers.get('x-reliefgrid-pod'), elapsed=time.time() - stamp)
            with self.lock, (self.out / 'attempts.jsonl').open('a') as log:
                log.write(json.dumps(row) + '\n')
            if status in expected:
                return json.loads(raw), response_headers, raw
            if status not in (0, 502, 503, 504):
                raise AssertionError(f'{method} {path}: expected {sorted(expected)}, got {status}: {raw[:500]!r}')
            # Preserve key and byte-identical body on an unknown create result.
            time.sleep(min(.2, max(0, end - time.monotonic())))
        raise TimeoutError(f'{method} {path}: fixed demo deadline expired; retry inputs retained in {self.out}')


class Demo:
    def __init__(self, number, out):
        self.number, self.out = number, out
        self.out.mkdir(parents=True, exist_ok=True)
        self.api = Client(os.environ.get('API_URL', 'http://127.0.0.1:8080/api/v1'), out, budget=300)
        self.coordinator = Client(os.environ['DEMO_COORD_URL'], out, budget=300,
                                  auth='Bearer ' + os.environ['DEMO_OPERATOR_TOKEN'])
        self.ctx = os.environ.get('KUBE_CONTEXT', 'kind-' + os.environ.get('CLUSTER', 'reliefgrid'))
        if not self.ctx.startswith('kind-'):
            raise ValueError('app demos operate only on an explicitly selected kind context')
        self.namespace = os.environ.get('NAMESPACE', 'reliefgrid')
        self.release = os.environ.get('APP_RELEASE', 'reliefgrid')
        saved = self.out / 'scenario.json'
        scenario = json.loads(saved.read_text()) if saved.exists() else {}
        self.now = dt.datetime.fromisoformat(scenario['now']) if scenario else dt.datetime.now(dt.timezone.utc)
        self.seed = uuid.UUID(scenario['seed']) if scenario else uuid.uuid4()
        self.checks = []
        self.save('scenario', dict(seed=str(self.seed), demo=number, now=self.now.isoformat(), context=self.ctx, namespace=self.namespace))

    def save(self, name, data):
        (self.out / (name + '.json')).write_text(json.dumps(data, indent=2) + '\n')

    def check(self, condition, text):
        self.checks.append(dict(passed=bool(condition), assertion=text))
        print(('PASS  ' if condition else 'FAIL  ') + text, flush=True)
        self.save('checks', self.checks)
        assert condition, text

    def key(self, name):
        return str(uuid.uuid5(self.seed, name))

    def location(self, offset=0):
        # Distinct demo/scenario areas, each separated by much more than 50 km.
        return dict(lat=-60 + self.number * 10 + offset * 2, lng=30)

    def resource_body(self, name, kind='MEDICINE', location=None, expired=False):
        start, end = (-120, -60) if expired else (-1, 120)
        stamp = lambda minutes: (self.now + dt.timedelta(minutes=minutes)).isoformat(timespec='seconds').replace('+00:00', 'Z')
        return dict(resourceType=kind, name='Synthetic demo ' + name, location=location or self.location(),
                    windowStart=stamp(start), windowEnd=stamp(end), reusable=True)

    def request_body(self, name, kind='MEDICINE', location=None):
        return dict(requestType=kind, details='Synthetic demo ' + name, peopleAffected=1,
                    urgency='HIGH', location=location or self.location())

    def create(self, collection, name, body):
        with self.api.lock, (self.out / 'inputs.jsonl').open('a') as log:
            log.write(json.dumps(dict(name=name, collection=collection, key=self.key(name), body=body)) + '\n')
        result = self.api.call('POST', '/' + collection, {201}, body, self.key(name))
        entity = result[0]
        field = 'requestId' if collection == 'requests' else 'resourceId'
        entity_id = str(uuid.UUID(entity[field]))
        assert result[1].get('location') == '/api/v1/' + collection + '/' + entity_id, result
        with self.api.lock, (self.out / 'created.jsonl').open('a') as log:
            log.write(json.dumps(dict(name=name, entity=entity)) + '\n')
        return entity_id, result

    def resource(self, name, **options):
        return self.create('resources', name, self.resource_body(name, **options))[0]

    def request(self, name, **options):
        return self.create('requests', name, self.request_body(name, **options))[0]

    def record(self, prefix, entity_id):
        result = self.coordinator.call('GET', '/internal/data/' + prefix + ':' + entity_id + '?consistency=strong', {200})[0]
        record = result['record']
        assert not record.get('deleted'), record
        return record

    def get_request(self, entity_id, deadline=None):
        return self.api.call('GET', '/requests/' + entity_id, {200}, deadline=deadline)[0]

    def parallel(self, values, fn, workers=8):
        with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as pool:
            return list(pool.map(fn, values))

    def settle(self, ids, matches, seconds=120):
        deadline = min(self.api.deadline, time.monotonic() + seconds)
        while time.monotonic() < deadline:
            rows = self.parallel(ids, lambda entity_id: self.get_request(entity_id, deadline))
            states = [row['status'] for row in rows]
            self.save('settled-requests', rows)
            if all(state in ('MATCHED', 'WAITING') for state in states) and states.count('MATCHED') == matches:
                resources = [row['assignedResourceId'] for row in rows if row['status'] == 'MATCHED']
                self.check(len(resources) == len(set(resources)), 'no resource assigned to two requests')
                for row in rows:
                    if row['status'] == 'MATCHED':
                        require_reciprocal(row, self.record('res', row['assignedResourceId'])['resource'])
                return rows
            time.sleep(.25)
        raise TimeoutError(f'work did not settle to {matches} matches within {seconds}s; final observations saved')

    def kube(self, *args, stdin=None):
        command = ['kubectl', '--request-timeout=10s', '--context', self.ctx, '-n', self.namespace, *args]
        result = subprocess.run(command, input=stdin, text=True, capture_output=True, timeout=130, check=False)
        if result.returncode:
            raise RuntimeError(f'kubectl {args[:2]} exit {result.returncode}: {result.stderr[:1500]}')
        return result.stdout

    def pods(self, component):
        return json.loads(self.kube('get', 'pods', '-l', f'app.kubernetes.io/instance={self.release},app.kubernetes.io/component={component}', '-o', 'json'))['items']

    def ready(self, pod):
        return not pod['metadata'].get('deletionTimestamp') and any(c['type'] == 'Ready' and c['status'] == 'True' for c in pod.get('status', {}).get('conditions', []))

    def replicas(self, component):
        return json.loads(self.kube('get', 'deployment', self.release + '-' + component, '-o', 'json'))['spec']['replicas']

    def scale(self, component, replicas):
        self.kube('scale', 'deployment/' + self.release + '-' + component, '--replicas=' + str(replicas))
        self.kube('rollout', 'status', 'deployment/' + self.release + '-' + component, '--timeout=90s')
        if replicas == 0:
            deadline = time.monotonic() + 90
            while self.pods(component) and time.monotonic() < deadline:
                time.sleep(.25)
            assert not self.pods(component), 'workers did not stop before explicit matching'

    def wait_outbox_empty(self):
        deadline = min(self.coordinator.deadline, time.monotonic() + 30)
        pages = []
        while time.monotonic() < deadline:
            pages = [self.coordinator.call('GET', f'/internal/outbox?shard={shard}&limit=100&consistency=strong', {200}, deadline=deadline)[0] for shard in range(3)]
            assert all(page['shard'] == shard and isinstance(page['events'], list) for shard, page in enumerate(pages)), pages
            if all(not page['events'] for page in pages):
                return
            time.sleep(min(.25, max(0, deadline - time.monotonic())))
        raise TimeoutError('outbox did not drain within 30s; refusing a quiescent audit')

    def invariants(self):
        self.wait_outbox_empty()
        name = f'demo{self.number}-check-{self.seed.hex[:10]}'
        pod = dict(metadata=dict(labels={'reliefgrid.io/coordinator-client': 'true'}),
                   spec=dict(restartPolicy='Never', automountServiceAccountToken=False,
                             containers=[dict(name='check', image=os.environ.get('TOOLS_IMAGE', 'reliefgrid/tools:dev'), imagePullPolicy='Never',
                                              args=['check', '--coordinator', 'http://storage-coordinator:8080', '--timeout', '90s', '--max-backlog', '0', '--json'],
                                              resources=dict(requests=dict(cpu='100m', memory='128Mi'), limits=dict(memory='512Mi')))]))
        manifest = dict(apiVersion='batch/v1', kind='Job', metadata=dict(name=name, namespace=self.namespace),
                        spec=dict(backoffLimit=0, activeDeadlineSeconds=110, template=pod))
        self.kube('create', '-f', '-', stdin=json.dumps(manifest))
        try:
            deadline = time.monotonic() + 115
            while time.monotonic() < deadline:
                job = json.loads(self.kube('get', 'job', name, '-o', 'json'))
                if job.get('status', {}).get('succeeded') or job.get('status', {}).get('failed'):
                    break
                time.sleep(.5)
            logs = self.kube('logs', 'job/' + name)
            (self.out / 'invcheck.log').write_text(logs)
            self.check(job.get('status', {}).get('succeeded') == 1, 'invcheck: complete, reciprocal, no terminal claim and outbox drained')
        finally:
            self.kube('delete', 'job', name, '--ignore-not-found', '--wait=false')

    def demo2(self):
        body = self.resource_body('resource', kind='WATER')
        entity_id, original = self.create('resources', 'resource', body)
        created = original[0]
        self.check(all(created[key] == body[key] for key in body) and created['status'] == 'AVAILABLE' and created['activeClaim'] is None,
                   '201 resource retains validated type, location, availability and unclaimed state')
        self.check(all(created[key].endswith(('Z', '+00:00')) for key in ('createdAt', 'updatedAt')), 'creation timestamps are UTC')
        query = urllib.parse.urlencode(dict(type='WATER', near=f"{body['location']['lat']},{body['location']['lng']}", radiusKm=1, limit=100))
        def listing():
            page = self.api.call('GET', '/resources?' + query, {200})[0]
            require_complete(page)
            return page
        before = listing()
        self.check(sum(row['resourceId'] == entity_id for row in before['items']) == 1, 'complete authoritative resource list contains the created resource exactly once')
        for n, invalid in enumerate([dict(body, location=dict(lat=91, lng=30)), dict(body, resourceType='BOGUS'), dict(body, windowEnd=body['windowStart'])]):
            error = self.api.call('POST', '/resources', {400}, invalid, self.key('invalid-' + str(n)))[0]
            self.check(error['code'] == 'INVALID' and error['outcomeUnknown'] is False, f'invalid registration {n} is a definite 400 refusal')
        self.check({r['resourceId'] for r in listing()['items']} == {r['resourceId'] for r in before['items']}, 'invalid registrations create no resources')
        changed = self.api.call('PUT', '/resources/' + entity_id + '/status', {200}, dict(status='OFFLINE'))[0]
        self.check(changed['version'] != created['version'] and changed['status'] == 'OFFLINE', 'status update persists at a different opaque version')
        update = dict(windowStart=(self.now + dt.timedelta(minutes=30)).isoformat(timespec='seconds').replace('+00:00', 'Z'),
                      windowEnd=(self.now + dt.timedelta(minutes=90)).isoformat(timespec='seconds').replace('+00:00', 'Z'))
        changed = self.api.call('PUT', '/resources/' + entity_id + '/status', {200}, update)[0]
        self.check(all(changed[k] == v for k, v in update.items()), 'partial availability update persists')
        before_invalid = self.record('res', entity_id)
        for invalid in [dict(status='ASSIGNED'), dict(status='CONSUMED')]:
            error = self.api.call('PUT', '/resources/' + entity_id + '/status', {409}, invalid)[0]
            self.check(error['code'] == 'DOMAIN_CONFLICT' and error['outcomeUnknown'] is False, 'public claim-managed status is a definite 409 refusal')
        self.api.call('PUT', '/resources/' + entity_id + '/status', {400}, dict(windowStart=update['windowEnd'], windowEnd=update['windowStart']))
        self.check(self.record('res', entity_id) == before_invalid, 'all refused updates preserve the complete stored record and version')
        stored = self.record('res', entity_id)['resource']
        self.check(stored['status'] == 'OFFLINE' and stored['location'] == body['location'] and not stored.get('activeClaim'), 'strong stored record preserves location and refuses public claim mutations')
        replay = self.api.call('POST', '/resources', {201}, body, self.key('resource'))
        self.check(replay[2] == original[2] and replay[1]['location'] == original[1]['location'], 'resource create replay retains original body and Location after updates')
        self.audit_without_admission()

    def demo3(self):
        for offset, mode in [(0, 'async'), (1, 'explicit')]:
            location = self.location(offset)
            good = self.resource(mode + '-good', location=location)
            wrong = [self.resource(mode + '-type', kind='WATER', location=location),
                     self.resource(mode + '-far', location=dict(lat=location['lat'], lng=location['lng'] + 2)),
                     self.resource(mode + '-expired', location=location, expired=True)]
            old = self.replicas('matcher')
            if mode == 'explicit':
                self.scale('matcher', 0)
            try:
                request_id = self.request(mode + '-request', location=location)
                if mode == 'explicit':
                    outcome = self.api.call('POST', '/match/' + request_id, {200, 202})[0]
                    self.check(outcome['outcome'] == 'MATCHED', 'explicit match completes while matcher Deployment is stopped')
                rows = self.settle([request_id], 1)
                self.check(rows[0]['assignedResourceId'] == good, mode + ' match respects type, 50 km radius and current availability window')
                self.check(all(not self.record('res', rid)['resource'].get('activeClaim') for rid in wrong), mode + ' incompatible resources remain unclaimed')
            finally:
                if mode == 'explicit':
                    self.scale('matcher', old)
        location = self.location(2)
        self.resource('contended', location=location)
        ids = self.parallel(range(2), lambda n: self.request('competitor-' + str(n), location=location), workers=2)
        rows = self.settle(ids, 1)
        self.check(sorted(row['status'] for row in rows) == ['MATCHED', 'WAITING'], 'two requests competing for one resource produce one match and one waiting request')
        self.audit_without_admission()

    def broker(self):
        secret = json.loads(self.kube('get', 'secret', self.release + '-rabbitmq', '-o', 'json'))['data']
        username = base64.b64decode(secret['username']).decode()
        password = base64.b64decode(secret['password']).decode()
        authorization = base64.b64encode((username + ':' + password).encode()).decode()
        return Client(os.environ['DEMO_BROKER_URL'], self.out, budget=300, auth='Basic ' + authorization)

    def drain_outbox(self):
        # Consumers are stopped. Finish their last committed handoffs using the
        # existing routed-confirm relay, without admitting any new retry work.
        secret = json.loads(self.kube('get', 'secret', self.release + '-rabbitmq', '-o', 'json'))['data']
        username = base64.b64decode(secret['username']).decode()
        password = base64.b64decode(secret['password']).decode()
        endpoint = urllib.parse.urlsplit(os.environ['DEMO_AMQP_ENDPOINT'])
        assert endpoint.scheme == 'amqp' and endpoint.hostname == '127.0.0.1' and endpoint.port
        credentials = urllib.parse.quote(username, safe='') + ':' + urllib.parse.quote(password, safe='')
        env = dict(os.environ, COORDINATOR_URL=self.coordinator.base,
                   AMQP_URL=urllib.parse.urlunsplit(('amqp', credentials + '@' + endpoint.netloc, '/', '', '')))
        result = subprocess.run([os.environ['DEMO_OUTBOX_DRAIN']], env=env,
                                capture_output=True, text=True, timeout=40, check=False)
        # Credentials stay in child environment; no command line/evidence copy.
        diagnostic = result.stderr.replace(password, '[redacted]').replace(urllib.parse.quote(password, safe=''), '[redacted]')
        (self.out / 'handoff-drain.log').write_text(result.stdout + diagnostic)
        self.check(result.returncode == 0, 'last committed handoffs drained with routed confirms while admission stays stopped')

    def audit_without_admission(self):
        # C13: stop new retries before the final audit. Unconsumed hints remain
        # durable in RabbitMQ; no delivery is acked just to quiet the workload.
        workloads = json.loads(self.kube('get', 'deployments', '-l', 'app.kubernetes.io/instance=' + self.release, '-o', 'json'))['items']
        saved = {}
        for workload in workloads:
            component = workload['metadata'].get('labels', {}).get('app.kubernetes.io/component')
            if component in ('matcher', 'reconciler'):
                assert workload['metadata']['name'] == self.release + '-' + component
                saved[component] = workload['spec']['replicas']
        self.check('matcher' in saved, 'retry admission workers are identified before pausing')
        try:
            for component in saved:
                self.scale(component, 0)
            self.drain_outbox()
            self.invariants()
        finally:
            for component, replicas in saved.items():
                self.scale(component, replicas)

    def demo5(self):
        broker = self.broker()
        samples, errors, stopped = [], [], threading.Event()
        def observe():
            try:
                while not stopped.is_set():
                    queue = broker.call('GET', '/api/queues/%2F/matching.requests', {200})[0]
                    counts = queue_counts(queue)
                    samples.append(dict(at=time.time(), ready=counts[0] if counts else None, unacked=counts[1] if counts else None))
                    stopped.wait(.25)
            except Exception as error:
                errors.append(str(error))
        thread = threading.Thread(target=observe)
        thread.start()
        try:
            self.parallel(range(50), lambda n: self.resource('burst-resource-' + str(n), kind='FOOD'))
            started = time.monotonic()
            ids = self.parallel(range(500), lambda n: self.request('burst-request-' + str(n), kind='FOOD'), workers=16)
            self.check(len(set(ids)) == 500, '500 distinct request creates acknowledged against 50 seeded resources')
            rows = self.settle(ids, 50)
            self.check(sum(r['status'] == 'WAITING' for r in rows) == 450, 'burst settles to 50 reciprocal matches and 450 waiting requests')
            deadline = time.monotonic() + 30
            while time.monotonic() < deadline:
                queue = broker.call('GET', '/api/queues/%2F/matching.requests', {200})[0]
                if queue_counts(queue) == (0, 0):
                    break
                time.sleep(.25)
            self.check(queue_counts(queue) == (0, 0), 'matching queue ready/unacked returns to zero within bounded quiescence')
            self.check(any(s['ready'] is not None and (s['ready'] > 0 or s['unacked'] > 0) for s in samples), 'broker observations show asynchronous work in flight')
            self.save('burst', dict(resources=50, requests=500, matched=50, waiting=450, elapsedSeconds=time.monotonic() - started))
        finally:
            stopped.set()
            thread.join(timeout=6)
            self.save('queue-samples', samples)
        self.check(not thread.is_alive() and not errors, 'all broker samples completed without hidden observation errors: ' + repr(errors))
        deadline = time.monotonic() + 30
        while time.monotonic() < deadline:
            pages = [self.coordinator.call('GET', f'/internal/outbox?shard={shard}&limit=100&consistency=strong', {200})[0] for shard in range(3)]
            assert all(page['shard'] == shard and isinstance(page['events'], list) for shard, page in enumerate(pages)), pages
            if all(not page['events'] for page in pages):
                break
            time.sleep(.25)
        self.check(all(not page['events'] for page in pages), 'all shard outboxes drained before retry admission is paused')
        self.audit_without_admission()

    def demo8(self):
        for component in ('api', 'matcher'):
            initial = self.pods(component)
            self.check(len(initial) == 2 and all(self.ready(p) for p in initial), component + ': two Ready pods before deletion')
            old_uids = {p['metadata']['uid'] for p in initial}
            victim = initial[0]['metadata']['name']
            done, started = threading.Event(), threading.Event()
            successes, failures, replay_inputs = [], [], []
            def traffic():
                try:
                    n = 0
                    while not done.is_set():
                        name = component + '-traffic-' + str(n)
                        body = self.request_body(name, kind='SHELTER')
                        entity_id, original = self.create('requests', name, body)
                        successes.append((time.time(), entity_id))
                        replay_inputs.append((name, body, original))
                        started.set()
                        n += 1
                        if n >= 1000:
                            raise RuntimeError('traffic safety bound reached before recovery')
                        done.wait(.1)
                except Exception as error:
                    failures.append(str(error))
                    started.set()
            thread = threading.Thread(target=traffic)
            thread.start()
            try:
                assert started.wait(10) and successes, failures
                deleted = time.time()
                if component == 'api':
                    self.interrupt_create(victim)
                else:
                    self.kube('delete', 'pod', victim, '--wait=false')
                deadline = time.monotonic() + 90
                while time.monotonic() < deadline:
                    current = self.pods(component)
                    replacements = [p for p in current if p['metadata']['uid'] not in old_uids and self.ready(p)]
                    if replacements and sum(self.ready(p) for p in current) == 2:
                        break
                    time.sleep(.25)
                self.check(bool(replacements), component + ': a new UID becomes Ready within 90s')
                recovered = time.time()
                deadline = time.monotonic() + 10
                while time.monotonic() < deadline and not any(stamp > recovered for stamp, _ in successes):
                    time.sleep(.1)
                self.save(component + '-recovery', dict(deletedAt=deleted, recoveredAt=recovered, before=initial, after=current))
            finally:
                done.set()
                thread.join(timeout=6)
            self.check(not thread.is_alive() and not failures, component + ': traffic completed with no lost create outcome: ' + repr(failures))
            self.check(any(stamp > deleted for stamp, _ in successes) and any(stamp > recovered for stamp, _ in successes), component + ': successful calls continued during and after pod replacement')
            ids = [entity_id for _, entity_id in successes]
            self.settle(ids, 0)
            for name, body, original in replay_inputs:
                replay = self.api.call('POST', '/requests', {201}, body, self.key(name))
                assert replay[2] == original[2] and replay[1]['location'] == original[1]['location'], name
            self.check(len(replay_inputs) == len(successes) and len(successes) > 0, component + ': every successful create replay returned its original result')
        attempts = [json.loads(line) for line in (self.out / 'attempts.jsonl').read_text().splitlines()]
        interrupted = sum(row['status'] in (0, 502, 503, 504) for row in attempts)
        self.save('interrupted-calls', dict(interrupted=interrupted, attempts=len(attempts)))
        recovered = require_recovered_interruptions(attempts)
        self.check(interrupted > 0 and recovered == interrupted, f'{interrupted} interrupted calls observed; every one recovered using its original key and payload')
        self.audit_without_admission()

    def interrupt_create(self, victim):
        # Hold an actual create halfway through its body on the selected pod,
        # then crash that pod. This guarantees a measured transport interruption
        # even when the Service routes ordinary traffic to the surviving replica.
        name = 'api-interrupted-create'
        body = self.request_body(name, kind='SHELTER')
        raw = json.dumps(body, sort_keys=True, separators=(',', ':')).encode()
        key = self.key(name)
        with socket.socket() as reservation:
            reservation.bind(('127.0.0.1', 0))
            port = reservation.getsockname()[1]
        command = ['kubectl', '--context', self.ctx, '-n', self.namespace, 'port-forward',
                   'pod/' + victim, f'{port}:8080', '--address=127.0.0.1']
        with (self.out / 'interrupted-forward.log').open('w') as diagnostic:
            forward = subprocess.Popen(command, stdout=diagnostic, stderr=subprocess.STDOUT)
            connection = None
            try:
                deadline = time.monotonic() + 10
                while time.monotonic() < deadline:
                    try:
                        connection = socket.create_connection(('127.0.0.1', port), timeout=1)
                        break
                    except OSError:
                        time.sleep(.1)
                assert connection is not None, 'victim API forward unavailable'
                connection.settimeout(10)
                headers = (f'POST /api/v1/requests HTTP/1.1\r\nHost: localhost\r\nContent-Type: application/json\r\n'
                           f'Idempotency-Key: {key}\r\nContent-Length: {len(raw)}\r\nConnection: close\r\n\r\n').encode()
                connection.sendall(headers + raw[:len(raw)//2])
                stamp = time.time()
                self.kube('delete', 'pod', victim, '--force', '--grace-period=0', '--wait=false')
                try:
                    reply = connection.recv(4096)
                    assert not reply, f'expected interrupted transport, received {reply[:100]!r}'
                except (ConnectionResetError, BrokenPipeError):
                    pass
                row = dict(at=stamp, method='POST', path='/requests', status=0, key=key,
                           payloadSHA256=hashlib.sha256(raw).hexdigest(), pod=victim,
                           reason='pod crashed while create body was in flight')
                with self.api.lock, (self.out / 'attempts.jsonl').open('a') as log:
                    log.write(json.dumps(row) + '\n')
                _, original = self.create('requests', name, body)
                replay = self.api.call('POST', '/requests', {201}, body, key)
                self.check(replay[2] == original[2], 'interrupted create retried and replayed its original result')
            finally:
                if connection is not None:
                    connection.close()
                forward.terminate()
                try:
                    forward.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    forward.kill()
                    forward.wait(timeout=5)

    def demo9(self):
        deployment = self.release + '-api'
        hpas = json.loads(self.kube('get', 'hpa', '-o', 'json'))['items']
        self.check(not any(h['spec']['scaleTargetRef']['name'] == deployment for h in hpas), 'manual API scale demo has no competing HPA')
        self.check(self.replicas('api') == 2, 'API starts at two replicas')
        before = self.pods('api')
        seen, errors, stopped, warmed = set(), [], threading.Event(), threading.Event()
        def traffic():
            try:
                while not stopped.is_set():
                    _, headers, _ = self.api.call('GET', '/requests?limit=1', {200})
                    pod = headers.get('x-reliefgrid-pod')
                    assert pod, 'missing X-ReliefGrid-Pod observation header'
                    seen.add(pod)
                    warmed.set()
                    stopped.wait(.02)
            except Exception as error:
                errors.append(str(error))
                warmed.set()
        thread = threading.Thread(target=traffic)
        thread.start()
        try:
            assert warmed.wait(10) and seen, errors
            self.scale('api', 6)
            after = self.pods('api')
            names = {p['metadata']['name'] for p in after if self.ready(p)}
            self.check(len(names) == 6, 'all six API pods become Ready under traffic')
            deadline = time.monotonic() + 60
            while time.monotonic() < deadline and not names <= seen and not errors:
                time.sleep(.1)
            self.check(names <= seen, 'every API pod, including all four new pods, serves through the Service')
            self.save('scaling', dict(before=[p['metadata']['name'] for p in before], ready=sorted(names), served=sorted(seen)))
        finally:
            stopped.set()
            thread.join(timeout=6)
            self.scale('api', 2)
        self.check(not thread.is_alive() and not errors, 'Service traffic completed without hidden failures: ' + repr(errors))
        self.check(self.replicas('api') == 2, 'API restored to two replicas')


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('number', type=int, choices=[2, 3, 5, 8, 9])
    parser.add_argument('--out', type=pathlib.Path, required=True)
    args = parser.parse_args()
    demo = Demo(args.number, args.out)
    try:
        getattr(demo, 'demo' + str(args.number))()
        demo.save('result', dict(result='PASS', checks=len(demo.checks)))
    except Exception as error:
        demo.save('result', dict(result='FAIL', error=str(error), checks=len(demo.checks)))
        raise


if __name__ == '__main__':
    main()
