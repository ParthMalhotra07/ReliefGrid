#!/usr/bin/env python3
"""Check committed app behavior over Traefik, retaining immutable retry inputs."""
import datetime
import json
import os
import pathlib
import re
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid

from app_evidence import evidence_dir


def main():
    base = os.environ.get('E2E_BASE_URL', 'http://127.0.0.1:8080').rstrip('/')
    url = urllib.parse.urlsplit(base)
    if url.scheme not in ('http', 'https') or url.hostname not in ('127.0.0.1', 'localhost', '::1') or url.username is not None or url.password is not None or url.query or url.fragment or url.path:
        raise ValueError('E2E_BASE_URL must be a credential-free loopback HTTP(S) origin')
    run_id = os.environ.get('E2E_RUN_ID', datetime.datetime.now(datetime.timezone.utc).strftime('%Y%m%dT%H%M%SZ-') + uuid.uuid4().hex[:8])
    if not re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9_.-]{0,79}', run_id):
        raise ValueError('invalid E2E_RUN_ID')
    out = evidence_dir() / ('e2e-' + run_id)
    print('application smoke evidence:', out, flush=True)
    out.mkdir(parents=True, exist_ok=True)
    scenario_path = out / 'scenario.json'
    if scenario_path.exists():
        scenario = json.loads(scenario_path.read_text())
    else:
        now = datetime.datetime.now(datetime.timezone.utc)
        stamp = lambda value: value.isoformat(timespec='milliseconds').replace('+00:00', 'Z')
        location = dict(lat=28.6139, lng=77.209)
        scenario = dict(requestKey=str(uuid.uuid4()), resourceKey=str(uuid.uuid4()),
                        requestPayload=dict(requestType='MEDICINE', details='Synthetic A1.5 smoke request', peopleAffected=1, urgency='HIGH', location=location),
                        resourcePayload=dict(resourceType='MEDICINE', name='Synthetic A1.5 medical resource', location=location,
                                             windowStart=stamp(now - datetime.timedelta(minutes=1)), windowEnd=stamp(now + datetime.timedelta(hours=2)), reusable=True))
        scenario_path.write_text(json.dumps(scenario, indent=2) + '\n')
    deadline = time.monotonic() + 90
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
    attempts = out / 'attempts.jsonl'

    def call(method, path, expected, payload=None, key=None):
        body = None if payload is None else json.dumps(payload, separators=(',', ':')).encode()
        headers = {'Content-Type': 'application/json'}
        if key:
            headers['Idempotency-Key'] = key
        while time.monotonic() < deadline:
            request = urllib.request.Request(base + path, body, headers, method=method)
            try:
                response = opener.open(request, timeout=min(5, max(0.1, deadline - time.monotonic())))
                with response:
                    status, data, result_headers = response.status, response.read(), dict(response.headers.items())
            except urllib.error.HTTPError as error:
                with error:
                    status, data, result_headers = error.code, error.read(), dict(error.headers.items())
            except (urllib.error.URLError, TimeoutError, ConnectionError) as error:
                status, data, result_headers = 0, str(error).encode(), {}
            with attempts.open('a') as log:
                log.write(json.dumps(dict(method=method, path=path, status=status)) + '\n')
            if status == expected:
                return data, {k.lower(): v for k, v in result_headers.items()}
            if status not in (0, 503):
                raise AssertionError(f'{method} {path}: expected {expected}, got {status}: {data[:1000]!r}')
            print(f'observed retry: {method} {path} status={status}', flush=True)
            time.sleep(min(0.25, max(0, deadline - time.monotonic())))
        raise TimeoutError(f'{method} {path} did not reach {expected} within the smoke budget; inputs retained in {out}')

    page, _ = call('GET', '/', 200)
    assert b'ReliefGrid' in page, 'Traefik did not route the UI placeholder'
    original, original_headers = call('POST', '/api/v1/requests', 201, scenario['requestPayload'], scenario['requestKey'])
    request = json.loads(original)
    request_id = str(uuid.UUID(request['requestId']))
    assert original_headers.get('location') == '/api/v1/requests/' + request_id
    (out / 'created-request.json').write_bytes(original)
    resource_data, _ = call('POST', '/api/v1/resources', 201, scenario['resourcePayload'], scenario['resourceKey'])
    resource_id = str(uuid.UUID(json.loads(resource_data)['resourceId']))
    (out / 'created-resource.json').write_bytes(resource_data)
    while True:
        current_data, _ = call('GET', '/api/v1/requests/' + request_id, 200)
        current = json.loads(current_data)
        (out / 'current-request.json').write_bytes(current_data)
        if current['status'] == 'MATCHED':
            assert current['assignedResourceId'] == resource_id, current
            break
        if current['status'] not in ('PENDING', 'WAITING', 'MATCHING'):
            raise AssertionError(f'unexpected request state: {current}')
        if time.monotonic() >= deadline:
            raise TimeoutError(f'no committed match: {current}; attempts retained in {out}')
        time.sleep(min(0.25, max(0, deadline - time.monotonic())))
    resource_list_data, _ = call('GET', '/api/v1/resources?type=MEDICINE&limit=100', 200)
    resource_list = json.loads(resource_list_data)
    (out / 'resource-list.json').write_bytes(resource_list_data)
    assert resource_list['partial'] is False and resource_list['failedShards'] == [], resource_list
    assert resource_list['consistency'] in ('strong', 'linearizable'), resource_list
    positions = resource_list['positions']
    assert len(positions) == 3 and {p['shard'] for p in positions} == {0, 1, 2}, resource_list
    assert all(p['hasMore'] is False for p in positions), 'resource listing truncated; no claim of completeness'
    resources = [r for r in resource_list['items'] if r['resourceId'] == resource_id]
    assert len(resources) == 1, 'created resource absent or duplicated in complete authoritative listing'
    current_resource = resources[0]
    current_resource_data = json.dumps(current_resource, separators=(',', ':')).encode()
    (out / 'current-resource.json').write_bytes(current_resource_data)
    assert current_resource['status'] == 'ASSIGNED', current_resource
    claim = current_resource['activeClaim']
    assert claim and claim['requestId'] == request_id and uuid.UUID(claim['attemptId']), current_resource
    replay, replay_headers = call('POST', '/api/v1/requests', 201, scenario['requestPayload'], scenario['requestKey'])
    assert replay == original and replay_headers.get('location') == original_headers['location'], 'create replay changed after match'
    report = dict(result='PASS', requestId=request_id, resourceId=resource_id, reciprocalClaim=True, originalCreateReplay=True)
    (out / 'result.json').write_text(json.dumps(report, indent=2) + '\n')
    print('A1.5 E2E SMOKE: PASS ' + json.dumps(report), flush=True)
    print('evidence=' + str(out), flush=True)


if __name__ == '__main__':
    try:
        main()
    except Exception as error:
        print('A1.5 E2E SMOKE: FAIL ' + str(error), file=sys.stderr)
        sys.exit(1)
