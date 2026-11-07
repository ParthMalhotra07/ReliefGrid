#!/usr/bin/env python3
"""Real kind autoscaling acceptance, including every removed pod's final exit."""
import base64
import concurrent.futures
import contextlib
import datetime as dt
import json
import os
from pathlib import Path
import re
import subprocess
import time
import urllib.parse
import urllib.request
import uuid
from autoscaling_evidence import PodWatch, no_mid_load_downscale, clean_termination

ROOT = Path(__file__).resolve().parents[1]


def queue_counts(queue):
    values = tuple(queue[name] for name in ('messages_ready', 'messages_unacknowledged'))
    assert all(type(value) is int and value >= 0 for value in values), 'invalid broker observation'
    return values


def acceptance(samples):
    assert len(samples) >= 3, 'missing scaling observations'
    assert samples[0]['desired'] == samples[0]['ready'] == 1
    up = [s for s in samples if s['ready'] > 1 and s['queueReady'] > 0]
    assert up, 'no Ready matcher scale-up alongside queue load'
    final = samples[-1]
    assert final['loadFinished'] and final['desired'] == final['ready'] == 1
    assert final['queueReady'] == final['unacked'] == 0, 'queue not drained'
    assert any(s['time'] < final['time'] for s in up)
    assert all(1 <= s['desired'] <= 8 and s['restarts'] == 0 for s in samples)


def assert_no_active_requests(http, api):
    # Query every nonterminal status separately. Old terminal history may span
    # many pages, but must neither block a new run nor hide unfinished work.
    for status in ('PENDING', 'WAITING', 'MATCHING', 'MATCHED', 'IN_PROGRESS',
                   'RETRYING', 'CANCELLING', 'FULFILLING'):
        page = http(api+'/requests?type=SHELTER&status='+status+'&limit=100')
        assert page['partial'] is False and page['failedShards'] == [], 'incomplete fixture scan'
        assert {p['shard'] for p in page['positions']} == {0, 1, 2}, 'missing fixture shard'
        assert not page['items'] and all(p['hasMore'] is False for p in page['positions']), 'fixture type has pre-existing unfinished work'


def main():
    os.chdir(ROOT)
    context = 'kind-' + os.environ.get('CLUSTER', 'reliefgrid')
    stamp = dt.datetime.now(dt.timezone.utc).strftime('%Y%m%dT%H%M%SZ')
    out = Path(os.environ.get('A43_EVIDENCE_DIR', str(ROOT / 'docs/evidence/A4.3' / stamp)))
    out.mkdir(parents=True, exist_ok=False)
    kc = ['kubectl', '--context', context, '--request-timeout=10s', '-n', 'reliefgrid']
    watch = PodWatch(kc, out)
    samples, created, resources = [], [], []
    report = dict(result='FAIL', workload='bounded real lifecycles: 32 VU bulk90s, 1 VU tail120..420s, API CPU120..210s', samples=samples)

    def run(args, timeout=20):
        return subprocess.check_output(args, text=True, timeout=timeout)

    def kube(*args):
        return json.loads(run(kc + list(args) + ['-o', 'json']))

    def save(name, value):
        (out / name).write_text(json.dumps(value, indent=2)+'\n')

    @contextlib.contextmanager
    def forward(service, port):
        with (out / ('forward-'+service+'.log')).open('w+') as log:
            p = subprocess.Popen(kc+['port-forward','svc/'+service,':'+str(port),'--address=127.0.0.1'],stdout=log,stderr=subprocess.STDOUT)
            try:
                deadline = time.monotonic()+20
                while time.monotonic() < deadline:
                    log.seek(0)
                    match = re.search(r'Forwarding from 127\.0\.0\.1:(\d+)',log.read())
                    if match:
                        yield 'http://127.0.0.1:'+match[1]
                        return
                    assert p.poll() is None, 'port-forward exited'
                    time.sleep(.2)
                raise TimeoutError('port-forward unavailable')
            finally:
                p.terminate()
                p.wait(timeout=10)

    secret = kube('get','secret','reliefgrid-rabbitmq')['data']
    address = urllib.parse.urlsplit(base64.b64decode(secret['amqp-url']).decode())
    auth = 'Basic '+base64.b64encode((urllib.parse.unquote(address.username)+':'+urllib.parse.unquote(address.password)).encode()).decode()

    def http(url, method='GET', body=None, broker=False, key=None):
        headers = {'Content-Type':'application/json'}
        if broker: headers['Authorization'] = auth
        if key: headers['Idempotency-Key'] = key
        request = urllib.request.Request(url, method=method, headers=headers, data=None if body is None else json.dumps(body).encode())
        with urllib.request.urlopen(request,timeout=15) as response:
            return json.load(response)

    scaled = kube('get','scaledobject','reliefgrid-matcher')
    hpas = kube('get','hpa')['items']
    assert sorted(h['spec']['scaleTargetRef']['name'] for h in hpas) == ['reliefgrid-api','reliefgrid-matcher']
    matcher_hpa = next(h for h in hpas if h['spec']['scaleTargetRef']['name']=='reliefgrid-matcher')
    assert any(o['uid']==scaled['metadata']['uid'] and o.get('controller') for o in matcher_hpa['metadata']['ownerReferences'])
    save('controllers.json',dict(scaledObject=scaled,hpas=hpas))
    save('source.json',dict(head=run(['git','rev-parse','HEAD']).strip(),changes=run(['git','status','--short'])))
    load = None
    with forward('reliefgrid-rabbitmq',15672) as broker, forward('reliefgrid-traefik',80) as ingress:
        api = ingress+'/api/v1'
        queue = lambda name: http(broker+'/api/queues/%2F/'+name,broker=True)
        dead_before = sum(queue_counts(queue('matching.dead')))
        watch.start()
        try:
            def sample(phase, done=False):
                qready, unacked = queue_counts(queue('matching.requests'))
                deployments = {d['metadata']['name']:d for d in kube('get','deploy')['items']}
                matcher, apid = deployments['reliefgrid-matcher'], deployments['reliefgrid-api']
                pods = kube('get','pods','-l','app.kubernetes.io/component in (api,matcher)')['items']
                s = dict(time=dt.datetime.now(dt.timezone.utc).isoformat(),phase=phase,desired=matcher['spec']['replicas'],
                    ready=matcher.get('status',{}).get('readyReplicas',0), apiDesired=apid['spec']['replicas'],
                    apiReady=apid.get('status',{}).get('readyReplicas',0),terminating=sum(bool(p['metadata'].get('deletionTimestamp')) for p in pods),queueReady=qready,unacked=unacked,loadFinished=done,
                    restarts=sum(c['restartCount'] for p in pods for c in p.get('status',{}).get('containerStatuses',[])))
                samples.append(s)
                save('report.json',report)
                print(json.dumps(s),flush=True)
                return s

            deadline = time.monotonic()+240
            while True:
                s=sample('baseline')
                if s['desired']==s['ready']==1 and s['apiDesired']==s['apiReady']==2 and s['queueReady']==s['unacked']==0:
                    samples[:] = [s]
                    break
                assert time.monotonic()<deadline, 'idle baseline did not settle'
                time.sleep(3)

            assert_no_active_requests(http, api)
            now=dt.datetime.now(dt.timezone.utc)
            def make_resource(n):
                body=dict(resourceType='SHELTER',name='Synthetic A4.3 '+str(n),location=dict(lat=56,lng=-123),
                    windowStart=(now-dt.timedelta(minutes=1)).isoformat(),windowEnd=(now+dt.timedelta(hours=2)).isoformat(),reusable=True)
                value=http(api+'/resources','POST',body,key=str(uuid.uuid4()))
                return value.get('resourceId',value.get('id'))
            with concurrent.futures.ThreadPoolExecutor(max_workers=4) as pool:
                resources.extend(pool.map(make_resource,range(4)))
            read_fixture=http(api+'/requests','POST',dict(requestType='WATER',details='x'*4096,peopleAffected=1,urgency='NORMAL',location=dict(lat=79,lng=140)),key=str(uuid.uuid4()))
            http(api+'/requests/'+read_fixture['requestId']+'/status','PUT',dict(status='CANCELLED'))
            save('fixtures.json',dict(resources=resources,readRequest=read_fixture['requestId']))
            version=run(['make','-s','print-K6_VERSION']).strip().removeprefix('v')
            binary=Path(os.environ.get('XDG_CACHE_HOME',str(Path.home()/'.cache')))/'reliefgrid-autoscaling'/('k6-v'+version+'-linux-amd64/k6')
            env=dict(os.environ,API_URL=ingress,RUN_ID=str(uuid.uuid4()),READ_REQUEST=read_fixture['requestId'])
            start=time.monotonic()
            start_wall=dt.datetime.now(dt.timezone.utc)
            cleaned=False
            with (out/'k6.log').open('w') as log:
                load=subprocess.Popen([str(binary),'run','--no-usage-report','--log-format=json','--summary-export',str(out/'k6-summary.json'),'scripts/autoscaling-load.js'],env=env,stdout=log,stderr=subprocess.STDOUT)
                deadline=start+780
                stable=0
                while time.monotonic()<deadline:
                    done=load.poll() is not None
                    phase='bulk' if time.monotonic()-start<90 else 'api' if 120<=time.monotonic()-start<210 else 'tail' if not done else 'settle'
                    s=sample(phase,done)
                    if done and not cleaned:
                        assert load.wait(timeout=10)==0, 'k6 failed: inspect the retained log'
                        for line in (out/'k6.log').read_text().splitlines():
                            try: entry=json.loads(line)
                            except json.JSONDecodeError: continue
                            if entry.get('msg','').startswith('A43_CREATED '):created.append(json.loads(entry['msg'][12:]))
                        assert len(created)>=32, 'real-request workload incomplete'
                        assert len({r['requestId'] for r in created})==len(created), 'duplicate created request IDs'
                        handoffs=[]
                        for line in (out/'k6.log').read_text().splitlines():
                            try: entry=json.loads(line)
                            except json.JSONDecodeError: continue
                            if entry.get('msg','').startswith('A43_HANDOFF '):handoffs.append(json.loads(entry['msg'][12:]))
                        assert {r['requestId'] for r in handoffs}=={r['requestId'] for r in created}, 'a real creation has no durable handoff observation'
                        assert any(r['status']=='MATCHED' and r['reciprocalVerified'] for r in handoffs), 'no reciprocal real match was observed'
                        byid={r['requestId']:r for r in handoffs}
                        for row in created:
                            assert byid[row['requestId']]['version']!=row['initialVersion']
                            current=http(api+'/requests/'+row['requestId'])
                            assert current['status']=='CANCELLED' and not current.get('assignedResourceId'), 'real lifecycle did not finish claim-free'
                        save('workflows.json',handoffs)
                        def cancel(row):
                            rid=row['requestId']
                            http(api+'/requests/'+rid+'/status','PUT',dict(status='CANCELLED'))
                            end=time.monotonic()+180
                            while True:
                                current=http(api+'/requests/'+rid)
                                if current['status']=='CANCELLED':
                                    assert not current.get('assignedResourceId') and not current.get('currentAttempt')
                                    return
                                assert time.monotonic()<end, 'fixture cancellation did not settle'
                                time.sleep(.2)
                        with concurrent.futures.ThreadPoolExecutor(max_workers=8) as pool:list(pool.map(cancel,created))
                        for rid in resources:
                            resource=http(api+'/resources/'+rid+'/status','PUT',dict(status='OFFLINE'))
                            assert resource['status']=='OFFLINE' and not resource.get('activeClaim')
                        cleaned=True
                    idle=done and cleaned and s['desired']==s['ready']==1 and s['apiDesired']==s['apiReady']==2 and s['queueReady']==s['unacked']==0 and s['terminating']==0
                    stable=stable+1 if idle else 0
                    if stable>=3:break
                    time.sleep(3)
                else:raise TimeoutError('real load/downscale/cleanup acceptance exceeded its bound')
            acceptance(samples)
            no_mid_load_downscale(samples)
            assert any(s['apiReady']>2 and s['phase'] in ('api','bulk') for s in samples), 'API HPA did not scale up under CPU load'
            assert sum(queue_counts(queue('matching.dead')))==dead_before, 'matching hints were rejected'
            pods=kube('get','pods','-l','app.kubernetes.io/component in (api,matcher)')['items']
            exits=watch.finish({p['metadata']['uid'] for p in pods})
            for component in ('api','matcher'):
                assert any(e['component']==component and any(90 <= (dt.datetime.fromisoformat(t['finishedAt'].replace('Z','+00:00'))-start_wall).total_seconds() <= 420 for t in e['exits']) for e in exits), 'no clean '+component+' downscale observed during real tail traffic'
            report.update(result='PASS',createdRequests=len(created),cleanPodExits=len(exits),peakReadyMatchers=max(s['ready'] for s in samples),peakReadyAPI=max(s['apiReady'] for s in samples))
        finally:
            if load is not None and load.poll() is None:
                load.terminate()
                load.wait(timeout=15)
            watch.stop()
            save('report.json',report)
    print('A4.3 LIVE VERIFY: '+report['result']+' evidence='+str(out))


if __name__=='__main__':main()
