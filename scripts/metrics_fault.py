"""Fail-closed observations for a real leader kill/hold/recover run."""
import concurrent.futures
import http.client
import json
import math
import re
import subprocess
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid
from internal_auth import Sender



def mutation_version(payload, operation, key, ring):
    result = payload.get('result', {})
    assert payload.get('ringVersion') == ring and result.get('outcome') == 'OK', 'mutation not committed OK in the expected ring'
    assert result.get('operationId') == operation and result.get('key') == key, 'mutation result belongs to another operation/key'
    version = result.get('version')
    assert type(version) is int and version > 0, 'mutation omitted its committed version'
    return version


def judge(samples):
    assert samples, 'fault window observed nothing'
    for row in samples:
        for name in ('oldUp', 'oldDown', 'lag', 'age', 'targets'):
            value = row.get(name)
            assert value is None or (type(value) in (int, float) and math.isfinite(value) and value >= 0), 'invalid ' + name
    baseline, final = samples[0], samples[-1]
    assert baseline['phase'] == 'baseline' and baseline['oldUp'] == 1 and baseline['oldDown'] == 0 and baseline['targets'] == 9
    assert baseline['lag'] is not None and baseline['age'] is not None
    fault = [r for r in samples if r['phase'] == 'fault' and r['oldDown'] == 1 and r['oldUp'] == 0 and r['leader'] and r['leader'] != baseline['leader']]
    assert fault, 'no down member and replacement leader actually observed'
    lag = [r['lag'] for r in samples[1:] if r['lag'] is not None]
    assert lag and any(value != baseline['lag'] for value in lag), 'raw replication index-gap panel did not move'
    assert final['phase'] == 'healed' and final['targets'] == 9 and final['oldUp'] == 1 and final['oldDown'] == 0 and final['lag'] is not None and final.get('caughtUp') is True, 'recovery incomplete'
    assert final['age'] is not None and final['age'] >= baseline['age'] + 2, 'workflow-age panel did not move'
    return dict(oldLeader=baseline['leader'], newLeaders=sorted({r['leader'] for r in fault}),
                peakLag=max(lag), workflowAgeChange=final['age'] - baseline['age'], samples=len(samples))


class Traffic:
    """Bounded scratch KV writes; every retry preserves its operationId + At.

    Four workers own32keys each. All keys are tombstoned on stop; unrelated
    requests, resources, claims and outbox rows are never changed.
    """
    def __init__(self, origin, ring, out=None):
        self.origin, self.ring, self.out = origin, ring, out
        self.sender = Sender(origin)
        self.pending = {}
        self.prefix = 'a42-metrics-' + uuid.uuid4().hex
        self.stop_event = threading.Event()
        self.errors, self.writes = [], 0
        self.lock = threading.Lock()
        self.pool = concurrent.futures.ThreadPoolExecutor(max_workers=4)
        self.futures = []

    def command(self, key, version, delete=False):
        meta = dict(operationId=str(uuid.uuid4()), at=int(time.time()*1000), ringVersion=self.ring)
        url = self.origin + '/internal/data/' + urllib.parse.quote(key, safe='')
        if delete:
            url += '?' + urllib.parse.urlencode(meta)
            data = None
        else:
            data = json.dumps(dict(meta, value={'syntheticMetricsLoad':'x'*1024})).encode()
        with self.lock:
            self.pending[meta['operationId']] = dict(key=key, expectedVersion=version, delete=delete,
                meta=meta, body=None if data is None else data.decode())
            if self.out is not None:
                (self.out/'scratch-intents.json').write_text(json.dumps(self.pending,indent=2)+'\n')
        end = time.monotonic() + 20
        while True:
            request = urllib.request.Request(url, data=data, method='DELETE' if delete else 'PUT',
                headers={'Content-Type':'application/json', 'If-Match':'"'+str(version)+'"'})
            try:
                with self.sender.open(request, timeout=3) as response:
                    result = json.load(response)
                    committed_version = mutation_version(result,meta['operationId'],key,self.ring)
                    with self.lock:
                        self.pending.pop(meta['operationId'])
                        if self.out is not None:
                            (self.out/'scratch-intents.json').write_text(json.dumps(self.pending,indent=2)+'\n')
                    return committed_version
            except urllib.error.HTTPError as error:
                failure = json.load(error)
                if error.code != 503 or failure.get('code') not in ('OUTCOME_UNKNOWN','NO_LEADER','NOT_LEADER','STORAGE_UNAVAILABLE'):
                    raise RuntimeError('scratch command: HTTP'+str(error.code)+' '+str(failure)) from error
            except (OSError, json.JSONDecodeError, http.client.IncompleteRead):
                pass  # Retry the identical bytes and conditional version, never a fresh command.
            if time.monotonic() >= end:
                raise TimeoutError('scratch operation unresolved within its retry budget')
            time.sleep(.05)

    def worker(self, worker):
        versions = {}
        n = 0
        try:
            while not self.stop_event.is_set():
                key = self.prefix + ':' + str(worker) + ':' + str(n % 32)
                versions[key] = self.command(key, versions.get(key, 0))
                n += 1
                with self.lock:
                    self.writes += 1
        except Exception as error:
            self.errors.append(str(error))
            self.stop_event.set()
        finally:
            for key, version in versions.items():
                try:
                    self.command(key, version, delete=True)
                except Exception as error:
                    self.errors.append('scratch cleanup: ' + str(error))

    def start(self):
        self.futures = [self.pool.submit(self.worker, i) for i in range(4)]

    def recover_scratch(self):
        """Resolve immutable intents, then clean only authoritatively read KV.

        Called after workers stop. Recovery never clears the original errors or
        turns the failed acceptance run into PASS.
        """
        def persist():
            if self.out is not None:
                (self.out/'scratch-intents.json').write_text(json.dumps(self.pending,indent=2)+'\n')
        proof=dict(resolvedIntents=0,deletedKeys=0)
        for operation,intent in list(self.pending.items()):
            key,meta=intent['key'],intent['meta']
            assert re.fullmatch(re.escape(self.prefix)+r':[0-3]:(?:[0-9]|[12][0-9]|3[01])',key), 'foreign scratch key'
            assert operation==meta['operationId'] and meta['ringVersion']==self.ring and type(meta['at']) is int and meta['at']>0
            assert type(intent['expectedVersion']) is int and intent['expectedVersion']>=0
            url=self.origin+'/internal/data/'+urllib.parse.quote(key,safe='')
            if intent['delete']:
                assert intent['body'] is None
                url+='?'+urllib.parse.urlencode(meta);data=None
            else:
                body=json.loads(intent['body'])
                assert all(body.get(name)==value for name,value in meta.items())
                assert body.get('value')=={'syntheticMetricsLoad':'x'*1024}
                data=intent['body'].encode()
            end=time.monotonic()+20
            while True:
                request=urllib.request.Request(url,data=data,method='DELETE' if intent['delete'] else 'PUT',
                    headers={'Content-Type':'application/json','If-Match':'"'+str(intent['expectedVersion'])+'"'})
                try:
                    with self.sender.open(request,timeout=3) as response:
                        mutation_version(json.load(response),operation,key,self.ring)
                    break
                except urllib.error.HTTPError as error:
                    failure=json.load(error)
                    if error.code==412 and failure.get('code')=='VERSION_CONFLICT' and failure.get('outcomeUnknown') is False:
                        result=failure.get('result',{})
                        assert result.get('operationId')==operation and result.get('key')==key
                        break  # Definite rejection, not proof of current state.
                    if error.code!=503 or failure.get('code') not in ('OUTCOME_UNKNOWN','NO_LEADER','NOT_LEADER','STORAGE_UNAVAILABLE'):
                        raise
                except (OSError,json.JSONDecodeError,http.client.IncompleteRead):
                    pass
                assert time.monotonic()<end, 'original scratch intent still unresolved'
                time.sleep(.05)
            self.pending.pop(operation);persist();proof['resolvedIntents']+=1
        def owned_rows():
            cursor='';rows=[];expected={0,1,2}
            for _ in range(64):
                query=dict(kind='kv',limit=1000,ringVersion=self.ring,consistency='strong')
                if cursor:query['cursor']=cursor
                with self.sender.open(self.origin+'/internal/scan?'+urllib.parse.urlencode(query),timeout=3) as response:
                    page=json.load(response)
                assert page.get('partial') is False and page.get('failedShards',[])==[], 'incomplete scratch recovery scan'
                assert page.get('consistency') in ('strong','linearizable') and page.get('ringVersion')==self.ring, 'weak or wrong-ring recovery read'
                positions=page.get('positions',[]);answered={position['shard'] for position in positions}
                # Compare with the set fixed BEFORE this request. The coordinator
                # retains exactly hasMore shards after a complete page; exhausted
                # shards are not requested again (contracts/internal.yaml, scan).
                assert positions and len(positions)==len(answered) and answered==expected, 'incomplete or invalid answered shards'
                rows.extend(row for row in page['records'] if row['key'].startswith(self.prefix+':') and not row.get('deleted'))
                cursor=page.get('nextCursor','')
                expected={position['shard'] for position in positions if position.get('hasMore',False)}
                assert bool(cursor)==bool(expected), 'incomplete recovery continuation'
                if not cursor:return rows
            raise AssertionError('scratch recovery scan did not finish')
        rows=owned_rows();assert len(rows)<=128
        for row in rows:
            assert re.fullmatch(re.escape(self.prefix)+r':[0-3]:(?:[0-9]|[12][0-9]|3[01])',row['key']), 'foreign scratch key'
            assert row['kind']=='kv' and row['value']=={'syntheticMetricsLoad':'x'*1024} and type(row['version']) is int and row['version']>0, 'foreign scratch content'
            # Strong scan supplies current state; committed CAS protects against
            # changes after that read. Historical operation results never do.
            self.command(row['key'],row['version'],delete=True);proof['deletedKeys']+=1
        assert not owned_rows() and not self.pending, 'scratch cleanup incomplete'
        if self.out is not None:
            (self.out/'scratch-recovery.json').write_text(json.dumps(proof,indent=2)+'\n')

    def stop(self):
        self.stop_event.set()
        self.pool.shutdown(wait=True)
        if self.pending:
            try:
                self.recover_scratch()
            except Exception as error:
                self.errors.append('scratch recovery: '+str(error))
        assert not self.errors and self.writes >= 32, dict(errors=self.errors, writes=self.writes)


def run_fault(context, namespace, old_leader, number, query, get, coord, out):
    assert context.startswith('kind-') and old_leader.startswith('storage-shard-0-'), 'local shard0 fault only'
    kc = ['kubectl','--context',context,'--request-timeout=10s','-n',namespace]
    def kube(*args):
        return subprocess.check_output(kc+list(args), text=True, timeout=25)
    pod = json.loads(kube('get','pod',old_leader,'-o','json'))
    node = pod['spec']['nodeName']
    before = json.loads(kube('get','node',node,'-o','json'))
    assert not before['spec'].get('unschedulable',False), 'refuse to change an existing cordon'
    samples = []
    expr_age = 'max(relief_workflow_age_seconds{state="WAITING",urgency="HIGH"} and on(instance,job) (relief_metrics_observation_success == 1) and on(instance,job) (up == 1))'
    def sample(phase, caught_up=None):
        leaders = query('storage_raft_leader{shard="0"} == 1 and on(instance,job) (up == 1)')
        row = dict(phase=phase, time=time.time(), caughtUp=caught_up, leader=leaders[0]['metric']['node'] if len(leaders)==1 else None,
            oldUp=number('coordinator_node_state{shard="0",node="'+old_leader+'",state="UP"}'),
            oldDown=number('coordinator_node_state{shard="0",node="'+old_leader+'",state="DOWN"}'),
            lag=number('max(storage_replication_lag_entries and on(instance,job) (up == 1))'),
            targets=number('count(storage_raft_applied_index and on(instance,job) (up == 1))'), age=number(expr_age))
        samples.append(row)
        (out/'fault-samples.json').write_text(json.dumps(samples,indent=2)+'\n')
        return row
    baseline = sample('baseline')
    assert baseline['leader']==old_leader and baseline['oldUp']==1 and baseline['targets']==9 and baseline['lag'] is not None
    traffic = Traffic(coord, get(coord,'/internal/ring')['ringVersion'],out)
    (out/'scratch-prefix.txt').write_text(traffic.prefix+'\n')
    cordoned, stopped = False, False
    try:
        traffic.start()
        cordoned = True  # also restore when a cordon response is lost
        kube('cordon',node)
        kube('delete','pod',old_leader,'--grace-period=0','--force','--wait=false')
        end = time.monotonic()+35
        while time.monotonic()<end:
            sample('fault')
            time.sleep(1)
        kube('uncordon',node)
        cordoned = False
        end = time.monotonic()+120
        while time.monotonic()<end:
            row = sample('recovery')
            if row['targets']==9 and row['oldUp']==1:
                break
            time.sleep(1)
        else:
            raise TimeoutError('storage member did not recover after uncordon')
        try:
            traffic.stop()
        finally:
            stopped=True
        end = time.monotonic()+45
        while time.monotonic()<end:
            def status(name):
                return json.loads(kube('exec',name,'-c','storagenode','--','/storagectl','status'))
            names=['storage-shard-'+str(shard)+'-'+str(replica) for shard in range(3) for replica in range(3)]
            try:
                with concurrent.futures.ThreadPoolExecutor(max_workers=3) as pool:
                    statuses=list(pool.map(status,names))
                caught_up=all(s.get('caughtUp') is True for s in statuses)
                (out/'recovery-statuses.json').write_text(json.dumps(statuses,indent=2)+'\n')
            except (subprocess.CalledProcessError,subprocess.TimeoutExpired,json.JSONDecodeError) as error:
                caught_up=False
                (out/'recovery-status-error.txt').write_text(str(error)+'\n')
            row = sample('healed',caught_up)
            if caught_up and row['targets']==9 and row['oldUp']==1:
                break
            time.sleep(1)
        proof = judge(samples)
        proof['scratchWrites'] = traffic.writes
        (out/'fault-proof.json').write_text(json.dumps(proof,indent=2)+'\n')
        return proof
    finally:
        if cordoned:
            kube('uncordon',node)
        if not stopped:
            traffic.stop()
