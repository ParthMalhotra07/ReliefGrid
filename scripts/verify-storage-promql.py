#!/usr/bin/env python3
"""Evaluate dashboard PromQL against leader/follower and failed observations."""
import json
from pathlib import Path
import subprocess
import tempfile

root=Path(__file__).resolve().parents[1]
dashboard=json.loads((root/'deploy/helm/observability/dashboards/storage.json').read_text())
panel=next(p for p in dashboard['panels'] if 'unpublished outbox' in p['title'].lower() or p['title']=='Leader-observed pending rows by shard')
expression=panel['targets'][0]['expr']
def series(name,node,value):
    return dict(series=name+'{shard="0",node="'+node+'",job="test",instance="'+node+'"}',values=str(value)+'+0x2')
def observation(success=1,timestamp=60,up=1,pending=5):
    return [series('storage_raft_leader','leader',1),series('storage_raft_leader','follower',0),
        series('relief_outbox_pending','follower',999),series('relief_outbox_pending','leader',pending),
        series('relief_outbox_observation_success','leader',success),series('relief_outbox_observation_timestamp_seconds','leader',timestamp),
        dict(series='up{job="test",instance="leader"}',values=str(up)+'+0x2')]
cases=[]
for name,inputs,want in [('leader excludes lagging follower',observation(),5),('failed observation absent',observation(success=0),None),
    ('stale observation absent',observation(timestamp=0),None),('unreachable exporter absent',observation(up=0),None),
    ('observed empty is zero',observation(pending=0),0)]:
    cases.append(dict(name=name,interval='30s',input_series=inputs,promql_expr_test=[dict(expr=expression,eval_time='1m',
        exp_samples=[] if want is None else [dict(labels='{shard="0"}',value=want)])]))
config=dict(evaluation_interval='30s',tests=cases)
version=subprocess.check_output(['make','-s','print-PROMETHEUS_VERSION'],cwd=root,text=True).strip()
digest=subprocess.check_output(['make','-s','print-PROMETHEUS_IMAGE_DIGEST'],cwd=root,text=True).strip()
with tempfile.TemporaryDirectory(prefix='reliefgrid-promql-') as directory:
    Path(directory).chmod(0o755) # pinned image runs as nobody; synthetic test input only
    Path(directory,'cases.yaml').write_text(json.dumps(config))
    subprocess.run(['docker','run','--rm','--network=none','--entrypoint=/bin/promtool','-v',directory+':/test:ro',
        'prom/prometheus:'+version+'@'+digest,'test','rules','/test/cases.yaml'],check=True,timeout=45)
print('A4.2 storage PromQL leader/failure/freshness tests: PASS')
