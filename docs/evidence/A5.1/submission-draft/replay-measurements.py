"""Read-only replay of retained reports; it starts no load or cluster."""
from decimal import Decimal
import json
from pathlib import Path
import statistics
import subprocess
import sys

root=Path(sys.argv[1]).resolve()
evidence=root/'docs/evidence/A5.1/submission-draft'
runs=root/'docs/evidence/S5.2'
def read(p): return json.loads(p.read_text(),parse_float=Decimal)
def at(doc,key):
 for name in key.split('.'): doc=doc[name]
 return doc
replayed=[]
with (evidence/'load-replay.txt').open('w') as log:
 for folder in sorted(runs.glob('S5.2-*')):
  if not (folder/'summary.json').is_file(): continue
  doc=read(folder/'summary.json')
  assert doc['git']=={'sha':'7dd19404d6df0f7c098810d4c1ad093355a0cd3a','dirty':False}
  args=['python3','-I','scripts/load-report-check.py',str(folder)]
  if doc['scenario']=='step':args.append('--require-saturation')
  result=subprocess.run(args,cwd=root,capture_output=True,text=True,timeout=30)
  log.write(result.stdout+result.stderr)
  assert result.returncode==0,folder
  replayed.append(folder.name)
assert len(replayed)==7
for scenario in ('baseline','step'):
 aggregate=read(runs/f'aggregate-{scenario}.json')
 docs=[read(runs/name/'summary.json') for name in aggregate['runs']]
 for key,spread in aggregate['figures'].items():
  values=[at(doc,key) for doc in docs]
  assert spread==dict(median=statistics.median(values),min=min(values),max=max(values),n=len(values)),key
base=read(runs/'aggregate-baseline.json')['figures']
step=read(runs/'aggregate-step.json')['figures']
soak=read(runs/'S5.2-soak-20261111T112933Z/summary.json')
assert base['results.classes.write.p99Ms']['median']==Decimal('684')
assert base['results.errors.rate']['median']*100==Decimal('0.100')
assert step['results.errors.rate']['median']*100==Decimal('0.095')
assert step['saturation.rps']['median']==50
assert all(read(runs/name/'summary.json')['saturation']['brokenAt']==60 for name in read(runs/'aggregate-step.json')['runs'])
assert soak['results']['errors']['rate']*100==Decimal('0.1275')
assert soak['load']['durationSeconds']==1800
assert soak['results']['droppedIterations']==0
for name in read(runs/'aggregate-baseline.json')['runs']:
 doc=read(runs/name/'summary.json')
 assert doc['load']['durationSeconds']==600 and doc['load']['warmupSeconds']==120
 assert doc['results']['droppedIterations']==0
text=(root/'docs/report/requirements-and-results.md').read_text()
for row in ('684.00 / 79.94 / 291.22','1049.34 / 102.43 / 367.06','838.84 / 93.75 / 330.37',
            '0.001 = 0.100%','0.00095 = 0.095%','0.001275 = 0.1275%'):
 assert row in text,row
print('MEASUREMENTS PASS: seven retained reports, three saturation brackets, all aggregate medians/min/max/n recomputed; fractions and percentages retain precision')
