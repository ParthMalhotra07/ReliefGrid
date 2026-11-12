from pathlib import Path
import re

root=Path(__file__).resolve().parents[4]
architecture=(root/'docs/report/architecture.md').read_text()
results=(root/'docs/report/requirements-and-results.md').read_text()
deployment=(root/'docs/report/deployment-and-demo.md').read_text()
errors=[]
for marker in ('hash mod 3', '0.2493', '0.75', 'PS Table 3', '1 s', '3 s', '5 s',
               '/internal/nodes/register', '/internal/nodes/heartbeat', '/internal/data/{key}'):
    if marker not in architecture: errors.append('architecture missing '+marker)
if 'PS §9.1' not in deployment: errors.append('Docker mapping missing')
gate=next((root/'docs/evidence').glob('gate-20261104T144814Z-*'))
for round in (1,2):
    raw4=(gate/f'round-{round}/demo-4.txt').read_text()
    raw6=(gate/f'round-{round}/demo-6.txt').read_text()
    raw7=(gate/f'round-{round}/demo-7.txt').read_text()
    dataset=next(line for line in raw4.splitlines() if line.startswith('PASS  dataset:'))
    counts='/'.join(re.findall(r'shard [0-2]: (\d+)',dataset))
    election=re.search(r'(\d+) ms after the stop',raw6).group(1)
    ack,down=re.search(r'PASS  load: exit 0, (\d+) writes acknowledged, (\d+) of them while',raw6).groups()
    start=re.search(r'0s \{"state":"Follower","appliedIndex":(\d+)',raw7).group(1)
    end=re.search(r'FSM at command (\d+)',raw7).group(1)
    expected=f'- Round {round}: shard counts `{counts}`; election `{election} ms`; acknowledged `{ack}`; while down `{down}`; replay command index `{start} -> {end}`.'
    if expected not in results: errors.append('recorded gate numbers differ/missing: '+expected)
for scenario in ('kill-leader','kill-follower','restart-shard','isolate-replica','isolate-leader','delay-isolate-leader','lose-quorum','delay-follower','stop-worker','restart-coordinator','control-lost-write','control-stale-reader'):
    if '`'+scenario+'`' not in results:errors.append('missing chaos scenario '+scenario)
for goal in ('S4.2','S4.3','S4.5','S4.6','S4.6b'):
    if goal not in results:errors.append('missing goal credit '+goal)
if errors:raise SystemExit('REVIEW137 VERIFY FAIL\n'+'\n'.join(errors))
print('REVIEW137 VERIFY PASS: two gate-round measurement rows match raw evidence; PS/Docker/scenario coverage present')
