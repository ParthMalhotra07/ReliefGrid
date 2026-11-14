#!/usr/bin/env python3
"""classify-check.py <make check log>: decide whether a failed make check is only one of
the two known test-only flakes fixed in #180.

Prints one line: PASS (exit 0), KNOWN-FLAKE <details> (exit 3) or OTHER-FAILURE <details>
(exit 1). KNOWN-FLAKE requires every failing test to be one of:
  - TestDeployInternalAuth{UnsetIsUnchanged,False,True} whose got/want call logs are equal
    once the coordinator port-forward line is set aside (the background-order race);
  - TestHeartbeatAfterCloseIsRejected failing only with a connection reset after Close
    (the closed-connection error shape);
and no other failure: no other failing test, no compile, vet, lint or gofmt error.
"""
import re
import sys

log = open(sys.argv[1], errors='replace').read()
lines = log.splitlines()

exit_ok = re.search(r'^check=0$', log, re.M) is not None
make_err = re.search(r'^make: \*\*\* \[Makefile:\d+: (\S+)\]', log, re.M)
if exit_ok and not make_err:
    print('PASS')
    sys.exit(0)

problems, flakes = [], []
# Anything make failed on other than the test target is not a test flake.
if make_err and make_err.group(1) != 'test':
    problems.append(f'make target {make_err.group(1)} failed')
for pat, what in [(r'^\S+\.go:\d+:\d+: .*\((\w+)\)$', 'lint'), (r'^# \S+$', 'build'),
                  (r'^\S+\.go:\d+:\d+: (?!.*\(\w+\)$).*', 'compile/vet')]:
    for m in re.finditer(pat, log, re.M):
        problems.append(f'{what}: {m.group(0)[:160]}')

KNOWN_DEPLOY = {'TestDeployInternalAuthUnsetIsUnchanged', 'TestDeployInternalAuthFalse', 'TestDeployInternalAuthTrue'}
fails = [(i, m.group(1)) for i, l in enumerate(lines) for m in [re.match(r'^\s*--- FAIL: (\S+)', l)] if m]
if not fails and not problems:
    problems.append('make check failed with no failing test identified')


def block(start):
    """The indented failure output after a --- FAIL line."""
    out = []
    for l in lines[start + 1:]:
        if l.startswith('    ') or l.startswith('\t'):
            out.append(l)
        else:
            break
    return out


def calls(text):
    return [l.strip() for l in text if l.strip()]


for i, name in fails:
    top = name.split('/')[0]
    body = block(i)
    if top in KNOWN_DEPLOY:
        text = '\n'.join(body)
        m = re.search(r'got:\n(.*?)\n\s*want:\n(.*?)(?:\n\s*(?:reset|app):|\Z)', text, re.S)
        if not m:
            problems.append(f'{name}: no got/want call logs to compare')
            continue
        strip = lambda s: [l for l in calls(s.splitlines()) if ' port-forward svc/storage-coordinator ' not in l]
        got, want = strip(m.group(1)), strip(m.group(2))
        pf_got = sum(' port-forward svc/storage-coordinator ' in l for l in m.group(1).splitlines())
        pf_want = sum(' port-forward svc/storage-coordinator ' in l for l in m.group(2).splitlines())
        if got == want and pf_got == pf_want == 1:
            flakes.append(f'{name}: only the coordinator port-forward line moved')
        else:
            problems.append(f'{name}: call logs differ beyond the port-forward line')
    elif top == 'TestHeartbeatAfterCloseIsRejected':
        text = '\n'.join(body)
        if 'heartbeat after Close: error' in text and 'connection reset by peer' in text and 'reached Raft' not in text:
            flakes.append(f'{name}: connection reset after Close (handler not reached)')
        else:
            problems.append(f'{name}: not the connection-reset shape: {text[:200]!r}')
    else:
        problems.append(f'{name}: not a known flake')

if problems:
    print('OTHER-FAILURE ' + ' | '.join(problems))
    sys.exit(1)
print('KNOWN-FLAKE ' + ' | '.join(sorted(set(flakes))))
sys.exit(3)
