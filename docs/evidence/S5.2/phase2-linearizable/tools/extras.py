#!/usr/bin/env python3
"""S5.2 Phase 2 extras for one run directory (phase<N>-run.sh's $OUT/<name>).

  extras.py <run-out-dir>  ->  JSON on stdout

From files the runner already keeps (no cluster access):
  raft    per shard, over the k6 window (raw/metrics-start -> raw/metrics-end)
          and, when <dir>/idle exists, over the idle sample (idle/metrics-0 -> -1):
            raftEntries  growth of the shard's highest storage_raft_commit_index
            commands     growth of storage_command_latency_seconds_count summed over
                         the shard's replicas (each mutation is counted once, on the
                         replica that received it; ok and error)
            nonCommand   raftEntries - commands: barriers, no-ops and configuration
                         entries (plus any errored command that still committed)
            fsmApplies   growth of the shard's highest storage_raft_fsm_apply_seconds_count:
                         entries that reached FSM.Apply (commands only)
            barriers     raftEntries - fsmApplies: committed entries that never reached
                         FSM.Apply (barriers, no-ops, configuration)
            appliedUncounted  fsmApplies - commands: commands applied but not yet
                         observed by the receiving replica's latency counter when
                         the end scrape was taken (in flight at the scrape)
            reads        growth of storage_read_latency_seconds_count by consistency
  coordinatorCpu  the storage-coordinator pod's CPU (millicores) per step window
                  (facts.json steps, ramp excluded), from raw/cgroup.jsonl.
"""
import datetime
import json
import os
import re
import sys

LINE = re.compile(r'^([a-zA-Z_:][a-zA-Z0-9_:]*)\{([^}]*)\}\s+(\S+)$')


def prom(path):
    out = []
    with open(path) as f:
        for line in f:
            m = LINE.match(line.strip())
            if m:
                labels = dict(re.findall(r'(\w+)="([^"]*)"', m.group(2)))
                out.append((m.group(1), labels, float(m.group(3))))
    return out


def snapshot(d):
    """Per shard: max commit index, summed command count, reads by consistency."""
    shards = {}
    for name in sorted(os.listdir(d)):
        if not (name.startswith('storage-shard-') and name.endswith('.prom')):
            continue
        for metric, lab, v in prom(os.path.join(d, name)):
            if 'shard' not in lab:
                continue
            s = shards.setdefault(int(lab['shard']), {'commit': 0.0, 'fsm': 0.0, 'commands': 0.0, 'reads': {}})
            if metric == 'storage_raft_commit_index':
                s['commit'] = max(s['commit'], v)
            elif metric == 'storage_raft_fsm_apply_seconds_count':
                s['fsm'] = max(s['fsm'], v)
            elif metric == 'storage_command_latency_seconds_count':
                s['commands'] += v
            elif metric == 'storage_read_latency_seconds_count':
                c = lab.get('consistency', '?')
                s['reads'][c] = s['reads'].get(c, 0.0) + v
    return shards


def delta(a_dir, b_dir, seconds):
    a, b = snapshot(a_dir), snapshot(b_dir)
    rows = []
    for shard in sorted(b):
        x, y = a.get(shard), b[shard]
        if x is None:
            continue
        entries = y['commit'] - x['commit']
        commands = y['commands'] - x['commands']
        reads = {c: int(y['reads'].get(c, 0) - x['reads'].get(c, 0)) for c in sorted(set(x['reads']) | set(y['reads']))}
        fsm = y['fsm'] - x['fsm']
        row = {'shard': shard, 'raftEntries': int(entries), 'commands': int(commands),
               'nonCommand': int(entries - commands), 'fsmApplies': int(fsm),
               'barriers': int(entries - fsm), 'appliedUncounted': int(fsm - commands), 'reads': reads}
        if seconds:
            row['seconds'] = round(seconds, 1)
            row['nonCommandPerS'] = round((entries - commands) / seconds, 2)
            row['commandsPerS'] = round(commands / seconds, 2)
        rows.append(row)
    return rows


def ts(s):
    return datetime.datetime.fromisoformat(s).timestamp() * 1000


def coordinator_cpu(run):
    facts = json.load(open(os.path.join(run, 'facts.json')))
    samples = []
    with open(os.path.join(run, 'raw', 'cgroup.jsonl')) as f:
        for line in f:
            r = json.loads(line)
            if r.get('pod', '').startswith('storage-coordinator-'):
                samples.append((r['tMs'], r['usageUsec']))
    samples.sort()
    steps = facts.get('steps') or []
    if not steps:  # baseline: one window, warm-up excluded
        start = ts(facts['startedAt']) + 1000 * facts.get('warmupSeconds', 0)
        steps = [{'targetRps': None, 'fromMs': start, 'toMs': ts(facts['endedAt'])}]
    out = []
    for st in steps:
        lo = st.get('fromMs') or ts(st['from'])
        hi = st.get('toMs') or ts(st['to'])
        w = [s for s in samples if lo <= s[0] <= hi]
        if len(w) < 2:
            out.append({'targetRps': st.get('targetRps'), 'millicores': None, 'samples': len(w)})
            continue
        (t0, u0), (t1, u1) = w[0], w[-1]
        out.append({'targetRps': st.get('targetRps'), 'millicores': round((u1 - u0) / 1000 / ((t1 - t0) / 1000), 1), 'samples': len(w)})
    return out


def main(out_dir):
    run = os.path.join(out_dir, 'run')
    res = {'dir': out_dir.replace(os.path.expanduser('~'), '~', 1)}
    if os.path.isdir(run):
        facts = json.load(open(os.path.join(run, 'facts.json')))
        res['runId'] = facts['runId']
        res['scenario'] = facts['scenario']
        res['git'] = facts['git']['sha']
        res['load'] = delta(os.path.join(run, 'raw', 'metrics-start'), os.path.join(run, 'raw', 'metrics-end'), None)
        res['coordinatorCpu'] = coordinator_cpu(run)
    idle = os.path.join(out_dir, 'idle')
    if os.path.isdir(idle):
        t0 = float(open(os.path.join(idle, 'metrics-0', 'at.txt')).read())
        t1 = float(open(os.path.join(idle, 'metrics-1', 'at.txt')).read())
        res['idle'] = delta(os.path.join(idle, 'metrics-0'), os.path.join(idle, 'metrics-1'), t1 - t0)
    json.dump(res, sys.stdout, indent=1)
    print()


if __name__ == '__main__':
    main(sys.argv[1])
