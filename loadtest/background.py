#!/usr/bin/env python3
"""One S5.2 background-load sample, as one JSON line on stdout.

    python3 -I loadtest/background.py --broker <url> --api <url> --phase <name>

The load a run cannot see from k6: retry traffic that keeps going after k6
stops (every WAITING request goes round the retry tiers) and broker
backpressure (a full quorum queue nacks under reject-publish). Read through
kubectl port-forward to the broker's Prometheus port (15692) and the API's
metrics port (9090):

  brokerReceivedConfirm, brokerConfirmed  AMQP 0.9.1 publishes that asked for
      a confirm and those confirmed positively. Between two samples, the
      growth of the first minus the second is nacks, plus or minus at most
      one in-flight publish per publisher at either end (the relay keeps one
      outstanding), so a quiet window can read slightly negative.
  queues  ready and unacked per named queue (exclusive amq.gen-* queues are
      renamed per connection and left out).
  waiting  WAITING requests from the API's last complete workflow walk;
      waitingValid is false when that walk failed (the count is then stale,
      never zero by default), waitingObservedAtMs when it last completed.

A source that cannot be read gives a line with "error" and exit 1, so a gap
in the samples is recorded rather than silently skipped.
"""
import argparse
import json
import re
import sys
import time
import urllib.request

LINE = re.compile(r'^([a-zA-Z_:][a-zA-Z0-9_:]*)(?:\{([^}]*)\})?\s+(\S+)')
LABEL = re.compile(r'([a-zA-Z_][a-zA-Z0-9_]*)="((?:[^"\\]|\\.)*)"')


def metrics(url):
    with urllib.request.urlopen(url, timeout=4) as r:  # noqa: S310 - local port-forward
        text = r.read().decode()
    out = []
    for line in text.splitlines():
        m = LINE.match(line)
        if m and not line.startswith('#'):
            out.append((m.group(1), dict(LABEL.findall(m.group(2) or '')), float(m.group(3))))
    return out


def broker_figures(base):
    s = {}
    for name, labels, v in metrics(base + '/metrics'):
        if labels.get('protocol') != 'amqp091':
            continue
        if name == 'rabbitmq_global_messages_received_confirm_total':
            s['brokerReceivedConfirm'] = int(v)
        elif name == 'rabbitmq_global_messages_confirmed_total':
            s['brokerConfirmed'] = int(v)
    if len(s) != 2:
        raise ValueError('broker: confirm counters missing')
    queues = {}
    for name, labels, v in metrics(base + '/metrics/detailed?family=queue_coarse_metrics'):
        q = labels.get('queue', '')
        if not q or q.startswith('amq.gen-'):
            continue
        key = {'rabbitmq_detailed_queue_messages_ready': 'ready',
               'rabbitmq_detailed_queue_messages_unacked': 'unacked'}.get(name)
        if key:
            queues.setdefault(q, {'ready': 0, 'unacked': 0})[key] = int(v)
    if not queues:
        raise ValueError('broker: no queue depths')
    s['queues'] = dict(sorted(queues.items()))
    return s


def api_figures(base):
    waiting, valid, observed = 0, None, None
    for name, labels, v in metrics(base + '/metrics'):
        if name == 'relief_workflow_pending' and labels.get('state') == 'WAITING':
            waiting += int(v)
        elif name == 'relief_metrics_observation_success':
            valid = v == 1
        elif name == 'relief_metrics_last_success_timestamp_seconds':
            observed = round(v * 1000)
    if valid is None:
        raise ValueError('api: no workflow observation')
    return {'waiting': waiting, 'waitingValid': valid, 'waitingObservedAtMs': observed}


def main():
    p = argparse.ArgumentParser()
    p.add_argument('--broker', required=True)
    p.add_argument('--api', required=True)
    p.add_argument('--phase', required=True)
    a = p.parse_args()
    s = {'tMs': time.time_ns() // 1_000_000, 'phase': a.phase}
    errors = []
    for what, fn, url in (('broker', broker_figures, a.broker), ('api', api_figures, a.api)):
        try:
            s.update(fn(url.rstrip('/')))
        except Exception as e:  # noqa: BLE001 - every failure is a recorded gap
            errors.append(f'{what}: {e}')
    if errors:
        s = {'tMs': s['tMs'], 'phase': a.phase, 'error': '; '.join(errors)}
    print(json.dumps(s, separators=(',', ':')), flush=True)
    return 1 if errors else 0


if __name__ == '__main__':
    sys.exit(main())
