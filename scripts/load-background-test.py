#!/usr/bin/env python3
"""loadtest/background.py: one background-load sample from Prometheus text."""
import http.server
import json
from pathlib import Path
import subprocess
import sys
import threading
import unittest

ROOT = Path(__file__).resolve().parents[1]
SAMPLER = ROOT / 'loadtest' / 'background.py'

BROKER = '''# TYPE rabbitmq_global_messages_received_confirm_total counter
rabbitmq_global_messages_received_confirm_total{protocol="amqp091"} 425166.0
rabbitmq_global_messages_received_confirm_total{protocol="amqp10"} 7.0
# TYPE rabbitmq_global_messages_confirmed_total counter
rabbitmq_global_messages_confirmed_total{protocol="amqp091"} 412087.0
rabbitmq_global_messages_confirmed_total{protocol="amqp10"} 7.0
'''
DETAIL = '''# TYPE rabbitmq_detailed_queue_messages_ready gauge
rabbitmq_detailed_queue_messages_ready{vhost="/",queue="matching.requests"} 4549
rabbitmq_detailed_queue_messages_ready{vhost="/",queue="retry.120s"} 2104
rabbitmq_detailed_queue_messages_ready{vhost="/",queue="amq.gen-Ro6HeA9eTGeHCQapN54Log"} 0
# TYPE rabbitmq_detailed_queue_messages_unacked gauge
rabbitmq_detailed_queue_messages_unacked{vhost="/",queue="matching.requests"} 16
rabbitmq_detailed_queue_messages_unacked{vhost="/",queue="retry.120s"} 0
rabbitmq_detailed_queue_messages_unacked{vhost="/",queue="amq.gen-Ro6HeA9eTGeHCQapN54Log"} 0
'''
API = '''# TYPE relief_workflow_pending gauge
relief_workflow_pending{service="api",state="WAITING",urgency="CRITICAL"} 65
relief_workflow_pending{service="api",state="WAITING",urgency="HIGH"} 159
relief_workflow_pending{service="api",state="WAITING",urgency="NORMAL"} 2029
relief_workflow_pending{service="api",state="MATCHING",urgency="NORMAL"} 3
relief_metrics_observation_success{service="api"} 1
relief_metrics_last_success_timestamp_seconds{service="api"} 1.79437279708e+09
'''


def serve(pages):
    class Handler(http.server.BaseHTTPRequestHandler):
        def do_GET(self):
            body = pages.get(self.path)
            if body is None:
                self.send_response(404)
                self.end_headers()
                return
            self.send_response(200)
            self.end_headers()
            self.wfile.write(body.encode())

        def log_message(self, *args):
            pass

    server = http.server.ThreadingHTTPServer(('127.0.0.1', 0), Handler)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    return server, f'http://127.0.0.1:{server.server_address[1]}'


def sample(broker, api, phase='run'):
    r = subprocess.run([sys.executable, '-I', str(SAMPLER), '--broker', broker, '--api', api, '--phase', phase],
                       capture_output=True, text=True, timeout=10)
    return r.returncode, r.stdout, r.stderr


class Sample(unittest.TestCase):
    def setUp(self):
        self.broker, self.broker_url = serve({
            '/metrics': BROKER,
            '/metrics/detailed?family=queue_coarse_metrics': DETAIL,
        })
        self.api, self.api_url = serve({'/metrics': API})

    def tearDown(self):
        self.broker.shutdown()
        self.api.shutdown()

    def test_one_line_with_every_figure(self):
        rc, out, err = sample(self.broker_url, self.api_url, 'pause')
        self.assertEqual(rc, 0, err)
        lines = out.splitlines()
        self.assertEqual(len(lines), 1, out)
        s = json.loads(lines[0])
        self.assertIsInstance(s['tMs'], int)
        self.assertEqual(s['phase'], 'pause')
        # AMQP 0.9.1 only: the app's publishers; the difference is nacks
        # plus at most one in-flight publish per publisher.
        self.assertEqual(s['brokerReceivedConfirm'], 425166)
        self.assertEqual(s['brokerConfirmed'], 412087)
        # Named queues only: exclusive amq.gen-* queues change name per connection.
        self.assertEqual(s['queues'], {'matching.requests': {'ready': 4549, 'unacked': 16},
                                       'retry.120s': {'ready': 2104, 'unacked': 0}})
        self.assertEqual(s['waiting'], 2253)
        self.assertIs(s['waitingValid'], True)
        self.assertEqual(s['waitingObservedAtMs'], 1794372797080)
        self.assertNotIn('error', s)

    def test_invalid_walk_is_not_a_count(self):
        self.api.shutdown()
        self.api, self.api_url = serve({'/metrics': API.replace('relief_metrics_observation_success{service="api"} 1',
                                                                 'relief_metrics_observation_success{service="api"} 0')})
        rc, out, err = sample(self.broker_url, self.api_url)
        self.assertEqual(rc, 0, err)
        s = json.loads(out)
        self.assertIs(s['waitingValid'], False)
        self.assertEqual(s['waiting'], 2253)

    def test_unreachable_source_is_a_recorded_gap(self):
        self.api.shutdown()
        rc, out, err = sample(self.broker_url, 'http://127.0.0.1:9')
        self.assertEqual(rc, 1)
        s = json.loads(out)
        self.assertEqual(s['phase'], 'run')
        self.assertIsInstance(s['tMs'], int)
        self.assertIn('api', s['error'])
        self.assertNotIn('waiting', s)


if __name__ == '__main__':
    unittest.main(verbosity=2)
