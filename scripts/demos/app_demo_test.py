import importlib.util
import json
import pathlib
import tempfile
import threading
import unittest
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

spec = importlib.util.spec_from_file_location('app_demo', pathlib.Path(__file__).with_name('app_demo.py'))
app = importlib.util.module_from_spec(spec)
spec.loader.exec_module(app)


class ClientTests(unittest.TestCase):
    def test_lost_response_retries_identical_create_inputs_and_records_interruption(self):
        calls = []
        class Handler(BaseHTTPRequestHandler):
            def do_POST(self):
                calls.append((self.path, self.headers['Idempotency-Key'], self.rfile.read(int(self.headers['Content-Length']))))
                if len(calls) == 1:
                    self.connection.close()
                    return
                self.send_response(201)
                self.send_header('Content-Type', 'application/json')
                self.end_headers()
                self.wfile.write(b'{"requestId":"created"}')
            def log_message(self, *args):
                pass
        server = ThreadingHTTPServer(('127.0.0.1', 0), Handler)
        thread = threading.Thread(target=server.serve_forever)
        thread.start()
        try:
            with tempfile.TemporaryDirectory() as out:
                client = app.Client(f'http://127.0.0.1:{server.server_port}', pathlib.Path(out), budget=3)
                result = client.call('POST', '/requests', {201}, {'details': 'synthetic'}, 'same-key')
                self.assertEqual(result[0], {'requestId': 'created'})
                self.assertEqual(len(calls), 2)
                self.assertEqual(calls[0], calls[1])
                attempts = [json.loads(line) for line in (pathlib.Path(out) / 'attempts.jsonl').read_text().splitlines()]
                self.assertEqual([row['status'] for row in attempts], [0, 201])
        finally:
            server.shutdown()
            thread.join()
            server.server_close()

    def test_partial_or_truncated_list_is_never_complete(self):
        good = dict(partial=False, failedShards=[], consistency='strong', positions=[dict(shard=n, hasMore=False) for n in range(3)])
        app.require_complete(good)
        for bad in [dict(good, partial=True), dict(good, failedShards=[1]), dict(good, positions=good['positions'][:2]), dict(good, positions=[dict(shard=n, hasMore=(n == 1)) for n in range(3)])]:
            with self.assertRaises(AssertionError):
                app.require_complete(bad)

    def test_burst_audit_restores_admission_workers_after_failure(self):
        demo = app.Demo.__new__(app.Demo)
        demo.release = 'reliefgrid'
        demo.kube = lambda *args: json.dumps({'items': [
            {'metadata': {'name': 'reliefgrid-matcher', 'labels': {'app.kubernetes.io/component': 'matcher'}}, 'spec': {'replicas': 2}},
            {'metadata': {'name': 'reliefgrid-reconciler', 'labels': {'app.kubernetes.io/component': 'reconciler'}}, 'spec': {'replicas': 1}},
        ]})
        changes = []
        demo.scale = lambda component, replicas: changes.append((component, replicas))
        demo.check = lambda condition, text: self.assertTrue(condition, text)
        def audit():
            changes.append(('audit', 0))
            raise RuntimeError('audit failed')
        demo.invariants = audit
        with self.assertRaisesRegex(RuntimeError, 'audit failed'):
            demo.audit_without_admission()
        self.assertEqual(changes, [('matcher', 0), ('reconciler', 0), ('audit', 0), ('matcher', 2), ('reconciler', 1)])

    def test_missing_broker_counters_are_unknown_not_empty(self):
        self.assertIsNone(app.queue_counts({'name': 'matching.requests'}))
        self.assertEqual(app.queue_counts({'messages_ready': 0, 'messages_unacknowledged': 0}), (0, 0))

    def test_reciprocity_uses_current_claim_not_only_assignment(self):
        request = dict(requestId='r', status='MATCHED', assignedResourceId='s')
        resource = dict(id='s', status='ASSIGNED', activeClaim=dict(requestId='r', attemptId='attempt'))
        app.require_reciprocal(request, resource)
        for bad in [dict(resource, activeClaim=None), dict(resource, activeClaim=dict(requestId='other', attemptId='attempt')), dict(resource, status='AVAILABLE')]:
            with self.assertRaises(AssertionError):
                app.require_reciprocal(request, bad)


if __name__ == '__main__':
    unittest.main()
