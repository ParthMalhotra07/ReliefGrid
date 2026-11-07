"""Fail-closed evidence for autoscaler decisions and Kubernetes pod exits."""
import datetime as dt
import json
import subprocess
import threading


def no_mid_load_downscale(samples):
    peak = 1
    for sample in samples[1:]:
        bulk = sample.get('phase', 'bulk' if not sample['loadFinished'] else 'settle') == 'bulk'
        if bulk and sample['desired'] < peak:
            raise AssertionError('matcher scaled down during the bulk workload')
        peak = max(peak, sample['desired'])


def clean_termination(pod):
    meta, status = pod['metadata'], pod['status']
    first = meta.get('firstDeletionTimestamp')
    grace = meta.get('firstDeletionGracePeriodSeconds')
    assert first and type(grace) is int and grace > 0, 'first positive-grace deletion deadline was not observed'
    deadline = dt.datetime.fromisoformat(first.replace('Z', '+00:00'))
    containers = status.get('containerStatuses', [])
    assert containers, 'deleted pod has no observed container exit'
    exits = []
    for container in containers:
        term = container.get('state', {}).get('terminated')
        assert term and term['exitCode'] == 0, 'pod was killed or its clean exit was not observed'
        finished = dt.datetime.fromisoformat(term['finishedAt'].replace('Z', '+00:00'))
        assert finished <= deadline, 'pod exceeded its deletion grace deadline'
        exits.append(dict(container=container['name'], **term))
    return dict(uid=meta['uid'], name=meta['name'], deadline=first, firstGracePeriodSeconds=grace, exits=exits)


class PodWatch:
    def __init__(self, kc, out):
        self.kc, self.out = kc, out
        self.pods, self.deleted, self.errors = {}, {}, []
        self.first_deletions = {}
        self.lock = threading.Lock()

    def start(self):
        self.process = subprocess.Popen(self.kc + ['--request-timeout=15m', 'get', 'pods', '-l', 'app.kubernetes.io/component in (api,matcher)',
            '--watch', '--output-watch-events', '-o', 'json'], stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        self.thread = threading.Thread(target=self._read, daemon=True)
        self.thread.start()

    def _read(self):
        decoder, buf = json.JSONDecoder(), ''
        try:
            for line in self.process.stdout:
                buf += line
                while buf.strip():
                    buf = buf.lstrip()
                    try:
                        event, end = decoder.raw_decode(buf)
                    except json.JSONDecodeError:
                        break
                    buf = buf[end:]
                    assert event['type'] in ('ADDED', 'MODIFIED', 'DELETED', 'BOOKMARK'), event
                    if event['type'] == 'BOOKMARK':
                        continue
                    pod = event['object']
                    # Preserve status and deletion timestamps, not pod env/Secrets.
                    record = dict(metadata={k:v for k,v in pod['metadata'].items() if k in
                        ('name', 'uid', 'labels', 'deletionTimestamp', 'deletionGracePeriodSeconds')},
                        spec=dict(terminationGracePeriodSeconds=pod['spec']['terminationGracePeriodSeconds']), status=pod['status'])
                    with self.lock:
                        uid = pod['metadata']['uid']
                        if record['metadata'].get('deletionTimestamp'):
                            first = self.first_deletions.setdefault(uid, dict(
                                firstDeletionTimestamp=record['metadata']['deletionTimestamp'],
                                firstDeletionGracePeriodSeconds=record['metadata'].get('deletionGracePeriodSeconds')))
                            record['metadata'].update(first)
                        self.pods[uid] = record
                        if event['type'] == 'DELETED':
                            self.deleted[pod['metadata']['uid']] = record
        except Exception as error:
            self.errors.append(str(error))

    def finish(self, live_uids):
        self.process.terminate()
        self.process.wait(timeout=10)
        self.thread.join(timeout=10)
        assert not self.thread.is_alive() and not self.errors, self.errors
        assert self.pods, 'pod watch observed nothing'
        missing = set(self.pods) - live_uids
        assert missing and missing <= set(self.deleted), 'a removed pod lacks its final watch event'
        exits = [dict(clean_termination(self.deleted[uid]), component=self.deleted[uid]['metadata']['labels']['app.kubernetes.io/component']) for uid in sorted(missing)]
        components = {self.deleted[uid]['metadata']['labels']['app.kubernetes.io/component'] for uid in missing}
        assert components == {'api', 'matcher'}, 'both API and matcher scale-down exits must be observed'
        (self.out / 'pod-exits.json').write_text(json.dumps(exits, indent=2)+'\n')
        return exits

    def stop(self):
        with self.lock:
            (self.out / 'pod-watch-last.json').write_text(json.dumps(dict(pods=self.pods, deleted=self.deleted, errors=self.errors), indent=2)+'\n')
        if self.process.poll() is None:
            self.process.terminate()
            self.process.wait(timeout=10)
        self.thread.join(timeout=10)
