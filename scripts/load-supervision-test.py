#!/usr/bin/env python3
"""Focused supervisor tests. RG_TEST_SYSTEMD=1 enables harmless user services."""
import importlib.util
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import tempfile
import time
import unittest
from unittest.mock import patch

sys.dont_write_bytecode = True

ROOT = Path(__file__).resolve().parents[1]
SUPERVISOR = ROOT/'loadtest/supervise.py'


class ExitUnconfirmed(unittest.TestCase):
    def test_failed_stop_preserves_evidence_and_returns_seven(self):
        spec = importlib.util.spec_from_file_location('supervisor', SUPERVISOR)
        mod = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(mod)
        def fake_call(args, timeout=15):
            if args[0] == 'systemd-run':
                return subprocess.CompletedProcess(args, 0, '', '')
            if 'stop' in args:
                return subprocess.CompletedProcess(args, 1, '', 'mock stop rejected')
            return subprocess.CompletedProcess(args, 0, 'LoadState=loaded\nActiveState=active\nSubState=exited\nExecMainCode=1\nExecMainStatus=0\nControlGroup=\nResult=success\n', '')
        with tempfile.TemporaryDirectory() as d:
            evidence = Path(d)/'evidence'
            with patch.object(sys, 'argv', ['supervise', '--evidence-dir', str(evidence), '--', '/bin/true']), patch.object(mod, 'call', fake_call):
                handlers = {s: signal.getsignal(s) for s in (signal.SIGINT, signal.SIGTERM, signal.SIGHUP)}
                try:
                    self.assertEqual(mod.main(), 7)
                finally:
                    for sig, handler in handlers.items():
                        signal.signal(sig, handler)
            result = json.loads((evidence/'result.json').read_text())
            self.assertFalse(result['exitConfirmed'])
            self.assertEqual(result['supervisorExit'], 7)
            self.assertTrue((evidence/'stop.log').exists())


@unittest.skipUnless(os.environ.get('RG_TEST_SYSTEMD') == '1', 'set RG_TEST_SYSTEMD=1 for local user-systemd tests')
class RealProcesses(unittest.TestCase):
    def test_normal(self):
        self.exercise('normal', 0)

    def test_sigterm(self):
        self.exercise('signal', 143)

    def test_child_outlives_runner(self):
        self.exercise('orphan', 6)

    def test_pilot_normal(self):
        self.exercise('normal', 0, through_shell=True)

    def test_pilot_sigterm(self):
        self.exercise('signal', 143, through_shell=True)

    def test_pilot_child_outlives_runner(self):
        self.exercise('orphan', 6, through_shell=True)

    def exercise(self, mode, expected, through_shell=False):
        base = Path(os.environ.get('RG_TEST_EVIDENCE', tempfile.gettempdir()))
        d = Path(tempfile.mkdtemp(prefix='supervision-'+mode+'-', dir=base))
        mocks = d/'bin'; mocks.mkdir()
        for name in ('docker', 'kind', 'powerprofilesctl'):
            p = mocks/name
            p.write_text('#!/bin/sh\necho forbidden >> "'+str(d/'mock-calls')+'"\nexit 97\n')
            p.chmod(0o755)
        worker = d/'worker.py'
        worker.write_text('''import os, pathlib, signal, subprocess, sys, time
p=pathlib.Path(sys.argv[1]); mode=sys.argv[2]
(p/'run-dir.txt').write_text(os.environ['RUN_DIR'])
if mode == 'orphan':
 child=subprocess.Popen([sys.executable,__file__,str(p),'child'],stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL,start_new_session=True)
 (p/'child.pid').write_text(str(child.pid))
 deadline=time.monotonic()+3
 while not (p/'ready').exists() and time.monotonic()<deadline: time.sleep(.01)
 (p/'runner-exited').write_text('exit0')
 sys.exit(0)
if mode == 'child': signal.signal(signal.SIGTERM, signal.SIG_IGN)
with (p/'writes').open('a',buffering=1) as f:
 for i in range(250 if mode != 'normal' else 1):
  f.write(str(i)+'\\n'); (p/'ready').touch(); time.sleep(.02)
''')
        env = os.environ.copy(); env['PATH'] = str(mocks)+':'+env['PATH']
        scope = d/'scope'
        env['RUN_DIR'] = str(d/mode)
        command = [sys.executable, str(SUPERVISOR), '--evidence-dir', str(d/'scope'), '--stop-timeout-seconds', '.3', '--timeout-seconds', '8', '--', sys.executable, str(worker), str(d), mode]
        if through_shell:
            scope = d/(mode+'.supervisor')
            shell = d/'pilot-stage.sh'
            shell.write_text('''set -euo pipefail
ROOT="$1"; OUT="$2"; mode="$3"
source "$ROOT/loadtest/pilot-supervision.sh"
pilot_capture_restoration() { echo "mock host restoration confirmed"; }
supervise "$mode" /usr/bin/python3 "$OUT/worker.py" "$OUT" "$mode"
exit "$service_rc"
''')
            command = ['bash', str(shell), str(ROOT), str(d), mode]
        with (d/'wrapper.log').open('w') as log:
            p = subprocess.Popen(command, env=env, stdout=log, stderr=subprocess.STDOUT)
            if mode == 'signal':
                deadline = time.monotonic()+5
                while not (d/'ready').exists() and p.poll() is None and time.monotonic()<deadline:
                    time.sleep(.01)
                self.assertTrue((d/'ready').exists(), str(d))
                p.send_signal(signal.SIGTERM)
            rc = p.wait(timeout=15)
        self.assertEqual(rc, expected, str(d))
        result = json.loads((scope/'result.json').read_text())
        self.assertTrue(result['exitConfirmed'], result)
        self.assertIn(result['afterStop'].get('LoadState'), ('loaded', 'not-found'))
        self.assertEqual(result['supervisorExit'], expected)
        if mode == 'normal':
            self.assertEqual(result['commandExit'], 0)
        if mode == 'orphan':
            self.assertTrue((d/'runner-exited').exists())
            self.assertTrue(result['orphanWriter'])
        size = (d/'writes').stat().st_size
        time.sleep(.25)
        self.assertEqual((d/'writes').stat().st_size, size, 'child wrote after wrapper returned: '+str(d))
        self.assertFalse((d/'mock-calls').exists())
        self.assertEqual((d/'run-dir.txt').read_text(), str(d/mode))
        print(json.dumps({'case': mode, 'pilotShell': through_shell, 'exit': rc, 'exitConfirmed': result['exitConfirmed'], 'orphanWriter': result['orphanWriter'], 'cgroupBeforeStop': result['beforeStop']['ControlGroup'], 'writesAfterReturn': False, 'evidence': str(d)}), flush=True)


if __name__ == '__main__':
    unittest.main(verbosity=2)
