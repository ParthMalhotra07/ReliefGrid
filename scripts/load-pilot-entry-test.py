#!/usr/bin/env python3
"""Actual pilot entrypoint with mocked cluster/host commands and bounded writers."""
import json
import os
from pathlib import Path
import shutil
import signal
import subprocess
import sys
import tempfile
import time
import unittest

ROOT=Path(__file__).resolve().parents[1]

def scratch(prefix):
    base=os.environ.get('RG_TEST_EVIDENCE')
    return Path(tempfile.mkdtemp(prefix=prefix,dir=base))

class Sidecars(unittest.TestCase):
    def test_existing_sidecars(self):
        for suffix in ('.runner.log','.runner-status.json','.report-check.log'):
            for dangling in (False,True):
                with self.subTest(suffix=suffix,dangling=dangling):
                    d=scratch('sidecar-');run=d/'run';side=Path(str(run)+suffix)
                    target=d/'missing-target'
                    if dangling: side.symlink_to(target)
                    else: side.write_text('retained evidence\n')
                    bin=d/'bin';bin.mkdir();called=d/'called'
                    mock=bin/'kubectl';mock.write_text('#!/bin/sh\ntouch "$CALLED"\nexit 1\n');mock.chmod(0o755)
                    env=os.environ.copy();env.update(RUN_DIR=str(run),K6_VERSION='v2.3.0',PATH=str(bin)+':'+env['PATH'],CALLED=str(called))
                    r=subprocess.run(['bash',str(ROOT/'loadtest/run.sh'),'smoke'],env=env,text=True,capture_output=True,timeout=5)
                    (d/'output.txt').write_text(r.stdout+r.stderr)
                    self.assertEqual(r.returncode,2,str(d))
                    self.assertFalse(run.exists())
                    self.assertFalse(called.exists())
                    if dangling:
                        self.assertTrue(side.is_symlink());self.assertEqual(side.readlink(),target);self.assertFalse(target.exists())
                    else: self.assertEqual(side.read_text(),'retained evidence\n')

    def test_fresh_and_concurrent_run_path(self):
        d=scratch('concurrent-');run=d/'run';bin=d/'bin';bin.mkdir()
        mock=bin/'kubectl';mock.write_text('#!/bin/sh\necho call >> "$CALLED"\nsleep .1\nexit 1\n');mock.chmod(0o755)
        env=os.environ.copy();env.update(RUN_DIR=str(run),K6_VERSION='v2.3.0',PATH=str(bin)+':'+env['PATH'],CALLED=str(d/'called'))
        command=['bash',str(ROOT/'loadtest/run.sh'),'smoke']
        processes=[subprocess.Popen(command,env=env,text=True,stdout=subprocess.PIPE,stderr=subprocess.PIPE) for _ in range(2)]
        outputs=[p.communicate(timeout=5) for p in processes]
        self.assertEqual(sorted(p.returncode for p in processes),[1,2])
        self.assertEqual((d/'called').read_text().splitlines(),['call'])
        self.assertEqual(json.loads(Path(str(run)+'.runner-status.json').read_text())['runnerExit'],1)
        self.assertTrue(Path(str(run)+'.runner.log').exists())
        (d/'outputs.json').write_text(json.dumps(outputs))

    def test_sidecar_reserved_between_check_and_open(self):
        d=scratch('reservation-');run=d/'run';bin=d/'bin';bin.mkdir()
        real_mkdir=shutil.which('mkdir')
        mock=bin/'mkdir';mock.write_text('#!/bin/sh\n'+real_mkdir+' "$@" || exit $?\nif [ "$*" = "$RUN_DIR" ]; then echo concurrent-evidence > "$RUN_DIR.runner-status.json"; fi\n');mock.chmod(0o755)
        env=os.environ.copy();env.update(RUN_DIR=str(run),K6_VERSION='v2.3.0',PATH=str(bin)+':'+env['PATH'])
        result=subprocess.run(['bash',str(ROOT/'loadtest/run.sh'),'smoke'],env=env,capture_output=True,text=True,timeout=5)
        (d/'output.txt').write_text(result.stdout+result.stderr)
        self.assertEqual(result.returncode,2)
        self.assertEqual(Path(str(run)+'.runner-status.json').read_text(),'concurrent-evidence\n')

class Entrypoint(unittest.TestCase):
    def prepare(self,mode):
        d=scratch('entry-'+mode+'-');repo=d/'repo';(repo/'loadtest').mkdir(parents=True)
        for name in ('pilot.sh','pilot-supervision.sh','supervise.py'):
            shutil.copy(ROOT/'loadtest'/name,repo/'loadtest'/name)
        (repo/'tools.mk').write_text('K6_VERSION := v2.3.0\n')
        a15=repo/'docs/evidence/A1.5';a15.mkdir(parents=True);(a15/'retained').write_text('evidence')
        (repo/'scripts').mkdir();(repo/'scripts/load-report-check.py').write_text('raise SystemExit(0)\n')
        (repo/'loadtest/cluster.sh').write_text('#!/bin/sh\nmkdir -p "$A15_EVIDENCE_DIR"\necho deployment > "$A15_EVIDENCE_DIR/log"\n')
        worker=repo/'worker.py'
        worker.write_text('''import json, os, pathlib, time
run=pathlib.Path(os.environ['RUN_DIR']);run.mkdir();(run/'raw').mkdir()
out=run.parent
(out/'writer-ready').touch()
with (out/'writes').open('a',buffering=1) as f:
 for i in range(COUNT):
  f.write(str(i)+'\\n');time.sleep(.02)
(run/'summary.json').write_text('{}')
(run/'raw/invcheck.json').write_text(json.dumps({'ok':True,'verdict':'clean','report':{'quiescent':True}}))
pathlib.Path(str(run)+'.runner-status.json').write_text(json.dumps({'runnerExit':0,'k6Exit':0,'reportExit':0,'teeExit':0,'safetyFailure':False}))
'''.replace('COUNT','1' if mode=='normal' else '150'))
        (repo/'loadtest/run.sh').write_text('exec /usr/bin/python3 "$(dirname "$0")/../worker.py"\n')
        bin=d/'bin';bin.mkdir();out=d/'evidence'
        real_systemctl=shutil.which('systemctl') or '/nonexistent/systemctl';real_run=shutil.which('systemd-run') or '/nonexistent/systemd-run'
        mocks={
          'git':'exit 0\n',
          'make':'echo "$*" >> "$TEST_ROOT/make.log"\ncase "$*" in *cluster-up*) touch "$TEST_ROOT/cluster";; *cluster-down*) rm -f "$TEST_ROOT/cluster"; touch "$TEST_ROOT/deleted";; esac\n',
          'kind':'case "$*" in "get clusters") if [ -e "$TEST_ROOT/cluster" ]; then echo reliefgrid; fi;; "get nodes --name reliefgrid") echo node;; *) exit 97;; esac\n',
          'docker':'echo \'[{"Id":"owned","Name":"/node","HostConfig":{"CpusetCpus":""}}]\'\n',
          'powerprofilesctl':'if [ "$*" != get ]; then exit 97; fi\nif [ "$TEST_MODE" = bad-restoration ] && [ -e "$TEST_OUT/writer-ready" ]; then echo performance; else echo balanced; fi\n',
          'systemd-run':('echo "$*" > "$TEST_ROOT/launch.log"\nexit 1\n' if mode=='launch-failure' else 'exec '+real_run+' "$@"\n'),
          'systemctl':'''if [ "$*" = "--user show-environment" ]; then touch "$TEST_ROOT/manager-inspected"; exit 0; fi
if [ "$TEST_MODE" = unconfirmed ] && [ "$2" = stop ] && [ -f "$TEST_OUT/smoke.supervisor/unit.txt" ]; then
 unit="$(cat "$TEST_OUT/smoke.supervisor/unit.txt")"
 if [ "$3" = "$unit" ]; then echo rejected >&2; exit 1; fi
fi
if [ "$TEST_MODE" = launch-failure ]; then
 if [ "$2" = stop ]; then exit 0; fi
 echo LoadState=not-found; exit 0
fi
exec REAL "$@"
'''.replace('REAL',real_systemctl),
        }
        for name,body in mocks.items():
            p=bin/name;p.write_text('#!/bin/sh\n'+body);p.chmod(0o755)
        env=os.environ.copy();env.update(AUTHORIZED_CLUSTER='reliefgrid',OUT=str(out),K6='/bin/false',PATH=str(bin)+':'+env['PATH'],TEST_ROOT=str(d),TEST_OUT=str(out),TEST_MODE=mode)
        # The manager receives required command paths explicitly; test behavior
        # lives in the mocked executables/payload, not an implementation hook.
        return d,repo,out,env,real_systemctl

    def test_launch_failure_before_cluster_creation(self):
        d,repo,out,env,_=self.prepare('launch-failure')
        r=subprocess.run(['bash',str(repo/'loadtest/pilot.sh')],env=env,text=True,capture_output=True,timeout=8)
        (d/'output.txt').write_text(r.stdout+r.stderr)
        self.assertNotEqual(r.returncode,0)
        self.assertFalse((d/'cluster').exists(),'cluster creation attempted: '+str(d))
        self.assertFalse((d/'make.log').exists(),'make invoked before launch proof: '+str(d))
        self.assertTrue((d/'launch.log').exists())
        self.assertTrue((d/'manager-inspected').exists())
        launch=(d/'launch.log').read_text()
        for option in ('--property=Type=exec','--property=RemainAfterExit=yes','--property=KillMode=control-group','--property=TimeoutStopSec=30s','/bin/true'):
            self.assertIn(option,launch)

    def test_missing_systemd_run_before_cluster_creation(self):
        d,repo,out,env,_=self.prepare('normal')
        bin=d/'bin';(bin/'systemd-run').unlink()
        for name in ('dirname','grep'):
            (bin/name).symlink_to(shutil.which(name))
        env['PATH']=str(bin)
        result=subprocess.run(['/bin/bash',str(repo/'loadtest/pilot.sh')],env=env,capture_output=True,text=True,timeout=5)
        (d/'output.txt').write_text(result.stdout+result.stderr)
        self.assertEqual(result.returncode,7)
        self.assertIn('systemd-run unavailable',result.stderr)
        self.assertFalse((d/'make.log').exists())

    @unittest.skipUnless(os.environ.get('RG_TEST_SYSTEMD')=='1','requires harmless user-systemd services')
    def test_actual_pilot(self):
        for mode,expected in (('normal',0),('signal',143),('unconfirmed',7),('bad-restoration',8)):
            with self.subTest(mode=mode):
                d,repo,out,env,ctl=self.prepare(mode)
                with (d/'output.txt').open('w') as log:
                    p=subprocess.Popen(['bash',str(repo/'loadtest/pilot.sh')],env=env,stdout=log,stderr=subprocess.STDOUT)
                    if mode!='normal':
                        deadline=time.monotonic()+10
                        while not (out/'writer-ready').exists() and p.poll() is None and time.monotonic()<deadline:time.sleep(.01)
                        self.assertTrue((out/'writer-ready').exists(),str(d))
                        p.send_signal(signal.SIGTERM)
                    rc=p.wait(timeout=15)
                # Harness-only cleanup after recording the pilot's outcome.
                scope=out/'smoke.supervisor';result=json.loads((scope/'result.json').read_text()) if (scope/'result.json').exists() else {}
                size=(out/'writes').stat().st_size if (out/'writes').exists() else 0
                time.sleep(.15)
                later=(out/'writes').stat().st_size if (out/'writes').exists() else 0
                if (scope/'unit.txt').exists():subprocess.run([ctl,'--user','stop',(scope/'unit.txt').read_text().strip()],capture_output=True,timeout=5)
                self.assertEqual(rc,expected,str(d)+' '+(d/'output.txt').read_text())
                self.assertTrue(result or mode=='normal')
                if mode in ('signal','bad-restoration'):
                    self.assertTrue(result['exitConfirmed']);self.assertEqual(size,later,'writer continued after wrapper return')
                    self.assertTrue((out/'host-after.json').exists())
                if mode=='signal':
                    self.assertIn('HOST_RESTORATION_CONFIRMED',(d/'output.txt').read_text())
                    self.assertIn(str(scope/'result.json'),(d/'output.txt').read_text())
                    self.assertEqual(result['signal'],15)
                if mode!='normal':
                    self.assertIn(str(scope/'result.json'),(d/'output.txt').read_text())
                    self.assertNotIn('unbound variable',(d/'output.txt').read_text())
                if mode=='unconfirmed':
                    self.assertFalse((out/'host-after.json').exists())
                    self.assertTrue((out/'host-before.json').exists())
                    self.assertFalse(result['exitConfirmed']);self.assertIn('EXIT_UNCONFIRMED',(d/'output.txt').read_text());self.assertIn('RESTORATION_UNVERIFIED',(d/'output.txt').read_text())
                if mode=='bad-restoration':self.assertIn('RESTORATION_UNVERIFIED',(d/'output.txt').read_text())
                if mode!='normal':self.assertTrue((d/'cluster').exists());self.assertFalse((d/'deleted').exists())
                self.assertEqual((repo/'docs/evidence/A1.5/retained').read_text(),'evidence')
                print(json.dumps({'mode':mode,'exit':rc,'exitConfirmed':result.get('exitConfirmed'),'writesAfterReturn':later>size,'evidence':str(d)}),flush=True)

if __name__=='__main__':unittest.main(verbosity=2)
