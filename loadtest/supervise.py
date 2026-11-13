#!/usr/bin/env python3
"""Run a pilot stage in an owned user-systemd service; retain exit/stop evidence.

Exit 6: an orphan writer was stopped. Exit 7: scope exit could not be confirmed.
Otherwise return the command's exit, or 128+signal when interrupted.
"""
import argparse
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import time
import uuid

FIELDS = ('LoadState', 'ActiveState', 'SubState', 'ExecMainCode', 'ExecMainStatus', 'ControlGroup', 'Result')
FORWARD = ('PATH', 'HOME', 'KUBECONFIG', 'DOCKER_HOST', 'DOCKER_CONTEXT', 'CLUSTER', 'NAMESPACE',
           'APP_RELEASE', 'K6', 'K6_VERSION', 'RUN_DIR', 'A15_EVIDENCE_DIR', 'API_URL', 'SEED',
           'K6_CPUS', 'KIND_CPUS', 'POWER_PROFILE', 'MAX_VUS', 'START_RPS', 'STEP_RPS', 'STEPS',
           'RAMP_S', 'HOLD_S', 'RATE', 'DURATION_S', 'WARMUP_SECONDS', 'OPS', 'SAMPLE_SECONDS',
           'SETTLE_SECONDS', 'SPILL_DIR', 'TOOLS_IMAGE')


def call(args, timeout=15):
    return subprocess.run(args, capture_output=True, text=True, timeout=timeout, check=False)


def state(unit):
    result = call(['systemctl', '--user', 'show', unit, *['--property='+k for k in FIELDS]])
    values = dict(line.split('=', 1) for line in result.stdout.splitlines() if '=' in line)
    if values.get('LoadState') == 'not-found':
        return values
    if result.returncode or not all(k in values for k in FIELDS):
        raise RuntimeError('cannot inspect owned service')
    return values


def populated(values):
    # systemd clears ControlGroup after removing an empty group. This is valid
    # only alongside its recorded main exit or a successful explicit stop.
    group = values.get('ControlGroup')
    if not group:
        return False
    try:
        events = dict(line.split() for line in (Path('/sys/fs/cgroup') / group.lstrip('/') / 'cgroup.events').read_text().splitlines())
    except FileNotFoundError:
        return False
    if events.get('populated') not in ('0', '1'):
        raise RuntimeError('cannot determine owned cgroup population')
    return events['populated'] == '1'


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--evidence-dir', required=True)
    parser.add_argument('--timeout-seconds', type=float, default=1800)
    parser.add_argument('--stop-timeout-seconds', type=float, default=30)
    parser.add_argument('command', nargs=argparse.REMAINDER)
    args = parser.parse_args()
    command = args.command[1:] if args.command[:1] == ['--'] else args.command
    if not command or args.timeout_seconds <= 0 or args.stop_timeout_seconds <= 0:
        parser.error('command and positive deadlines required')
    evidence = Path(args.evidence_dir).resolve()
    evidence.mkdir(parents=True, exist_ok=False)
    unit = 'rg-load-'+uuid.uuid4().hex+'.service'
    report = {'unit': unit, 'exitConfirmed': False, 'commandExit': None, 'signal': None,
              'orphanWriter': False, 'supervisorExit': 7}
    (evidence/'unit.txt').write_text(unit+'\n')
    received = []
    def interrupted(signum, _frame):
        if not received:
            received.append(signum)
    for signum in (signal.SIGINT, signal.SIGTERM, signal.SIGHUP):
        signal.signal(signum, interrupted)
    def save():
        temporary = evidence/'result.json.tmp'
        temporary.write_text(json.dumps(report, indent=2)+'\n')
        temporary.replace(evidence/'result.json')
    def stop():
        result = call(['systemctl', '--user', 'stop', unit], args.stop_timeout_seconds+10)
        (evidence/'stop.log').write_text(result.stdout+result.stderr)
        if result.returncode:
            raise RuntimeError('owned service stop failed')
        deadline = time.monotonic()+args.stop_timeout_seconds
        while True:
            after = state(unit)
            report['afterStop'] = after
            if after.get('LoadState') == 'not-found' or (
                    after.get('ActiveState') in ('inactive', 'failed') and not populated(after)):
                report['exitConfirmed'] = True
                return
            if time.monotonic() >= deadline:
                raise RuntimeError('owned scope still active after stop deadline')
            time.sleep(.05)
    launch = ['systemd-run', '--user', '--unit='+unit, '--property=Type=exec',
              '--property=RemainAfterExit=yes', '--property=KillMode=control-group',
              '--property=TimeoutStopSec='+str(args.stop_timeout_seconds)+'s',
              '--property=StandardOutput=append:'+str(evidence/'service.log'),
              '--property=StandardError=append:'+str(evidence/'service.log'),
              '--working-directory='+os.getcwd()]
    launch += ['--setenv='+key+'='+os.environ[key] for key in FORWARD if key in os.environ]
    launch += ['--', *command]
    try:
        result = call(launch)
        (evidence/'launch.log').write_text(result.stdout+result.stderr)
        if result.returncode:
            raise RuntimeError('owned service launch failed')
        deadline = time.monotonic()+args.timeout_seconds
        while True:
            before = state(unit)
            report['beforeStop'] = before
            if received:
                report['signal'] = received[0]
                report['supervisorExit'] = 128+received[0]
                break
            if before.get('SubState') in ('exited', 'failed') and before.get('ExecMainCode') == '1':
                report['commandExit'] = int(before['ExecMainStatus'])
                report['orphanWriter'] = populated(before)
                report['supervisorExit'] = 6 if report['orphanWriter'] else report['commandExit']
                break
            if before.get('ActiveState') == 'failed':
                raise RuntimeError('command terminated without a recorded normal exit')
            if time.monotonic() >= deadline:
                raise RuntimeError('stage deadline exceeded')
            time.sleep(.05)
        stop()
    except (OSError, ValueError, RuntimeError, subprocess.TimeoutExpired) as error:
        report['error'] = str(error)
        report['supervisorExit'] = 7
        try:
            stop()
        except (OSError, ValueError, RuntimeError, subprocess.TimeoutExpired) as stop_error:
            report['stopError'] = str(stop_error)
    if received:
        report['signal'] = received[0]
        if report['supervisorExit'] != 7:
            report['supervisorExit'] = 128+received[0]
    save()
    if not report['exitConfirmed']:
        print('EXIT_UNCONFIRMED: preserve cluster and evidence; inspect '+unit, file=sys.stderr)
    elif report['supervisorExit'] == 7:
        print('SUPERVISOR_FAILED: '+report.get('error', 'inspect evidence')+'; evidence '+str(evidence), file=sys.stderr)
    elif report['orphanWriter']:
        print('ORPHAN_WRITER_STOPPED: owned scope exited; preserve evidence', file=sys.stderr)
    return report['supervisorExit']


if __name__ == '__main__':
    sys.exit(main())
