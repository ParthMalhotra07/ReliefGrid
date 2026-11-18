# S5.2 pilot proposal (not executed)

Use only after separately authorizing the named fresh cluster, its deletion,
the runner's isolation changes, and the exclusive load slot. Freeze an integrated,
clean baseline containing #125 and the fixes in this diff first. The current
local dirty tree is implementation work, not acceptable performance evidence.

Confirmed tooling defects: smoke thresholds/sample counts were not acceptance
gates; zero requests crashed the checker; runner/checker/k6 outcomes were lost;
early failures had no run directory; restoration warnings did not fail the run;
cluster.sh could discard local A1.5 evidence. These are fixed locally.

Command-block defects are separate: an unguarded EXIT teardown can destroy a
cluster it did not create; log/clock guessing cannot identify output reliably;
a runner PID exiting does not prove every evidence writer exited; a snapshot
stored inside A1.5 changes what it is verifying; bare git diff misses untracked
and ignored files; host restoration must be checked after failures too.

Runner interfaces: RUN_DIR names a new directory (existing paths are refused).
Adjacent .runner.log, .runner-status.json and .report-check.log survive failure
without changing report checksums. Status fields retain runnerExit, k6Exit
(null if not reached), reportExit, teeExit and safetyFailure. Checker exit 3
means an inconclusive bracket (the first step already breaks a threshold) only
if all other validation passed; exit 1 is invalid evidence. A step run that
breaks no threshold up to its top step passes as "saturation > <top> req/s". Runner exit 4 means invariant/restoration safety failure;
exit 5 means its log writer failed on an otherwise successful run. A step
k6 exit 99 is usable only with a valid bracket; smoke requires k6 exit 0.

## Exact k6 setup

There is no k6 setup target/helper in #126 a5785ce or #125 bda2a4c.
#126 already contains the #125 pin; #125 supplies pin/install documentation. On the planned #123
baseline (inspected 468de44), the exact existing command is:

    bash scripts/autoscaling-load-tool.sh
    export K6="${XDG_CACHE_HOME:-$HOME/.cache}/reliefgrid-autoscaling/k6-v2.3.0-linux-amd64/k6"

That helper verifies the archive against make print-K6_SHA256 on every use.
For #126 plus #125 without #123, this equivalent explicit setup is available
as a proposed command (not executed here):

```bash
set -euo pipefail
version=v2.3.0
sha=39c3117b6af817592dcd0ce4242105c0a7af10948c2a425306f0be8f7a8a8ab1
cache="${XDG_CACHE_HOME:-$HOME/.cache}/reliefgrid-autoscaling"
mkdir -p "$cache"
archive="$cache/k6-$version-linux-amd64.tar.gz"
curl --fail --location --show-error --connect-timeout 10 --max-time 120 \
  "https://github.com/grafana/k6/releases/download/$version/k6-$version-linux-amd64.tar.gz" \
  -o "$archive.download"
printf '%s  %s\n' "$sha" "$archive.download" | sha256sum -c -
mv -- "$archive.download" "$archive"
tar -xzf "$archive" -C "$cache" "k6-$version-linux-amd64/k6"
export K6="$cache/k6-$version-linux-amd64/k6"
"$K6" version
```

## Executable pilot path

After a clean integrated baseline and authorization for the named cluster,
its isolation changes, and teardown, users execute the local executable entrypoint:

```bash
AUTHORIZED_CLUSTER=reliefgrid OUT=/absolute/new/evidence/path K6=/absolute/pinned/k6 \
  bash loadtest/pilot.sh
```

Set `PILOT_CALIBRATE=1` only when the fresh calibration stage is also authorized.
The default stops after accepted smoke and teardown. This is a proposed pilot;
no cluster, load or profile commands were exercised during local implementation.

`pilot.sh` sources the single `pilot-supervision.sh` implementation and calls
`loadtest/supervise.py` for deployment and every run, setting
`RUN_DIR=$OUT/<stage>` explicitly. Supervisor evidence is adjacent under
`$OUT/<stage>.supervisor`, separate from the runner's report checksums. The pilot
requires `exitConfirmed=true`, matching supervisor exit status, and the runner's
explicit `.runner-status.json` before acceptance or teardown.

Before any cluster creation, the entrypoint checks `systemd-run` availability
and manager inspection, then launches and reaps `/bin/true` through the exact
supervisor/options used for stages. Inspection success without transient launch
support is a preflight failure; it cannot create a cluster.

Every stage has its own supervisor deadline: `supervise <stage> <seconds>`
refuses a stage without one. A run stage gets its k6 duration plus 900 s
(`stage_timeout`: dataset, scrapes, settle, read-back, the audit and the
report) plus the 600 s bound on waiting for queue headroom before the audit's
pause; deployment 1800 s. The first pilot's step (960 s of load) hit the
old single 1800 s default during invcheck retries, and an 1800 s soak could
never have met it.

The user-systemd service has a unique recorded unit, `KillMode=control-group`
and a bounded stop deadline. A recorded main exit is valid when its cgroup is
already gone. On a signal or a child outliving the runner, the supervisor stops
the owned service and confirms its scope is gone/empty before returning. Exit 6
means an orphan writer was stopped; exit 7 means failure/uncertain scope exit.
Both preserve evidence and prevent pilot acceptance. The shell interruption
handler uses the shared stage evidence path, forwards the signal and waits for
the supervisor. Confirmed exit triggers an after-host snapshot and CPU/power
comparison before any possible teardown. For `pilot.sh`, exit 143 means confirmed SIGTERM exit
and verified restoration. Exit 7 explicitly reports unconfirmed exit and
unverified restoration; exit 8 reports missing/mismatched restoration after
confirmed exit. Both preserve cluster and evidence. No EXIT trap deletes clusters
or restores A1.5 blindly.

Ownership is armed only after fresh cluster creation and container-ID capture.
Exact IDs are checked before deletion. A1.5 tracked/untracked/ignored contents
are hashed with comparison output outside that directory; deployment evidence
is redirected outside it. CPU/power are captured before and after supervised
stages; failure or missing restoration proof blocks calibration and teardown.

## Available commands versus integrated prerequisites

Already on #126: cluster-up/down targets, smoke/step runner, checker, invcheck
JSON, and A15_EVIDENCE_DIR support in app deployment. Local to this patch:
pilot.sh, supervise.py, RUN_DIR/status sidecars, strict smoke gates and A1.5
preservation changes. The #123 k6 installation helper above requires that future
integration; it is absent from current #126. No new installation target is added.
The existing explicit checksum-verified setup block works independently of #123.

Runner startup refuses all three adjacent sidecars before mutation, including
dangling symlinks. Atomic mkdir and noclobber descriptor reservations prevent
concurrent invocations from replacing retained logs/status; later writes use the
reserved descriptors.

`load-pilot-entry-test.py` invokes the actual `pilot.sh` in a scratch copy with
mocked host/cluster commands and bounded payloads. It covers normal exit,
SIGTERM with restoration capture, unconfirmed exit preserving the cluster, and
restoration mismatch. It also covers failed transient launch before cluster
creation and runner sidecar/concurrency refusal. The existing supervisor checks
exercise harmless processes directly and through the shared library.

The actual user manager/cgroup v2 is tested with harmless bounded processes;
Docker, kind and power-profile commands remain mocked. Real deployment and host
restoration, full make check in a coordinated slot, a clean integrated SHA and
fresh pilot authorization remain required before performance evidence.
