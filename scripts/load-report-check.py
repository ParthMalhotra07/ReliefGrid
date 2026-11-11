#!/usr/bin/env python3
"""Check one S5.2 load report directory (docs/evidence/S5.2/<runId>/).

    python3 -I scripts/load-report-check.py <dir> [--require-saturation]

The directory holds summary.json (the report, schema below), the files it
lists under "raw" with committed=true, and SHA256SUMS covering every file in
the directory but itself. Prints one PASS or FAIL line per check and exits 1
on any FAIL, 2 on bad usage, 3 for an inconclusive saturation bracket
when every other validation passed.

What summary.json must state is what the plan asks a performance figure to
state: hardware, versions, dataset, concurrency, duration, error rate, the
percentile definition and, for a step run, the saturation point; matched
latency is labelled "matched only" with the waiting count beside it.
invcheck must have run with retry admission stopped (audit.mode
"without-admission") and the matcher restored afterwards; the background
load k6 cannot see (unconfirmed publishes, queue depth, WAITING) is stated
for the run, after it, at the pause and for every step.
"""
import hashlib
import json
import math
import os
import re
import sys

# Writes (creates, registrations, matches), single-key reads (GET by ID) and
# listings (requests and resources, a scan across every shard) each have a
# p99 limit (decided 2026-11-06).
THRESHOLDS = {"writeP99Ms": 1000, "readP99Ms": 250, "listP99Ms": 500, "errorRate": 0.01}
CLASSES = ("write", "read", "list")
SCENARIOS = {"smoke", "baseline", "step", "soak", "contention", "fault"}
SHA40 = re.compile(r"^[0-9a-f]{40}$")
SHA64 = re.compile(r"^[0-9a-f]{64}$")
DIGEST = re.compile(r"^sha256:[0-9a-f]{64}$")

fails = 0
inconclusive = False


def ok(msg):
    print(f"PASS  {msg}")


def bad(msg):
    global fails
    fails += 1
    print(f"FAIL  {msg}")


def get(doc, path):
    cur = doc
    for p in path.split("."):
        if not isinstance(cur, dict) or p not in cur:
            return None
        cur = cur[p]
    return cur


def num(v):
    return isinstance(v, (int, float)) and not isinstance(v, bool) and math.isfinite(v)


def need(doc, path, kind):
    """Check doc.path exists and is of kind: str, pos (number > 0), nonneg, int, bool."""
    v = get(doc, path)
    good = {
        "str": isinstance(v, str) and v.strip() != "",
        "pos": num(v) and v > 0,
        "nonneg": num(v) and v >= 0,
        "int": isinstance(v, int) and not isinstance(v, bool),
        "nonnegint": isinstance(v, int) and not isinstance(v, bool) and v >= 0,
        "posint": isinstance(v, int) and not isinstance(v, bool) and v > 0,
        "bool": isinstance(v, bool),
    }[kind]
    if not good:
        bad(f"{path}: missing or not {kind} (got {v!r})")
    return good


def is_int(v):
    return isinstance(v, int) and not isinstance(v, bool)


def check_background_window(doc, path):
    """One Background window: samples, gaps, unconfirmed publishes, queue peaks, WAITING."""
    w = get(doc, path)
    if not isinstance(w, dict):
        bad(f"{path}: missing")
        return False
    good = need(doc, f"{path}.samples", "nonnegint") and need(doc, f"{path}.gaps", "nonnegint")
    if not is_int(w.get("publishesNotConfirmed")):
        bad(f"{path}.publishesNotConfirmed: missing or not an integer (got {w.get('publishesNotConfirmed')!r})")
        good = False
    peak = w.get("queuePeak")
    if not (isinstance(peak, dict) and all(is_int(v) and v >= 0 for v in peak.values())):
        bad(f"{path}.queuePeak: want queue -> messages")
        good = False
    for k in ("waitingFirst", "waitingLast"):
        if k not in w or not (w[k] is None or (is_int(w[k]) and w[k] >= 0)):
            bad(f"{path}.{k}: want a count or null (got {w.get(k)!r})")
            good = False
    if good and w["samples"] > w["gaps"] and not peak:
        bad(f"{path}.queuePeak: good samples but no queue depths")
        good = False
    return good


def check_background(doc):
    if not isinstance(doc.get("background"), dict):
        bad("background: missing (retry load and broker backpressure)")
        return
    good = all([check_background_window(doc, f"background.{k}") for k in ("run", "afterRun")])
    if good:
        run = doc["background"]["run"]
        if run["samples"] == 0:
            bad("background.run.samples: the run was not sampled")
            good = False
        elif run["gaps"] >= run["samples"]:
            bad(f"background.run: every one of {run['samples']} samples is a gap")
            good = False
    pause = get(doc, "background.atPause")
    if not (isinstance(pause, dict) and isinstance(pause.get("queues"), dict) and pause["queues"]
            and is_int(pause.get("waiting")) and isinstance(pause.get("waitingValid"), bool)):
        bad(f"background.atPause: want the pause sample's queues, waiting and waitingValid (got {pause!r})")
        good = False
    for i, st in enumerate(doc.get("steps") or []):
        if isinstance(st, dict) and "background" not in st:
            bad(f"steps[{i}].background: missing")
            good = False
        elif isinstance(st, dict) and not check_background_window(st, "background"):
            bad(f"steps[{i}].background: invalid")
            good = False
    if good:
        run, after = doc["background"]["run"], doc["background"]["afterRun"]
        top = max(run["queuePeak"].items(), key=lambda kv: kv[1])
        ok(f"background: {run['samples']} samples ({run['gaps']} gaps), {run['publishesNotConfirmed']} unconfirmed publishes, "
           f"peak {top[0]} {top[1]}, waiting {run['waitingFirst']}->{run['waitingLast']}; after the run "
           f"{after['publishesNotConfirmed']} unconfirmed, waiting {after['waitingLast']} at the pause")


def check_audit(doc):
    if get(doc, "audit.mode") != "without-admission":
        bad(f"audit.mode: invcheck needs retry admission stopped, want 'without-admission', got {get(doc, 'audit.mode')!r}")
        return
    good = need(doc, "audit.matcherReplicas", "posint")
    if good and get(doc, "audit.matcherReplicasAfter") != get(doc, "audit.matcherReplicas"):
        bad(f"audit.matcherReplicasAfter: {get(doc, 'audit.matcherReplicasAfter')!r}, "
            f"the run found {get(doc, 'audit.matcherReplicas')}: matcher not restored")
        good = False
    for k in ("read", "published", "marked"):
        good = need(doc, f"audit.drain.{k}", "nonnegint") and good
    if good:
        ok(f"audit: without admission (matcher {doc['audit']['matcherReplicas']} -> 0 -> {doc['audit']['matcherReplicasAfter']}), "
           f"drained {doc['audit']['drain']['marked']} handoffs")


def cpus(spec):
    """A cpuset list ("0-5", "6,7") as a set; empty if unparsable."""
    out = set()
    if not isinstance(spec, str):
        return out
    for part in spec.split(","):
        lo, _, hi = part.strip().partition("-")
        try:
            out.update(range(int(lo), int(hi or lo) + 1))
        except ValueError:
            return set()
    return out


def check_files(d, doc):
    sums_path = os.path.join(d, "SHA256SUMS")
    if not os.path.isfile(sums_path):
        bad("SHA256SUMS: missing")
        return
    listed = {}
    with open(sums_path) as f:
        for line in f:
            line = line.rstrip("\n")
            if not line:
                continue
            m = re.match(r"^([0-9a-f]{64})  (.+)$", line)
            if not m:
                bad(f"SHA256SUMS: malformed line {line!r}")
                continue
            listed[m.group(2)] = m.group(1)
    present = set()
    for root, _, files in os.walk(d):
        for name in files:
            rel = os.path.relpath(os.path.join(root, name), d)
            if rel != "SHA256SUMS":
                present.add(rel)
    good = True
    for rel in sorted(present - set(listed)):
        bad(f"SHA256SUMS: {rel} is not listed")
        good = False
    for rel, want in sorted(listed.items()):
        p = os.path.join(d, rel)
        if not os.path.isfile(p):
            bad(f"SHA256SUMS: lists {rel}, which is missing")
            good = False
            continue
        with open(p, "rb") as f:
            got = hashlib.sha256(f.read()).hexdigest()
        if got != want:
            bad(f"SHA256SUMS: {rel} does not match its checksum")
            good = False
    if good:
        ok(f"SHA256SUMS: {len(listed)} files match")

    raw = doc.get("raw")
    if not isinstance(raw, list) or not raw:
        bad("raw: missing (list every raw data file, committed or not)")
        return
    good = True
    for i, r in enumerate(raw):
        path = r.get("path") if isinstance(r, dict) else None
        if not isinstance(path, str) or not path:
            bad(f"raw[{i}]: no path")
            good = False
        elif r.get("committed") is True:
            if path not in listed:
                bad(f"raw[{i}] {path}: committed but not in the directory and SHA256SUMS")
                good = False
        elif r.get("committed") is False:
            if not (num(r.get("bytes")) and isinstance(r.get("sha256"), str) and SHA64.match(r["sha256"])):
                bad(f"raw[{i}] {path}: not committed, so bytes and sha256 are required")
                good = False
        else:
            bad(f"raw[{i}] {path}: committed must be true or false")
            good = False
    if good:
        ok(f"raw: {len(raw)} files accounted for")


def check_percentiles(doc, path):
    if not all(need(doc, f"{path}.{k}", kind) for k, kind in
               (("count", "posint"), ("p50Ms", "nonneg"), ("p95Ms", "nonneg"), ("p99Ms", "nonneg"))):
        return False
    c = get(doc, path)
    if not c["p50Ms"] <= c["p95Ms"] <= c["p99Ms"]:
        bad(f"{path}: p50 <= p95 <= p99 does not hold ({c['p50Ms']}, {c['p95Ms']}, {c['p99Ms']})")
        return False
    return True


def broken_by(s):
    out = []
    if s["writeP99Ms"] > THRESHOLDS["writeP99Ms"]:
        out.append("writeP99")
    if s["readP99Ms"] > THRESHOLDS["readP99Ms"]:
        out.append("readP99")
    if s["listP99Ms"] > THRESHOLDS["listP99Ms"]:
        out.append("listP99")
    if s["errorRate"] > THRESHOLDS["errorRate"]:
        out.append("errorRate")
    if s["droppedIterations"] > 0:
        out.append("droppedIterations")
    return out


def check_saturation(doc):
    global inconclusive
    steps = doc.get("steps")
    if not isinstance(steps, list) or len(steps) < 2:
        bad("steps: a saturation run needs at least two steps")
        return
    for i, s in enumerate(steps):
        for k in ("targetRps", "achievedRps", "writeP99Ms", "readP99Ms", "listP99Ms", "errorRate", "droppedIterations"):
            if not (isinstance(s, dict) and num(s.get(k)) and s[k] >= 0):
                bad(f"steps[{i}].{k}: missing or negative")
                return
    targets = [s["targetRps"] for s in steps]
    if any(b <= a for a, b in zip(targets, targets[1:])):
        bad(f"steps: targetRps must strictly increase (got {targets})")
        return
    first_broken = next((i for i, s in enumerate(steps) if broken_by(s)), None)
    if first_broken is None:
        inconclusive = True
        print(f"INCONCLUSIVE  saturation: no step breaks a threshold up to {targets[-1]} req/s; raise the top step")
        return
    if first_broken == 0:
        inconclusive = True
        print(f"INCONCLUSIVE  saturation: the first step ({targets[0]} req/s) already breaks {broken_by(steps[0])}; start lower")
        return
    sat = doc.get("saturation")
    want_rps, want_at = targets[first_broken - 1], targets[first_broken]
    if not isinstance(sat, dict) or sat.get("rps") != want_rps or sat.get("brokenAt") != want_at:
        bad(f"saturation: the steps give {want_rps} req/s (broken at {want_at}), the report says {sat!r}")
        return
    want_by = broken_by(steps[first_broken])
    if sorted(sat.get("brokenBy") or []) != sorted(want_by):
        bad(f"saturation.brokenBy: step {want_at} breaks {want_by}, the report says {sat.get('brokenBy')!r}")
        return
    ok(f"saturation: {want_rps} req/s, broken at {want_at} by {','.join(want_by)}")


def main(argv):
    if len(argv) not in (2, 3) or (len(argv) == 3 and argv[2] != "--require-saturation"):
        print(__doc__.strip().splitlines()[2], file=sys.stderr)
        return 2
    d, require_sat = argv[1], len(argv) == 3
    try:
        with open(os.path.join(d, "summary.json")) as f:
            doc = json.load(f)
    except (OSError, ValueError) as e:
        bad(f"summary.json: {e}")
        return 1
    print(f"== {d}: {doc.get('runId')} ({doc.get('scenario')})")

    if doc.get("goal") != "S5.2":
        bad(f"goal: want S5.2, got {doc.get('goal')!r}")
    need(doc, "runId", "str")
    if doc.get("scenario") not in SCENARIOS:
        bad(f"scenario: {doc.get('scenario')!r} is not one of {sorted(SCENARIOS)}")
    need(doc, "startedAt", "str")

    # Provenance: the tree, the host and every version a figure depends on.
    if not (isinstance(get(doc, "git.sha"), str) and SHA40.match(get(doc, "git.sha"))):
        bad(f"git.sha: want a full 40-hex commit, got {get(doc, 'git.sha')!r}")
    if get(doc, "git.dirty") is not False:
        bad("git.dirty: the run must come from a clean tree")
    for path, kind in (("host.cpuModel", "str"), ("host.logicalCpus", "pos"), ("host.memTotalGiB", "pos"),
                       ("host.kernel", "str"), ("host.powerProfile", "str"), ("host.k6Cpus", "str"),
                       ("host.kindCpus", "str"), ("host.powerProfileRestored", "str"),
                       ("host.swapUsedMiB", "nonneg"), ("host.loadAvgStart", "nonneg"),
                       ("host.cpuMHzStart", "pos"), ("host.cpuMHzEnd", "pos")):
        need(doc, path, kind)
    kind_cpus, k6_cpus = cpus(get(doc, "host.kindCpus")), cpus(get(doc, "host.k6Cpus"))
    if kind_cpus and k6_cpus and kind_cpus & k6_cpus:
        bad(f"host: k6 CPUs {get(doc, 'host.k6Cpus')} overlap the kind nodes' {get(doc, 'host.kindCpus')}")
    for v in ("k6", "kind", "kindNodeImage", "kubectl", "helm", "go"):
        need(doc, f"versions.{v}", "str")
    images = get(doc, "versions.images")
    if not isinstance(images, dict) or not images:
        bad("versions.images: missing")
    else:
        for name, digest in sorted(images.items()):
            if not (isinstance(digest, str) and DIGEST.match(digest)):
                bad(f"versions.images.{name}: want sha256:<64 hex>, got {digest!r}")
    for path in ("config.raftProfile", "config.readFence", "config.tracing", "config.autoscaling"):
        need(doc, path, "str")

    # What was offered: dataset, shape, mix, thresholds, percentile definition.
    for path, kind in (("dataset.seed", "int"), ("dataset.ops", "pos"), ("dataset.requests", "pos"),
                       ("dataset.resources", "pos")):
        need(doc, path, kind)
    if not (isinstance(get(doc, "dataset.sha256"), str) and SHA64.match(get(doc, "dataset.sha256"))):
        bad("dataset.sha256: want 64 hex")
    for path, kind in (("load.executor", "str"), ("load.durationSeconds", "pos"),
                       ("load.warmupSeconds", "nonneg"), ("load.maxVUs", "pos")):
        need(doc, path, kind)
    mix = get(doc, "load.mix")
    if not (isinstance(mix, dict) and mix and all(num(v) and v >= 0 for v in mix.values())
            and abs(sum(mix.values()) - 1) < 1e-6):
        bad(f"load.mix: want shares that sum to 1, got {mix!r}")
    if doc.get("thresholds") != THRESHOLDS:
        bad(f"thresholds: want {THRESHOLDS}, got {doc.get('thresholds')!r}")
    need(doc, "percentiles", "str")

    # What came back.
    need(doc, "results.throughputRps", "pos")
    need(doc, "results.requests", "posint")
    good = all([check_percentiles(doc, f"results.classes.{c}") for c in CLASSES])
    errs = get(doc, "results.errors")
    if not isinstance(errs, dict):
        bad("results.errors: missing")
    else:
        if need(doc, "results.errors.total", "nonnegint") and need(doc, "results.errors.rate", "nonneg") \
                and num(get(doc, "results.requests")) and get(doc, "results.requests") > 0:
            want = errs["total"] / doc["results"]["requests"]
            if abs(errs["rate"] - want) > 1e-4:
                bad(f"results.errors.rate: {errs['rate']} but total/requests is {want:.6f}")
        for k in ("byStatus", "byCode"):
            v = errs.get(k)
            if not (isinstance(v, dict) and all(isinstance(n, int) and n >= 0 for n in v.values())):
                bad(f"results.errors.{k}: missing or not counts")
            elif isinstance(errs.get("total"), int) and k == "byStatus" and sum(v.values()) != errs["total"]:
                bad(f"results.errors.byStatus: sums to {sum(v.values())}, total is {errs['total']}")
    need(doc, "results.droppedIterations", "nonnegint")
    if good and num(get(doc, "results.throughputRps")):
        c = doc["results"]["classes"]
        per = "; ".join(f"{k} p50/p95/p99 {c[k]['p50Ms']}/{c[k]['p95Ms']}/{c[k]['p99Ms']} ms" for k in CLASSES)
        ok(f"results: {doc['results']['throughputRps']} req/s; {per}; errors {get(doc, 'results.errors.rate')}")

    if doc.get("scenario") == "smoke":
        for c in CLASSES:
            value = get(doc, f"results.classes.{c}.p99Ms")
            if num(value) and value > THRESHOLDS[f"{c}P99Ms"]:
                bad(f"smoke: {c} p99 exceeds threshold")
        rate = get(doc, "results.errors.rate")
        if num(rate) and rate > THRESHOLDS["errorRate"]:
            bad("smoke: error rate exceeds threshold")
        if get(doc, "results.droppedIterations") != 0:
            bad("smoke: dropped iterations must be zero")

    if get(doc, "match.label") != "matched only":
        bad(f"match.label: want 'matched only', got {get(doc, 'match.label')!r}")
    need(doc, "match.matched", "nonnegint")
    need(doc, "match.waiting", "nonnegint")
    for k in ("p50Ms", "p95Ms", "p99Ms"):
        need(doc, f"match.{k}", "nonneg")

    shards = get(doc, "raft.shards")
    if not (isinstance(shards, list) and shards):
        bad("raft: missing per-shard commit latency and snapshots")
    else:
        for i, s in enumerate(shards):
            for k in ("shard", "commitP50Ms", "commitP99Ms", "snapshots", "logBytesMax"):
                if not (isinstance(s, dict) and num(s.get(k)) and s[k] >= 0):
                    bad(f"raft.shards[{i}].{k}: missing")
    pods = doc.get("pods")
    if not (isinstance(pods, list) and pods):
        bad("pods: missing per-pod CPU and memory")
    else:
        for i, p in enumerate(pods):
            if not (isinstance(p, dict) and isinstance(p.get("name"), str)
                    and all(num(p.get(k)) and p[k] >= 0 for k in ("cpuMillicoresMax", "memMiBMax", "cpuThrottledPeriods"))):
                bad(f"pods[{i}]: want name, cpuMillicoresMax, memMiBMax, cpuThrottledPeriods")

    check_background(doc)
    check_audit(doc)
    for k in ("ledger", "invcheck"):
        if get(doc, f"invariants.{k}") != "PASS":
            bad(f"invariants.{k}: want PASS, got {get(doc, f'invariants.{k}')!r}")

    if require_sat or doc.get("scenario") == "step":
        if require_sat and doc.get("scenario") != "step":
            bad(f"saturation: --require-saturation needs a step run, this is {doc.get('scenario')!r}")
        else:
            check_saturation(doc)

    check_files(d, doc)
    print(f"{'FAIL' if fails else ('INCONCLUSIVE' if inconclusive else 'PASS')}  report {doc.get('runId')}" + (f" ({fails} failures)" if fails else ""))
    return 1 if fails else (3 if inconclusive else 0)


if __name__ == "__main__":
    sys.exit(main(sys.argv))
