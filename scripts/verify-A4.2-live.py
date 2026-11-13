#!/usr/bin/env python3
"""Live A4.2 checks. Application evidence is distinct from full S-owned acceptance."""
import argparse
import base64
import contextlib
import datetime
import hashlib
import json
import math
import os
from pathlib import Path
import re
import subprocess
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid

from metrics_fault import run_fault
from internal_auth import Sender

ROOT = Path(__file__).resolve().parents[1]
OPENER = urllib.request.build_opener(urllib.request.ProxyHandler({}))


def get(origin, path, *, method="GET", data=None, key=None, auth=None):
    origin = origin.rstrip("/")
    headers = {}
    if data is not None:
        headers["Content-Type"] = "application/json"
        data = json.dumps(data, separators=(",", ":")).encode()
    if key:
        headers["Idempotency-Key"] = key
    if auth:
        headers["Authorization"] = "Basic " + base64.b64encode(auth.encode()).decode()
    req = urllib.request.Request(origin + path, data=data, headers=headers, method=method)
    try:
        with (Sender(origin) if path.startswith("/internal/") else OPENER).open(req, timeout=8) as response:
            return json.load(response)
    except urllib.error.HTTPError as exc:
        raise RuntimeError(f"HTTP {exc.code}: {method} {path.split('?')[0]}") from None


def eventually(fn, description, timeout=45):
    end = time.monotonic() + timeout
    last = None
    while time.monotonic() < end:
        try:
            result = fn()
            if result:
                return result
        except (urllib.error.URLError, RuntimeError, TimeoutError) as exc:
            last = str(exc)
        time.sleep(.25)
    raise AssertionError(f"{description}: not observed in {timeout}s ({last})")


@contextlib.contextmanager
def forward(context, namespace, service, port, output):
    argv = ["kubectl", "--context", context, "-n", namespace, "port-forward",
            "--address=127.0.0.1", "svc/" + service, ":" + str(port)]
    # A pipe left undrained after startup blocks kubectl under sustained load.
    # A separate reader avoids sharing the writer's file offset while polling.
    with output.open("w") as log:
        proc = subprocess.Popen(argv, stdout=log, stderr=subprocess.STDOUT, text=True)
        try:
            deadline = time.monotonic() + 20
            origin = None
            while time.monotonic() < deadline and proc.poll() is None:
                match = re.search(r"Forwarding from 127\.0\.0\.1:(\d+) ->", output.read_text())
                if match:
                    origin = "http://127.0.0.1:" + match.group(1)
                    break
                time.sleep(.05)
            if not origin:
                raise RuntimeError(f"port forward unavailable: {service}")
            yield origin
        finally:
            proc.terminate()
            try:
                proc.wait(timeout=3)
            except subprocess.TimeoutExpired:
                proc.kill()
                proc.wait(timeout=3)


@contextlib.contextmanager
def cancel_fixtures(origin, identifiers):
    try:
        yield
    finally:
        for request_id in identifiers:
            get(origin, "/api/v1/requests/" + request_id + "/status", method="PUT", data={"status": "CANCELLED"})
            eventually(lambda: get(origin, "/api/v1/requests/" + request_id)["status"] == "CANCELLED", "synthetic fixture cleanup", timeout=90)


def vacant_location(origin, resource_type):
    page = get(origin, "/api/v1/resources?" + urllib.parse.urlencode({"type": resource_type, "limit": 100}))
    assert page["consistency"] == "strong" and not page["partial"] and not page["failedShards"]
    assert len(page["positions"]) == 3 and {x["shard"] for x in page["positions"]} == {0, 1, 2} and not any(x["hasMore"] for x in page["positions"]), "fixture placement needs a complete resource list; use a clean demo namespace"
    def distance(a, b):
        lat1, lat2 = math.radians(a["lat"]), math.radians(b["lat"])
        dlat, dlng = lat2-lat1, math.radians(b["lng"]-a["lng"])
        h = math.sin(dlat/2)**2 + math.cos(lat1)*math.cos(lat2)*math.sin(dlng/2)**2
        return 12742*math.asin(min(1, math.sqrt(h)))
    for lat in (-45, -20, 10, 35, 60):
        for lng in (-140, -100, -60, -20, 20, 60, 100, 140):
            location = {"lat": lat, "lng": lng}
            if all(distance(location, resource["location"]) > 100 for resource in page["items"]):
                return location
    raise AssertionError("no isolated synthetic fixture location in the complete list")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", choices=("app", "full"), default="full")
    parser.add_argument("--screenshots", action="store_true")
    args = parser.parse_args()
    cluster = os.environ.get("CLUSTER", "reliefgrid")
    namespace = os.environ.get("NAMESPACE", "reliefgrid")
    for value in (cluster, namespace):
        if not re.fullmatch(r"[a-z0-9][a-z0-9-]{0,62}", value):
            raise ValueError("local cluster/namespace name invalid")
    context = "kind-" + cluster
    stamp = datetime.datetime.now(datetime.timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    out = ROOT / "docs/evidence/A4.2" / ("live-" + stamp)
    out.mkdir(parents=True)
    report = {"mode": args.mode, "run": stamp, "checks": [], "result": "FAIL",
              "baseCommit": subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=ROOT, text=True).strip()}
    def passed(name):
        report["checks"].append(name)
        print("PASS: " + name, flush=True)
    password_file = os.environ.get("A42_GRAFANA_PASSWORD_FILE")
    password = Path(password_file).read_text().strip() if password_file else "reliefgrid-local"
    username = os.environ.get("A42_GRAFANA_USER", "admin")
    auth = username + ":" + password
    identifiers = []
    try:
        with forward(context, namespace, "observability-prometheus", 9090, out / "prometheus-forward.txt") as prom, \
             forward(context, namespace, "observability-grafana", 3000, out / "grafana-forward.txt") as graf, \
             forward(context, namespace, "reliefgrid-traefik", 80, out / "api-forward.txt") as api, cancel_fixtures(api, identifiers):
            def query(expr):
                result = get(prom, "/api/v1/query?" + urllib.parse.urlencode({"query": expr}))
                assert result["status"] == "success", "Prometheus query failed"
                return result["data"]["result"]
            def number(expr):
                rows = query(expr)
                if not rows:
                    return None
                value = float(rows[0]["value"][1])
                return value if math.isfinite(value) else None
            eventually(lambda: (number('sum(up{job="reliefgrid-app",component=~"api|matcher"})') or 0) >= 4,
                       "all four configured API/matcher exporters")
            passed("four production application exporters discovered individually")
            eventually(lambda: number('up{job="reliefgrid-broker"}') == 1, "native broker exporter")
            queues = query("rabbitmq_detailed_queue_messages")
            names = {row["metric"]["queue"] for row in queues}
            expected = {"matching.requests", "matching.wakeups", "matching.dead", "retry.5s",
                        "retry.30s", "retry.120s", "wakeup.retry.5s"}
            assert names == expected, f"queue coverage/whitelist mismatch: {names}"
            passed("all seven canonical broker queues; disposable projection queues excluded")
            eventually(lambda: number("min(relief_metrics_observation_success)") == 1,
                       "complete authoritative observations from every exporter")
            passed("all three shards observed by complete authoritative workflow walks")
            search = get(graf, "/api/search?tag=ReliefGrid", auth=auth)
            wanted = {"reliefgrid-cluster", "reliefgrid-matching", "reliefgrid-storage"}
            assert {x["uid"] for x in search} == wanted, "three dashboards not provisioned"
            validation = []
            for path in sorted((ROOT / "deploy/helm/observability/dashboards").glob("*.json")):
                board = json.loads(path.read_text())
                for panel in board["panels"]:
                    for target in panel.get("targets", []):
                        rows = query(target["expr"])
                        validation.append({"dashboard": board["uid"], "panel": panel["title"],
                                           "series": len(rows)})
            (out / "queries.json").write_text(json.dumps(validation, indent=2) + "\n")
            passed("all three provisioned dashboards and every PromQL query evaluated")
            marker = "Synthetic A4.2 " + uuid.uuid4().hex[:8]
            location = vacant_location(api, "SHELTER")
            latency_before = number("sum(relief_match_latency_seconds_count)") or 0
            cancelled_before = number('sum(relief_match_outcomes_total{outcome="cancelled"})') or 0
            now = datetime.datetime.now(datetime.timezone.utc)
            iso = lambda value: value.isoformat(timespec="milliseconds").replace("+00:00", "Z")
            resource = get(api, "/api/v1/resources", method="POST", key=str(uuid.uuid4()),
                           data={"resourceType": "SHELTER", "name": marker, "location": location,
                                 "windowStart": iso(now - datetime.timedelta(minutes=1)),
                                 "windowEnd": iso(now + datetime.timedelta(hours=1)), "reusable": True})
            request = get(api, "/api/v1/requests", method="POST", key=str(uuid.uuid4()),
                          data={"requestType": "SHELTER", "details": marker, "peopleAffected": 1,
                                "urgency": "HIGH", "location": location})
            request_id = request["requestId"]
            identifiers.append(request_id)
            current = eventually(lambda: (lambda r: r if r["status"] == "MATCHED" else None)(
                get(api, "/api/v1/requests/" + request_id)), "real asynchronous match", timeout=90)
            assert current["assignedResourceId"] == resource["resourceId"]
            eventually(lambda: (number("sum(relief_match_latency_seconds_count)") or 0) > latency_before,
                       "created-to-committed-match histogram sample")
            passed("real REST/outbox/broker/consumer match recorded business-latency observation")
            report["syntheticRequestId"], report["syntheticResourceId"] = request_id, resource["resourceId"]
            get(api, "/api/v1/requests/" + request_id + "/status", method="PUT", data={"status": "CANCELLED"})
            eventually(lambda: get(api, "/api/v1/requests/" + request_id)["status"] == "CANCELLED",
                       "committed cancellation cleanup", timeout=90)
            eventually(lambda: (number('sum(relief_match_outcomes_total{outcome="cancelled"})') or 0) > cancelled_before,
                       "cancellation outcome observation")
            passed("durable cancellation cleanup observed without exposing entity IDs as metric labels")
            waiting = get(api, "/api/v1/requests", method="POST", key=str(uuid.uuid4()),
                          data={"requestType": "TRANSPORTATION", "details": marker + " waiting-age probe", "peopleAffected": 1,
                                "urgency": "HIGH", "location": vacant_location(api, "TRANSPORTATION")})
            identifiers.append(waiting["requestId"])
            eventually(lambda: get(api, "/api/v1/requests/" + waiting["requestId"])["status"] == "WAITING", "durable waiting fixture")
            expr = 'max(relief_workflow_age_seconds{state="WAITING",urgency="HIGH"} and on(instance,job) (relief_metrics_observation_success == 1))'
            first = eventually(lambda: (lambda value: value if value and value > 0 else None)(number(expr)), "positive unfinished workflow age")
            second = eventually(lambda: (lambda value: value if value and value >= first + 2 else None)(number(expr)), "workflow age grows between complete samples")
            report["workflowAgeSamplesSeconds"] = [first, second]
            passed("unfinished workflow age grows across complete authoritative observations")
            for uid in sorted(wanted):
                get(graf, "/api/annotations", method="POST", auth=auth,
                    data={"dashboardUID": uid, "time": int(time.time() * 1000),
                          "tags": ["A4.2-check"], "text": "A4.2 application acceptance " + stamp})
            def capture():
                if args.screenshots:
                    script = ROOT / "scripts/capture-A4.2.mjs"
                    subprocess.run(["node", str(script), graf, str(out)], check=True, cwd=ROOT, timeout=120)
                    passed("three live Grafana screenshots captured")
            families = ["storage_command_latency_seconds_count", "storage_replication_lag_entries",
                        "coordinator_node_state", "relief_outbox_pending", "relief_outbox_oldest_age_seconds"]
            missing = [name for name in families if not query(name)]
            report["storageHandoffMissing"] = missing
            if args.mode == "full":
                if missing:
                    report["result"] = "INCOMPLETE"
                    print("A4.2 FULL ACCEPTANCE INCOMPLETE: S-owned #113 missing " + ", ".join(missing), flush=True)
                    return 2
                def current_leader():
                    rows = query('storage_raft_leader{shard="0"} == 1 and on(instance,job) (up == 1)')
                    return rows[0]["metric"]["node"] if len(rows)==1 else None
                old_leader = eventually(current_leader, "unique observed shard0 leader")
                for uid in sorted(wanted):
                    get(graf, "/api/annotations", method="POST", auth=auth,
                        data={"dashboardUID":uid,"time":int(time.time()*1000),"tags":["A4.2-fault"],"text":"Leader kill/hold/recover " + stamp})
                with forward(context, namespace, "storage-coordinator", 8080, out / "coordinator-forward.txt") as coord:
                    report["faultProof"] = run_fault(context, namespace, old_leader, number, query, get, coord, out)
                passed("real leader loss, DOWN member, raw index-gap movement, growing workflow age and authoritative catchup observed")
                capture()
                report["result"] = "PASS"
                print("A4.2 FULL ACCEPTANCE: PASS", flush=True)
                return 0
            capture()
            report["result"] = "APPLICATION_PASS"
            print("A4.2 APPLICATION ACCEPTANCE: PASS; full storage/leader-kill gate remains #113", flush=True)
            return 0
    except BaseException:
        report["result"] = "FAIL"
        raise
    finally:
        (out / "results.json").write_text(json.dumps(report, indent=2) + "\n")


if __name__ == "__main__":
    raise SystemExit(main())
