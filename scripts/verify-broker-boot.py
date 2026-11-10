#!/usr/bin/env python3
"""Verify a fresh RabbitMQ PVC and a same-PVC restart in an isolated namespace.

Requires a running ReliefGrid kind cluster, Helm dependencies and Python 3.
Creates and removes only its own randomly named namespace/release. No shared
broker, queue or storage PVC is touched. Credentials stay in a temporary file.
"""
import base64
import json
from pathlib import Path
import re
import secrets
import select
import subprocess
import tempfile
import time
from urllib.request import Request, urlopen

ROOT = Path(__file__).resolve().parent.parent
NS = "broker-boot-" + secrets.token_hex(4)
RELEASE = "bootcheck"
BROKER = RELEASE + "-reliefgrid-rabbitmq"
USER = "boot-test"
PASSWORD = "boot:" + secrets.token_hex(16) + "@/\""


def run(*args, timeout=300):
    result = subprocess.run(args, check=False, capture_output=True, text=True,
                            timeout=timeout, cwd=ROOT)
    if result.returncode:
        # Helm values are passed by filename, never on the command line.
        raise RuntimeError(f"{args[0]} failed: {result.stderr.strip()}")
    return result.stdout


def kjson(*args):
    return json.loads(run("kubectl", "-n", NS, *args, "-o", "json"))


def ready():
    run("kubectl", "-n", NS, "wait", "--for=condition=Ready",
        "pod/" + BROKER + "-0", "--timeout=240s")
    pod = kjson("get", "pod", BROKER + "-0")
    status = pod["status"]["containerStatuses"][0]
    assert status["ready"] and status["restartCount"] == 0, "broker restarted during boot"
    return pod["metadata"]["uid"]


def forward():
    process = subprocess.Popen(
        ["kubectl", "-n", NS, "port-forward", "--address=127.0.0.1",
         "svc/" + BROKER, ":15672"], stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT, text=True)
    deadline = time.monotonic() + 20
    while time.monotonic() < deadline:
        readable, _, _ = select.select([process.stdout], [], [], deadline - time.monotonic())
        if not readable:
            break
        line = process.stdout.readline()
        match = re.search(r"127\.0\.0\.1:(\d+) -> 15672", line)
        if match:
            return process, "http://127.0.0.1:" + match.group(1)
        if process.poll() is not None:
            break
    process.terminate()
    process.wait(timeout=10)
    raise RuntimeError("management port-forward did not start")


def api(endpoint, method, path, body=None):
    data = None if body is None else json.dumps(body).encode()
    token = base64.b64encode((USER + ":" + PASSWORD).encode()).decode()
    request = Request(endpoint + "/api/" + path, data=data, method=method,
                      headers={"Authorization": "Basic " + token,
                               "Content-Type": "application/json"})
    with urlopen(request, timeout=15) as response:
        raw = response.read()
        return json.loads(raw) if raw else None


def topology(endpoint):
    expected = json.loads((ROOT / "deploy/helm/reliefgrid/files/rabbitmq-definitions.json").read_text())
    for category in ("queues", "exchanges", "policies"):
        actual = api(endpoint, "GET", category + "/%2F")
        names = {item["name"] for item in actual}
        assert all(item["name"] in names for item in expected[category]), category + " missing"
    assert api(endpoint, "GET", "whoami")["name"] == USER


def main():
    process = None
    print("Isolated native boot verification:", NS, flush=True)
    try:
        run("kubectl", "create", "namespace", NS)
        with tempfile.TemporaryDirectory(prefix="broker-boot-") as directory:
            values = Path(directory) / "values.json"
            values.write_text(json.dumps({"app": {"enabled": False},
                "storage": {"enabled": False}, "traefik": {"enabled": False},
                "rabbitmq": {"username": USER, "password": PASSWORD}}))
            values.chmod(0o600)
            run("helm", "install", RELEASE, "deploy/helm/reliefgrid", "-n", NS,
                "-f", str(values), "--wait", "--timeout", "240s")
        uid = ready()
        pvc = kjson("get", "pvc", "data-" + BROKER + "-0")["metadata"]["uid"]
        process, endpoint = forward()
        topology(endpoint)
        queue = "boot-persistence-proof"
        api(endpoint, "PUT", "queues/%2F/" + queue,
            {"durable": True, "auto_delete": False, "arguments": {"x-queue-type": "quorum"}})
        result = api(endpoint, "POST", "exchanges/%2F/amq.default/publish",
            {"properties": {"delivery_mode": 2}, "routing_key": queue,
             "payload": "survives-native-boot", "payload_encoding": "string"})
        assert result["routed"], "persistence probe was not routed"
        print("PASS fresh PVC: restartCount=0; custom credentials; canonical topology; durable publish", flush=True)
        process.terminate()
        process.wait(timeout=10)
        process = None
        run("kubectl", "-n", NS, "delete", "pod", BROKER + "-0", "--wait=true")
        new_uid = ready()
        assert uid != new_uid, "pod was not replaced"
        assert pvc == kjson("get", "pvc", "data-" + BROKER + "-0")["metadata"]["uid"], "PVC changed"
        process, endpoint = forward()
        topology(endpoint)
        messages = api(endpoint, "POST", "queues/%2F/" + queue + "/get",
            {"count": 1, "ackmode": "ack_requeue_false", "encoding": "auto"})
        assert len(messages) == 1 and messages[0]["payload"] == "survives-native-boot", "message lost on restart"
        print("PASS same PVC restart: new pod; restartCount=0; topology and persistent message retained", flush=True)
    finally:
        if process is not None:
            process.terminate()
            process.wait(timeout=10)
        run("kubectl", "delete", "namespace", NS, "--ignore-not-found=true", "--wait=false")
        print("Isolated namespace cleanup requested:", NS, flush=True)


if __name__ == "__main__":
    main()
