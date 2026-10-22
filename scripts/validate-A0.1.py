#!/usr/bin/env python3
"""Validate the frozen A0.1 public and event contract requirements."""

from __future__ import annotations

import json
import sys
from pathlib import Path

import jsonschema
import yaml


ROOT = Path(__file__).resolve().parents[1]
PUBLIC = ROOT / "contracts" / "public.yaml"
EVENTS = ROOT / "contracts" / "events"

REQUIRED_OPERATIONS = {
    "/requests": {"get", "post"},
    "/requests/{requestId}": {"get"},
    "/requests/{requestId}/status": {"put"},
    "/resources": {"get", "post"},
    "/resources/{resourceId}/status": {"put"},
    "/match/{requestId}": {"post"},
    "/events": {"get"},
}

ENUMS = {
    "RequestStatus": {
        "PENDING",
        "WAITING",
        "MATCHING",
        "MATCHED",
        "IN_PROGRESS",
        "RETRYING",
        "CANCELLING",
        "FULFILLING",
        "CANCELLED",
        "FULFILLED",
    },
    "ResourceStatus": {"AVAILABLE", "ASSIGNED", "OFFLINE", "CONSUMED"},
    "ResourceType": {"WATER", "FOOD", "MEDICINE", "TRANSPORTATION", "SHELTER"},
    "RequestType": {"WATER", "FOOD", "MEDICINE", "TRANSPORTATION", "SHELTER"},
    "Disposition": {"REUSABLE", "CONSUMABLE"},
}

EVENT_REQUIRED = {
    "schemaVersion",
    "eventId",
    "entityId",
    "entityVersion",
    "eventType",
    "occurredAt",
    "traceparent",
    "payload",
}


def fail(errors: list[str], message: str) -> None:
    errors.append(message)


def main() -> int:
    errors: list[str] = []
    spec = yaml.safe_load(PUBLIC.read_text(encoding="utf-8"))
    paths = spec.get("paths", {})

    for path, methods in REQUIRED_OPERATIONS.items():
        actual = set(paths.get(path, {})) & {"get", "post", "put", "patch", "delete"}
        missing = methods - actual
        if missing:
            fail(errors, f"{path}: missing methods {sorted(missing)}")

    schemas = spec.get("components", {}).get("schemas", {})
    for name, expected in ENUMS.items():
        actual = set(schemas.get(name, {}).get("enum", []))
        if actual != expected:
            fail(errors, f"enum {name}: got {sorted(actual)}, want {sorted(expected)}")

    envelope = schemas.get("ErrorEnvelope", {})
    required_error_fields = {
        "code",
        "message",
        "retryable",
        "outcomeUnknown",
        "correlationId",
    }
    missing_error_fields = required_error_fields - set(envelope.get("required", []))
    if missing_error_fields:
        fail(errors, f"ErrorEnvelope: missing required fields {sorted(missing_error_fields)}")

    for operation in (paths.get("/requests", {}).get("post", {}), paths.get("/resources", {}).get("post", {})):
        refs = [item.get("$ref") for item in operation.get("parameters", [])]
        if "#/components/parameters/IdempotencyKey" not in refs:
            fail(errors, f"{operation.get('operationId', '<unknown>')}: Idempotency-Key is not required")

    if not any("503" in operation.get("responses", {}) for item in paths.values() for operation in item.values() if isinstance(operation, dict)):
        fail(errors, "public contract: no operation documents a 503 outcome")

    event_paths = sorted(EVENTS.glob("*.json"))
    if not event_paths:
        fail(errors, "no event schemas found")
    for path in event_paths:
        schema = json.loads(path.read_text(encoding="utf-8"))
        try:
            jsonschema.Draft202012Validator.check_schema(schema)
        except jsonschema.SchemaError as exc:
            fail(errors, f"{path.name}: invalid JSON Schema: {exc.message}")
            continue

        missing = EVENT_REQUIRED - set(schema.get("required", []))
        if missing:
            fail(errors, f"{path.name}: missing required fields {sorted(missing)}")

        validator = jsonschema.Draft202012Validator(
            schema,
            format_checker=jsonschema.FormatChecker(),
        )
        for index, example in enumerate(schema.get("examples", []), 1):
            example_errors = sorted(validator.iter_errors(example), key=lambda item: list(item.path))
            for error in example_errors:
                location = ".".join(str(part) for part in error.path) or "<root>"
                fail(errors, f"{path.name} example {index} at {location}: {error.message}")

        encoded = json.dumps(schema)
        for stale in ("CLAIMED", "MEDICAL_EMERGENCY", '"MEDICAL"'):
            if stale in encoded:
                fail(errors, f"{path.name}: contains stale value {stale}")

    if errors:
        for error in errors:
            print(f"FAIL  {error}")
        print(f"A0.1 contract requirements: {len(errors)} failure(s)")
        return 1

    print("ok    A0.1 public routes, enums, error envelope and event requirements")
    return 0


if __name__ == "__main__":
    sys.exit(main())
