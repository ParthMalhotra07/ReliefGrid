#!/usr/bin/env python3
"""Validates that every example in contracts/events/*.json validates against its own schema."""
import json, sys, glob
try:
    import jsonschema
except ImportError:
    print("FATAL: jsonschema not installed")
    sys.exit(1)

ok = True
files = sorted(glob.glob("contracts/events/*.json"))
if not files:
    print("WARN: no event schema files found in contracts/events/")
    sys.exit(0)

for path in files:
    with open(path) as fh:
        schema = json.load(fh)
    examples = schema.get("examples", [])
    if not examples:
        print(f"WARN  {path}: no examples defined")
        continue
    errs = []
    for ex in examples:
        try:
            jsonschema.validate(instance=ex, schema=schema)
        except jsonschema.ValidationError as e:
            errs.append(e.message)
    if errs:
        print(f"FAIL  {path}: " + "; ".join(errs))
        ok = False
    else:
        print(f"ok    {path}: {len(examples)} example(s) valid")

sys.exit(0 if ok else 1)
