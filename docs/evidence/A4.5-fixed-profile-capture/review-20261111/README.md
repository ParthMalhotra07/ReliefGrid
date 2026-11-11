# Owner-review follow-up

The missing-shape regression first reproduced four escaping AttributeErrors,
including Helm app:null and tools status:null (red-malformed.txt). Capture now
records these as a sanitized malformed-observation FAIL in fixed-profile.json;
no raw exception/object contents are persisted and output is not overwritten.

The expanded suite directly targets the requested scaled obsolete ReplicaSet,
deleting Deployment, RS/Deployment image mismatch, phase/ContainersReady,
incomplete container status, and tools pod count/phase/deletion/name guards.
Named cases assert the rejection message so an earlier check cannot mask the
intended path. Extra controller/pod counts, identity, revisions and startup name
guards are covered too. All33 require conditions were independently replaced
with True in disposable copies; every mutant is rejected by the suite.
guard-mutations.txt retains the full per-line result. No guard was weakened.

focused.txt records16 passing tests; go-focused.txt, check.txt and gen-check.txt
record the race wrapper/full/generation checks. No local cluster run occurred.
Author self-review completed; owner re-review remains required.
