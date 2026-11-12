# S-29 authentication verification — 2026-11-12

Changes: default-off auth flags in both charts and receiver binaries, explicit
sender staging during overlap, operational sender-file wiring, mounted-key pod
checks in the rotation runbook, and authentication regression tests.

Validation:

- `go test ./... -count=1`: PASS.
- `make check`: PASS (vet, lint, race suite, chaos-tagged invcheck).
- `make gen-check`: PASS; no generated changes.
- `go test -tags integration ./scripts -run '^TestAppInternalAuth' -count=1`: PASS.
- Final `go test -race ./cmd/storagenode -count=1`: PASS, including valid startup
  fixtures and live enabled/disabled auth modes.
- Disabled storage chart output with a fixed fixture operator token is byte-for-byte
  identical to `origin/main` at ed108ea2b17adfc2c29cf66eb05d91897c1612e5.
- Disabled application deployments/services/ingress output is byte-for-byte
  identical to that same main commit (rendered with values-app-kind.yaml).

All 16 mutation checks are detected by tests. Checks 14 and 16 were each applied separately
to coordinator and storage, yielding 18 individual mutant runs. Every target has
a passing unmodified baseline. RED means an actual test assertion failed under
the mutation; build failures do not count. No mutant survives. Sources were
restored after every run. Reproduce with:

```sh
python3 scripts/internal-auth-mutations.py /tmp/s29-mutations
```

| Check | Mutation | Baseline | Mutant |
| --- | --- | --- | --- |
| 1 | reject receiver overlap | GREEN | RED |
| 2 | accept empty header | GREEN | RED |
| 3 | skip receiver config validation | GREEN | RED |
| 4 | protect only data prefix | GREEN | RED |
| 5 | app prefix before node prefix | GREEN | RED |
| 6 | bypass storage receiver | GREEN | RED |
| 7 | erase earlier forwarding uncertainty | GREEN | RED |
| 8 | malformed refusal loses uncertainty | GREEN | RED |
| 9 | forget earlier client uncertainty | GREEN | RED |
| 10 | never resolve committed operation | GREEN | RED |
| 11 | drop DELETE operation identity | GREEN | RED |
| 12 | accept 401 carrying result or unknown | GREEN | RED |
| 13 | ignore unreadable previous file | GREEN | RED |
| 14 | ignore node receiver load error (both binaries) | GREEN | RED |
| 15 | force sender to receiver current | GREEN | RED |
| 16 | load auth after Listen (both binaries) | GREEN | RED |

The CI schedules filter includes internal/auth. Regression tests cover empty
headers without a previous token, isolated credential-file failures, listener
startup ordering, and enabled/disabled authentication behavior.

No chart was deployed and no credential rotation was performed. API and matcher
startup wiring remains separately owned; keep auth disabled until those callers
and operational pod mounts are ready.
