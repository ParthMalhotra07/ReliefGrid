# Storage pod-label correction [S1.5]

31 October 2026. Parth explicitly authorized this separate, narrow storage-label
fix PR; Hrishabh's approval and human merge remain required. Base: main d28a1da.

The storage StatefulSet pod labels included both storage.labels and
storage.shardSelector. Both emitted app.kubernetes.io/name: storage. Strict YAML
decoding therefore rejected the rendered chart, blocking A1.5's manifest Verify.

The production change keeps storage.labels and writes only component/shard next
to it in the pod labels. All selector helpers and their other uses are unchanged.
The effective label values, shard names and RF3 counts are preserved. No Raft,
claim, admission, PVC, service-address, contract or dependency-version change.

## Test-first evidence

The new regression renders the actual storage chart with Helm, strictly decodes
its YAML and checks all three StatefulSets' pod/selector labels, replicas=3 and
release identity. Before the fix it failed:

```
storage chart must render unique YAML mapping keys: yaml: unmarshal errors:
line 709: mapping key "app.kubernetes.io/name" already defined at line 706
EXPECTED_RED_EXIT=1
```

After the fix, the unchanged test passed. See red.log and green.log.

## Validation

```
make verify-storage-labels                         PASS
helm lint deploy/helm/storage                     1 chart(s) linted, 0 failed
GITHUB_ACTIONS=true make check                     PASS; lint 0 issues; race/chaos tests
go vet -tags integration ./scripts/...             PASS
golangci-lint run --build-tags integration ./scripts/...  0 issues
make gen-check                                    clean
go mod tidy -diff                                 clean
```

The ci-smoke target depends on verify-storage-labels, so the existing smoke job
also executes this regression using its pinned Helm/Go tools. No .github file
was changed. Real cluster smoke remains the CI job's responsibility; a local
strict render does not establish live Raft health. No local cluster/PVC mutation.

The fix is proposed independently of A1.5/#66. Do not treat this as owner approval
or a merge; post-merge verification and the remaining A1.5 checks still apply.
