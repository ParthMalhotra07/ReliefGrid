# Requirement-to-demo matrix and measured results

## Course requirement mapping

| Requirement | Demonstration | Source and evidence |
| --- | --- | --- |
| Create and retrieve request | Demo1 | [script](../../scripts/demos/demo-1.sh) |
| Register and update resource availability | Demo2 | [script](../../scripts/demos/demo-2.sh) |
| Match by type/location/window with reciprocal ownership | Demo3 | [script](../../scripts/demos/demo-3.sh), [workflow tests](../../internal/workflow) |
| Distribute data across three logical shards | Demo4 | [script](../../scripts/demos/demo-4.sh), [ring contract](../../contracts/ring.yaml) |
| Asynchronous burst processing through RabbitMQ | Demo5 | [script](../../scripts/demos/demo-5.sh), [messaging tests](../../internal/mq) |
| Continue via replicas after storage leader failure | Demo6 | [script](../../scripts/demos/demo-6.sh) |
| Recover/replay/catch up a restarted node | Demo7 | [script](../../scripts/demos/demo-7.sh) |
| Application pod failure and Kubernetes recovery | Demo8 | [script](../../scripts/demos/demo-8.sh), [recovery evidence](../evidence/A3.2) |
| Kubernetes microservice scaling | Demo9 | [script](../../scripts/demos/demo-9.sh) |

All nine scripts were recorded twice in the
[4November gate report](../evidence/gate-20261104T144814Z-hrishabh-sharrma-ThinkPad-E14-Gen-7/gate.txt):
18/18PASS. The report records clean deployment per round, reset durations129s and
104s, no cordons and the exact tested code-tree hash. These are observed times
on Hrishabh's machine, not guaranteed setup times on every laptop.

## Evidence interpretation

The gate is a repeatable assertion suite, not a collection of screenshots showing
healthy pods. Demo3 checks ownership; demo5 drains before strict invariant audit;
demo6 records acknowledged writes; demo7 checks recovery; demo8 records interrupted
calls and retries; demo9 observes Ready replicas receiving traffic. See the
[shared demo library](../../scripts/demos/lib.sh) and
[application demo verifier](../../scripts/demos/app_demo.py).

[A2.3 evidence](../evidence/A2.3/README.md) covers the reproducible adversarial
schedule campaign. [S4.3 evidence](../evidence/S4.3) covers named chaos experiments;
claims about lost writes/stale reads apply to those observed windows and judge
criteria. [S4.4 evidence](../evidence/S4.4) covers tracing and
[S4.5 evidence](../evidence/S4.5) covers Merkle comparisons at an agreed index.
Neither scrape availability nor an unequal-index digest comparison proves safety.

## Limitations and pending measurements

- Main's course gate does not certify every later branch; rebase and current CI
  remain necessary before merging combinations of changes.
- No throughput/p95/p99 table is fabricated. Hrishabh's final k6 workload, machine
  configuration, saturation signals and recorded output must supply that chapter.
- A single coordinator is a known routing availability limit; coordinator failure
  is not advertised as transparent high availability.
- Quorum loss can make reads/writes unavailable. A stale candidate hint cannot
  become a definitive no-candidate result or ownership proof.
- List pages are per-shard authoritative observations, not a global snapshot.
- Metrics/dashboards, automated scaling and ETags/cursors need their own final
  reviewed evidence. Their pending PRs are not counted as completed course-gate
  features here.
- The bare PS route aliases need their separately reviewed ingress change; do not
  silently equate /api/v1 endpoints with exact unprefixed route compliance.

The final report should add the instructor-confirmed length, final reviewed SHA,
public repository URL, selected experiment ADR and any remaining measured results.
An unfinished experiment is described as future work rather than a delivered
distributed-systems guarantee.
