# Report figures F1–F4

Each figure has editable Mermaid source, an SVG and a white-background PNG rendered at **2× scale**. The shared configuration uses an 18 px sans-serif font and the same blue/grey palette throughout. Use SVG for report layout; keep its aspect ratio and avoid raster resampling. Sources: the F1–F4 drawing appendix of `../S-sections-v3.md`.

| Figure | Source | SVG | PNG | Boxes / decisions |
|---|---|---|---|---:|
| F1 — System architecture | [Mermaid](F1-system-architecture.mmd) | [SVG](F1-system-architecture.svg) | [PNG](F1-system-architecture.png) | 9 |
| F2 — Physical schema and logical states | [Mermaid](F2-schema-and-states.mmd) | [SVG](F2-schema-and-states.svg) | [PNG](F2-schema-and-states.png) | 12 |
| F3 — Commit/publish/claim/ack | [Mermaid](F3-commit-publish-claim-ack.mmd) | [SVG](F3-commit-publish-claim-ack.svg) | [PNG](F3-commit-publish-claim-ack.png) | 12 |
| F4 — Placement and persistence | [Mermaid](F4-placement-and-persistence.mmd) | [SVG](F4-placement-and-persistence.svg) | [PNG](F4-placement-and-persistence.png) | 12 |

## Captions

**F1.** The broker schedules work; shard consensus authorizes state changes. Each shard box groups one illustrative leader and two followers: Raft replicates ordered commands to their deterministic SQLite FSMs and logical outboxes. The coordinator selects the fixed ring owner and discovers its leader. Management and read-only observability use separate interfaces.

**F2.** Historical operation success records a past command; it does not establish current ownership. The five physical table boxes participate in one committed apply transaction. Their layout is not a set of foreign-key constraints. Request and resource states live in the `records` JSON body, not separate SQL tables. Reciprocal ownership applies to MATCHED/IN_PROGRESS requests and their attempt-bound resource claim; terminal requests hold no live claim. Closure must commit before retargeting.

**F3.** #168 keeps an earlier unknown outcome through a later 4xx without a committed Result. A timeout is not rejection: look up or retry the same operationId at the unresolved matching stage. Committed CloseAttempt fences delayed claims before a new attempt creates its durable intent. Broker confirmation precedes publication marking; delivery ack follows the durable handoff. Publication and matching are separate asynchronous paths, not one atomic transaction.

**F4.** Worker separation supports local node-fault experiments; all workers still share one physical host. S0/S1/S2 identify the same shards numbered 0/1/2 in F1. Each data worker hosts one voter of every shard. PVCs retain Raft history; emptyDir SQLite is rebuilt. An empty disk stays unadmitted until guarded replacement admits its incarnation. API/matcher placement is chart-controlled. Raft, data HTTP, operator management and restricted metrics are distinct interfaces; Collector/Jaeger is optional.

## Rendering

Rendered with mermaid-cli 12.2.0 (Mermaid 12.3.0). From this directory, with an available Chrome installation:

```sh
for source in F*.mmd; do
  npx -y @mermaid-js/mermaid-cli@12.2.0 -c mermaid-config.json -i "$source" -o "${source%.mmd}.svg"
  npx -y @mermaid-js/mermaid-cli@12.2.0 -c mermaid-config.json -i "$source" -o "${source%.mmd}.png" -s 2
done
```

For an existing browser, pass a Puppeteer configuration with its `executablePath` using `-p`; this avoids downloading another browser. Invisible Mermaid links in F2/F4 control layout only and represent no data dependency or network interface. Explanatory captions stay outside the artwork so labels remain short.
