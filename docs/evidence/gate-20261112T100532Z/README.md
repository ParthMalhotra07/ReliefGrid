# Enhanced gate (scoped) — 12 November 2026 [S4.7]

Gate SHA `1623c23fef7a881990f7470c7ddc3f85cf33c3b6` (tree `fabe5acf73ce`), host `hrishabh-sharrma-ThinkPad-E14-Gen-7`, run 10:05:31–11:12:16Z. **Result: PASS.** Scope: deployed aging `0s`, autoscaling off (fixed API 2 / matcher 2), #29 auth excluded. See `report.md` for the full results, open goals and limitations.

| File | What it is |
|---|---|
| `gate.txt` | Course gate summary: two clean rounds, demos 1–9, `PASS (18/18 passed)`, resets 107 s / 116 s (whole reset wrapper) |
| `round-1/`, `round-2/` | Per-demo reports and app-demo artifacts, same layout as earlier gates (large files omitted, below) |
| `report.md` | Full gate report: every section, timings, scope, open goals, limitations |
| `run-notes.md` | Run notes: source identity, per-reset core/wrapper times, reset wrapper sha256, finish and clean-checkout line |
| `fixed-profile.json` | #157 fixed-profile collector output (`ok: true`, api 2 / matcher 2, 0 HPA / 0 ScaledObject, `collectorDirty: false`) |
| `kind-20261112T111126Z.txt` | Merkle audit summary (`verify-S4.5 --kind`, seed 20261112): PASSED 7/7, equal roots on all replicas |
| `retained-invcheck.json` | Strict invcheck from the retained Job `s45-check-seed20261112`: clean, quiescent, outbox pending `[0,0,0]` |

`gate.txt` and `run-notes.md` are kept byte-for-byte as the run wrote them, so their paths point at the original run root (`~/.cache/reliefgrid/gate-20261112T100532Z/`), which also keeps the console logs, chaos/replacement/dashboard evidence, deployed-UI screenshots and the trace.

## Omitted bulky files

The demo-5 burst records over 100 KB stay in the run root only. Their sha256 and paths (relative to the course directory `gate-20261112/`):

```
8b13e2664be5ebdaa6f40f9f87bc4b73237f6d907af9bf2bd766355dfe217199  round-1/demo-5-app/attempts.jsonl
80f550f700afd895ad1545c2bd6836dc9ba28f670ffa933e0bcc9d2f5b74ae08  round-1/demo-5-app/created.jsonl
5b453382f82c5ab7e8c8c126aee20d92e332b2e9163f04985156a866f69dbb29  round-1/demo-5-app/inputs.jsonl
06bad7cb6722680f89532a511550ea268a0e50f79716262d776ce50a0e2bd279  round-1/demo-5-app/settled-requests.json
be28f4aea8827d4a0dfd49f8cbfbea25151d2c6b9870cace58ec7f12800456de  round-2/demo-5-app/attempts.jsonl
28e03555cd15f913aa937505015ff6a7515342efa677f494e2934c124a72d56f  round-2/demo-5-app/created.jsonl
dd9349760bd12b2dc4b09de0573f781ce79f1924d021381b27b6c0a5d67660f3  round-2/demo-5-app/inputs.jsonl
3086cb5ecbaf63789e0f90b16f2bbdd72f5a19e9cfb53c1a436a5d55257dd482  round-2/demo-5-app/settled-requests.json
```
