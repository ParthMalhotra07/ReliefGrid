# Build provenance clarification for #140

The retained local `nov7-ui-review-green.sh` command built from the review
checkout's `ui` directory after exporting the pinned Node/Go/tool paths, then
ran `npm test`, `npm run build` and `npm run test:browser`. Its retained build
output records revision `e09f94693ffdbb7e`. That checkout had `node_modules`
symlinked to the A4.1 checkout's dependency directory. The retained log did not
print Node/npm runtime versions, so those historical runtime versions are not
independently established merely by the pinned PATH/source declaration.

Current reproduction on source head `9d97197`, using Node v24.16.0, npm 11.13.0,
esbuild 0.28.2 and Playwright 1.64.1 with that symlink layout produces the exact
committed `app-6PQBRLMY.js` and revision `e09f94693ffdbb7e`, with no tracked
static-asset diff. A clean local `npm ci --ignore-scripts` instead produces
`app-5XZLAUGD.js` and revision `3f7b7425738d5e55`. Both JavaScript files have
SHA-256 `932c22cb5ca778357c177411394726e96276ddb8cce2dfb6025c77baeb1ed917`.

The API metadata probe used the existing build's exact esbuild options with
`write: false`. Its input metadata records `../../reliefgrid-a41/ui/node_modules/…`
under the symlink layout and `node_modules/…` under a local install. The probe
reproduces both names with identical JS hashes. Thus the difference is resolved
dependency-path layout in esbuild's output naming, not different JavaScript
behavior. This does not imply that a filename hash is simply a JS byte checksum.

A same-working-directory control keeps the original dependency symlink and
changes only the metadata probe's `preserveSymlinks` option to true (supported
by pinned esbuild 0.28.2). It then records local logical dependency paths and
produces `app-5XZLAUGD.js` with the same JS SHA-256. No dependency files or
working directory changed for this control. The production build option is
unchanged; the control isolates symlink resolution as the naming difference.

The canonical build is the existing `make ui-build` recipe:

```sh
cd ui
npm ci --ignore-scripts --no-audit --no-fund
npm run build
```

This follow-up normalizes checked-in assets to that clean local-install layout;
the JS bytes are unchanged. Index, service worker and manifest references are
regenerated together. Two independent `npm ci`/build cycles after merging main
`42741b1` produced identical complete asset manifests (`canonical-1.sha256` and
`canonical-2.sha256`). The first clean build differing from the former committed
assets is retained as `canonical-red.txt`. No custom hash/buildsystem change was
needed. The old dependency symlink was preserved in the original detached
checkout; the canonical implementation uses a separate local-install worktree.

Actual browser verification against the canonical assets passed all twelve
tests in 8.4s, including initial read failure, saved Resources view, same-render
action reentry, unknown write outcomes, cancellation, offline and reconnect.
All six Node tests and format checks passed. These are local Go-served/browser
fixtures, not a new deployed-kind or broker acceptance run.

Fresh full `make check` and `make gen-check` passed; their actual outputs are
retained in `check.txt` and `gen-check.txt`. No cluster or broker was changed.
