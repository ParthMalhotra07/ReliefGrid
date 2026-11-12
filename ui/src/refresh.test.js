import test from "node:test";
import assert from "node:assert/strict";
import { coalescedRefresh } from "./refresh.js";
const deferred = () => Promise.withResolvers();

test("an overlapping refresh waits for the requested fresh reread", async () => {
  const gates = [deferred(), deferred()];
  let runs = 0;
  const refresh = coalescedRefresh(() => gates[runs++].promise);
  const first = refresh();
  await Promise.resolve();
  const second = refresh();
  let settled = false;
  second.then(() => {
    settled = true;
  });
  await Promise.resolve();
  assert.equal(
    settled,
    false,
    "post-write refresh resolved before a fresh read",
  );
  gates[0].resolve();
  await first;
  await Promise.resolve();
  assert.equal(runs, 2);
  assert.equal(settled, false);
  gates[1].resolve();
  await second;
});

test("overlapping callers coalesce into one reread and recover after failure", async () => {
  const gates = [deferred(), deferred()];
  let runs = 0;
  const refresh = coalescedRefresh(() => gates[runs++].promise);
  const first = refresh();
  await Promise.resolve();
  const next = refresh(),
    also = refresh();
  gates[0].reject(new Error("first read failed"));
  await assert.rejects(first, /first read failed/);
  await Promise.resolve();
  assert.equal(runs, 2, "the queued reread was lost after failure");
  gates[1].resolve();
  await Promise.all([next, also]);
  assert.equal(runs, 2);
});
