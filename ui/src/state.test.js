import test from "node:test";
import assert from "node:assert/strict";
import { EventIDs, list, observe, reciprocal } from "./state.js";
const a = "11111111-1111-4111-8111-111111111111",
  b = "22222222-2222-4222-8222-222222222222",
  c = "33333333-3333-4333-8333-333333333333";
test("at-least-once events are deduplicated with bounded memory", () => {
  const ids = new EventIDs(2);
  assert.equal(ids.accept(a), true);
  assert.equal(ids.accept(a), false);
  assert.equal(ids.accept("bad"), false);
  assert.equal(ids.accept(b), true);
  assert.equal(ids.accept(c), true);
  assert.equal(ids.ids.size, 2);
  assert.equal(ids.accept(a), true);
});
test("missing or partial shard observations never become an empty list", () => {
  const value = {
    consistency: "strong",
    items: [],
    positions: [0, 1, 2].map((shard) => ({ shard, appliedIndex: 2 })),
    partial: false,
    failedShards: [],
  };
  assert.equal(list(value, "request"), value);
  const fenced = { ...value, consistency: "linearizable" };
  assert.equal(list(fenced, "request"), fenced);
  const { consistency: _served, ...unlabelled } = value;
  for (const bad of [
    { ...value, consistency: "stale" },
    unlabelled,
    { ...value, consistency: "" },
    { ...value, consistency: null },
    { ...value, consistency: "eventual" },
    { ...value, partial: true },
    { ...value, positions: value.positions.slice(1) },
    {
      ...value,
      positions: [value.positions[0], value.positions[0], value.positions[2]],
    },
    { ...value, failedShards: [1] },
  ])
    assert.throws(() => list(bad, "request"));
});
test("cancelling stays cancelling until a newer authoritative record is observed", () => {
  const row = {
    requestId: a,
    version: "v4",
    status: "CANCELLING",
    updatedAt: "2026-11-05T06:00:00Z",
  };
  let history = observe({}, [row]);
  history = observe(history, [row]);
  assert.equal(history[a].length, 1);
  assert.equal(history[a][0].status, "CANCELLING");
  history = observe(history, [{ ...row, status: "CANCELLED", version: "v5" }]);
  assert.deepEqual(
    history[a].map((v) => v.status),
    ["CANCELLING", "CANCELLED"],
  );
});
test("an assignment line requires the current reciprocal claim", () => {
  const req = { requestId: a, assignedResourceId: b };
  assert.equal(reciprocal(req, { resourceId: b, activeClaim: null }), false);
  assert.equal(
    reciprocal(req, { resourceId: b, activeClaim: { requestId: c } }),
    false,
  );
  assert.equal(
    reciprocal(req, { resourceId: b, activeClaim: { requestId: a } }),
    true,
  );
});
