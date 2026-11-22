import test from "node:test";
import assert from "node:assert/strict";
import { EventIDs, list, observe, reciprocal, retryReason } from "./state.js";
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
test("retry reasons are shown only for the state they explain", () => {
  assert.equal(
    retryReason("WAITING", "NO_CANDIDATE"),
    "No matching resource within 50 km yet",
  );
  assert.match(retryReason("RETRYING", "OUTCOME_UNKNOWN"), /not confirmed/);
  assert.match(retryReason("WAITING", "SOMETHING_NEW"), /retry/i);
  for (const status of [
    "FULFILLED",
    "CANCELLED",
    "MATCHED",
    "IN_PROGRESS",
    "CANCELLING",
    "FULFILLING",
  ])
    for (const code of [
      "NO_CANDIDATE",
      "RESOURCE_AVAILABLE",
      "OUTCOME_UNKNOWN",
    ])
      assert.equal(retryReason(status, code), "");
  assert.equal(retryReason("MATCHING", "NO_CANDIDATE"), "");
  assert.equal(retryReason("WAITING", ""), "");
});

test("eligibility mirrors the backend and reports the most relevant reason", async () => {
  const { ineligible } = await import("./state.js");
  const now = Date.parse("2026-11-22T12:00:00Z");
  const request = {
    requestType: "WATER",
    location: { lat: 19.08, lng: 72.88 },
  };
  const base = {
    resourceType: "WATER",
    status: "AVAILABLE",
    activeClaim: null,
    location: { lat: 19.1, lng: 72.9 },
    windowStart: "2026-11-22T11:00:00Z",
    windowEnd: "2026-11-22T18:00:00Z",
  };
  const check = (change) => ineligible(request, { ...base, ...change }, now);
  assert.equal(check({}), "");
  // 50 km is still eligible, as in the backend (distance <= radiusKM).
  assert.equal(
    check({ location: { lat: 19.08 + 49.99 / 111.195, lng: 72.88 } }),
    "",
  );
  assert.equal(
    check({ location: { lat: 20.16, lng: 72.88 } }),
    "Too far: 120 km",
  );
  assert.equal(check({ windowEnd: "2026-11-22T12:00:00Z" }), "Window closed");
  assert.equal(check({ windowStart: "2026-11-22T12:00:00Z" }), "");
  assert.equal(check({ resourceType: "FOOD" }), "Wrong type");
  assert.equal(check({ activeClaim: { requestId: "x" } }), "Already assigned");
  assert.equal(check({ status: "OFFLINE" }), "Not available");
  assert.equal(
    check({ resourceType: "FOOD", windowEnd: "2026-11-22T11:30:00Z" }),
    "Window closed",
  );
});

test("timeline timings use the timeline's own timestamps", async () => {
  const { timings, duration } = await import("./state.js");
  assert.equal(duration(3800), "3.8 s");
  assert.equal(duration(42), "under 0.1 s");
  assert.equal(duration(60), "0.1 s");
  assert.equal(duration(132000), "2 min 12 s");
  assert.equal(duration(600000), "10 min");
  assert.equal(duration(3900000), "1 h 5 min");
  const t = (s) =>
    new Date(Date.parse("2026-11-05T06:00:00Z") + s * 1000).toISOString();
  assert.deepEqual(
    timings(t(0), [
      { status: "WAITING", at: t(1.5) },
      { status: "MATCHING", at: t(6.5) },
      { status: "WAITING", at: t(7) },
      { status: "MATCHING", at: t(133.5) },
      { status: "MATCHED", at: t(135) },
      { status: "CANCELLED", at: t(140) },
    ]),
    [
      "",
      "",
      "Waited 2 min 12 s",
      "",
      "Matched 2 min 15 s after creation",
      "Cancelled 2 min 20 s after creation",
    ],
  );
  // A wait that has not ended yet has no total.
  assert.deepEqual(timings(t(0), [{ status: "WAITING", at: t(2) }]), [""]);
});

test("window badges, waiting time and request sorts", async () => {
  const { windowLabel, waitingLabel, sortRequests } =
    await import("./state.js");
  const now = Date.parse("2026-11-05T12:00:00Z");
  const at = (m) => new Date(now + m * 60000).toISOString();
  const resource = (from, until) => ({
    windowStart: at(from),
    windowEnd: at(until),
  });
  assert.equal(windowLabel(resource(-60, 45), now), "Open · closes in 45 min");
  assert.equal(windowLabel(resource(20, 180), now), "Opens in 20 min");
  assert.equal(windowLabel(resource(-180, -60), now), "Expired");
  // The end is exclusive, as in the backend.
  assert.equal(windowLabel(resource(-60, 0), now), "Expired");
  assert.equal(windowLabel(resource(0, 30), now), "Open · closes in 30 min");
  assert.equal(windowLabel(resource(-60, 0.2), now), "Open · closes in 1 min");
  assert.equal(windowLabel({ windowStart: "x", windowEnd: at(5) }, now), "");
  const req = { createdAt: at(-30) };
  assert.equal(waitingLabel(req, [], now), "waiting 30 min");
  assert.equal(
    waitingLabel(
      req,
      [
        { status: "MATCHING", at: at(-20) },
        { status: "WAITING", at: at(-12.5) },
      ],
      now,
    ),
    "waiting 12 min",
  );
  assert.equal(
    waitingLabel({ createdAt: at(-73.5) }, [], now),
    "waiting 1 h 13 min",
  );
  assert.equal(
    waitingLabel({ createdAt: at(-1500) }, [], now),
    "waiting 1 d 1 h",
  );
  const r = (requestId, urgency, created, status) => ({
    requestId,
    urgency,
    status,
    createdAt: at(created),
  });
  const rows = [
    r("a", "NORMAL", -60, "WAITING"),
    r("b", "CRITICAL", -10, "MATCHED"),
    r("c", "HIGH", -30, "RETRYING"),
    r("d", "CRITICAL", -40, "WAITING"),
  ];
  const ids = (mode, history) =>
    sortRequests(rows, mode, history).map((v) => v.requestId);
  assert.deepEqual(ids("urgency"), ["d", "b", "c", "a"]);
  assert.deepEqual(ids("newest"), ["b", "c", "d", "a"]);
  assert.deepEqual(
    ids("oldest-waiting", { d: [{ status: "WAITING", at: at(-70) }] }),
    ["d", "a", "c", "b"],
  );
  assert.deepEqual(
    rows.map((v) => v.requestId),
    ["a", "b", "c", "d"],
  );
});
