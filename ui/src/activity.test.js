import test from "node:test";
import assert from "node:assert/strict";
import { Activity, describe, relative, FEED_SIZE, Toasts } from "./activity.js";

const request = (id, status, version, details = "Meals " + id) => ({
  requestId: id,
  requestType: "FOOD",
  status,
  version: "v" + version,
  details,
});
const snapshot = (requests, resources = []) => ({
  requests: { items: requests },
  resources: { items: resources },
});

test("stream events are described from the read that reaches their version", () => {
  let clock = 0;
  const feed = new Activity(() => clock);
  feed.resolve(snapshot([request("a", "WAITING", 2)]));
  feed.observe({
    entityId: "a",
    entityVersion: 3,
    eventType: "request.state_changed",
  });
  // A read that predates the event keeps it pending.
  assert.deepEqual(feed.resolve(snapshot([request("a", "WAITING", 2)])), []);
  clock = 400;
  const items = feed.resolve(snapshot([request("a", "MATCHED", 3)]));
  assert.equal(items.length, 1);
  assert.equal(describe(items[0]), "Request matched");
  assert.equal(items[0].title, "Meals a");
  assert.equal(items[0].at, 0);
});

test("creation, the listed states and nothing else are announced", () => {
  const feed = new Activity(() => 0);
  feed.resolve(snapshot([]));
  const steps = [
    ["request.created", "WAITING", "Request created"],
    ["request.state_changed", "MATCHING", null],
    ["request.state_changed", "MATCHED", "Request matched"],
    ["request.state_changed", "IN_PROGRESS", "Request work started"],
    ["request.state_changed", "FULFILLING", null],
    ["request.state_changed", "FULFILLED", "Request fulfilled"],
  ];
  let seen = [];
  steps.forEach(([eventType, status, text], i) => {
    feed.observe({ entityId: "a", entityVersion: i + 1, eventType });
    seen = feed.resolve(snapshot([request("a", status, i + 1)]));
    if (text) assert.equal(describe(seen[0]), text);
  });
  assert.equal(seen.length, 4);
  feed.observe({
    entityId: "r",
    entityVersion: 1,
    eventType: "resource.registered",
  });
  seen = feed.resolve(
    snapshot(
      [request("a", "FULFILLED", 6)],
      [{ resourceId: "r", name: "Van", status: "AVAILABLE", version: "v1" }],
    ),
  );
  assert.equal(describe(seen[0]), "Resource registered");
  assert.equal(seen[0].title, "Van");
});

test("a burst of more than five events a second is grouped", () => {
  let clock = 1000;
  const feed = new Activity(() => clock);
  feed.resolve(snapshot([]));
  const rows = [];
  for (let i = 0; i < 50; i++) {
    rows.push(request("b" + i, "MATCHED", 3));
    feed.observe({
      entityId: "b" + i,
      entityVersion: 3,
      eventType: "request.state_changed",
    });
    clock += 5;
  }
  const items = feed.resolve(snapshot(rows));
  assert.equal(items.length, 1);
  assert.equal(describe(items[0]), "50 requests matched");
  assert.equal(items[0].ids.length, 50);
  // Five events in a second are not a burst.
  clock = 10000;
  for (let i = 0; i < 5; i++) {
    rows.push(request("c" + i, "MATCHED", 3));
    feed.observe({
      entityId: "c" + i,
      entityVersion: 3,
      eventType: "request.state_changed",
    });
    clock += 150;
  }
  assert.equal(feed.resolve(snapshot(rows)).length, 6);
});

test("the feed keeps the last 20 entries and never edits returned items", () => {
  let clock = 0;
  const feed = new Activity(() => clock);
  feed.resolve(snapshot([]));
  const rows = [];
  let first;
  for (let i = 0; i < 25; i++) {
    clock += 2000;
    rows.push(request("d" + i, "WAITING", 1));
    feed.observe({
      entityId: "d" + i,
      entityVersion: 1,
      eventType: "request.created",
    });
    const items = feed.resolve(snapshot(rows));
    if (i === 0) first = items[0];
  }
  const items = feed.resolve(snapshot(rows));
  assert.equal(items.length, FEED_SIZE);
  assert.equal(items[0].id, "d24");
  assert.equal(first.id, "d0");
  assert.equal(first.count, 1);
});

test("relative times", () => {
  assert.equal(relative(0, 4000), "just now");
  assert.equal(relative(0, 42000), "42 s ago");
  assert.equal(relative(0, 5 * 60000), "5 min ago");
  assert.equal(relative(0, 2 * 3600000), "2 h ago");
});

test("an event shown just before an unrelated burst is not absorbed by it", () => {
  let clock = 0;
  const feed = new Activity(() => clock);
  feed.resolve(snapshot([]));
  const rows = [request("solo", "WAITING", 1, "Solo meals")];
  feed.observe({
    entityId: "solo",
    entityVersion: 1,
    eventType: "request.created",
  });
  feed.resolve(snapshot(rows));
  clock = 700;
  for (let i = 0; i < 8; i++) {
    rows.push(request("t" + i, "WAITING", 1));
    feed.observe({
      entityId: "t" + i,
      entityVersion: 1,
      eventType: "request.created",
    });
    clock += 20;
  }
  const items = feed.resolve(snapshot(rows));
  assert.deepEqual(
    items.map((v) => describe(v)),
    ["8 requests created", "Request created"],
  );
  assert.equal(items[1].title, "Solo meals");
});

test("an event that arrives while the first read is in flight is kept", () => {
  const feed = new Activity(() => 0);
  feed.observe({
    entityId: "a",
    entityVersion: 6,
    eventType: "request.state_changed",
  });
  feed.observe({
    entityId: "b",
    entityVersion: 2,
    eventType: "request.state_changed",
  });
  // The baseline already shows b at v2 but a only at v5.
  assert.deepEqual(
    feed.resolve(
      snapshot([request("a", "WAITING", 5), request("b", "MATCHED", 2)]),
    ),
    [],
  );
  const items = feed.resolve(
    snapshot([request("a", "MATCHED", 6), request("b", "MATCHED", 2)]),
  );
  assert.deepEqual(
    items.map((v) => [describe(v), v.id]),
    [["Request matched", "a"]],
  );
});

test("a request created and matched within one read gets both entries", () => {
  const feed = new Activity(() => 0);
  feed.resolve(snapshot([]));
  feed.observe({
    entityId: "a",
    entityVersion: 1,
    eventType: "request.created",
  });
  feed.observe({
    entityId: "a",
    entityVersion: 3,
    eventType: "request.state_changed",
  });
  const items = feed.resolve(snapshot([request("a", "MATCHED", 3)]));
  assert.deepEqual(items.map(describe), ["Request matched", "Request created"]);
  assert.deepEqual(
    feed.fresh.map((v) => v.action),
    ["created", "matched"],
  );
});

test("toasts group a burst but not an event a second before it in the same read", () => {
  const notices = new Toasts(() => 5000);
  const entry = (id, at) => ({
    kind: "request",
    action: "matched",
    id,
    at,
    resource: "",
  });
  const list = notices.add([
    entry("early-request", 1000),
    ...Array.from({ length: 50 }, (_, i) => entry("burst-" + i, 2000 + i)),
  ]);
  assert.deepEqual(
    list.map((v) => v.text),
    ["50 requests matched", "Request #early-re matched"],
  );
});
test("toasts never show more than three and expire after five seconds", () => {
  let clock = 0;
  const notices = new Toasts(() => clock);
  for (let i = 0; i < 4; i++) {
    clock = i * 1000;
    notices.add([
      {
        kind: "request",
        action: "cancelled",
        id: "r" + i + "-xxxxxx",
        at: clock,
      },
    ]);
  }
  assert.deepEqual(
    notices.list.map((v) => v.text),
    [
      "Request #r3-xxxxx cancelled",
      "Request #r2-xxxxx cancelled",
      "Request #r1-xxxxx cancelled",
    ],
  );
  // r1 was shown at 1 s, so it expires at 6 s; r2 and r3 remain.
  clock = 6500;
  assert.deepEqual(
    notices.prune().map((v) => v.text),
    ["Request #r3-xxxxx cancelled", "Request #r2-xxxxx cancelled"],
  );
  clock = 8001;
  assert.deepEqual(notices.prune(), []);
});

test("toasts shown before a burst was recognised are folded into it", () => {
  let clock = 0;
  const notices = new Toasts(() => clock);
  const entry = (id, at) => ({
    kind: "request",
    action: "matched",
    id,
    at,
    resource: "Tent",
  });
  notices.add([entry("unrelated-1", 0)]);
  for (let i = 0; i < 10; i++) {
    clock = 1000 + i * 150;
    notices.add([entry("burst-" + i + "-xx", clock)]);
  }
  assert.deepEqual(
    notices.list.map((v) => v.text),
    ["10 requests matched", "Request #unrelate matched to Tent"],
  );
});

test("a two-minute read outage keeps the feed bounded and expires old events", () => {
  let clock = 0;
  const feed = new Activity(() => clock);
  feed.resolve(snapshot([]));
  let peak = 0;
  // 50 events a second for 120 s, no successful read; the timer runs every 5 s.
  for (let ms = 0; ms < 120000; ms += 20) {
    clock = ms;
    feed.observe({
      entityId: "e" + ms,
      entityVersion: 1,
      eventType: "request.state_changed",
    });
    if (ms % 5000 === 0) feed.expire();
    peak = Math.max(peak, feed.held());
    assert.ok(feed.held() <= 200, `held ${feed.held()} at ${ms} ms`);
    assert.ok(feed.arrivals.length <= 200);
  }
  assert.equal(peak, 200);
  assert.ok(feed.pending.every((v) => clock - v.at < 30000));
  clock += 31000;
  feed.expire();
  assert.equal(feed.held(), 0);
  assert.equal(feed.arrivals.length, 0);
});

test("toast state stays bounded through the outage and a heavy match stream", () => {
  let clock = 0;
  const feed = new Activity(() => clock);
  const notices = new Toasts(() => clock);
  feed.resolve(snapshot([]));
  // Parth's outage: 50 events a second for two minutes, no successful read.
  for (let ms = 0; ms < 120000; ms += 20) {
    clock = ms;
    feed.observe({
      entityId: "o" + ms,
      entityVersion: 1,
      eventType: "request.state_changed",
    });
    if (ms % 5000 === 0) {
      feed.expire();
      notices.expire();
    }
    assert.ok(notices.held() <= 200);
  }
  // Reads recover and every event is a match, 50 a second for two minutes.
  const rows = [];
  for (let ms = 120000; ms < 240000; ms += 20) {
    clock = ms;
    const id = "m" + ms + "-xxxxxxxx";
    rows.push(request(id, "MATCHED", 1));
    if (rows.length > 100) rows.shift();
    feed.observe({
      entityId: id,
      entityVersion: 1,
      eventType: "request.state_changed",
    });
    if (ms % 100 === 0) notices.add(feed.resolve(snapshot(rows)) && feed.fresh);
    if (ms % 5000 === 0) notices.expire();
    assert.ok(
      notices.held() <= 200,
      `toast state ${notices.held()} at ${ms} ms`,
    );
  }
  assert.ok(notices.list.length >= 1);
  // The timer alone drops everything once events stop.
  clock += 25000;
  notices.expire();
  assert.equal(notices.held(), 0);
  assert.deepEqual(notices.list, []);
});
