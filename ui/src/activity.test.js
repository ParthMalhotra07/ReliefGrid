import test from "node:test";
import assert from "node:assert/strict";
import { Activity, describe, relative, FEED_SIZE } from "./activity.js";

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
