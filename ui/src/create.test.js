import { test } from "node:test";
import assert from "node:assert/strict";
import { CreateSubmission, payload } from "./create.js";

const id = "77777777-7777-4777-8777-777777777777";
const key = "88888888-8888-4888-8888-888888888888";
const input = {
  requestType: "WATER",
  details: "School supplies",
  urgency: "HIGH",
  peopleAffected: 4,
  location: { lat: 19, lng: 72 },
};
const resource = {
  resourceType: "WATER",
  name: "Water team",
  location: input.location,
  reusable: true,
  windowStart: "2026-11-12T10:00:00.000Z",
  windowEnd: "2026-11-12T12:00:00.000Z",
};
const row = (kind, version = "v2") => ({
  [kind + "Id"]: id,
  version,
  status: kind === "request" ? "WAITING" : "AVAILABLE",
});
const reply = (body, status = 200) =>
  new Response(JSON.stringify(body), { status });
const page = (items, nextCursor) => ({
  items,
  nextCursor,
  partial: false,
  failedShards: [],
  consistency: "strong",
  positions: [0, 1, 2].map((shard) => ({ shard, appliedIndex: 20 })),
});
function storage() {
  const values = new Map();
  return {
    getItem: (k) => values.get(k) ?? null,
    setItem: (k, v) => values.set(k, v),
    removeItem: (k) => values.delete(k),
  };
}
function setup(kind = "request", overrides = {}) {
  const calls = [];
  const responses = [];
  const saved = storage();
  const request = async (path, options) => {
    calls.push({ path, ...options });
    const result = responses.shift();
    if (result instanceof Error) throw result;
    return typeof result === "function" ? result() : result;
  };
  const client = new CreateSubmission(kind, {
    storage: saved,
    request,
    uuid: () => key,
    ...overrides,
  });
  return { client, calls, responses, saved, request };
}
const fresh = async () => true;

test("same-task submit reentry sends once and waits for the current read and view refresh", async () => {
  const f = setup();
  let release;
  const refresh = () =>
    new Promise((resolve) => {
      release = resolve;
    });
  f.responses.push(
    reply(row("request", "v1"), 201),
    reply(row("request", "v7")),
  );
  const first = f.client.submit(input, refresh);
  assert.equal(f.client.submit(input, refresh), first);
  await new Promise((resolve) => setImmediate(resolve));
  assert.equal(f.calls.filter((c) => c.method === "POST").length, 1);
  assert.ok(f.client.pending());
  release(true);
  assert.equal((await first).version, "v7");
  assert.equal(f.client.pending(), null);
  assert.equal(f.calls[0].headers["Idempotency-Key"], key);
  assert.equal(f.calls[0].redirect, "error");
  assert.equal(f.calls[0].cache, "no-store");
});

test("lost reply then reload retries identical bytes/key and keeps edits locked", async () => {
  const f = setup();
  f.responses.push(new TypeError("network lost"));
  await assert.rejects(f.client.submit(input, fresh));
  const first = f.calls[0];
  const again = new CreateSubmission("request", {
    storage: f.saved,
    request: f.request,
    uuid: () => {
      throw Error("must not regenerate");
    },
  });
  await assert.rejects(
    again.submit({ ...input, details: "changed" }, fresh),
    /saved submission/,
  );
  f.responses.push(
    reply(row("request", "v1"), 201),
    reply(row("request", "v3")),
  );
  await again.submit(undefined, fresh);
  assert.equal(f.calls[1].body, first.body);
  assert.equal(
    f.calls[1].headers["Idempotency-Key"],
    first.headers["Idempotency-Key"],
  );
  assert.equal(again.pending(), null);
});

test("a later definite INVALID cannot erase an earlier unknown creation", async () => {
  const f = setup();
  f.responses.push(
    new Error("lost"),
    reply({ code: "INVALID", outcomeUnknown: false }, 400),
  );
  await assert.rejects(f.client.submit(input, fresh));
  await assert.rejects(f.client.submit(undefined, fresh));
  assert.equal(f.client.pending().uncertain, true);
  assert.throws(() => f.client.editRejected(), /unresolved/);
});

test("only a first definite validation refusal permits editing", async () => {
  const f = setup();
  f.responses.push(reply({ code: "INVALID", outcomeUnknown: false }, 400));
  await assert.rejects(f.client.submit(input, fresh));
  assert.equal(f.client.pending().rejected, true);
  f.client.editRejected();
  assert.equal(f.client.pending(), null);
});

test("known creation with a failed reread or view refresh resumes reads without another POST", async () => {
  const f = setup();
  f.responses.push(
    reply(row("request", "v1"), 201),
    reply({}, 503),
    reply(row("request")),
    reply(row("request")),
  );
  await assert.rejects(f.client.submit(input, fresh));
  assert.ok(f.client.pending().ack);
  await assert.rejects(
    f.client.submit(undefined, async () => false),
    /refresh/,
  );
  assert.ok(f.client.pending().ack);
  await f.client.submit(undefined, fresh);
  assert.equal(f.calls.filter((c) => c.method === "POST").length, 1);
});

test("storage failure prevents the first network write and corrupt saved state fails closed", async () => {
  const f = setup("request", {
    storage: {
      getItem: () => null,
      setItem: () => {
        throw Error("quota");
      },
    },
  });
  await assert.rejects(f.client.submit(input, fresh), /save/);
  assert.equal(f.calls.length, 0);
  assert.throws(
    () =>
      new CreateSubmission("request", { storage: { getItem: () => "broken" } }),
    /saved submission/,
  );
});

test("resource confirmation walks public pages and never infers absence from the first page", async () => {
  const f = setup("resource");
  f.responses.push(
    reply(row("resource", "v1"), 201),
    reply(page([], "next + token")),
    reply(page([row("resource", "v4")])),
  );
  assert.equal((await f.client.submit(resource, fresh)).version, "v4");
  assert.equal(
    f.calls[2].path,
    "/api/v1/resources?limit=100&cursor=next%20%2B%20token",
  );
  assert.deepEqual(JSON.parse(f.calls[0].body), resource);
});

test("stale, partial, failed or missing shard metadata cannot confirm resource creation", async () => {
  for (const change of [
    (p) => (p.consistency = "stale"),
    (p) => delete p.consistency,
    (p) => (p.partial = true),
    (p) => p.failedShards.push(1),
    (p) => p.positions.pop(),
    (p) => (p.positions[1].shard = 0),
  ]) {
    const f = setup("resource");
    const observation = page([row("resource")]);
    change(observation);
    f.responses.push(reply(row("resource", "v1"), 201), reply(observation));
    await assert.rejects(f.client.submit(resource, fresh));
    assert.ok(f.client.pending().ack);
  }
});

test("cursor loops, missing current identity and older versions keep the saved intent", async () => {
  for (const bad of [
    { ...row("request"), requestId: key },
    row("request", "v0"),
  ]) {
    const f = setup();
    f.responses.push(reply(row("request", "v1"), 201), reply(bad));
    await assert.rejects(f.client.submit(input, fresh));
    assert.ok(f.client.pending());
  }
  const f = setup("resource");
  f.responses.push(
    reply(row("resource", "v1"), 201),
    reply(page([], "loop")),
    reply(page([], "loop")),
  );
  await assert.rejects(f.client.submit(resource, fresh), /confirm/);
  assert.ok(f.client.pending());
});

test("input is validated before persisting and frozen windows are UTC", () => {
  assert.deepEqual(payload("request", input), input);
  for (const bad of [
    { ...input, peopleAffected: 0 },
    { ...input, location: { lat: 91, lng: 0 } },
    { ...input, urgency: "NOW" },
  ])
    assert.throws(() => payload("request", bad));
  assert.throws(() =>
    payload("resource", { ...resource, windowEnd: resource.windowStart }),
  );
  assert.throws(() => payload("resource", { ...resource, name: "" }));
});
