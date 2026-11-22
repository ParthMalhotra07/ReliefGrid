import test from "node:test";
import assert from "node:assert/strict";
import { QueueTrend, sparkline, TREND_MS, TREND_LIMIT } from "./trend.js";

test("samples within 10 s replace each other and the window keeps 15 minutes", () => {
  const trend = new QueueTrend();
  trend.add(0, 3);
  trend.add(4000, 5);
  assert.deepEqual(trend.samples, [{ at: 4000, count: 5 }]);
  trend.add(15000, 2);
  assert.equal(trend.samples.length, 2);
  for (let t = 15000; t <= 40 * 60000; t += 2000) trend.add(t, t % 7);
  assert.ok(trend.samples.length <= TREND_LIMIT);
  const last = trend.samples.at(-1).at;
  assert.ok(trend.samples.every((v) => last - v.at <= TREND_MS));
});

test("the summary gives the current count and the peak of the last 15 minutes", () => {
  const trend = new QueueTrend();
  assert.equal(trend.summary(0), "Queue: collecting samples");
  trend.add(0, 7);
  trend.add(60000, 4);
  assert.equal(trend.summary(60000), "Queue: 4 now, peak 7 in last 15 min");
  // The peak sample leaves the window.
  assert.equal(
    trend.summary(TREND_MS + 30000),
    "Queue: 4 now, peak 4 in last 15 min",
  );
});

test("sparkline points span the window and scale to the peak", () => {
  const now = TREND_MS;
  const points = sparkline(
    [
      { at: 0, count: 0 },
      { at: TREND_MS / 2, count: 4 },
      { at: now, count: 2 },
    ],
    now,
    120,
    24,
  );
  assert.equal(points, "0,22 60,2 120,12");
  // One sample: a flat line across the width.
  assert.equal(sparkline([{ at: now, count: 3 }], now, 120, 24), "0,2 120,2");
  // All zero: a flat line on the baseline.
  assert.equal(
    sparkline(
      [
        { at: 0, count: 0 },
        { at: now, count: 0 },
      ],
      now,
      120,
      24,
    ),
    "0,22 120,22",
  );
  assert.equal(sparkline([], now, 120, 24), "");
});

test("samples exactly 10 s apart are capped at 90", () => {
  const trend = new QueueTrend();
  for (let t = 0; t <= TREND_MS; t += 10000) trend.add(t, 1);
  // 91 reads in the window; the oldest is dropped.
  assert.equal(trend.samples.length, TREND_LIMIT);
  assert.equal(trend.samples[0].at, 10000);
});
