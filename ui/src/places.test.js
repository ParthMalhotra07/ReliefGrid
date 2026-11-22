import test from "node:test";
import assert from "node:assert/strict";
import { gazetteer, PlaceSearch, NOMINATIM, fourDecimals } from "./places.js";
import places from "./places.json" with { type: "json" };
import { distanceKm } from "./state.js";

test("gazetteer matches case-insensitive prefixes before substrings", () => {
  assert.equal(gazetteer("ANDH")[0].label, "Andheri, Maharashtra");
  assert.deepEqual(
    gazetteer("mumbai").map((p) => p.label),
    ["Mumbai, Maharashtra", "Navi Mumbai, Maharashtra"],
  );
  assert.equal(gazetteer("rugra")[0].label, "Gurugram, Haryana");
  assert.ok(gazetteer("a").length <= 5);
  assert.deepEqual(gazetteer("   "), []);
  assert.deepEqual(gazetteer("lonavala"), []);
});
test("the bundled list holds about 30 valid places including the required ones", () => {
  assert.ok(places.length >= 28 && places.length <= 32);
  for (const name of [
    "Andheri",
    "Bandra",
    "Dadar",
    "Kurla",
    "Thane",
    "Navi Mumbai",
    "Borivali",
    "Pune",
    "Delhi",
    "Gurugram",
    "Noida",
    "Bengaluru",
    "Hyderabad",
    "Chennai",
    "Kolkata",
    "Ahmedabad",
    "Jaipur",
    "Lucknow",
    "Kochi",
    "Guwahati",
  ])
    assert.ok(
      places.some((p) => p.name === name),
      name,
    );
  for (const p of places) {
    assert.ok(
      p.state && Math.abs(p.lat) <= 90 && Math.abs(p.lng) <= 180,
      p.name,
    );
    // Every entry is inside India's bounding box.
    assert.ok(p.lat > 6 && p.lat < 37 && p.lng > 68 && p.lng < 98, p.name);
  }
});
test("online search is throttled, never retried, and validates rows", async () => {
  let clock = 0;
  const calls = [];
  const search = new PlaceSearch({
    now: () => clock,
    fetch: async (url, options) => {
      calls.push({ url, options });
      return {
        ok: true,
        json: async () => [
          { display_name: "A", lat: "18.5", lon: "73.8" },
          { display_name: "bad", lat: "x", lon: "1" },
          ...Array.from({ length: 6 }, (_, i) => ({
            display_name: "P" + i,
            lat: "1",
            lon: "2",
          })),
        ],
      };
    },
  });
  const first = await search.online(" Lonavala ");
  assert.equal(first.results.length, 5);
  assert.deepEqual(first.results[0], {
    label: "A",
    lat: 18.5,
    lng: 73.8,
    source: "nominatim",
  });
  assert.equal(calls[0].url, NOMINATIM + "?format=jsonv2&limit=5&q=Lonavala");
  assert.equal(
    calls[0].options.referrerPolicy,
    "strict-origin-when-cross-origin",
  );
  clock = 999;
  assert.deepEqual(await search.online("x"), { throttled: true, results: [] });
  assert.equal(calls.length, 1);
  clock = 1000;
  await search.online("x");
  assert.equal(calls.length, 2);
  const failing = new PlaceSearch({
    fetch: async () => ({ ok: false, status: 503 }),
  });
  await assert.rejects(failing.online("x"), /PLACE_SEARCH_503/);
});
test("coordinates are rounded to four decimals and distance matches the backend", () => {
  assert.equal(fourDecimals(18.52043), "18.5204");
  assert.equal(fourDecimals(-0.00004), "0.0000");
  assert.ok(
    Math.abs(
      distanceKm({ lat: 19.08, lng: 72.88 }, { lat: 19.1, lng: 72.9 }) - 3.0598,
    ) < 0.0005,
  );
  assert.equal(distanceKm({ lat: 1, lng: 2 }, { lat: 1, lng: 2 }), 0);
});
