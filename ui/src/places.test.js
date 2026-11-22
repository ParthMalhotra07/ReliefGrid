import test from "node:test";
import assert from "node:assert/strict";
import { gazetteer, fourDecimals } from "./places.js";
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
test("the bundled list covers capitals, the largest cities and metro areas", () => {
  assert.ok(places.length >= 150 && places.length <= 200, places.length);
  const names = places.map((p) => p.name);
  assert.equal(new Set(names).size, names.length, "names are unique");
  // Every state capital, plus the union territory capitals.
  for (const name of [
    "Amaravati",
    "Itanagar",
    "Dispur",
    "Patna",
    "Raipur",
    "Panaji",
    "Gandhinagar",
    "Chandigarh",
    "Shimla",
    "Ranchi",
    "Bengaluru",
    "Thiruvananthapuram",
    "Bhopal",
    "Mumbai",
    "Imphal",
    "Shillong",
    "Aizawl",
    "Kohima",
    "Bhubaneswar",
    "Jaipur",
    "Gangtok",
    "Chennai",
    "Hyderabad",
    "Agartala",
    "Lucknow",
    "Dehradun",
    "Kolkata",
    "New Delhi",
    "Srinagar",
    "Jammu",
    "Leh",
    "Puducherry",
    "Port Blair",
    "Kavaratti",
    "Daman",
  ])
    assert.ok(names.includes(name), name);
  const count = (pred) => places.filter(pred).length;
  assert.equal(
    count((p) => p.group === "city"),
    100,
  );
  for (const metro of ["Mumbai", "Delhi NCR", "Bengaluru"])
    assert.ok(count((p) => p.metro === metro) >= 15, metro);
  for (const p of places) {
    assert.ok(p.state && Number.isInteger(p.geonameid), p.name);
    // Four decimals at most, and inside India's bounding box.
    for (const v of [p.lat, p.lng])
      assert.equal(Number(v.toFixed(4)), v, p.name);
    assert.ok(p.lat > 6 && p.lat < 37 && p.lng > 68 && p.lng < 98, p.name);
  }
});
test("new entries match case-insensitively by prefix and substring", () => {
  assert.deepEqual(gazetteer("KORAM")[0], {
    label: "Koramangala, Karnataka",
    lat: 12.932,
    lng: 77.6227,
  });
  assert.equal(gazetteer("gangt")[0].label, "Gangtok, Sikkim");
  assert.equal(gazetteer("chowk")[0].label, "Chandni Chowk, Delhi");
  assert.equal(gazetteer("layout")[0].label, "BTM Layout, Karnataka");
  assert.equal(gazetteer("Electronic")[0].label, "Electronic City, Karnataka");
  assert.equal(gazetteer("tiruchi")[0].label, "Tiruchirappalli, Tamil Nadu");
  assert.deepEqual(
    gazetteer("nagaland").map((p) => p.label),
    ["Kohima, Nagaland"],
  );
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
