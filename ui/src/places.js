import places from "./places.json" with { type: "json" };

export const NOMINATIM = "https://nominatim.openstreetmap.org/search";
export const UNAVAILABLE =
  "Place search unavailable, use the map or enter coordinates.";
const round = (value) => Number(value.toFixed(4));

// Case-insensitive match on the bundled list: prefixes rank before substrings.
export function gazetteer(query, list = places) {
  const q = query.trim().toLowerCase();
  if (!q) return [];
  const scored = [];
  for (const place of list) {
    const name = place.name.toLowerCase();
    const where = (place.name + " " + place.state).toLowerCase();
    const rank = name.startsWith(q)
      ? 0
      : name.split(/\s+/).some((word) => word.startsWith(q))
        ? 1
        : where.includes(q)
          ? 2
          : -1;
    if (rank >= 0) scored.push([rank, place]);
  }
  return scored
    .sort((a, b) => a[0] - b[0] || a[1].name.localeCompare(b[1].name))
    .slice(0, 5)
    .map(([, p]) => ({
      label: `${p.name}, ${p.state}`,
      lat: p.lat,
      lng: p.lng,
      source: "gazetteer",
    }));
}

// At most one Nominatim request per interval and no automatic retries. A search
// inside the interval is refused rather than queued.
export class PlaceSearch {
  constructor({
    fetch = globalThis.fetch,
    now = () => Date.now(),
    interval = 1000,
  } = {}) {
    // Called as a plain function: window.fetch throws when invoked as a method.
    this.fetch = (...args) => fetch(...args);
    this.now = now;
    this.interval = interval;
    this.last = -Infinity;
  }
  ready() {
    return this.now() - this.last >= this.interval;
  }
  async online(query, signal) {
    if (!this.ready()) return { throttled: true, results: [] };
    this.last = this.now();
    const url =
      NOMINATIM +
      "?" +
      new URLSearchParams({ format: "jsonv2", limit: "5", q: query.trim() });
    const response = await this.fetch(url, {
      referrerPolicy: "strict-origin-when-cross-origin",
      headers: { Accept: "application/json" },
      signal,
    });
    if (!response.ok) throw new Error("PLACE_SEARCH_" + response.status);
    const body = await response.json();
    if (!Array.isArray(body)) throw new Error("PLACE_SEARCH_INVALID");
    const results = [];
    for (const row of body) {
      const lat = Number(row?.lat),
        lng = Number(row?.lon);
      if (
        typeof row?.display_name !== "string" ||
        !Number.isFinite(lat) ||
        !Number.isFinite(lng) ||
        Math.abs(lat) > 90 ||
        Math.abs(lng) > 180
      )
        continue;
      results.push({ label: row.display_name, lat, lng, source: "nominatim" });
      if (results.length === 5) break;
    }
    return { throttled: false, results };
  }
}
export const fourDecimals = (value) => round(value).toFixed(4);
