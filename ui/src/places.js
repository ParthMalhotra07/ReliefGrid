import places from "./places.json" with { type: "json" };

// Place search is offline only: the bundled list, the map picker and typed
// coordinates. Nothing is sent to an online geocoder.
export const NOT_LISTED =
  "Place not in the offline list, use the map or enter coordinates.";
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
    }));
}
export const fourDecimals = (value) => round(value).toFixed(4);
