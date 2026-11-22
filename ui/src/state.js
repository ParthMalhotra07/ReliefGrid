export const REQUEST_STATUSES = [
  "PENDING",
  "WAITING",
  "MATCHING",
  "MATCHED",
  "IN_PROGRESS",
  "RETRYING",
  "CANCELLING",
  "FULFILLING",
  "CANCELLED",
  "FULFILLED",
];
export const TYPES = ["WATER", "FOOD", "MEDICINE", "TRANSPORTATION", "SHELTER"];
export const uuid =
  /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;
export class EventIDs {
  constructor(limit = 2000) {
    this.ids = new Set();
    this.limit = limit;
  }
  accept(id) {
    if (!uuid.test(id) || this.ids.has(id)) return false;
    this.ids.add(id);
    if (this.ids.size > this.limit)
      this.ids.delete(this.ids.values().next().value);
    return true;
  }
}
export function list(value, kind) {
  if (
    !value ||
    !["strong", "linearizable"].includes(value.consistency) ||
    value.partial !== false ||
    !Array.isArray(value.failedShards) ||
    value.failedShards.length ||
    !Array.isArray(value.items) ||
    !Array.isArray(value.positions)
  )
    throw new Error("INCOMPLETE_READ");
  const shards = new Set();
  for (const p of value.positions) {
    if (
      !Number.isInteger(p.shard) ||
      p.shard < 0 ||
      p.shard > 2 ||
      shards.has(p.shard) ||
      !Number.isSafeInteger(p.appliedIndex) ||
      p.appliedIndex < 0
    )
      throw new Error("INCOMPLETE_READ");
    shards.add(p.shard);
  }
  if (shards.size !== 3) throw new Error("INCOMPLETE_READ");
  const ids = new Set();
  for (const row of value.items) {
    const id = row[kind + "Id"];
    if (
      !uuid.test(id) ||
      ids.has(id) ||
      !row.location ||
      !Number.isFinite(row.location.lat) ||
      Math.abs(row.location.lat) > 90 ||
      !Number.isFinite(row.location.lng) ||
      Math.abs(row.location.lng) > 180
    )
      throw new Error("INVALID_RECORD");
    ids.add(id);
    if (kind === "request" && !REQUEST_STATUSES.includes(row.status))
      throw new Error("INVALID_RECORD");
  }
  return value;
}
export function observe(previous, requests) {
  const next = {};
  for (const row of requests) {
    const old = previous[row.requestId] || [];
    const item = {
      version: row.version,
      status: row.status,
      at: row.updatedAt,
    };
    next[row.requestId] =
      old.at(-1)?.version === row.version ? old : [...old, item].slice(-12);
  }
  return next;
}
export function reciprocal(request, resource) {
  return (
    request.assignedResourceId === resource.resourceId &&
    resource.activeClaim?.requestId === request.requestId
  );
}
export const terminal = (status) =>
  status === "CANCELLED" || status === "FULFILLED";
// lastError is sticky in the FSM: it keeps the code of the most recent retry
// even after the request moves on. Translate a code only while the request is
// still in a state that code can explain; other states show nothing.
const waitingReasons = {
  NO_CANDIDATE: "No matching resource within 50 km yet",
  RESOURCE_AVAILABLE: "A resource became available nearby; matching again",
  ATTEMPT_CLOSED: "The previous match was released; matching again",
};
const retryReasons = {
  OUTCOME_UNKNOWN: "Last matching step was not confirmed; it will be retried",
  INCOMPLETE_SCAN: "Some resources could not be checked; matching will retry",
  CONCURRENT_CHANGE: "The request changed during matching; matching will retry",
  RECOVERY_PENDING: "Checking a previous match before retrying",
  WORKFLOW_RETRY: "Matching hit a temporary problem and will retry",
};
const retryStates = new Set(["PENDING", "WAITING", "MATCHING", "RETRYING"]);
export function retryReason(status, code) {
  if (!code || !retryStates.has(status)) return "";
  if (code in waitingReasons)
    return status === "WAITING" ? waitingReasons[code] : "";
  return retryReasons[code] || "Matching will retry automatically";
}
// Straight-line (great-circle) distance with the backend's mean Earth radius
// (internal/geo.HaversineKM); display only, never a matching decision.
export function distanceKm(a, b) {
  const radians = Math.PI / 180;
  const dlat = (b.lat - a.lat) * radians;
  const dlng = (b.lng - a.lng) * radians;
  const h =
    Math.sin(dlat / 2) ** 2 +
    Math.cos(a.lat * radians) *
      Math.cos(b.lat * radians) *
      Math.sin(dlng / 2) ** 2;
  return 2 * 6371.0088 * Math.asin(Math.sqrt(Math.max(0, Math.min(1, h))));
}

// Mirrors internal/workflow/candidates.go: same type, AVAILABLE with no active
// claim, from <= now < until, and within 50 km (distance <= 50). Returns ""
// for an eligible resource, otherwise the single most relevant reason.
export const RADIUS_KM = 50;
export function ineligible(request, resource, now = Date.now()) {
  const distance = distanceKm(request.location, resource.location);
  if (distance > RADIUS_KM) return `Too far: ${Math.round(distance)} km`;
  const from = Date.parse(resource.windowStart),
    until = Date.parse(resource.windowEnd);
  if (!(from <= now && now < until)) return "Window closed";
  if (resource.resourceType !== request.requestType) return "Wrong type";
  if (resource.activeClaim || ["ASSIGNED", "BUSY"].includes(resource.status))
    return "Already assigned";
  if (resource.status !== "AVAILABLE") return "Not available";
  return "";
}

// "3.8 s", "2 min 12 s", "10 min", "1 h 5 min".
export function duration(ms) {
  if (!(ms >= 0)) return "";
  if (ms < 50) return "under 0.1 s";
  if (ms < 60000) return `${(Math.round(ms / 100) / 10).toFixed(1)} s`;
  const seconds = Math.floor(ms / 1000);
  if (seconds < 3600) {
    const m = Math.floor(seconds / 60),
      s = seconds % 60;
    return s ? `${m} min ${s} s` : `${m} min`;
  }
  const h = Math.floor(seconds / 3600),
    m = Math.floor((seconds % 3600) / 60);
  return m ? `${h} h ${m} min` : `${h} h`;
}

// Timings for observed timeline entries, from the timeline's own timestamps:
// time since creation on Matched, Fulfilled and Cancelled, and on the last
// Waiting entry the span from the first wait until the last one ended.
export function timings(createdAt, entries) {
  const at = (v) => Date.parse(v);
  const created = at(createdAt);
  const out = entries.map(() => "");
  entries.forEach((entry, i) => {
    const name = {
      MATCHED: "Matched",
      FULFILLED: "Fulfilled",
      CANCELLED: "Cancelled",
    }[entry.status];
    const since = duration(at(entry.at) - created);
    if (name && since) out[i] = `${name} ${since} after creation`;
  });
  const waits = entries.flatMap((v, i) => (v.status === "WAITING" ? [i] : []));
  const last = waits.at(-1);
  if (last !== undefined && entries[last + 1]) {
    const waited = duration(
      at(entries[last + 1].at) - at(entries[waits[0]].at),
    );
    if (waited) out[last] = `Waited ${waited}`;
  }
  return out;
}
