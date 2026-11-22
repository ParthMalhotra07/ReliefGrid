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
  if (windowState(resource, now).state !== "open") return "Window closed";
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

// The resource availability window as the backend checks it:
// open when from <= now < until.
export function windowState(resource, now = Date.now()) {
  const from = Date.parse(resource.windowStart),
    until = Date.parse(resource.windowEnd);
  if (!Number.isFinite(from) || !Number.isFinite(until))
    return { state: "unknown", ms: 0 };
  if (now < from) return { state: "future", ms: from - now };
  if (now < until) return { state: "open", ms: until - now };
  return { state: "expired", ms: now - until };
}
// Whole minutes, rounded up, as "45 min" or "2 h 39 min".
// Whole minutes as "45 min", "5 h", "2 h 39 min", "1 d" or "1 d 2 h"; a zero
// smaller part is left out.
export function span(total) {
  if (total < 60) return `${total} min`;
  if (total < 1440) {
    const h = Math.floor(total / 60),
      m = total % 60;
    return m ? `${h} h ${m} min` : `${h} h`;
  }
  const d = Math.floor(total / 1440),
    h = Math.floor((total % 1440) / 60);
  return h ? `${d} d ${h} h` : `${d} d`;
}
// Windows round up, so a window never reads as closing in 0 min.
const minutes = (ms) => span(Math.max(1, Math.ceil(ms / 60000)));
export function windowLabel(resource, now = Date.now()) {
  const { state, ms } = windowState(resource, now);
  if (state === "open") return `Open · closes in ${minutes(ms)}`;
  if (state === "future") return `Opens in ${minutes(ms)}`;
  if (state === "expired") return "Expired";
  return "";
}

export const waiting = (status) =>
  status === "WAITING" || status === "RETRYING";
// When a request started waiting: its first observed Waiting entry, or its
// creation time if no Waiting entry was observed.
export function waitingSince(request, history = []) {
  const first = history.find((v) => v.status === "WAITING");
  const at = Date.parse(first?.at ?? request.createdAt);
  return Number.isFinite(at) ? at : Date.parse(request.createdAt);
}
// "waiting 12 min", "waiting 1 h 13 min", "waiting 2 d 3 h".
export function waitingLabel(request, history, now = Date.now()) {
  const total = Math.max(
    0,
    Math.floor((now - waitingSince(request, history)) / 60000),
  );
  return `waiting ${span(total)}`;
}

export const SORTS = [
  ["urgency", "Urgency"],
  ["oldest-waiting", "Oldest waiting"],
  ["newest", "Newest"],
];
const urgencyRank = { CRITICAL: 0, HIGH: 1, NORMAL: 2 };
const created = (r) => Date.parse(r.createdAt) || 0;
// Urgency: most urgent first, then oldest created. Oldest waiting: waiting
// and retrying requests by how long they have waited, then the rest, oldest
// created first. Newest: most recently created first.
export function sortRequests(rows, mode, history = {}) {
  const byCreated = (a, b) =>
    created(a) - created(b) || a.requestId.localeCompare(b.requestId);
  const sorted = [...rows];
  if (mode === "newest") return sorted.sort((a, b) => -byCreated(a, b));
  if (mode === "oldest-waiting")
    return sorted.sort((a, b) => {
      const wa = waiting(a.status),
        wb = waiting(b.status);
      if (wa !== wb) return wa ? -1 : 1;
      if (wa)
        return (
          waitingSince(a, history[a.requestId]) -
            waitingSince(b, history[b.requestId]) || byCreated(a, b)
        );
      return byCreated(a, b);
    });
  // Urgency: unfinished before finished, then most urgent, then oldest.
  return sorted.sort(
    (a, b) =>
      terminal(a.status) - terminal(b.status) ||
      (urgencyRank[a.urgency] ?? 3) - (urgencyRank[b.urgency] ?? 3) ||
      byCreated(a, b),
  );
}

// The soonest window start or end after `now` among loaded resources.
export function nextBoundary(resources, now) {
  let next;
  for (const row of resources)
    for (const value of [row.windowStart, row.windowEnd]) {
      const at = Date.parse(value);
      if (at > now && (next === undefined || at < next)) next = at;
    }
  return next;
}
