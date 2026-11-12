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
