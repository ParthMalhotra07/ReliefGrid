// The live stream carries only identities (entityId, entityVersion,
// eventType), so each stream event waits until an authoritative read shows
// that entity at or past the announced version, then is described from the
// record's current state.
export const FEED_SIZE = 20;
// More than BURST events within WINDOW ms of each other are grouped.
export const BURST = 5;
export const WINDOW = 1000;
// A stream event whose record never appears in a read is dropped.
const PENDING_MS = 30000;

const ACTIONS = {
  MATCHED: "matched",
  WAITING: "waiting",
  IN_PROGRESS: "work started",
  FULFILLED: "fulfilled",
  CANCELLED: "cancelled",
};
const version = (value) => Number(String(value ?? "").replace(/^v/, "")) || 0;
const words = (text) => (text || "").replaceAll("_", " ").toLowerCase();
const created = (eventType) => /\.(created|registered)$/.test(eventType);

export function relative(at, now) {
  const seconds = Math.max(0, Math.round((now - at) / 1000));
  if (seconds < 10) return "just now";
  if (seconds < 60) return `${seconds} s ago`;
  if (seconds < 3600) return `${Math.floor(seconds / 60)} min ago`;
  return `${Math.floor(seconds / 3600)} h ago`;
}

export function describe(item) {
  const noun = item.kind === "request" ? "request" : "resource";
  if (item.count > 1) return `${item.count} ${noun}s ${item.action}`;
  return `${noun.charAt(0).toUpperCase() + noun.slice(1)} ${item.action}`;
}

export class Activity {
  constructor(now = () => Date.now()) {
    this.now = now;
    this.pending = [];
    this.arrivals = [];
    this.seen = null;
    this.items = [];
    this.sequence = 0;
  }
  observe({ entityId, entityVersion, eventType }) {
    if (!entityId) return;
    const at = this.now();
    this.arrivals.push(at);
    this.pending.push({ entityId, entityVersion, eventType, at });
  }
  rate(at) {
    return this.arrivals.filter((t) => Math.abs(t - at) <= WINDOW).length;
  }
  add(entry) {
    // Only entries that were themselves part of a burst are grouped, so an
    // event shown just before an unrelated burst keeps its own line.
    entry.burst = this.rate(entry.at) > BURST;
    if (entry.burst) {
      const group = this.items.find(
        (v) =>
          v.burst &&
          v.kind === entry.kind &&
          v.action === entry.action &&
          Math.abs(v.at - entry.at) <= WINDOW,
      );
      if (group) {
        group.count++;
        group.ids.push(entry.ids[0]);
        group.id = entry.ids[0];
        group.title = "";
        group.at = Math.max(group.at, entry.at);
        return;
      }
    }
    this.items.unshift(entry);
  }
  // Returns the feed, newest first, after reading a successful snapshot.
  resolve(snapshot) {
    const records = new Map();
    for (const row of snapshot.requests.items)
      records.set(row.requestId, {
        kind: "request",
        row,
        title: row.details || `${words(row.requestType)} request`,
      });
    for (const row of snapshot.resources.items)
      records.set(row.resourceId, { kind: "resource", row, title: row.name });
    if (!this.seen) {
      // The first read is the baseline; nothing before it is announced.
      this.seen = new Map([...records].map(([id, v]) => [id, v.row.status]));
      this.pending = [];
      return this.items;
    }
    const now = this.now();
    // Earlier results belong to React state; work on copies.
    const previous = this.items;
    this.items = this.items.map((v) => ({ ...v, ids: [...v.ids] }));
    const waiting = [];
    let changed = false;
    for (const event of this.pending) {
      const record = records.get(event.entityId);
      if (!record || version(record.row.version) < event.entityVersion) {
        if (now - event.at < PENDING_MS) waiting.push(event);
        continue;
      }
      const status = record.row.status;
      const before = this.seen.get(event.entityId);
      this.seen.set(event.entityId, status);
      const action =
        created(event.eventType) && before === undefined
          ? record.kind === "request"
            ? "created"
            : "registered"
          : status !== before
            ? ACTIONS[status]
            : undefined;
      if (!action) continue;
      changed = true;
      this.add({
        key: ++this.sequence,
        kind: record.kind,
        action,
        id: event.entityId,
        ids: [event.entityId],
        title: record.title,
        count: 1,
        at: event.at,
      });
    }
    this.pending = waiting;
    this.arrivals = this.arrivals.filter((t) => now - t <= 10 * WINDOW);
    this.items = changed ? this.items.slice(0, FEED_SIZE) : previous;
    return this.items;
  }
}
