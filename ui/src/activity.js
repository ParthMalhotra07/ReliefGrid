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
    // Entries first described by the latest resolve, before any grouping.
    this.fresh = [];
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
    this.fresh = [];
    const records = new Map();
    const names = new Map(
      snapshot.resources.items.map((row) => [row.resourceId, row.name]),
    );
    for (const row of snapshot.requests.items)
      records.set(row.requestId, {
        kind: "request",
        row,
        title: row.details || `${words(row.requestType)} request`,
        resource: names.get(row.assignedResourceId) || "",
      });
    for (const row of snapshot.resources.items)
      records.set(row.resourceId, { kind: "resource", row, title: row.name });
    if (!this.seen) {
      // The first read is the baseline; nothing it already shows is
      // announced. Pending events are kept: those it already reflects
      // compare equal to it, and those that arrived while it was in flight
      // are described by the next read.
      this.seen = new Map([...records].map(([id, v]) => [id, v.row.status]));
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
      const birth = created(event.eventType) && before === undefined;
      // A creation is recorded in its initial state, so a later event for
      // the same record that the same read already shows (created and
      // matched together) is still described.
      this.seen.set(
        event.entityId,
        birth ? (record.kind === "request" ? "PENDING" : "AVAILABLE") : status,
      );
      const action = birth
        ? record.kind === "request"
          ? "created"
          : "registered"
        : status !== before
          ? ACTIONS[status]
          : undefined;
      if (!action) continue;
      changed = true;
      const entry = {
        key: ++this.sequence,
        kind: record.kind,
        action,
        id: event.entityId,
        ids: [event.entityId],
        title: record.title,
        resource: record.resource || "",
        count: 1,
        at: event.at,
      };
      this.fresh.push({ ...entry, ids: [event.entityId] });
      this.add(entry);
    }
    this.pending = waiting;
    this.arrivals = this.arrivals.filter((t) => now - t <= 10 * WINDOW);
    this.items = changed ? this.items.slice(0, FEED_SIZE) : previous;
    return this.items;
  }
}

// Live toasts for matched, fulfilled and cancelled requests. More than
// TOAST_BURST such events within TOAST_WINDOW ms form one grouped toast; an
// event belongs to a burst only if it is joined to it by gaps of at most
// TOAST_GAP ms, so an unrelated event a second earlier keeps its own toast.
export const TOAST_LIMIT = 3;
export const TOAST_MS = 5000;
const TOAST_BURST = 5;
const TOAST_WINDOW = 2000;
const TOAST_GAP = 500;
const toastable = new Set(["matched", "fulfilled", "cancelled"]);
const tag = (id) => "#" + id.slice(0, 8);

export class Toasts {
  constructor(now = () => Date.now()) {
    this.now = now;
    this.list = [];
    this.times = [];
    this.sequence = 0;
  }
  // The burst an arrival time belongs to: neighbours joined by short gaps.
  cluster(at) {
    const times = this.times;
    let i = times.indexOf(at),
      j = i;
    while (i > 0 && times[i] - times[i - 1] <= TOAST_GAP) i--;
    while (j < times.length - 1 && times[j + 1] - times[j] <= TOAST_GAP) j++;
    return times.slice(i, j + 1);
  }
  add(entries) {
    const now = this.now();
    const fresh = entries.filter(
      (v) => v.kind === "request" && toastable.has(v.action),
    );
    if (!fresh.length) return this.list;
    this.times = [
      ...this.times.filter((t) => now - t <= 10 * TOAST_WINDOW),
      ...fresh.map((v) => v.at),
    ].sort((a, b) => a - b);
    let list = this.list.map((v) => ({ ...v }));
    for (const entry of fresh) {
      const cluster = this.cluster(entry.at);
      const near = cluster.filter(
        (t) => Math.abs(t - entry.at) <= TOAST_WINDOW,
      );
      if (near.length > TOAST_BURST) {
        const group = list.find(
          (v) =>
            v.group && v.action === entry.action && cluster.includes(v.last),
        );
        if (group) {
          group.count++;
          group.last = entry.at;
          group.text = `${group.count} requests ${entry.action}`;
          group.expires = now + TOAST_MS;
          continue;
        }
      }
      list.unshift({
        key: ++this.sequence,
        group: near.length > TOAST_BURST,
        action: entry.action,
        count: 1,
        last: entry.at,
        expires: now + TOAST_MS,
        text:
          `Request ${tag(entry.id)} ${entry.action}` +
          (entry.action === "matched" && entry.resource
            ? ` to ${entry.resource}`
            : ""),
      });
    }
    this.list = list.slice(0, TOAST_LIMIT);
    return this.list;
  }
  prune() {
    const now = this.now();
    const kept = this.list.filter((v) => v.expires > now);
    if (kept.length !== this.list.length) this.list = kept;
    return this.list;
  }
  dismiss(key) {
    this.list = this.list.filter((v) => v.key !== key);
    return this.list;
  }
}
