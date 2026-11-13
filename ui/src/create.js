import { TYPES, REQUEST_STATUSES, uuid as validUUID } from "./state.js";

const version = (v) => typeof v === "string" && /^v[1-9][0-9]{0,127}$/.test(v);
const require = (ok, message) => {
  if (!ok) throw new Error(message);
};
const collection = (kind) => (kind === "request" ? "requests" : "resources");

export function payload(kind, input) {
  require(input &&
    TYPES.includes(input[kind + "Type"]), "Choose a resource type.");
  const location = input.location;
  require(location &&
    Number.isFinite(location.lat) &&
    Math.abs(location.lat) <= 90 &&
    Number.isFinite(location.lng) &&
    Math.abs(location.lng) <= 180, "Enter valid latitude and longitude.");
  const point = { lat: location.lat, lng: location.lng };
  if (kind === "request") {
    require(typeof input.details === "string" &&
      input.details.length <= 4096, "Details must be at most 4096 characters.");
    require(["NORMAL", "HIGH", "CRITICAL"].includes(
      input.urgency,
    ), "Choose an urgency.");
    require(Number.isInteger(input.peopleAffected) &&
      input.peopleAffected >= 1 &&
      input.peopleAffected <=
        2147483647, "People affected must be a positive whole number.");
    return {
      requestType: input.requestType,
      details: input.details,
      urgency: input.urgency,
      peopleAffected: input.peopleAffected,
      location: point,
    };
  }
  require(kind === "resource" &&
    typeof input.name === "string" &&
    input.name.trim() &&
    input.name.length <=
      255, "Enter a resource name of at most 255 characters.");
  require(typeof input.reusable ===
    "boolean", "Choose whether the resource is reusable.");
  const utc = (s) =>
    typeof s === "string" && /Z$/.test(s) && Number.isFinite(Date.parse(s));
  require(utc(input.windowStart) &&
    utc(input.windowEnd) &&
    Date.parse(input.windowEnd) >
      Date.parse(input.windowStart), "Availability must end after it starts.");
  return {
    resourceType: input.resourceType,
    name: input.name,
    location: point,
    reusable: input.reusable,
    windowStart: input.windowStart,
    windowEnd: input.windowEnd,
  };
}

function record(kind, row, expected, minimum = "v1") {
  require(row &&
    validUUID.test(row[kind + "Id"]) &&
    (!expected || row[kind + "Id"] === expected) &&
    version(row.version) &&
    BigInt(row.version.slice(1)) >= BigInt(minimum.slice(1)) &&
    (kind === "request"
      ? REQUEST_STATUSES
      : ["AVAILABLE", "BUSY", "OFFLINE"]
    ).includes(row.status), "Could not confirm the current saved record.");
  return row;
}

// One explicit creation intent per kind and browser tab. No background/offline
// writes. The exact body and UUID survive reloads until a successful current
// read and view refresh. An unresolved outcome cannot authorize a new key.
export class CreateSubmission {
  constructor(
    kind,
    {
      storage = globalThis.sessionStorage,
      request = (path, options) => globalThis.fetch(path, options),
      uuid = () => globalThis.crypto.randomUUID(),
    } = {},
  ) {
    require(["request", "resource"].includes(kind), "Invalid creation kind.");
    this.kind = kind;
    this.storage = storage;
    this.request = request;
    this.uuid = uuid;
    this.slot = "reliefgrid.create." + kind + ".v1";
    this.entry = null;
    this.active = null;
    try {
      const saved = storage.getItem(this.slot);
      if (saved !== null) {
        const entry = JSON.parse(saved);
        require(entry.schema === 1 &&
          entry.kind === kind &&
          validUUID.test(entry.key) &&
          typeof entry.uncertain === "boolean" &&
          typeof entry.body === "string" &&
          JSON.stringify(payload(kind, JSON.parse(entry.body))) ===
            entry.body &&
          (!entry.ack ||
            (validUUID.test(entry.ack.id) &&
              version(entry.ack.version))), "invalid saved intent");
        this.entry = entry;
      }
    } catch {
      throw new Error(
        "Could not read the saved submission. Keep this tab and recover it before creating another.",
      );
    }
  }
  pending() {
    return this.entry ? structuredClone(this.entry) : null;
  }
  save(next) {
    try {
      this.storage.setItem(this.slot, JSON.stringify(next));
    } catch {
      throw new Error(
        "Could not save the submission safely in this browser. No new submission can start.",
      );
    }
    this.entry = next;
  }
  prepare(input) {
    if (this.entry) {
      require(input === undefined ||
        JSON.stringify(payload(this.kind, input)) ===
          this.entry
            .body, "Finish the saved submission before changing its details.");
      return;
    }
    const body = JSON.stringify(payload(this.kind, input));
    const key = this.uuid();
    require(validUUID.test(key), "Secure submission identity is unavailable.");
    this.save({
      schema: 1,
      kind: this.kind,
      key,
      body,
      uncertain: false,
      ack: null,
    });
  }
  editRejected() {
    require(!this.active &&
      this.entry?.rejected === true &&
      !this.entry.uncertain &&
      !this.entry
        .ack, "The saved submission is unresolved; retry it without changing details.");
    this.clear();
  }
  clear() {
    try {
      this.storage.removeItem(this.slot);
    } catch {
      throw new Error(
        "Could not clear the saved submission safely. Retry its confirmation.",
      );
    }
    this.entry = null;
  }
  async read(path, signal) {
    const response = await this.request(path, {
      method: "GET",
      cache: "no-store",
      redirect: "error",
      signal,
    });
    require(response.ok, "Could not confirm the current saved record.");
    return response.json();
  }
  async confirm(signal) {
    const { id, version: minimum } = this.entry.ack;
    if (this.kind === "request")
      return record(
        this.kind,
        await this.read("/api/v1/requests/" + id, signal),
        id,
        minimum,
      );
    // There is no public GET-resource-by-ID. Walk the real public cursor rather
    // than treating absence from the first loaded page as a failed create.
    let cursor = "";
    const seen = new Set();
    for (let n = 0; n < 32; n++) {
      const p = await this.read(
        "/api/v1/resources?limit=100" +
          (cursor ? "&cursor=" + encodeURIComponent(cursor) : ""),
        signal,
      );
      require(p &&
        ["strong", "linearizable"].includes(p.consistency) &&
        p.partial === false &&
        Array.isArray(p.failedShards) &&
        p.failedShards.length === 0 &&
        Array.isArray(p.items) &&
        Array.isArray(p.positions) &&
        p.positions.length > 0 &&
        (n > 0 ||
          p.positions.length ===
            3), "Could not confirm a complete resource read.");
      const shards = new Set();
      for (const position of p.positions) {
        require(Number.isInteger(position.shard) &&
          position.shard >= 0 &&
          position.shard <= 2 &&
          !shards.has(position.shard) &&
          Number.isSafeInteger(position.appliedIndex) &&
          position.appliedIndex >=
            0, "Could not confirm the resource shard observations.");
        shards.add(position.shard);
      }
      const found = p.items.find((item) => item.resourceId === id);
      if (found) return record(this.kind, found, id, minimum);
      require(typeof p.nextCursor === "string" &&
        p.nextCursor.length > 0 &&
        p.nextCursor.length <= 16384 &&
        !seen.has(
          p.nextCursor,
        ), "Could not confirm the saved resource in this read.");
      cursor = p.nextCursor;
      seen.add(cursor);
    }
    throw new Error(
      "Could not confirm the saved resource within the page limit.",
    );
  }
  submit(input, refresh) {
    if (this.active) return this.active;
    this.active = this.run(input, refresh).finally(() => {
      this.active = null;
    });
    return this.active;
  }
  async run(input, refresh) {
    this.prepare(input);
    const signal = AbortSignal.timeout(15000);
    if (!this.entry.ack) {
      const wasUnknown = this.entry.uncertain;
      this.save({ ...this.entry, uncertain: true, rejected: false });
      const response = await this.request("/api/v1/" + collection(this.kind), {
        method: "POST",
        cache: "no-store",
        redirect: "error",
        signal: AbortSignal.any([signal, AbortSignal.timeout(6000)]),
        headers: {
          "Content-Type": "application/json",
          "Idempotency-Key": this.entry.key,
        },
        body: this.entry.body,
      });
      const data = await response.json();
      if (response.status !== 201) {
        if (
          !wasUnknown &&
          response.status === 400 &&
          data.code === "INVALID" &&
          data.outcomeUnknown === false
        )
          this.save({ ...this.entry, uncertain: false, rejected: true });
        throw new Error(
          this.entry.rejected
            ? "The service rejected these details. Edit them before trying again."
            : "Creation is not confirmed. Retry the saved submission without changing its details.",
        );
      }
      record(this.kind, data);
      this.save({
        ...this.entry,
        uncertain: false,
        ack: { id: data[this.kind + "Id"], version: data.version },
      });
    }
    const current = await this.confirm(signal);
    require((await refresh()) ===
      true, "The record was saved, but the view refresh is not confirmed. Retry confirmation.");
    this.clear();
    return current;
  }
}
