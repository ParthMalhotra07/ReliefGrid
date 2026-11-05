import http from "node:http";
const requestId = "11111111-1111-4111-8111-111111111111",
  resourceId = "22222222-2222-4222-8222-222222222222";
const clients = new Set();
let reads = 0,
  status = "CANCELLING",
  version = 4,
  fail = false,
  unknownWrite = false;
const claimed = () =>
  ["MATCHED", "IN_PROGRESS", "CANCELLING", "FULFILLING"].includes(status);
const record = () => ({
  requestId,
  requestType: "WATER",
  status,
  urgency: "CRITICAL",
  peopleAffected: 6,
  details: "Water needed at the school",
  location: { lat: 19.08, lng: 72.88 },
  version: "v" + version,
  createdAt: "2026-11-05T06:00:00Z",
  updatedAt: "2026-11-05T06:01:00Z",
  assignedResourceId: claimed() ? resourceId : null,
  retryCount: 0,
  lastError: "",
  nextAttemptAt: null,
});
const positions = [0, 1, 2].map((shard) => ({
  shard,
  appliedIndex: 20 + shard,
  hasMore: false,
}));
const send = (res, status, body) => {
  res.writeHead(status, { "Content-Type": "application/json" });
  res.end(JSON.stringify(body));
};
const body = async (req) => {
  let b = "";
  for await (const chunk of req) b += chunk;
  return b ? JSON.parse(b) : {};
};
http
  .createServer(async (req, res) => {
    const p = new URL(req.url, "http://localhost").pathname;
    if (p === "/__test/ready") return send(res, 200, { ok: true });
    if (p === "/__test/reset") {
      status = "CANCELLING";
      version = 4;
      fail = false;
      unknownWrite = false;
      reads = 0;
      for (const c of clients) c.end();
      clients.clear();
      return send(res, 200, { ok: true });
    }
    if (p === "/__test/count") return send(res, 200, { reads });
    if (p === "/__test/state") {
      const b = await body(req);
      if (b.status) {
        status = b.status;
        version++;
      }
      if ("fail" in b) fail = b.fail;
      if ("unknownWrite" in b) unknownWrite = b.unknownWrite;
      if (b.disconnect) for (const c of clients) c.end();
      if (b.eventId)
        for (const c of clients)
          c.write(
            `id: ${b.eventId}\nevent: change\ndata: ${JSON.stringify({ entityId: requestId, entityVersion: 5, eventType: "request.state_changed", ...b.hint, eventId: b.eventId })}\n\n`,
          );
      return send(res, 200, { ok: true });
    }
    if (p === "/api/v1/events") {
      res.writeHead(200, {
        "Content-Type": "text/event-stream",
        "Cache-Control": "no-cache",
      });
      res.write(": heartbeat\n\nretry: 100\n\n");
      clients.add(res);
      res.on("close", () => clients.delete(res));
      return;
    }
    if (p === `/api/v1/requests/${requestId}/status` && req.method === "PUT") {
      const input = await body(req);
      if (unknownWrite)
        return send(res, 503, {
          code: "OUTCOME_UNKNOWN",
          message: "Outcome unconfirmed",
          retryable: true,
          outcomeUnknown: true,
        });
      status =
        input.status === "CANCELLED"
          ? "CANCELLING"
          : input.status === "FULFILLED"
            ? "FULFILLING"
            : input.status;
      version++;
      return send(res, 200, record());
    }
    if (p === "/api/v1/requests") {
      reads++;
      if (fail)
        return send(res, 503, {
          code: "STORAGE_UNAVAILABLE",
          retryable: true,
          outcomeUnknown: false,
          message: "Shard read unavailable",
        });
      return send(res, 200, {
        items: [record()],
        positions,
        partial: false,
        failedShards: [],
        consistency: "strong",
      });
    }
    if (p === "/api/v1/resources")
      return send(res, 200, {
        items: [
          {
            resourceId,
            resourceType: "WATER",
            name: "Water team Alpha",
            status: claimed() ? "BUSY" : "AVAILABLE",
            location: { lat: 19.1, lng: 72.9 },
            version: "v3",
            activeClaim: !claimed()
              ? null
              : {
                  requestId,
                  attemptId: "33333333-3333-4333-8333-333333333333",
                },
            reusable: true,
            windowStart: "2026-11-05T00:00:00Z",
            windowEnd: "2026-11-06T00:00:00Z",
            createdAt: "2026-11-05T00:00:00Z",
            updatedAt: "2026-11-05T06:01:00Z",
          },
        ],
        positions,
        partial: false,
        failedShards: [],
        consistency: "strong",
      });
    if (p === "/ui/status")
      return send(res, 200, {
        available: true,
        sampledAt: new Date().toISOString(),
        ready: 3,
        unacknowledged: 1,
        dead: 0,
      });
    const up = http.request(
      {
        hostname: "127.0.0.1",
        port: 3411,
        path: req.url,
        method: req.method,
        headers: req.headers,
      },
      (reply) => {
        res.writeHead(reply.statusCode, reply.headers);
        reply.pipe(res);
      },
    );
    up.on("error", () => send(res, 502, { error: "UI server unavailable" }));
    req.pipe(up);
  })
  .listen(3412, "127.0.0.1");
