import React, {
  useCallback,
  useEffect,
  useMemo,
  useRef,
  useState,
} from "react";
import { createRoot } from "react-dom/client";
import "leaflet/dist/leaflet.css";
import "./app.css";
import { CreateDialog } from "./CreateDialog.jsx";
import { DispatchMap } from "./DispatchMap.jsx";
import { coalescedRefresh } from "./refresh.js";
import {
  EventIDs,
  list,
  observe,
  reciprocal,
  REQUEST_STATUSES,
  TYPES,
  terminal,
} from "./state.js";
const CACHE = "reliefgrid.dispatch.snapshot.v1";
const attentionStates = new Set([
  "WAITING",
  "RETRYING",
  "CANCELLING",
  "FULFILLING",
]);
const time = (value) =>
  value
    ? new Date(value).toLocaleTimeString([], {
        hour: "2-digit",
        minute: "2-digit",
        second: "2-digit",
      })
    : "Not yet observed";
const short = (id) => id?.slice(0, 8) || "—";
const coordinates = ({ lat, lng }) => lat.toFixed(5) + ", " + lng.toFixed(5);
const dateTime = (value) => {
  const date = new Date(value);
  return value && Number.isFinite(date.getTime())
    ? date.toLocaleString()
    : "Not reported";
};
const label = (text) =>
  text
    ?.replaceAll("_", " ")
    .toLowerCase()
    .replace(/\b\w/g, (c) => c.toUpperCase()) || "Unknown";
function saved() {
  try {
    const v = JSON.parse(localStorage.getItem(CACHE));
    if (v?.schema !== 1) return null;
    list(v.requests, "request");
    list(v.resources, "resource");
    return v;
  } catch {
    return null;
  }
}
async function json(path, options = {}) {
  const response = await fetch(path, {
    cache: "no-store",
    ...options,
    signal: options.signal || AbortSignal.timeout(6000),
  });
  const data = await response.json();
  if (!response.ok) {
    const error = new Error(data.code || "SERVICE_UNAVAILABLE");
    error.unknown = data.outcomeUnknown !== false;
    throw error;
  }
  return data;
}
function useDispatch() {
  const [snapshot, setSnapshot] = useState(saved),
    [stale, setStale] = useState(true),
    [error, setError] = useState(""),
    [online, setOnline] = useState(navigator.onLine),
    [stream, setStream] = useState("Connecting"),
    [loading, setLoading] = useState(false),
    [offlineReady, setOfflineReady] = useState(false);
  const current = useRef(snapshot),
    alive = useRef(true),
    active = useRef(null);
  const refresh = useMemo(
    () =>
      coalescedRefresh(async () => {
        if (!alive.current || !navigator.onLine) return;
        setLoading(true);
        const controller = new AbortController();
        active.current = controller;
        try {
          const signal = AbortSignal.any([
            controller.signal,
            AbortSignal.timeout(6000),
          ]);
          const [requests, resources] = await Promise.all([
            json("/api/v1/requests?limit=100", { signal }),
            json("/api/v1/resources?limit=100", { signal }),
          ]);
          list(requests, "request");
          list(resources, "resource");
          let queue = { available: false };
          try {
            queue = await json("/ui/status", { signal });
          } catch {
            /* Counts remain unavailable, never fabricated as zero. */
          }
          const next = {
            schema: 1,
            requests,
            resources,
            queue,
            savedAt: new Date().toISOString(),
            history: observe(current.current?.history || {}, requests.items),
          };
          if (!alive.current) return;
          current.current = next;
          setSnapshot(next);
          setStale(false);
          setError("");
          try {
            localStorage.setItem(CACHE, JSON.stringify(next));
          } catch {
            setError(
              "Live data loaded. Offline saving is unavailable on this device.",
            );
          }
          return true;
        } catch (e) {
          if (alive.current) {
            setStale(true);
            setError(
              `Could not confirm current state (${e.message}). ${current.current ? "Showing saved data." : "Try refreshing when the service is available."}`,
            );
          }
          return false;
        } finally {
          if (alive.current) setLoading(false);
        }
      }),
    [],
  );
  useEffect(() => {
    alive.current = true;
    let source;
    const ids = new EventIDs();
    const connect = () => {
      source?.close();
      if (!navigator.onLine) return;
      setStream("Connecting");
      source = new EventSource("/api/v1/events");
      source.onopen = () => {
        setStream("Live");
        refresh();
      };
      source.onerror = () =>
        setStream(
          source.readyState === EventSource.CLOSED
            ? "Updates unavailable"
            : "Reconnecting",
        );
      source.addEventListener("unavailable", () => setStream("Updates paused"));
      source.addEventListener("refresh", () => {
        setStream("Live");
        refresh();
      });
      source.addEventListener("change", (event) => {
        try {
          const v = JSON.parse(event.data);
          if (v.eventId === event.lastEventId && ids.accept(v.eventId))
            refresh();
        } catch {
          refresh();
        }
      });
    };
    const up = () => {
      setOnline(true);
      connect();
      refresh();
    };
    const down = () => {
      setOnline(false);
      setStale(true);
      setStream("Offline");
      source?.close();
    };
    addEventListener("online", up);
    addEventListener("offline", down);
    connect();
    refresh();
    const poll = setInterval(() => refresh(), 15000);
    if ("serviceWorker" in navigator)
      navigator.serviceWorker
        .register("/sw.js")
        .then(() => navigator.serviceWorker.ready)
        .then(() => {
          if (alive.current) setOfflineReady(true);
        })
        .catch(() => {
          if (alive.current) setOfflineReady(false);
        });
    return () => {
      alive.current = false;
      source?.close();
      active.current?.abort();
      clearInterval(poll);
      removeEventListener("online", up);
      removeEventListener("offline", down);
    };
  }, [refresh]);
  return {
    snapshot,
    stale,
    error,
    online,
    stream,
    loading,
    offlineReady,
    refresh,
    setError,
  };
}
function Badge({ status }) {
  return (
    <span className={`badge status-${status}`}>
      {status?.replaceAll("_", " ") || "UNKNOWN"}
    </span>
  );
}
function showDetailsOnMobile(panel) {
  if (matchMedia("(max-width: 720px)").matches) {
    requestAnimationFrame(() => document.getElementById(panel)?.focus());
  }
}
function App() {
  const d = useDispatch(),
    [search, setSearch] = useState(""),
    [type, setType] = useState(""),
    [status, setStatus] = useState(""),
    [urgency, setUrgency] = useState(""),
    [attentionOnly, setAttentionOnly] = useState(false),
    [selected, setSelected] = useState(""),
    [resourceSelection, setResourceSelection] = useState(""),
    [focus, setFocus] = useState(null),
    [view, setView] = useState("map"),
    [busy, setBusy] = useState(false),
    [actionError, setActionError] = useState(""),
    [uncertain, setUncertain] = useState(null),
    [creating, setCreating] = useState(null);
  const actionPending = useRef(false);
  const requests = d.snapshot?.requests.items || [],
    resources = d.snapshot?.resources.items || [];
  const query = search.trim().toLowerCase();
  const visibleRequests = requests.filter(
    (r) =>
      (!type || r.requestType === type) &&
      (!status || r.status === status) &&
      (!urgency || r.urgency === urgency) &&
      (!attentionOnly || attentionStates.has(r.status)) &&
      (!query ||
        `${r.details} ${r.requestId} ${r.requestType}`
          .toLowerCase()
          .includes(query)),
  );
  const visibleResources = resources.filter(
    (r) =>
      (!type || r.resourceType === type) &&
      (!query ||
        `${r.name} ${r.resourceId} ${r.resourceType}`
          .toLowerCase()
          .includes(query)),
  );
  const selectedResource = resources.find(
    (r) => r.resourceId === resourceSelection,
  );
  const chosen = resourceSelection
    ? selectedResource && requests.find((r) => reciprocal(r, selectedResource))
    : requests.find((r) => r.requestId === selected);
  const assigned = chosen && resources.find((r) => reciprocal(chosen, r));
  const mapSelection = useMemo(
    () =>
      resourceSelection
        ? { kind: "resource", id: resourceSelection }
        : { kind: "request", id: selected },
    [resourceSelection, selected],
  );
  const canAct = d.online && !d.stale && !busy;
  const hasFilters = !!(search || type || status || urgency || attentionOnly);
  const clearFilters = () => {
    setSearch("");
    setType("");
    setStatus("");
    setUrgency("");
    setAttentionOnly(false);
  };
  const more = [
    ...(d.snapshot?.requests.positions || []),
    ...(d.snapshot?.resources.positions || []),
  ].some((p) => p.hasMore);
  const action = async (next) => {
    if (!chosen || !canAct || actionPending.current) return;
    if (
      (next === "CANCELLED" || next === "FULFILLED") &&
      !confirm(
        next === "CANCELLED"
          ? "Cancel this request? Cleanup may take a moment."
          : "Mark this request fulfilled?",
      )
    )
      return;
    actionPending.current = true;
    setBusy(true);
    setActionError("");
    setUncertain(null);
    try {
      await json(
        next === "MATCH"
          ? `/api/v1/match/${chosen.requestId}`
          : `/api/v1/requests/${chosen.requestId}/status`,
        {
          method: next === "MATCH" ? "POST" : "PUT",
          headers: { "Content-Type": "application/json" },
          ...(next === "MATCH"
            ? {}
            : { body: JSON.stringify({ status: next }) }),
        },
      );
      await d.refresh();
    } catch (e) {
      const warning =
        e.unknown !== false
          ? "The outcome is not confirmed. Refresh to check the current request before retrying."
          : `Request update was not completed (${e.message}). Refresh and try again.`;
      await d.refresh();
      setActionError(warning);
      if (e.unknown !== false)
        setUncertain({ id: chosen.requestId, version: chosen.version });
    } finally {
      actionPending.current = false;
      setBusy(false);
    }
  };
  useEffect(() => {
    if (uncertain) {
      const current = requests.find((r) => r.requestId === uncertain.id);
      if (current && current.version !== uncertain.version) {
        setUncertain(null);
        setActionError("");
      }
    }
  }, [d.snapshot, uncertain]);
  const pickRequest = useCallback((row) => {
    setSelected(row.requestId);
    setResourceSelection("");
    setFocus({ ...row.location });
  }, []);
  const pickResource = useCallback((row) => {
    setSelected("");
    setResourceSelection(row.resourceId);
    setFocus({ ...row.location });
  }, []);
  return (
    <>
      {creating && (
        <CreateDialog
          kind={creating}
          online={d.online}
          canCreate={d.online && !d.stale}
          refresh={d.refresh}
          onClose={() => setCreating(null)}
        />
      )}
      <header className="topbar">
        <a className="brand" href="/" aria-label="ReliefGrid home">
          <span className="brand-mark">
            R<span>+</span>
          </span>
          <span>
            ReliefGrid<small>DISPATCH WORKSPACE</small>
          </span>
        </a>
        <div className="top-right">
          <span
            className={`connection ${!d.online ? "offline" : d.stream === "Live" ? "live" : "paused"}`}
            role="status"
            aria-label="Connection status"
          >
            <i />
            {!d.online ? "Offline · saved view" : d.stream}
          </span>
          <span className="session-label">Local dispatcher</span>
        </div>
      </header>
      <main className="workspace">
        <div className="page-heading">
          <div>
            <h1>Dispatch overview</h1>
            <p>Find the next request and see which resources can help.</p>
          </div>
          <div
            className="create-toolbar"
            role="group"
            aria-label="Dispatch actions"
          >
            <button
              className="button primary"
              disabled={!canAct}
              onClick={() => setCreating("request")}
            >
              Create request
            </button>
            <button
              className="button"
              disabled={!canAct}
              onClick={() => setCreating("resource")}
            >
              Register resource
            </button>
            <button
              className="button refresh"
              onClick={d.refresh}
              disabled={!d.online || d.loading}
            >
              {d.loading ? "Refreshing…" : "Refresh data"}
            </button>
          </div>
        </div>
        {(d.error || actionError) && (
          <div className="notice" role="alert">
            {d.error || actionError}
          </div>
        )}
        {!d.online && (
          <div className="offline-banner">
            {d.snapshot
              ? "Offline list available. Last saved " +
                time(d.snapshot.savedAt) +
                ". Actions resume after a successful refresh."
              : "Offline. No saved records are available on this device yet."}
          </div>
        )}
        {more && (
          <div className="notice">
            This view contains up to 100 requests and 100 resources. More
            records exist; filters apply to the loaded view.
          </div>
        )}
        <div className="summary">
          <Metric
            title="Open requests"
            value={
              d.snapshot
                ? requests.filter((r) => !terminal(r.status)).length
                : "—"
            }
            note="In the loaded view"
            color="teal"
          />
          <Metric
            title="Active assignments"
            value={
              d.snapshot
                ? requests.filter((r) =>
                    resources.some((v) => reciprocal(r, v)),
                  ).length
                : "—"
            }
            note="Reciprocal claims observed"
            color="blue"
          />
          <Metric
            title="Resources available"
            value={
              d.snapshot
                ? resources.filter((r) => r.status === "AVAILABLE").length
                : "—"
            }
            note="As last reported"
            color="green"
          />
          <Metric
            title="Needs attention"
            value={
              d.snapshot
                ? requests.filter((r) => attentionStates.has(r.status)).length
                : "—"
            }
            note="Waiting, retrying, or finishing cleanup"
            color="amber"
            pressed={attentionOnly}
            onClick={() => setAttentionOnly(!attentionOnly)}
          />
        </div>
        <div className="filters">
          <label className="search">
            <span>⌕</span>
            <input
              aria-label="Search requests or resources"
              placeholder="Search requests or resources"
              value={search}
              onChange={(e) => setSearch(e.target.value)}
            />
          </label>
          <Select
            label="Resource type"
            value={type}
            onChange={setType}
            options={TYPES}
            all="All resource types"
          />
          <Select
            label="Request status"
            value={status}
            onChange={setStatus}
            options={REQUEST_STATUSES}
            all="All statuses"
          />
          <Select
            label="Request urgency"
            value={urgency}
            onChange={setUrgency}
            options={["CRITICAL", "HIGH", "NORMAL"]}
            all="All urgency levels"
          />
          {hasFilters && (
            <button
              type="button"
              className="button clear-filters"
              onClick={clearFilters}
            >
              Clear filters
            </button>
          )}
          <div className="view-switch">
            <button
              aria-pressed={view === "map"}
              onClick={() => setView("map")}
            >
              Map
            </button>
            <button
              aria-pressed={view === "list"}
              onClick={() => setView("list")}
            >
              List
            </button>
          </div>
        </div>
        <div
          className={`dispatch-layout ${view === "list" ? "list-only" : ""}`}
        >
          <div className="records-column">
            <section className="panel requests-panel" aria-label="Requests">
              <div className="panel-heading">
                <h2>
                  Requests{" "}
                  <span>{d.snapshot ? visibleRequests.length : "—"}</span>
                </h2>
                <small>{d.stale ? "SAVED VIEW" : "LATEST READ"}</small>
              </div>
              <div className="request-cards">
                {visibleRequests.map((r) => (
                  <button
                    key={r.requestId}
                    className={`request-card ${chosen?.requestId === r.requestId ? "selected" : ""}`}
                    aria-pressed={chosen?.requestId === r.requestId}
                    onClick={() => {
                      pickRequest(r);
                      showDetailsOnMobile("request-details");
                    }}
                  >
                    <div className="card-top">
                      <span className={`urgency urgency-${r.urgency}`}>
                        {r.urgency}
                      </span>
                      <span className="mono">#{short(r.requestId)}</span>
                    </div>
                    <strong>
                      {r.details || `${label(r.requestType)} request`}
                    </strong>
                    <div className="card-meta">
                      {label(r.requestType)} <span>·</span> {r.peopleAffected}{" "}
                      people
                    </div>
                    <div className="card-bottom">
                      <Badge status={r.status} />
                      <small>{time(r.updatedAt)}</small>
                    </div>
                    {r.lastError && (
                      <span className="retry-reason">{r.lastError}</span>
                    )}
                  </button>
                ))}
                {!visibleRequests.length && (
                  <div className="empty">
                    {!d.snapshot
                      ? !d.online
                        ? "No saved request data on this device."
                        : d.error && !d.loading
                          ? "Request data unavailable. Use Refresh to retry."
                          : "Loading requests…"
                      : "No requests match these filters."}
                  </div>
                )}
              </div>
            </section>
            <section className="panel resources-panel" aria-label="Resources">
              <div className="panel-heading">
                <h2>
                  Resources{" "}
                  <span>{d.snapshot ? visibleResources.length : "—"}</span>
                </h2>
                <small>{d.stale ? "SAVED VIEW" : "LATEST READ"}</small>
              </div>
              <div className="resource-cards">
                {visibleResources.map((r) => (
                  <button
                    className="resource-card"
                    key={r.resourceId}
                    aria-pressed={resourceSelection === r.resourceId}
                    onClick={() => {
                      pickResource(r);
                      showDetailsOnMobile("resource-details");
                    }}
                  >
                    <div className="resource-icon">
                      {r.resourceType === "WATER"
                        ? "◈"
                        : r.resourceType === "FOOD"
                          ? "✦"
                          : r.resourceType === "MEDICINE"
                            ? "+"
                            : "◇"}
                    </div>
                    <div>
                      <strong>{r.name}</strong>
                      <small>
                        {label(r.resourceType)} ·{" "}
                        {r.activeClaim
                          ? "Assigned #" + short(r.activeClaim.requestId)
                          : "No active claim reported"}
                      </small>
                      <Badge status={r.status} />
                    </div>
                  </button>
                ))}
                {!visibleResources.length && (
                  <div className="empty">
                    {d.snapshot
                      ? "No resources match."
                      : !d.online
                        ? "No saved resource data on this device."
                        : d.error && !d.loading
                          ? "Resource data unavailable. Use Refresh to retry."
                          : "Loading resources…"}
                  </div>
                )}
              </div>
            </section>
          </div>
          <section className="panel map-panel" aria-label="Area overview">
            <div className="panel-heading">
              <h2>Area overview</h2>
            </div>
            <DispatchMap
              requests={visibleRequests}
              resources={visibleResources}
              selection={mapSelection}
              onSelectRequest={pickRequest}
              onSelectResource={pickResource}
              focus={focus}
              visible={view === "map"}
              ready={!!(d.snapshot || d.error)}
              online={d.online}
            />
            <div className="map-legend">
              <span>
                <i className="dot critical" /> Critical request
              </span>
              <span>
                <i className="dot normal" /> Request
              </span>
              <span>
                <i className="resource-dot" /> Resource
              </span>
              <span>
                <i className="assignment-line" /> Assignment
              </span>
            </div>
          </section>
          <aside className="inspector-column" aria-label="Selected records">
            {selectedResource && (
              <section
                className="panel resource-details"
                aria-label="Resource details"
                id="resource-details"
                tabIndex={-1}
              >
                <div className="panel-heading">
                  <h2>{selectedResource.name}</h2>
                  <Badge status={selectedResource.status} />
                </div>
                <dl className="resource-facts">
                  <div>
                    <dt>Coordinates</dt>
                    <dd>{coordinates(selectedResource.location)}</dd>
                  </div>
                  <div>
                    <dt>Available from</dt>
                    <dd>{dateTime(selectedResource.windowStart)}</dd>
                  </div>
                  <div>
                    <dt>Available until</dt>
                    <dd>{dateTime(selectedResource.windowEnd)}</dd>
                  </div>
                  <div>
                    <dt>Request</dt>
                    <dd>
                      {chosen ? (
                        <button
                          type="button"
                          className="detail-link"
                          onClick={() => pickRequest(chosen)}
                        >
                          #{short(chosen.requestId)}
                        </button>
                      ) : selectedResource.activeClaim ? (
                        "Assignment unconfirmed in loaded view"
                      ) : (
                        "Unassigned"
                      )}
                    </dd>
                  </div>
                </dl>
              </section>
            )}
            <section
              className="panel timeline"
              aria-label="Request details"
              id="request-details"
              tabIndex={-1}
            >
              <div className="panel-heading">
                <h2>Request timeline</h2>
                {chosen && (
                  <span className="mono">#{short(chosen.requestId)}</span>
                )}
              </div>
              {chosen ? (
                <>
                  <div className="detail-heading">
                    <div>
                      <h3>
                        {chosen.details ||
                          `${label(chosen.requestType)} request`}
                      </h3>
                      <p>
                        {label(chosen.requestType)} · {chosen.peopleAffected}{" "}
                        people · {chosen.location.lat.toFixed(3)},{" "}
                        {chosen.location.lng.toFixed(3)}
                      </p>
                    </div>
                    <Badge status={chosen.status} />
                  </div>
                  {assigned && (
                    <div className="assignment-detail">
                      <span>Assigned resource</span>
                      <button
                        type="button"
                        className="detail-link"
                        onClick={() => pickResource(assigned)}
                      >
                        {assigned.name}
                      </button>
                    </div>
                  )}
                  <ol className="timeline-items">
                    <li>
                      <i />
                      <div>
                        <strong>Request created</strong>
                        <small>{time(chosen.createdAt)}</small>
                      </div>
                    </li>
                    {(d.snapshot.history?.[chosen.requestId] || []).map(
                      (v, i) => (
                        <li key={v.version + "-" + i}>
                          <i />
                          <div>
                            <strong>{label(v.status)}</strong>
                            <small>
                              {time(v.at)} · observed {v.version}
                            </small>
                          </div>
                        </li>
                      ),
                    )}
                  </ol>
                  <p className="timeline-note">
                    Observed changes only. Reconnecting refreshes the current
                    state.
                  </p>
                  <div className="actions">
                    {["PENDING", "WAITING"].includes(chosen.status) && (
                      <button
                        className="button primary"
                        disabled={!canAct}
                        onClick={() => action("MATCH")}
                      >
                        Match request
                      </button>
                    )}
                    {chosen.status === "MATCHED" && (
                      <button
                        className="button primary"
                        disabled={!canAct}
                        onClick={() => action("IN_PROGRESS")}
                      >
                        Start work
                      </button>
                    )}
                    {chosen.status === "IN_PROGRESS" && (
                      <button
                        className="button primary"
                        disabled={!canAct}
                        onClick={() => action("FULFILLED")}
                      >
                        Mark fulfilled
                      </button>
                    )}
                    {!terminal(chosen.status) &&
                      !["CANCELLING", "FULFILLING"].includes(chosen.status) && (
                        <button
                          className="button danger"
                          disabled={!canAct}
                          onClick={() => action("CANCELLED")}
                        >
                          Cancel request
                        </button>
                      )}
                    {["CANCELLING", "FULFILLING"].includes(chosen.status) && (
                      <p className="cleanup-note">
                        Cleanup is in progress. This status stays until the
                        server confirms completion.
                      </p>
                    )}
                  </div>
                </>
              ) : (
                <div className="empty detail-empty">
                  <span>↗</span>
                  <strong>Select a request</strong>
                  <p>Inspect its progress and available dispatch actions.</p>
                </div>
              )}
            </section>
          </aside>
        </div>
        <section className="operations-status" aria-label="System status">
          <section className="panel health-panel" aria-label="Shard reads">
            <div className="panel-heading">
              <h2>Shard reads</h2>
              <span className={`small-dot ${!d.stale ? "good" : ""}`} />
            </div>
            <p>Authoritative record reads</p>
            {[0, 1, 2].map((shard) => (
              <div className="shard-row" key={shard}>
                <span>
                  <i className={!d.stale ? "good" : ""} />
                  Shard {shard}
                </span>
                <strong>
                  {!d.snapshot
                    ? "Unknown"
                    : d.stale
                      ? "Unconfirmed"
                      : "Available"}
                </strong>
              </div>
            ))}
            <small>
              {d.stale
                ? "Current availability is not confirmed."
                : "Each shard answered the latest authoritative reads."}{" "}
              Pod readiness is not reported here.
            </small>
          </section>
          <section className="panel queue-panel" aria-label="Matching queue">
            <div className="panel-heading">
              <h2>Matching queue</h2>
              <span className="quiet-tag">BROKER</span>
            </div>
            <div className="queue-values">
              <div>
                <strong>
                  {d.snapshot?.queue.available ? d.snapshot.queue.ready : "—"}
                </strong>
                <span>Ready</span>
              </div>
              <div>
                <strong>
                  {d.snapshot?.queue.available
                    ? d.snapshot.queue.unacknowledged
                    : "—"}
                </strong>
                <span>In flight</span>
              </div>
              <div
                className={
                  d.snapshot?.queue.available && d.snapshot.queue.dead > 0
                    ? "queue-problem"
                    : ""
                }
              >
                <strong>
                  {d.snapshot?.queue.available ? d.snapshot.queue.dead : "—"}
                </strong>
                <span>Dead letter</span>
              </div>
            </div>
            <small>
              {d.snapshot?.queue.available
                ? `${d.stale ? "Saved observation" : "Measured"} ${time(d.snapshot.queue.sampledAt)}`
                : "Queue telemetry unavailable. Counts are unknown."}
            </small>
          </section>
        </section>
        <footer>
          <span>
            <i className="small-dot good" />{" "}
            {d.offlineReady && d.snapshot
              ? "Offline list and app shell ready"
              : d.offlineReady
                ? "App shell ready · no saved records"
                : "Preparing offline list"}{" "}
            ·{" "}
            {d.snapshot
              ? "Saved " + time(d.snapshot.savedAt)
              : "Waiting for first successful read"}
          </span>
          <span>Refresh on reconnect · Best-effort live updates</span>
        </footer>
      </main>
    </>
  );
}
function Metric({ title, value, note, color, pressed, onClick }) {
  const Container = onClick ? "button" : "section";
  return (
    <Container
      className={`metric metric-${color}`}
      type={onClick ? "button" : undefined}
      aria-pressed={pressed}
      onClick={onClick}
    >
      <span className="metric-content">
        <span className="metric-title">{title}</span>
        <strong>{value}</strong>
        <small>{note}</small>
      </span>
      {onClick && (
        <span className="metric-action" aria-hidden="true">
          {pressed ? "✓" : "→"}
        </span>
      )}
    </Container>
  );
}
function Select({ label: caption, value, onChange, options, all }) {
  return (
    <select
      aria-label={caption}
      value={value}
      onChange={(e) => onChange(e.target.value)}
    >
      <option value="">{all}</option>
      {options.map((v) => (
        <option key={v} value={v}>
          {label(v)}
        </option>
      ))}
    </select>
  );
}
createRoot(document.getElementById("root")).render(<App />);
