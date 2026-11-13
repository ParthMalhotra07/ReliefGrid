import React, { useEffect, useRef, useState } from "react";
import { CreateSubmission } from "./create.js";
import { TYPES } from "./state.js";

const localTime = (date) =>
  new Date(date.getTime() - date.getTimezoneOffset() * 60000)
    .toISOString()
    .slice(0, 16);
const label = (value) => value.charAt(0) + value.slice(1).toLowerCase();
function initial(kind, pending) {
  const body = pending ? JSON.parse(pending.body) : {};
  return {
    category: body[kind + "Type"] || "WATER",
    details: body.details || "",
    name: body.name || "",
    urgency: body.urgency || "NORMAL",
    people: body.peopleAffected ?? 1,
    lat: body.location?.lat ?? "",
    lng: body.location?.lng ?? "",
    reusable: body.reusable ?? true,
    start: localTime(
      body.windowStart ? new Date(body.windowStart) : new Date(),
    ),
    end: localTime(
      body.windowEnd
        ? new Date(body.windowEnd)
        : new Date(Date.now() + 3600000),
    ),
  };
}
export function CreateDialog({ kind, online, canCreate, refresh, onClose }) {
  const [setup] = useState(() => {
    try {
      return { client: new CreateSubmission(kind) };
    } catch (error) {
      return { error: error.message };
    }
  });
  const client = setup.client;
  const [pending, setPending] = useState(() => client?.pending()),
    [fields, setFields] = useState(() => initial(kind, client?.pending())),
    [busy, setBusy] = useState(false),
    [error, setError] = useState(setup.error || ""),
    [result, setResult] = useState(null);
  const dialog = useRef(null);
  useEffect(() => {
    const element = dialog.current;
    element.showModal();
    return () => element.close();
  }, []);
  const update = (key, value) => setFields((old) => ({ ...old, [key]: value }));
  const close = () => {
    if (!client?.active) onClose();
  };
  const submit = async (event) => {
    event.preventDefault();
    if (
      !client ||
      client.active ||
      !online ||
      (!client.pending() && !canCreate)
    )
      return;
    setError("");
    setBusy(true);
    try {
      const location = {
        lat: fields.lat === "" ? NaN : Number(fields.lat),
        lng: fields.lng === "" ? NaN : Number(fields.lng),
      };
      const input = client.pending()
        ? undefined
        : kind === "request"
          ? {
              requestType: fields.category,
              details: fields.details,
              urgency: fields.urgency,
              peopleAffected: Number(fields.people),
              location,
            }
          : {
              resourceType: fields.category,
              name: fields.name,
              reusable: fields.reusable,
              location,
              windowStart: new Date(fields.start).toISOString(),
              windowEnd: new Date(fields.end).toISOString(),
            };
      setResult(await client.submit(input, refresh));
    } catch (e) {
      setError(
        client.pending()?.uncertain
          ? "Creation is not confirmed. Retry this saved submission without changing its details."
          : e.message,
      );
    } finally {
      setPending(client.pending());
      setBusy(false);
    }
  };
  const edit = () => {
    try {
      client.editRejected();
      setPending(null);
      setError("");
    } catch (e) {
      setError(e.message);
    }
  };
  const disabled = busy || !!pending || !client;
  const noun = kind === "request" ? "request" : "resource";
  return (
    <dialog
      ref={dialog}
      className="create-dialog"
      aria-labelledby="create-title"
      onCancel={(event) => {
        event.preventDefault();
        close();
      }}
    >
      <div className="create-heading">
        <div>
          <h2 id="create-title">
            {kind === "request" ? "Create request" : "Register resource"}
          </h2>
          <p>
            Matching runs automatically when requests and eligible resources are
            available.
          </p>
        </div>
        <button
          type="button"
          className="button"
          aria-label="Close creation form"
          disabled={busy}
          onClick={close}
        >
          Close
        </button>
      </div>
      {error && (
        <p className="notice" role="alert">
          {error}
        </p>
      )}
      {!online && (
        <p className="notice">Offline. Submissions are not sent or queued.</p>
      )}
      {result ? (
        <>
          <p role="status" aria-label="Creation result">
            {label(noun)} saved. Current status: {result.status}.<br />
            <span className="created-id">{result[noun + "Id"]}</span>
          </p>
          <div className="create-actions">
            <button className="button primary" onClick={close}>
              Done
            </button>
            <button
              className="button"
              disabled={!canCreate}
              onClick={() => {
                setResult(null);
                setFields(initial(kind, null));
              }}
            >
              Create another
            </button>
          </div>
        </>
      ) : (
        <form
          onSubmit={submit}
          aria-label={kind === "request" ? "New request" : "New resource"}
        >
          {pending && (
            <p className="creation-saved">
              {pending.ack
                ? "The create was acknowledged. Confirming its current record and refreshing the view does not send another create."
                : "These saved details stay locked until the outcome is confirmed. Retry the same submission to avoid duplicates, including after reloading this tab."}
            </p>
          )}
          <fieldset disabled={disabled} className="create-fields">
            <label>
              {kind === "request" ? "Request type" : "Resource category"}
              <select
                value={fields.category}
                onChange={(e) => update("category", e.target.value)}
              >
                {TYPES.map((type) => (
                  <option key={type} value={type}>
                    {label(type)}
                  </option>
                ))}
              </select>
            </label>
            {kind === "request" ? (
              <>
                <label>
                  Urgency
                  <select
                    value={fields.urgency}
                    onChange={(e) => update("urgency", e.target.value)}
                  >
                    {["NORMAL", "HIGH", "CRITICAL"].map((value) => (
                      <option key={value} value={value}>
                        {label(value)}
                      </option>
                    ))}
                  </select>
                </label>
                <label>
                  People affected
                  <input
                    type="number"
                    required
                    min="1"
                    max="2147483647"
                    step="1"
                    value={fields.people}
                    onChange={(e) => update("people", e.target.value)}
                  />
                </label>
                <label className="wide">
                  Request details
                  <textarea
                    required
                    maxLength="4096"
                    rows="3"
                    value={fields.details}
                    onChange={(e) => update("details", e.target.value)}
                  />
                </label>
              </>
            ) : (
              <label>
                Resource name
                <input
                  required
                  maxLength="255"
                  value={fields.name}
                  onChange={(e) => update("name", e.target.value)}
                />
              </label>
            )}
            <label>
              Latitude
              <input
                type="number"
                required
                min="-90"
                max="90"
                step="any"
                placeholder="e.g. 19.08"
                value={fields.lat}
                onChange={(e) => update("lat", e.target.value)}
              />
            </label>
            <label>
              Longitude
              <input
                type="number"
                required
                min="-180"
                max="180"
                step="any"
                placeholder="e.g. 72.88"
                value={fields.lng}
                onChange={(e) => update("lng", e.target.value)}
              />
            </label>
            {kind === "resource" && (
              <>
                <label>
                  Available from (local time)
                  <input
                    type="datetime-local"
                    required
                    value={fields.start}
                    onChange={(e) => update("start", e.target.value)}
                  />
                </label>
                <label>
                  Available until (local time)
                  <input
                    type="datetime-local"
                    required
                    value={fields.end}
                    onChange={(e) => update("end", e.target.value)}
                  />
                </label>
                <label className="checkbox wide">
                  <input
                    type="checkbox"
                    checked={fields.reusable}
                    onChange={(e) => update("reusable", e.target.checked)}
                  />
                  Reusable after fulfilment
                </label>
              </>
            )}
          </fieldset>
          <div className="create-actions">
            <button
              className="button primary"
              type="submit"
              disabled={!client || busy || !online || (!pending && !canCreate)}
            >
              {busy
                ? "Confirming…"
                : pending?.ack
                  ? "Confirm saved " + noun
                  : pending
                    ? "Retry same " + noun
                    : kind === "request"
                      ? "Submit request"
                      : "Register resource"}
            </button>
            {pending?.rejected && !pending.uncertain && (
              <button
                className="button"
                type="button"
                disabled={busy}
                onClick={edit}
              >
                Edit details
              </button>
            )}
          </div>
        </form>
      )}
    </dialog>
  );
}
