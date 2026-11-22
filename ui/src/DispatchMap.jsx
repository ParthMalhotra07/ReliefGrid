import React, { useEffect, useRef, useState } from "react";
import L from "leaflet";
import { reciprocal, terminal } from "./state.js";
import worldLand from "./world-land.json";

const tileURL = "https://tile.openstreetmap.org/{z}/{x}/{y}.png";
const attribution =
  '© <a href="https://www.openstreetmap.org/copyright" target="_blank" rel="noopener noreferrer">OpenStreetMap contributors</a>';
const empty = [];
const initialCentre = [20.6, 78.9];
const toPoint = ({ lat, lng }) => [lat, lng];
const boundsOptions = { padding: [32, 32], maxZoom: 13 };

function mapMarker(point, className, title, select) {
  const marker = L.marker(point, {
    icon: L.divIcon({
      className: "dispatch-pin " + className,
      iconSize: [24, 24],
      iconAnchor: [12, 12],
      tooltipAnchor: [0, -12],
    }),
    title,
    keyboard: !!select,
    interactive: !!select,
  });
  if (select) {
    marker.on("add", () =>
      marker.getElement().setAttribute("aria-label", title),
    );
    marker.on("click", select);
    const tooltip = document.createElement("span");
    tooltip.textContent = title;
    marker.bindTooltip(tooltip, { direction: "top" });
  }
  return marker;
}

export function DispatchMap({
  requests = empty,
  resources = empty,
  selection,
  onSelectRequest,
  onSelectResource,
  focus,
  visible = true,
  online,
  onPick,
  position,
}) {
  const element = useRef(null);
  const map = useRef(null);
  const layers = useRef(null);
  const fitted = useRef(false);
  const [street, setStreet] = useState(true);
  const [attempt, setAttempt] = useState(0);
  const [tileState, setTileState] = useState("loading");
  const picker = !!onPick;

  useEffect(() => {
    const m = L.map(element.current, {
      zoomControl: false,
      minZoom: 2,
      maxZoom: 19,
    }).setView(initialCentre, 4);
    map.current = m;
    L.control.zoom({ position: "topright" }).addTo(m);
    L.control.scale({ position: "bottomleft", imperial: false }).addTo(m);
    m.attributionControl.setPrefix(false);
    m.createPane("world-overview").style.zIndex = "180";
    L.geoJSON(worldLand, {
      pane: "world-overview",
      interactive: false,
      attribution: "Natural Earth",
      style: {
        color: "#8ea8a4",
        weight: 1,
        fillColor: "#e7e7d6",
        fillOpacity: 1,
      },
    }).addTo(m);
    layers.current = L.layerGroup().addTo(m);
    return () => {
      m.remove();
      map.current = null;
    };
  }, []);

  useEffect(() => {
    if (!visible) return;
    const observer = new ResizeObserver(() => map.current?.invalidateSize());
    observer.observe(element.current);
    return () => observer.disconnect();
  }, [visible]);

  useEffect(() => {
    if (!street || !online || !visible) return;
    setTileState("loading");
    const m = map.current;
    const layer = L.tileLayer(tileURL, {
      attribution,
      referrerPolicy: "strict-origin-when-cross-origin",
      minZoom: 0,
      maxNativeZoom: 19,
      noWrap: true,
    });
    let active = true;
    let failed = false;
    let missing = false;
    const settle = () => {
      if (!active || failed) return;
      clearTimeout(timer);
      if (layer.getContainer()?.querySelector(".leaflet-tile-loaded")) {
        setTileState(missing ? "partial" : "ready");
        return;
      }
      failed = true;
      setTileState("failed");
      // Leaflet may still be completing its current load handler.
      queueMicrotask(() => {
        if (active) layer.remove();
      });
    };
    const slow = () => {
      missing = true;
      settle();
    };
    let timer = setTimeout(slow, 6500);
    layer.on("loading", () => {
      missing = false;
      clearTimeout(timer);
      timer = setTimeout(slow, 6500);
    });
    layer.on("tileerror", () => {
      missing = true;
    });
    layer.on("load", settle);
    layer.addTo(m);
    return () => {
      active = false;
      clearTimeout(timer);
      layer.off();
      if (m.hasLayer(layer)) layer.remove();
    };
  }, [attempt, online, street, visible]);

  useEffect(() => {
    if (!street) map.current.setZoom(Math.min(7, map.current.getZoom()));
  }, [street]);

  useEffect(() => {
    if (!onPick) return;
    const m = map.current;
    const select = (event) => onPick(event.latlng.wrap());
    m.on("click", select);
    return () => m.off("click", select);
  }, [onPick]);

  useEffect(() => {
    const group = layers.current;
    group.clearLayers();
    if (position) {
      mapMarker(toPoint(position), "picked-pin", "Selected location").addTo(
        group,
      );
    }
    for (const row of resources) {
      const active =
        selection?.kind === "resource" && selection.id === row.resourceId;
      mapMarker(
        toPoint(row.location),
        "resource-pin" + (active ? " pin-selected" : ""),
        "Resource: " + row.name,
        () => onSelectResource(row),
      ).addTo(group);
    }
    for (const row of requests) {
      const point = toPoint(row.location);
      const active =
        selection?.kind === "request" && selection.id === row.requestId;
      const state = terminal(row.status) ? " pin-terminal" : "";
      const urgency =
        row.urgency === "CRITICAL"
          ? " pin-critical"
          : row.urgency === "HIGH"
            ? " pin-high"
            : "";
      mapMarker(
        point,
        "request-pin" + state + urgency + (active ? " pin-selected" : ""),
        "Request: " + (row.details || row.requestType),
        () => onSelectRequest(row),
      ).addTo(group);
      const resource = resources.find((v) => reciprocal(row, v));
      if (resource) {
        L.polyline([point, toPoint(resource.location)], {
          color: row.status === "CANCELLING" ? "#986521" : "#286386",
          weight: 3,
          dashArray: "5 6",
          interactive: false,
        }).addTo(group);
      }
    }
    if (!fitted.current) {
      if (position) {
        map.current.setView(toPoint(position), 13);
        fitted.current = true;
      } else if (visible && (requests.length || resources.length)) {
        map.current.fitBounds(
          [...requests, ...resources].map((row) => toPoint(row.location)),
          boundsOptions,
        );
        fitted.current = true;
      }
    }
  }, [
    onSelectRequest,
    onSelectResource,
    position,
    requests,
    resources,
    selection,
    visible,
  ]);

  useEffect(() => {
    if (focus) {
      const m = map.current;
      m.setView(toPoint(focus), Math.max(m.getZoom(), 13));
    }
  }, [focus]);

  const background = !street
    ? "World overview · no street detail"
    : !online
      ? "Offline world map · no street detail"
      : tileState === "failed"
        ? "Street map unavailable · world overview shown"
        : tileState === "ready"
          ? "Street map"
          : tileState === "partial"
            ? "Street map · some tiles unavailable"
            : "Loading street map";
  return (
    <>
      <div className="map-tools">
        <span role="status" aria-label="Map background status">
          {background}
        </span>
        <div className="map-tool-buttons">
          <button
            type="button"
            className="map-tool"
            onClick={() => setStreet(!street)}
          >
            {street ? "World overview" : "Use street map"}
          </button>
          {street && online && ["failed", "partial"].includes(tileState) && (
            <button
              type="button"
              className="map-tool"
              onClick={() => setAttempt(attempt + 1)}
            >
              Retry street map
            </button>
          )}
          {picker ? (
            <button
              type="button"
              className="map-tool"
              onClick={() => onPick(map.current.getCenter().wrap())}
            >
              Use map centre
            </button>
          ) : (
            <button
              type="button"
              className="map-tool"
              disabled={!requests.length && !resources.length}
              onClick={() =>
                map.current.fitBounds(
                  [...requests, ...resources].map((row) =>
                    toPoint(row.location),
                  ),
                  boundsOptions,
                )
              }
            >
              Fit to results
            </button>
          )}
        </div>
      </div>
      <div
        ref={element}
        className={picker ? "map picker-map" : "map"}
        aria-label={picker ? "Location map" : "Dispatch map"}
        role="region"
      />
    </>
  );
}
