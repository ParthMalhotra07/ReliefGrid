import React, { useEffect, useRef, useState } from "react";
import L from "leaflet";
import { reciprocal, terminal } from "./state.js";

const tileURL = "https://tile.openstreetmap.org/{z}/{x}/{y}.png";
const attribution =
  '© <a href="https://www.openstreetmap.org/copyright" target="_blank" rel="noopener noreferrer">OpenStreetMap contributors</a>';
const empty = [];
const initialCentre = [20.6, 78.9];
const toPoint = ({ lat, lng }) => [lat, lng];
const boundsOptions = { padding: [32, 32], maxZoom: 13 };

function schematic() {
  const layers = [
    L.imageOverlay(
      "/assets/map-grid.svg",
      [
        [-85, -180],
        [85, 180],
      ],
      {
        opacity: 0.26,
        interactive: false,
      },
    ),
  ];
  const grid = { color: "#aabeb6", weight: 0.5, interactive: false };
  for (const lat of [-60, -30, 0, 30, 60]) {
    layers.push(
      L.polyline(
        [
          [lat, -180],
          [lat, 180],
        ],
        grid,
      ),
    );
  }
  for (const lng of [-120, -60, 0, 60, 120]) {
    layers.push(
      L.polyline(
        [
          [-85, lng],
          [85, lng],
        ],
        grid,
      ),
    );
  }
  return L.layerGroup(layers);
}

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
  const schematicLayer = useRef(null);
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
    m.attributionControl.setPrefix(false);
    schematicLayer.current = schematic().addTo(m);
    layers.current = L.layerGroup().addTo(m);
    return () => {
      m.remove();
      map.current = null;
    };
  }, []);

  useEffect(() => {
    if (!visible) return;
    const frame = requestAnimationFrame(() => map.current?.invalidateSize());
    return () => cancelAnimationFrame(frame);
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
    const grid = schematicLayer.current;
    const fallBack = () => {
      if (!active || failed) return;
      failed = true;
      clearTimeout(timer);
      setTileState("failed");
      // A tile error fires inside Leaflet's tile completion handler.
      queueMicrotask(() => {
        if (active) {
          layer.remove();
          grid.addTo(m);
        }
      });
    };
    let timer = setTimeout(fallBack, 6500);
    layer.on("loading", () => {
      clearTimeout(timer);
      timer = setTimeout(fallBack, 6500);
    });
    layer.on("tileerror", fallBack);
    layer.on("load", () => {
      if (!active || failed) return;
      clearTimeout(timer);
      grid.remove();
      setTileState("ready");
    });
    layer.addTo(m);
    return () => {
      active = false;
      clearTimeout(timer);
      layer.off();
      if (m.hasLayer(layer)) layer.remove();
      if (map.current === m && !m.hasLayer(grid)) grid.addTo(m);
    };
  }, [attempt, online, street, visible]);

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
    ? "Schematic map"
    : !online
      ? "Offline schematic"
      : tileState === "failed"
        ? "Street map unavailable. Schematic shown."
        : tileState === "ready"
          ? "Street map"
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
            {street ? "Use schematic" : "Use street map"}
          </button>
          {street && online && tileState === "failed" && (
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
