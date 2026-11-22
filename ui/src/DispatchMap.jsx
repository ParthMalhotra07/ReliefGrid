import React, { useEffect, useRef, useState } from "react";
import L from "leaflet";
import { RADIUS_KM, reciprocal, terminal } from "./state.js";
import worldLand from "./world-land.json";

const tileURL = "https://tile.openstreetmap.org/{z}/{x}/{y}.png";
const attribution =
  '© <a href="https://www.openstreetmap.org/copyright" target="_blank" rel="noopener noreferrer">OpenStreetMap contributors</a>';
const empty = [];
const noReasons = new Map();
// Kilometres per degree of latitude on the backend's mean Earth radius.
const kmPerDegree = (6371.0088 * Math.PI) / 180;
const initialCentre = [20.6, 78.9];
const initialZoom = 4;
// At or below this zoom the whole globe or a continent fills the map.
const worldZoom = 3;
const worldPadding = { padding: [12, 12], animate: false };
// Web Mercator's square world. With these bounds Leaflet never asks the tile
// host for columns or rows outside 0..2^z-1, which it answers with 400.
const tileBounds = L.latLngBounds([-85.0511, -180], [85.0511, 180]);
const inWorld = ({ x, y, z }) => x >= 0 && y >= 0 && x < 2 ** z && y < 2 ** z;
const toPoint = ({ lat, lng }) => [lat, lng];
const boundsOptions = { padding: [32, 32], maxZoom: 13 };
const continents = [
  [43, -106, "North America"],
  [-16, -59, "South America"],
  [53, 19, "Europe"],
  [7, 18, "Africa"],
  [44, 98, "Asia"],
  [-26, 136, "Oceania"],
  [-76, 0, "Antarctica"],
];

const words = (text) =>
  text
    ?.replaceAll("_", " ")
    .toLowerCase()
    .replace(/\b\w/g, (c) => c.toUpperCase()) || "Unknown";
const pinIcon = (className, html) =>
  L.divIcon({
    className: "dispatch-pin " + className,
    html: html || "",
    iconSize: [24, 24],
    iconAnchor: [12, 12],
    tooltipAnchor: [0, -12],
  });
const tooltipFor = (title) => {
  const tooltip = document.createElement("span");
  tooltip.textContent = title;
  return tooltip;
};

// One persistent marker per key. Live refreshes update it in place so a
// focused pin keeps its DOM element, and therefore keyboard focus.
function mapMarker(entry) {
  const marker = L.marker(entry.point, {
    icon: pinIcon(entry.className, entry.html),
    title: entry.title,
    keyboard: !!entry.select,
    interactive: !!entry.select,
  });
  marker.entry = entry;
  if (entry.select) {
    const choose = () => marker.entry.select(marker);
    marker.on("add", () =>
      marker.getElement().setAttribute("aria-label", marker.entry.title),
    );
    marker.on("click", choose);
    marker.on("keypress", (event) => {
      if (["Enter", " "].includes(event.originalEvent.key)) {
        event.originalEvent.preventDefault();
        choose();
      }
    });
    marker.bindTooltip(tooltipFor(entry.title), { direction: "top" });
  }
  return marker;
}
function updateMarker(marker, entry) {
  const old = marker.entry;
  marker.entry = entry;
  const [lat, lng] = entry.point;
  const at = marker.getLatLng();
  if (at.lat !== lat || at.lng !== lng) marker.setLatLng(entry.point);
  // DivIcon.createIcon reuses the existing element, so focus survives.
  if (old.className !== entry.className || old.html !== entry.html)
    marker.setIcon(pinIcon(entry.className, entry.html));
  if (old.title !== entry.title) {
    marker.options.title = entry.title;
    const element = marker.getElement();
    if (element) {
      element.title = entry.title;
      element.setAttribute("aria-label", entry.title);
    }
    if (marker.getTooltip()) marker.setTooltipContent(tooltipFor(entry.title));
  }
}

// Records at identical coordinates would stack into one clickable pin and
// hide each other, so they share a pin that opens a chooser listing each one.
function chooser(members, onChoose, onClose) {
  const list = document.createElement("ul");
  list.className = "record-chooser-list";
  list.setAttribute("aria-label", "Records at this location");
  for (const member of members) {
    const item = document.createElement("li");
    const button = document.createElement("button");
    button.type = "button";
    button.className = "record-choice" + (member.active ? " active" : "");
    button.dataset.key = member.key;
    if (member.active) button.setAttribute("aria-current", "true");
    for (const [name, text] of [
      ["record-kind", member.kind + " · " + words(member.type)],
      ["record-title", member.label],
      ["record-status", member.status?.replaceAll("_", " ") || "UNKNOWN"],
    ]) {
      const span = document.createElement("span");
      span.className = name;
      span.textContent = text;
      button.append(span);
    }
    button.addEventListener("click", () => onChoose(member));
    item.append(button);
    list.append(item);
  }
  list.addEventListener("keydown", (event) => {
    if (event.key === "Escape") {
      event.stopPropagation();
      onClose();
    }
  });
  return list;
}

export function DispatchMap({
  requests = empty,
  resources = empty,
  selection,
  onSelectRequest,
  onSelectResource,
  focus,
  visible = true,
  ready = true,
  online,
  onPick,
  position,
  radius,
  dimmed = noReasons,
}) {
  const element = useRef(null);
  const map = useRef(null);
  const layers = useRef(null);
  const lines = useRef(null);
  const radiusLayer = useRef(null);
  const markers = useRef(new Map());
  const openChooser = useRef(null);
  const settled = useRef((change) => change());
  const fitted = useRef(false);
  const streetView = useRef(null);
  const [street, setStreet] = useState(true);
  const [zoom, setZoom] = useState(initialZoom);
  const [attempt, setAttempt] = useState(0);
  const [tileState, setTileState] = useState("loading");
  const picker = !!onPick;

  useEffect(() => {
    const m = L.map(element.current, {
      zoomControl: false,
      minZoom: 0,
      maxZoom: 19,
      zoomSnap: 0.25,
    }).setView(initialCentre, initialZoom);
    map.current = m;
    // The mode label depends on the scale actually shown, not on the layer.
    // A view change made while a zoom animation runs would be undone when the
    // animation ends, so it waits for that animation's zoomend instead.
    let animating = false;
    m.on("zoomanim", () => {
      animating = true;
    });
    m.on("zoomend", () => {
      animating = false;
      setZoom(m.getZoom());
    });
    settled.current = (change) =>
      animating ? m.once("zoomend", change) : change();
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
    radiusLayer.current = L.layerGroup().addTo(m);
    lines.current = L.layerGroup().addTo(m);
    layers.current = L.layerGroup().addTo(m);
    return () => {
      markers.current.clear();
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
    const m = map.current;
    if (street) {
      if (streetView.current) {
        const { center, zoom } = streetView.current;
        m.setView(center, zoom, { animate: false });
        streetView.current = null;
      }
      return;
    }
    streetView.current = { center: m.getCenter(), zoom: m.getZoom() };
    const labels = L.layerGroup(
      continents.map(([lat, lng, name]) =>
        L.marker([lat, lng], {
          interactive: false,
          keyboard: false,
          icon: L.divIcon({
            className: "world-region",
            html: `<span>${name}</span>`,
            iconSize: [0, 0],
          }),
        }),
      ),
    );
    const updateLabels = () => {
      if (m.getZoom() <= 2) labels.addTo(m);
      else labels.remove();
    };
    settled.current(() => m.fitWorld(worldPadding));
    updateLabels();
    m.on("zoomend", updateLabels);
    return () => {
      m.off("zoomend", updateLabels);
      labels.remove();
    };
  }, [street]);

  useEffect(() => {
    if (!onPick) return;
    const m = map.current;
    const select = (event) => onPick(event.latlng.wrap());
    m.on("click", select);
    return () => m.off("click", select);
  }, [onPick]);

  useEffect(() => {
    const m = map.current;
    const live = markers.current;
    lines.current.clearLayers();
    const entries = [];
    if (position) {
      entries.push({
        key: "picked",
        point: toPoint(position),
        className: "picked-pin",
        title: "Selected location",
      });
    }
    const places = new Map();
    const place = (record) => {
      const at = record.point.join(",");
      if (!places.has(at)) places.set(at, []);
      places.get(at).push(record);
    };
    for (const row of resources) {
      const active =
        selection?.kind === "resource" && selection.id === row.resourceId;
      const reason = dimmed.get(row.resourceId);
      place({
        key: "resource:" + row.resourceId,
        kind: "Resource",
        type: row.resourceType,
        label: row.name,
        status: row.status,
        active,
        point: toPoint(row.location),
        className:
          "resource-pin" +
          (active ? " pin-selected" : "") +
          (reason ? " pin-dimmed" : ""),
        title: "Resource: " + row.name + (reason ? " · " + reason : ""),
        pick: () => onSelectResource(row),
      });
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
      place({
        key: "request:" + row.requestId,
        kind: "Request",
        type: row.requestType,
        label: row.details || words(row.requestType) + " request",
        status: row.status,
        active,
        point,
        className:
          "request-pin" + state + urgency + (active ? " pin-selected" : ""),
        title: "Request: " + (row.details || row.requestType),
        pick: () => onSelectRequest(row),
      });
      const resource = resources.find((v) => reciprocal(row, v));
      if (resource) {
        L.polyline([point, toPoint(resource.location)], {
          color: row.status === "CANCELLING" ? "#986521" : "#286386",
          weight: 3,
          dashArray: "5 6",
          interactive: false,
        }).addTo(lines.current);
      }
    }
    const close = (marker) => {
      m.closePopup();
      marker.getElement()?.focus({ preventScroll: true });
    };
    const content = (marker) =>
      chooser(
        marker.entry.members,
        (member) => {
          close(marker);
          member.pick();
        },
        () => close(marker),
      );
    const showChooser = (marker) => {
      const popup = L.popup({
        className: "record-chooser",
        offset: [0, -8],
        autoPanPadding: [16, 16],
      })
        .setLatLng(marker.getLatLng())
        .setContent(content(marker));
      popup.on("remove", () => {
        // Leaflet fades the old popup out before detaching it; keep it out of
        // the keyboard and accessibility tree meanwhile.
        const element = popup.getElement();
        element?.setAttribute("aria-hidden", "true");
        if (element) element.inert = true;
        if (openChooser.current?.popup === popup) openChooser.current = null;
      });
      openChooser.current = { key: marker.entry.key, popup };
      popup.openOn(m);
      popup
        .getElement()
        ?.querySelector(".record-choice")
        ?.focus({ preventScroll: true });
    };
    for (const [at, members] of places) {
      if (members.length === 1) {
        entries.push({ ...members[0], select: members[0].pick });
        continue;
      }
      const active = members.some((v) => v.active);
      const requestsHere = members.some((v) => v.kind === "Request");
      entries.push({
        key: "place:" + at,
        point: members[0].point,
        members,
        signature: members
          .map((v) => [v.key, v.label, v.status, v.type, v.active].join(" "))
          .join("|"),
        className:
          "group-pin" +
          (requestsHere ? "" : " resource-pin") +
          (active ? " pin-selected" : ""),
        html: `<span>${members.length}</span>`,
        title: members.length + " records at this location",
        select: showChooser,
      });
    }
    const wanted = new Set();
    for (const entry of entries) {
      wanted.add(entry.key);
      const marker = live.get(entry.key);
      if (!marker) {
        live.set(entry.key, mapMarker(entry).addTo(layers.current));
        continue;
      }
      const old = marker.entry;
      const before = old.signature;
      updateMarker(marker, entry);
      const open = openChooser.current;
      if (open?.key === entry.key && before !== entry.signature) {
        // Rebuilding the list would drop keyboard focus to the page body, so
        // it returns to the same record, or to the first one if it left.
        const shown = open.popup.getElement();
        const focused = shown
          ?.querySelector(".record-chooser-list")
          ?.contains(document.activeElement)
          ? document.activeElement.closest(".record-choice")?.dataset.key
          : null;
        const keys = (v) => v.members.map((member) => member.key).join("|");
        const same = keys(old) === keys(entry);
        open.popup.setContent(content(marker));
        if (focused !== null) {
          const choices = [...shown.querySelectorAll(".record-choice")];
          const target = same
            ? choices.find((v) => v.dataset.key === focused)
            : choices[0];
          (target || choices[0])?.focus({ preventScroll: true });
        }
      }
    }
    for (const [key, marker] of live) {
      if (wanted.has(key)) continue;
      if (openChooser.current?.key === key) m.closePopup();
      // marker.remove() would leave it registered in the layer group.
      layers.current.removeLayer(marker);
      live.delete(key);
    }
    // Exposed for the browser tests, which check hidden pins are released.
    element.current.dataset.markerLayers = layers.current.getLayers().length;
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
    dimmed,
    onSelectRequest,
    onSelectResource,
    position,
    requests,
    resources,
    selection,
    visible,
  ]);

  // The selected request's matching radius, on both the street map and the
  // world overview.
  useEffect(() => {
    const layer = radiusLayer.current;
    layer.clearLayers();
    if (!radius) return;
    const centre = toPoint(radius);
    L.circle(centre, {
      radius: RADIUS_KM * 1000,
      className: "matching-radius",
      color: "#286386",
      weight: 2,
      opacity: 0.55,
      fillColor: "#286386",
      fillOpacity: 0.05,
      interactive: false,
    }).addTo(layer);
    L.marker([radius.lat + RADIUS_KM / kmPerDegree, radius.lng], {
      interactive: false,
      keyboard: false,
      icon: L.divIcon({
        className: "radius-label",
        html: `<span>Matching radius ${RADIUS_KM} km</span>`,
        iconSize: [0, 0],
      }),
    }).addTo(layer);
  }, [radius]);

  useEffect(() => {
    if (focus) {
      const m = map.current;
      settled.current(() =>
        m.setView(toPoint(focus), Math.max(m.getZoom(), 13)),
      );
    }
  }, [focus]);

  useEffect(() => {
    if (!street || !online || !visible || !ready) return;
    setTileState("loading");
    const m = map.current;
    const layer = L.tileLayer(tileURL, {
      attribution,
      referrerPolicy: "strict-origin-when-cross-origin",
      minZoom: 0,
      maxNativeZoom: 19,
      noWrap: true,
      bounds: tileBounds,
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
    layer.on("tileerror", (event) => {
      // Only a real tile of the world counts as missing street detail.
      if (event.coords && !inWorld(event.coords)) return;
      missing = true;
    });
    layer.on("load", settle);
    layer.addTo(m);
    return () => {
      active = false;
      clearTimeout(timer);
      // Leaflet unregisters the map listeners in the layer's remove event.
      if (m.hasLayer(layer)) layer.remove();
      layer.off();
    };
  }, [attempt, online, ready, street, visible]);

  const world = zoom <= worldZoom;
  const background = !street
    ? world
      ? "World overview · no street detail"
      : "Outline map · no street detail"
    : !online
      ? world
        ? "Offline world map · no street detail"
        : "Offline outline map · no street detail"
      : tileState === "failed"
        ? world
          ? "Street map unavailable · world overview shown"
          : "Street map unavailable · outline map shown"
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
          <button
            type="button"
            className="map-tool"
            onClick={() =>
              settled.current(() => map.current.fitWorld(worldPadding))
            }
          >
            Fit globe
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
              onClick={() => {
                const points = [...requests, ...resources].map((row) =>
                  toPoint(row.location),
                );
                settled.current(() =>
                  map.current.fitBounds(points, boundsOptions),
                );
              }}
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
