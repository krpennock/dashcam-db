// Detected events: the list in the side panel and the dots on the map.

import { onStyleReady, removeLayers, EMPTY_FC } from "./basemap.js";
import { escapeHtml, fmtClock, fmtUtc, toMs } from "./format.js";

export const TYPE_ORDER = ["hard_brake", "hard_accel", "harsh_turn", "stop", "bump"];

const COLORS = {
  hard_brake: "#ff6b6b",
  hard_accel: "#ffd166",
  harsh_turn: "#4dabf7",
  stop: "#adb5bd",
  bump: "#69db7c",
};

export function typeColor(t) {
  return COLORS[t] || "#9fb0c0";
}

export function typeLabel(t) {
  return String(t || "").replaceAll("_", " ");
}

const SRC = "events-src";
const LAYER = "events-layer";

export function initEvents({ map, state, els }) {
  let data = EMPTY_FC;

  const colorExpr = ["match", ["get", "event_type"],
    ...TYPE_ORDER.flatMap(t => [t, COLORS[t]]),
    "#9fb0c0"];

  const install = () => {
    removeLayers(map, [LAYER], SRC);
    map.addSource(SRC, { type: "geojson", data });
    map.addLayer({
      id: LAYER, type: "circle", source: SRC,
      layout: { visibility: els.toggleMap.checked ? "visible" : "none" },
      paint: {
        "circle-radius": 5,
        "circle-stroke-width": 1,
        "circle-stroke-color": "#0b0f14",
        "circle-opacity": 0.9,
        "circle-color": colorExpr,
      },
    });
  };
  onStyleReady(map, install);

  map.on("click", LAYER, e => {
    const f = e.features?.[0];
    if (!f) return;
    const p = f.properties || {};
    const html = `
      <div class="event-popup">
        <div><b style="color:${typeColor(p.event_type)}">${escapeHtml(typeLabel(p.event_type))}</b>
          <span class="muted">sev=${Number(p.severity || 0).toFixed(2)}</span></div>
        <div class="muted t" title="${fmtUtc(p.ts_utc)}">${escapeHtml(fmtClock(p.ts_utc, { seconds: true }))}</div>
      </div>`;
    new maplibregl.Popup().setLngLat(f.geometry.coordinates).setHTML(html).addTo(map);
  });
  map.on("mouseenter", LAYER, () => { map.getCanvas().style.cursor = "pointer"; });
  map.on("mouseleave", LAYER, () => { map.getCanvas().style.cursor = ""; });

  els.toggleMap.addEventListener("change", () => {
    if (map.getLayer(LAYER)) {
      map.setLayoutProperty(LAYER, "visibility", els.toggleMap.checked ? "visible" : "none");
    }
  });

  function setData(fc) {
    data = fc;
    const src = map.getSource(SRC);
    if (src) src.setData(fc);
  }

  function renderList() {
    const sorted = [...state.events].sort((a, b) => {
      const ta = toMs(a.ts_utc) ?? 0, tb = toMs(b.ts_utc) ?? 0;
      if (ta !== tb) return ta - tb;
      return TYPE_ORDER.indexOf(a.event_type) - TYPE_ORDER.indexOf(b.event_type);
    });
    els.count.textContent = `(${sorted.length})`;
    els.list.innerHTML = sorted.map(e => {
      const sev = e.severity == null ? "—" : Number(e.severity).toFixed(2);
      const c = typeColor(e.event_type);
      const coord = (e.lat != null && e.lon != null) ? `${e.lat.toFixed(5)}, ${e.lon.toFixed(5)}` : "—";
      return `
        <div class="ev" data-ts="${escapeHtml(e.ts_utc)}" data-lat="${e.lat ?? ""}" data-lon="${e.lon ?? ""}">
          <div class="top">
            <div>
              <span class="badge" style="border-color:${c}; color:${c};">${escapeHtml(typeLabel(e.event_type))}</span>
              <span class="ts t" title="${fmtUtc(e.ts_utc)}">${escapeHtml(fmtClock(e.ts_utc, { seconds: true }))}</span>
            </div>
            <div class="sev">sev=${sev}</div>
          </div>
          <div class="sub">${coord}</div>
        </div>`;
    }).join("");
  }

  els.list.addEventListener("click", e => {
    const node = e.target.closest(".ev");
    if (!node) return;
    const ms = toMs(node.dataset.ts);
    if (ms == null) return;
    state.setCursor(ms, "events");
    const lat = Number(node.dataset.lat), lon = Number(node.dataset.lon);
    if (Number.isFinite(lat) && Number.isFinite(lon)) {
      map.easeTo({ center: [lon, lat], duration: 250 });
    }
  });

  state.addEventListener("load", () => {
    renderList();
    setData({
      type: "FeatureCollection",
      features: state.events
        .filter(e => e.lon != null && e.lat != null)
        .map(e => ({
          type: "Feature",
          geometry: { type: "Point", coordinates: [e.lon, e.lat] },
          properties: { event_id: e.event_id, ts_utc: e.ts_utc, event_type: e.event_type, severity: e.severity },
        })),
    });
  });

  state.addEventListener("clear", () => {
    els.list.innerHTML = "";
    els.count.textContent = "—";
    setData(EMPTY_FC);
  });
}
