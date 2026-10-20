// Every drive's route on one map. Hover a route for its date, click it to
// open that drive in the viewer; the side panel's filters narrow the set.

import { renderNav } from "./nav.js";
import { apiGet, toast } from "./api.js";
import { escapeHtml, fmtLocal, fmtLocalFull, fmtUtc, fmtMi, fmtDurationShort } from "./format.js";
import { createMap, onStyleReady, removeLayers, EMPTY_FC } from "./basemap.js";
import { initDrawer } from "./drawer.js";
import { initDrives } from "./drives.js";

renderNav("overview", "Overview");

const $ = id => document.getElementById(id);
const map = createMap("map");
window.__dashcam = { map };

initDrawer({ app: $("app"), onResize: () => map.resize() });

const SRC = "tracks-src";
const CASING = "tracks-casing";
const LINE = "tracks-line";
const HOVER = "tracks-hover";
const SEL_CASING = "tracks-selected-casing";
const SEL = "tracks-selected";

let data = EMPTY_FC;
let hoverId = "";
let selectedId = "";

const idFilter = id => ["==", ["get", "drive_session_id"], id];

function colorExpr() {
  const v = drives.vehicles;
  if (!v.length) return drives.vehicleColor("");
  return ["match", ["get", "vehicle_tag"], ...v.flatMap(tag => [tag, drives.vehicleColor(tag)]), "#9fb0c0"];
}

const install = () => {
  removeLayers(map, [SEL, SEL_CASING, HOVER, LINE, CASING], SRC);
  map.addSource(SRC, { type: "geojson", data });
  map.addLayer({ id: CASING, type: "line", source: SRC, layout: { "line-join": "round", "line-cap": "round" },
    paint: { "line-color": "#0b0f14", "line-width": 4, "line-opacity": 0.5 } });
  map.addLayer({ id: LINE, type: "line", source: SRC, layout: { "line-join": "round", "line-cap": "round" },
    paint: { "line-color": colorExpr(), "line-width": 2, "line-opacity": 0.8 } });
  map.addLayer({ id: HOVER, type: "line", source: SRC, filter: idFilter(hoverId), layout: { "line-join": "round", "line-cap": "round" },
    paint: { "line-color": "#ffffff", "line-width": 4 } });
  map.addLayer({ id: SEL_CASING, type: "line", source: SRC, filter: idFilter(selectedId), layout: { "line-join": "round", "line-cap": "round" },
    paint: { "line-color": "#ffffff", "line-width": 7 } });
  map.addLayer({ id: SEL, type: "line", source: SRC, filter: idFilter(selectedId), layout: { "line-join": "round", "line-cap": "round" },
    paint: { "line-color": "#4dabf7", "line-width": 3.5 } });
};
onStyleReady(map, install);

const tip = $("mapTip");
map.on("mousemove", LINE, e => {
  const f = e.features?.[0];
  if (!f) return;
  const p = f.properties;
  if (p.drive_session_id !== hoverId) {
    hoverId = p.drive_session_id;
    if (map.getLayer(HOVER)) map.setFilter(HOVER, idFilter(hoverId));
  }
  tip.innerHTML = `<b>${escapeHtml(p.vehicle_tag || "—")}</b> ${escapeHtml(fmtLocal(p.start_ts_utc))}
    <span class="muted">· ${escapeHtml(fmtMi(p.distance_mi))} · ${escapeHtml(fmtDurationShort(p.duration_s))}</span>`;
  tip.hidden = false;
  const box = tip.parentElement.getBoundingClientRect();
  const x = Math.min(e.point.x + 14, box.width - tip.offsetWidth - 8);
  const y = e.point.y > box.height - 50 ? e.point.y - 34 : e.point.y + 14;
  tip.style.transform = `translate(${Math.max(0, x)}px, ${y}px)`;
  map.getCanvas().style.cursor = "pointer";
});
map.on("mouseleave", LINE, () => {
  hoverId = "";
  if (map.getLayer(HOVER)) map.setFilter(HOVER, idFilter(""));
  tip.hidden = true;
  map.getCanvas().style.cursor = "";
});
map.on("click", LINE, e => {
  const f = e.features?.[0];
  if (f) location.href = `index.html?session=${encodeURIComponent(f.properties.drive_session_id)}`;
});

function boundsOf(features) {
  let minLon = Infinity, maxLon = -Infinity, minLat = Infinity, maxLat = -Infinity;
  for (const f of features) {
    for (const [lon, lat] of f.geometry.coordinates) {
      if (lon < minLon) minLon = lon;
      if (lon > maxLon) maxLon = lon;
      if (lat < minLat) minLat = lat;
      if (lat > maxLat) maxLat = lat;
    }
  }
  return Number.isFinite(minLon) ? [[minLon, minLat], [maxLon, maxLat]] : null;
}

function setStrip(session) {
  const n = data.features.length;
  const miles = data.features.reduce((s, f) => s + (f.properties.distance_mi || 0), 0);
  const total = `<span class="stat"><b>${n}</b> route${n === 1 ? "" : "s"} on the map</span><span class="stat"><b>${miles.toFixed(0)} mi</b></span>`;
  if (!session) {
    $("overviewStrip").innerHTML = `${total}<span class="muted">Hover a route for its date; click it to open the drive.</span>`;
    return;
  }
  $("overviewStrip").innerHTML = `${total}
    <span class="spacer"></span>
    <span class="veh">${escapeHtml(session.vehicle_tag || "—")}</span>
    <span class="t" title="${fmtUtc(session.start_ts_utc)}">${escapeHtml(fmtLocalFull(session.start_ts_utc))}</span>
    <span class="stat"><b>${escapeHtml(fmtMi(session.distance_mi))}</b></span>
    <span class="stat"><b>${escapeHtml(fmtDurationShort(session.duration_s))}</b></span>
    <a class="open" href="index.html?session=${encodeURIComponent(session.drive_session_id)}">Open in viewer</a>`;
}

function renderLegend() {
  $("vehicleLegend").innerHTML = drives.vehicles
    .map(v => `<span><span class="swatch" style="background:${drives.vehicleColor(v)}"></span>${escapeHtml(v)}</span>`)
    .join("");
}

async function loadTracks(params) {
  data = await apiGet("/tracks", { ...params, limit: 5000 });
  const src = map.getSource(SRC);
  if (src) src.setData(data);
  if (map.getLayer(LINE)) map.setPaintProperty(LINE, "line-color", colorExpr());
  renderLegend();
  // With an area filter on, the view is the filter: leave it where it is.
  const b = boundsOf(data.features);
  if (b && !params.bbox) map.fitBounds(b, { padding: 40, duration: 500 });
  selectedId = "";
  for (const id of [SEL, SEL_CASING]) if (map.getLayer(id)) map.setFilter(id, idFilter(""));
  setStrip(null);
}

function select(sid, session) {
  selectedId = sid;
  for (const id of [SEL, SEL_CASING]) if (map.getLayer(id)) map.setFilter(id, idFilter(sid));
  const f = data.features.find(x => x.properties.drive_session_id === sid);
  if (f) {
    const b = boundsOf([f]);
    if (b) map.fitBounds(b, { padding: 80, duration: 500, maxZoom: 14 });
  }
  setStrip(session);
}

function updateUrl() {
  const q = drives.toQuery(new URLSearchParams()).toString();
  history.replaceState(null, "", q ? `?${q}` : location.pathname);
}

const drives = initDrives({
  root: $("tab-drives"),
  onSelect: select,
  getBounds: () => map.getBounds(),
  onChange: params => {
    updateUrl();
    loadTracks(params).catch(err => {
      $("overviewStrip").innerHTML = `<span class="muted">The routes could not be loaded.</span>`;
      toast(err.message);
    });
  },
});

drives.init(new URLSearchParams(location.search)).catch(err => toast(err.message));
