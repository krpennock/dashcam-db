// The Stats page: totals, trends, when the cars are driven, records, and
// where events cluster. Everything follows one vehicle + period choice.

import { renderNav } from "./nav.js";
import { apiGet, toast } from "./api.js";
import { escapeHtml, fmtLocal, fmtUtc, fmtMi, fmtMph, fmtDurationShort, localDayIso, localDateStr } from "./format.js";
import { createMap, onStyleReady, removeLayers, EMPTY_FC } from "./basemap.js";
import { PRESETS, presetDates, VEHICLE_PALETTE } from "./drives.js";
import { TYPE_ORDER, typeColor, typeLabel } from "./events.js";

renderNav("stats", "Stats");

const $ = id => document.getElementById(id);
const PLOT_CONFIG = { displayModeBar: false, responsive: true };
const FONT = { color: "#9fb0c0", size: 11 };
const GRID = "#253244";
const DOW = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"];
const EVENT_TYPES = ["hard_brake", "hard_accel", "harsh_turn", "bump"]; // stops are waits, not events

const f = { vehicle: "", preset: "all", from: "", to: "", bucket: "month", metric: "miles", hot: new Set(EVENT_TYPES) };
let vehicles = [];
let series = [];
let heat = [];

const vehicleColor = tag => VEHICLE_PALETTE[Math.max(0, vehicles.indexOf(tag)) % VEHICLE_PALETTE.length];
const fmtN = (v, d = 0) => v == null ? "—" : Number(v).toLocaleString("en-US", { maximumFractionDigits: d, minimumFractionDigits: d });
const hourLabel = h => h === 0 ? "12a" : h < 12 ? `${h}a` : h === 12 ? "12p" : `${h - 12}p`;
const eventsOf = r => (r.hard_brake || 0) + (r.hard_accel || 0) + (r.harsh_turn || 0) + (r.bump || 0);

// --- Controls and address bar ---

$("preset").innerHTML = PRESETS.map(([v, l]) => `<option value="${v}">${l}</option>`).join("");

function fromQuery(p) {
  f.vehicle = p.get("v") || "";
  f.preset = PRESETS.some(([v]) => v === p.get("p")) ? p.get("p") : "all";
  f.from = p.get("from") || ""; f.to = p.get("to") || "";
  f.bucket = p.get("b") === "week" ? "week" : "month";
  f.metric = ["miles", "hours", "drives", "events"].includes(p.get("m")) ? p.get("m") : "miles";
  if (p.has("hot")) f.hot = new Set(p.get("hot").split(",").filter(t => EVENT_TYPES.includes(t)));
}

function updateUrl() {
  const p = new URLSearchParams();
  if (f.vehicle) p.set("v", f.vehicle);
  if (f.preset !== "all") p.set("p", f.preset);
  if (f.preset === "custom") { if (f.from) p.set("from", f.from); if (f.to) p.set("to", f.to); }
  if (f.bucket !== "month") p.set("b", f.bucket);
  if (f.metric !== "miles") p.set("m", f.metric);
  if (f.hot.size !== EVENT_TYPES.length) p.set("hot", [...f.hot].join(","));
  const q = p.toString();
  history.replaceState(null, "", q ? `?${q}` : location.pathname);
}

function syncControls() {
  $("vehicle").value = f.vehicle;
  $("preset").value = f.preset;
  $("dates").hidden = f.preset !== "custom";
  $("from").value = f.from; $("to").value = f.to;
  for (const b of $("metric").querySelectorAll("button")) b.classList.toggle("on", b.dataset.m === f.metric);
  for (const b of $("bucket").querySelectorAll("button")) b.classList.toggle("on", b.dataset.b === f.bucket);
}

function apiParams() {
  const [from, to] = f.preset === "custom" ? [f.from, f.to] : presetDates(f.preset);
  const p = { vehicle: f.vehicle || null };
  if (from) p.after = localDayIso(from);
  if (to) p.before = localDayIso(to, { end: true });
  return p;
}

$("vehicle").addEventListener("change", () => { f.vehicle = $("vehicle").value; reload(); });
$("preset").addEventListener("change", () => { f.preset = $("preset").value; $("dates").hidden = f.preset !== "custom"; if (f.preset !== "custom" || f.from || f.to) reload(); });
$("from").addEventListener("change", () => { f.from = $("from").value; reload(); });
$("to").addEventListener("change", () => { f.to = $("to").value; reload(); });
$("metric").addEventListener("click", e => {
  const b = e.target.closest("button[data-m]");
  if (!b) return;
  f.metric = b.dataset.m; syncControls(); updateUrl(); drawTrend();
});
$("bucket").addEventListener("click", e => {
  const b = e.target.closest("button[data-b]");
  if (!b) return;
  f.bucket = b.dataset.b; syncControls(); updateUrl(); loadSeries().catch(err => toast(err.message));
});

// --- Tiles ---

function tile(k, v, unit, sub, split) {
  return `
    <div class="tile">
      <div class="k">${k}</div>
      <div class="v">${v}${unit ? `<small>${unit}</small>` : ""}</div>
      ${sub ? `<div class="sub">${sub}</div>` : ""}
      ${split?.length > 1 ? `<div class="split">${split.map(([tag, val]) =>
        `<div><span class="dot" style="background:${vehicleColor(tag)}"></span>${escapeHtml(tag)} <b>${val}</b></div>`).join("")}</div>` : ""}
    </div>`;
}

function renderTiles(o) {
  const t = o.total, by = o.by_vehicle;
  const per100 = r => (r.miles > 0) ? eventsOf(r) / r.miles * 100 : null;
  $("tiles").innerHTML = [
    tile("Drives", fmtN(t.drives), "", t.first_drive ? `${fmtLocal(t.first_drive).split(" ").slice(0, 2).join(" ")} to ${fmtLocal(t.last_drive).split(" ").slice(0, 2).join(" ")}` : "", by.map(r => [r.vehicle_tag, fmtN(r.drives)])),
    tile("Miles", fmtN(t.miles), "mi", t.drives ? `${fmtN(t.miles / t.drives, 1)} mi per drive` : "", by.map(r => [r.vehicle_tag, fmtN(r.miles)])),
    tile("Hours", fmtN(t.hours, 1), "h", t.moving_hours != null ? `${fmtN(t.moving_hours, 1)} h moving` : "", by.map(r => [r.vehicle_tag, fmtN(r.hours, 1)])),
    tile("Top speed", fmtN(t.max_speed_mph), "mph", "", by.map(r => [r.vehicle_tag, fmtN(r.max_speed_mph)])),
    tile("Events per 100 mi", fmtN(per100(t), 1), "", `${fmtN(eventsOf(t))} events, ${fmtN(t.stop)} stops`, by.map(r => [r.vehicle_tag, fmtN(per100(r), 1)])),
  ].join("");
  const [from, to] = f.preset === "custom" ? [f.from, f.to] : presetDates(f.preset);
  $("range").textContent = from || to ? `${from || "…"} to ${to || "…"}` : "all time";
}

// --- Trend ---

function plotLayout(extra) {
  return {
    margin: { l: 56, r: 12, t: 8, b: 36 },
    paper_bgcolor: "rgba(0,0,0,0)", plot_bgcolor: "rgba(0,0,0,0)", font: FONT,
    xaxis: { gridcolor: GRID, zerolinecolor: GRID, fixedrange: true },
    yaxis: { gridcolor: GRID, zerolinecolor: GRID, fixedrange: true, rangemode: "tozero" },
    legend: { orientation: "h", y: 1.12, x: 0, bgcolor: "rgba(0,0,0,0)" },
    hovermode: "x unified",
    ...extra,
  };
}

function drawTrend() {
  const tags = [...new Set(series.map(r => r.vehicle_tag))].sort((a, b) => vehicles.indexOf(a) - vehicles.indexOf(b));
  const buckets = [...new Set(series.map(r => r.bucket))].sort();
  const value = r => f.metric === "events" ? eventsOf(r) : r[f.metric];
  const unit = { miles: "mi", hours: "h", drives: "", events: "" }[f.metric];
  const traces = tags.map(tag => {
    const byBucket = new Map(series.filter(r => r.vehicle_tag === tag).map(r => [r.bucket, value(r)]));
    return {
      type: "bar", name: tag, x: buckets, y: buckets.map(b => byBucket.get(b) ?? 0),
      marker: { color: vehicleColor(tag), line: { width: 0 } },
      hovertemplate: `%{y:,.${f.metric === "hours" ? 1 : 0}f} ${unit}<extra>${escapeHtml(tag)}</extra>`,
    };
  });
  const layout = plotLayout({
    barmode: "stack", bargap: 0.25,
    showlegend: tags.length > 1,
    yaxis: { gridcolor: GRID, zerolinecolor: GRID, fixedrange: true, rangemode: "tozero", title: { text: { miles: "miles", hours: "hours", drives: "drives", events: "events" }[f.metric], standoff: 6 } },
    xaxis: { gridcolor: GRID, zerolinecolor: GRID, fixedrange: true, type: "date", tickformat: f.bucket === "week" ? "%b %d" : "%b %Y" },
  });
  Plotly.react($("trend"), traces, layout, PLOT_CONFIG);

  $("trendTable").innerHTML = `<table><thead><tr><th>${f.bucket}</th>${tags.map(t => `<th>${escapeHtml(t)}</th>`).join("")}<th>total</th></tr></thead><tbody>${
    buckets.map(b => {
      const vals = tags.map(t => traces[tags.indexOf(t)].y[buckets.indexOf(b)]);
      return `<tr><td>${b}</td>${vals.map(v => `<td>${fmtN(v, f.metric === "hours" ? 1 : 0)}</td>`).join("")}<td>${fmtN(vals.reduce((a, c) => a + c, 0), f.metric === "hours" ? 1 : 0)}</td></tr>`;
    }).join("")}</tbody></table>`;
}

// --- Heatmap: weekday x hour ---

function drawHeat() {
  const z = DOW.map(() => Array(24).fill(0));
  for (const r of heat) z[r.dow - 1][r.hour] += r.minutes;
  const hours = [...Array(24).keys()];
  Plotly.react($("heat"), [{
    type: "heatmap", z, x: hours.map(hourLabel), y: DOW,
    colorscale: [[0, "#111823"], [0.3, "#184f95"], [0.65, "#3987e5"], [1, "#b7d3f6"]],
    showscale: false, xgap: 2, ygap: 2,
    hovertemplate: "%{y} %{x}: %{z:,.0f} min<extra></extra>",
  }], plotLayout({
    margin: { l: 40, r: 8, t: 8, b: 30 },
    xaxis: { fixedrange: true, showgrid: false, tickmode: "array", tickvals: hours.filter(h => h % 3 === 0).map(hourLabel) },
    yaxis: { fixedrange: true, showgrid: false, autorange: "reversed" },
    hovermode: "closest",
  }), PLOT_CONFIG);

  $("heatTable").innerHTML = `<table><thead><tr><th></th>${hours.map(h => `<th>${hourLabel(h)}</th>`).join("")}</tr></thead><tbody>${
    DOW.map((d, i) => `<tr><td>${d}</td>${z[i].map(v => `<td>${v ? fmtN(v) : ""}</td>`).join("")}</tr>`).join("")}</tbody></table>`;
}

// --- Records ---

function renderRecords(rec) {
  const link = r => r?.drive_session_id ? `<a href="index.html?session=${encodeURIComponent(r.drive_session_id)}">open ↗</a>` : "";
  const when = r => r?.start_ts_utc ? `<span class="d t" title="${fmtUtc(r.start_ts_utc)}">${escapeHtml(r.vehicle_tag || "")} · ${escapeHtml(fmtLocal(r.start_ts_utc))}</span>` : "";
  const rows = [];
  if (rec.longest?.distance_mi != null) rows.push(["Longest drive", `${fmtMi(rec.longest.distance_mi)} <span class="d">in ${fmtDurationShort(rec.longest.duration_s)}</span>`, when(rec.longest), link(rec.longest)]);
  if (rec.fastest?.max_speed_mph != null) rows.push(["Fastest drive", fmtMph(rec.fastest.max_speed_mph), when(rec.fastest), link(rec.fastest)]);
  if (rec.most_events?.events) rows.push(["Most events", `${rec.most_events.events} <span class="d">in one drive</span>`, when(rec.most_events), link(rec.most_events)]);
  if (rec.biggest_day?.miles != null) rows.push(["Biggest day", `${fmtMi(rec.biggest_day.miles)} <span class="d">in ${rec.biggest_day.drives} drive${rec.biggest_day.drives === 1 ? "" : "s"}</span>`, `<span class="d">${rec.biggest_day.day}</span>`,
    `<a href="overview.html?p=custom&from=${rec.biggest_day.day}&to=${rec.biggest_day.day}${f.vehicle ? `&v=${encodeURIComponent(f.vehicle)}` : ""}">see routes ↗</a>`]);
  $("records").innerHTML = rows.map(([k, v, d, a]) => `<div class="record"><span class="k">${k}</span><span class="v">${v}</span>${d}${a}</div>`).join("")
    || `<div class="small">No drives in this period.</div>`;
}

// --- Hotspot map ---

const hotMap = createMap("hotMap", { zoom: 10 });
window.__dashcam = { map: hotMap };
const HOT_SRC = "hot-src", HOT_HEAT = "hot-heat", HOT_DOTS = "hot-dots";
let hotData = EMPTY_FC;

onStyleReady(hotMap, () => {
  removeLayers(hotMap, [HOT_DOTS, HOT_HEAT], HOT_SRC);
  hotMap.addSource(HOT_SRC, { type: "geojson", data: hotData });
  hotMap.addLayer({
    id: HOT_HEAT, type: "heatmap", source: HOT_SRC, maxzoom: 15,
    paint: {
      "heatmap-weight": 1,
      "heatmap-intensity": ["interpolate", ["linear"], ["zoom"], 7, 0.15, 11, 0.6, 14, 2],
      "heatmap-radius": ["interpolate", ["linear"], ["zoom"], 7, 5, 11, 12, 14, 24],
      "heatmap-color": ["interpolate", ["linear"], ["heatmap-density"],
        0, "rgba(24,79,149,0)", 0.2, "rgba(24,79,149,0.6)", 0.5, "#3987e5", 0.8, "#86b6ef", 1, "#b7d3f6"],
      "heatmap-opacity": ["interpolate", ["linear"], ["zoom"], 12, 0.9, 15, 0.4],
    },
  });
  hotMap.addLayer({
    id: HOT_DOTS, type: "circle", source: HOT_SRC, minzoom: 12,
    paint: {
      "circle-radius": ["interpolate", ["linear"], ["zoom"], 12, 3, 16, 7],
      "circle-color": ["match", ["get", "event_type"], ...TYPE_ORDER.flatMap(t => [t, typeColor(t)]), "#9fb0c0"],
      "circle-stroke-width": 1, "circle-stroke-color": "#0b0f14",
      "circle-opacity": ["interpolate", ["linear"], ["zoom"], 12, 0.3, 14, 0.9],
    },
  });
});
hotMap.on("click", HOT_DOTS, e => {
  const p = e.features?.[0]?.properties;
  if (p) location.href = `index.html?session=${encodeURIComponent(p.drive_session_id)}&t=${encodeURIComponent(fmtUtc(p.ts_utc))}`;
});
hotMap.on("mouseenter", HOT_DOTS, () => { hotMap.getCanvas().style.cursor = "pointer"; });
hotMap.on("mouseleave", HOT_DOTS, () => { hotMap.getCanvas().style.cursor = ""; });

function renderHotChips() {
  $("hotChips").innerHTML = EVENT_TYPES.map(t =>
    `<button type="button" class="chip${f.hot.has(t) ? " on" : ""}" data-type="${t}" style="--chip:${typeColor(t)}">${escapeHtml(typeLabel(t))}</button>`).join("");
}
$("hotChips").addEventListener("click", e => {
  const b = e.target.closest(".chip[data-type]");
  if (!b) return;
  const t = b.dataset.type;
  if (f.hot.has(t)) f.hot.delete(t); else f.hot.add(t);
  renderHotChips(); updateUrl();
  loadHotspots().catch(err => toast(err.message));
});

async function loadHotspots() {
  if (!f.hot.size) { hotData = EMPTY_FC; hotMap.getSource(HOT_SRC)?.setData(hotData); return; }
  hotData = await apiGet("/events/hotspots", { ...apiParams(), types: [...f.hot].join(",") });
  hotMap.getSource(HOT_SRC)?.setData(hotData);
  if (hotData.features.length) {
    let minLon = Infinity, maxLon = -Infinity, minLat = Infinity, maxLat = -Infinity;
    for (const ft of hotData.features) {
      const [lon, lat] = ft.geometry.coordinates;
      if (lon < minLon) minLon = lon; if (lon > maxLon) maxLon = lon;
      if (lat < minLat) minLat = lat; if (lat > maxLat) maxLat = lat;
    }
    hotMap.fitBounds([[minLon, minLat], [maxLon, maxLat]], { padding: 30, duration: 400, maxZoom: 12 });
  }
}

// --- Loading ---

async function loadSeries() {
  series = await apiGet("/stats/series", { ...apiParams(), bucket: f.bucket });
  drawTrend();
}

async function reload() {
  syncControls();
  updateUrl();
  const params = apiParams();
  const [overview, , heatRows] = await Promise.all([
    apiGet("/stats/overview", params),
    loadSeries(),
    apiGet("/stats/heatmap", params),
    loadHotspots(),
  ]);
  heat = heatRows;
  renderTiles(overview);
  renderRecords(overview.records);
  drawHeat();
}

async function init() {
  fromQuery(new URLSearchParams(location.search));
  try {
    vehicles = await apiGet("/vehicles");
    $("vehicle").innerHTML = `<option value="">All vehicles</option>` +
      vehicles.map(v => `<option value="${escapeHtml(v)}">${escapeHtml(v)}</option>`).join("");
  } catch { /* keep "All" */ }
  renderHotChips();
  await reload();
}

init().catch(err => toast(err.message));
