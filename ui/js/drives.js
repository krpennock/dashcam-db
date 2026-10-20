// The list of drives and its filters. Renders its own panel so the viewer
// and the overview page share one implementation.
//
// Filters live in the address bar (short keys, see toQuery) so a filtered
// list can be shared, and are turned into API parameters by toParams.

import { apiGet, toast } from "./api.js";
import { fmtLocal, fmtUtc, fmtDurationShort, fmtMi, escapeHtml, localDayIso, localDateStr } from "./format.js";
import { TYPE_ORDER, typeColor, typeLabel } from "./events.js";

const PRESETS = [
  ["all", "All time"], ["7d", "Last 7 days"], ["30d", "Last 30 days"],
  ["month", "This month"], ["year", "This year"], ["custom", "Custom dates"],
];
const WEEKDAYS = [[1, "Mo"], [2, "Tu"], [3, "We"], [4, "Th"], [5, "Fr"], [6, "Sa"], [7, "Su"]];
const COUNT_COLS = {
  hard_brake: "hard_brake_count", hard_accel: "hard_accel_count", harsh_turn: "harsh_turn_count",
  stop: "stops_count", bump: "bump_count",
};

/** Colours for vehicles, in the order the API lists them. */
export const VEHICLE_PALETTE = ["#4dabf7", "#ff8c42", "#69db7c", "#ffd166", "#da77f2", "#2ec4b6"];

function presetDates(preset) {
  const today = localDateStr(0);
  switch (preset) {
    case "7d": return [localDateStr(-6), today];
    case "30d": return [localDateStr(-29), today];
    case "month": return [`${today.slice(0, 7)}-01`, today];
    case "year": return [`${today.slice(0, 4)}-01-01`, today];
    default: return ["", ""];
  }
}

export function initDrives({ root, onSelect, onChange, getBounds }) {
  const f = {
    vehicle: "", preset: "all", from: "", to: "",
    has: new Set(), minMi: "", minMin: "", approx: false,
    hfrom: "", hto: "", wd: new Set(), bbox: null,
  };
  let all = [];
  let vehicles = [];
  let sortCol = "start_ts_utc";
  let sortAsc = false; // newest first
  let activeSid = null;

  // --- Markup ---

  root.classList.add("drives-panel");
  root.innerHTML = `
    <div class="filters">
      <div class="frow">
        <select data-f="vehicle" title="Vehicle"><option value="">All vehicles</option></select>
        <select data-f="preset" title="Period">${PRESETS.map(([v, l]) => `<option value="${v}">${l}</option>`).join("")}</select>
      </div>
      <div class="frow dates" hidden>
        <label>From <input type="date" data-f="from" /></label>
        <label>To <input type="date" data-f="to" /></label>
      </div>
      <div class="frow chips" data-role="has" title="Only drives with at least one of the lit event types">
        ${TYPE_ORDER.map(t => `<button type="button" class="chip" data-type="${t}" style="--chip:${typeColor(t)}">${escapeHtml(typeLabel(t))}</button>`).join("")}
      </div>
      <div class="frow">
        <label class="num">≥ <input type="number" min="0" step="1" data-f="minMi" placeholder="0" /> mi</label>
        <label class="num">≥ <input type="number" min="0" step="1" data-f="minMin" placeholder="0" /> min</label>
        <label class="check"><input type="checkbox" data-f="approx" /> approx. time only</label>
      </div>
      <details class="more">
        <summary>More</summary>
        <div class="frow">
          <label class="num">Starts between <input type="number" min="0" max="23" data-f="hfrom" placeholder="0" /></label>
          <label class="num">and <input type="number" min="0" max="23" data-f="hto" placeholder="23" /> o'clock</label>
        </div>
        <div class="frow weekdays" data-role="wd">
          ${WEEKDAYS.map(([d, l]) => `<button type="button" class="wd" data-day="${d}">${l}</button>`).join("")}
        </div>
      </details>
      <div class="frow">
        <button type="button" class="inview" title="Only drives whose route crosses the area on the map">Drives in view</button>
        <button type="button" class="chip on bbox" hidden style="--chip:#e6edf3" title="Remove the area filter">area ×</button>
        <button type="button" class="reset" title="Clear every filter">Reset</button>
      </div>
    </div>
    <div class="session-table-wrap grow">
      <table class="session-table">
        <thead>
          <tr>
            <th data-col="vehicle_tag">Veh</th>
            <th data-col="start_ts_utc">Date</th>
            <th data-col="duration_s">Time</th>
            <th data-col="distance_mi">Dist</th>
            <th data-col="events_total" title="Events">Ev</th>
          </tr>
        </thead>
        <tbody></tbody>
      </table>
    </div>
    <div class="session-status">Loading…</div>
  `;
  const q = sel => root.querySelector(sel);
  const el = {
    vehicle: q('[data-f="vehicle"]'), preset: q('[data-f="preset"]'), dates: q(".frow.dates"),
    from: q('[data-f="from"]'), to: q('[data-f="to"]'), minMi: q('[data-f="minMi"]'), minMin: q('[data-f="minMin"]'),
    approx: q('[data-f="approx"]'), hfrom: q('[data-f="hfrom"]'), hto: q('[data-f="hto"]'),
    has: q('[data-role="has"]'), wd: q('[data-role="wd"]'), inview: q(".inview"), bbox: q(".bbox"), reset: q(".reset"),
    body: q("tbody"), table: q(".session-table"), status: q(".session-status"), more: q("details.more"),
  };

  // --- Filter state <-> controls ---

  function syncControls() {
    el.vehicle.value = f.vehicle;
    el.preset.value = f.preset;
    el.dates.hidden = f.preset !== "custom";
    el.from.value = f.from; el.to.value = f.to;
    el.minMi.value = f.minMi; el.minMin.value = f.minMin;
    el.approx.checked = f.approx;
    el.hfrom.value = f.hfrom; el.hto.value = f.hto;
    for (const b of el.has.querySelectorAll(".chip")) b.classList.toggle("on", f.has.has(b.dataset.type));
    for (const b of el.wd.querySelectorAll(".wd")) b.classList.toggle("on", f.wd.has(Number(b.dataset.day)));
    el.bbox.hidden = !f.bbox;
    if (f.hfrom || f.hto || f.wd.size) el.more.open = true;
  }

  function dateRange() {
    if (f.preset === "custom") return [f.from, f.to];
    return presetDates(f.preset);
  }

  /** API query parameters for the current filters. */
  function toParams() {
    const [from, to] = dateRange();
    const p = { limit: 5000, vehicle: f.vehicle || null };
    if (from) p.after = localDayIso(from);
    if (to) p.before = localDayIso(to, { end: true });
    if (f.minMi !== "") p.min_distance_mi = Number(f.minMi);
    if (f.minMin !== "") p.min_duration_s = Number(f.minMin) * 60;
    if (f.has.size) p.has_event = [...f.has].join(",");
    if (f.approx) p.time_confidence = "approximate";
    if (f.hfrom !== "") p.hour_from = Number(f.hfrom);
    if (f.hto !== "") p.hour_to = Number(f.hto);
    if (f.wd.size) p.weekdays = [...f.wd].sort().join(",");
    if (f.bbox) p.bbox = f.bbox.map(v => v.toFixed(5)).join(",");
    return p;
  }

  /** Short address-bar keys for the current filters. */
  function toQuery(params = new URLSearchParams()) {
    for (const k of ["v", "p", "from", "to", "has", "mi", "min", "approx", "hf", "ht", "wd", "bbox"]) params.delete(k);
    if (f.vehicle) params.set("v", f.vehicle);
    if (f.preset !== "all") params.set("p", f.preset);
    if (f.preset === "custom") { if (f.from) params.set("from", f.from); if (f.to) params.set("to", f.to); }
    if (f.has.size) params.set("has", [...f.has].join(","));
    if (f.minMi !== "") params.set("mi", f.minMi);
    if (f.minMin !== "") params.set("min", f.minMin);
    if (f.approx) params.set("approx", "1");
    if (f.hfrom !== "") params.set("hf", f.hfrom);
    if (f.hto !== "") params.set("ht", f.hto);
    if (f.wd.size) params.set("wd", [...f.wd].sort().join(","));
    if (f.bbox) params.set("bbox", f.bbox.map(v => v.toFixed(5)).join(","));
    return params;
  }

  function fromQuery(params) {
    f.vehicle = params.get("v") || "";
    f.preset = PRESETS.some(([v]) => v === params.get("p")) ? params.get("p") : "all";
    f.from = params.get("from") || ""; f.to = params.get("to") || "";
    f.has = new Set((params.get("has") || "").split(",").filter(t => TYPE_ORDER.includes(t)));
    f.minMi = params.get("mi") || ""; f.minMin = params.get("min") || "";
    f.approx = params.get("approx") === "1";
    f.hfrom = params.get("hf") || ""; f.hto = params.get("ht") || "";
    f.wd = new Set((params.get("wd") || "").split(",").map(Number).filter(d => d >= 1 && d <= 7));
    const bb = (params.get("bbox") || "").split(",").map(Number);
    f.bbox = bb.length === 4 && bb.every(Number.isFinite) ? bb : null;
  }

  // --- Table ---

  function eventsTotal(s) {
    return Object.values(COUNT_COLS).reduce((n, c) => n + (s[c] || 0), 0);
  }

  function sorted() {
    const val = s => sortCol === "events_total" ? eventsTotal(s) : s[sortCol];
    return [...all].sort((a, b) => {
      let va = val(a), vb = val(b);
      if (va == null && vb == null) return 0;
      if (va == null) return 1;
      if (vb == null) return -1;
      if (typeof va === "string") { va = va.toLowerCase(); vb = String(vb).toLowerCase(); }
      const cmp = va < vb ? -1 : va > vb ? 1 : 0;
      return sortAsc ? cmp : -cmp;
    });
  }

  function eventDots(s) {
    return TYPE_ORDER
      .filter(t => (s[COUNT_COLS[t]] || 0) > 0)
      .map(t => `<span class="evd" style="background:${typeColor(t)}" title="${s[COUNT_COLS[t]]}× ${escapeHtml(typeLabel(t))}">${s[COUNT_COLS[t]]}</span>`)
      .join("");
  }

  function render() {
    const list = sorted();
    el.body.innerHTML = list.map(s => `
      <tr data-sid="${s.drive_session_id}"${s.drive_session_id === activeSid ? ' class="active"' : ""}>
        <td>${escapeHtml(s.vehicle_tag || "—")}</td>
        <td title="${fmtUtc(s.start_ts_utc)}">${fmtLocal(s.start_ts_utc)}${String(s.time_confidence || "").startsWith("filename") ? ' <span class="approx" title="approximate time">~</span>' : ""}</td>
        <td>${fmtDurationShort(s.duration_s)}</td>
        <td>${fmtMi(s.distance_mi)}</td>
        <td class="evcol">${eventDots(s)}</td>
      </tr>`).join("");
    const miles = list.reduce((n, s) => n + (s.distance_mi || 0), 0);
    el.status.textContent = `${list.length} drive${list.length === 1 ? "" : "s"} · ${miles.toFixed(0)} mi`;
  }

  async function refresh() {
    el.status.textContent = "Loading…";
    const params = toParams();
    all = await apiGet("/sessions", params);
    render();
    if (onChange) onChange(params, all);
  }

  const changed = () => refresh().catch(err => {
    el.status.textContent = "Could not load the list.";
    toast(err.message);
  });

  async function loadVehicles() {
    try {
      vehicles = await apiGet("/vehicles");
      el.vehicle.innerHTML = `<option value="">All vehicles</option>` +
        vehicles.map(v => `<option value="${escapeHtml(v)}">${escapeHtml(v)}</option>`).join("");
      el.vehicle.value = f.vehicle;
    } catch { /* keep "All" */ }
  }

  function highlight(sid) {
    activeSid = sid;
    for (const tr of el.body.querySelectorAll("tr")) tr.classList.toggle("active", tr.dataset.sid === sid);
    el.body.querySelector("tr.active")?.scrollIntoView({ block: "nearest" });
  }

  // --- Wiring ---

  el.body.addEventListener("click", e => {
    const tr = e.target.closest("tr[data-sid]");
    if (!tr) return;
    highlight(tr.dataset.sid);
    onSelect(tr.dataset.sid, all.find(s => s.drive_session_id === tr.dataset.sid));
  });

  for (const th of el.table.querySelectorAll("th[data-col]")) {
    th.addEventListener("click", () => {
      const col = th.dataset.col;
      if (sortCol === col) sortAsc = !sortAsc; else { sortCol = col; sortAsc = col === "vehicle_tag"; }
      render();
    });
  }

  el.vehicle.addEventListener("change", () => { f.vehicle = el.vehicle.value; changed(); });
  el.preset.addEventListener("change", () => { f.preset = el.preset.value; el.dates.hidden = f.preset !== "custom"; if (f.preset !== "custom" || (f.from || f.to)) changed(); });
  el.from.addEventListener("change", () => { f.from = el.from.value; changed(); });
  el.to.addEventListener("change", () => { f.to = el.to.value; changed(); });
  el.minMi.addEventListener("change", () => { f.minMi = el.minMi.value; changed(); });
  el.minMin.addEventListener("change", () => { f.minMin = el.minMin.value; changed(); });
  el.approx.addEventListener("change", () => { f.approx = el.approx.checked; changed(); });
  el.hfrom.addEventListener("change", () => { f.hfrom = el.hfrom.value; changed(); });
  el.hto.addEventListener("change", () => { f.hto = el.hto.value; changed(); });
  el.has.addEventListener("click", e => {
    const b = e.target.closest(".chip[data-type]");
    if (!b) return;
    const t = b.dataset.type;
    if (f.has.has(t)) f.has.delete(t); else f.has.add(t);
    b.classList.toggle("on", f.has.has(t));
    changed();
  });
  el.wd.addEventListener("click", e => {
    const b = e.target.closest(".wd[data-day]");
    if (!b) return;
    const d = Number(b.dataset.day);
    if (f.wd.has(d)) f.wd.delete(d); else f.wd.add(d);
    b.classList.toggle("on", f.wd.has(d));
    changed();
  });
  el.inview.addEventListener("click", () => {
    if (!getBounds) return;
    const b = getBounds();
    if (!b) return;
    f.bbox = [b.getWest(), b.getSouth(), b.getEast(), b.getNorth()];
    el.bbox.hidden = false;
    changed();
  });
  el.bbox.addEventListener("click", () => { f.bbox = null; el.bbox.hidden = true; changed(); });
  el.reset.addEventListener("click", () => {
    fromQuery(new URLSearchParams());
    syncControls();
    changed();
  });

  return {
    refresh,
    highlight,
    toQuery,
    toParams,
    get vehicles() { return vehicles; },
    get sessions() { return all; },
    vehicleColor(tag) {
      const i = vehicles.indexOf(tag);
      return VEHICLE_PALETTE[(i < 0 ? 0 : i) % VEHICLE_PALETTE.length];
    },
    async init(params) {
      fromQuery(params || new URLSearchParams());
      await loadVehicles();
      syncControls();
      await refresh();
    },
  };
}
