// Detected events: type chips, the list, the dots on the map sized by
// severity, and the "what happened here" view with the still before and
// the still after an event.

import { onStyleReady, removeLayers, EMPTY_FC } from "./basemap.js";
import { mediaUrl } from "./api.js";
import { escapeHtml, fmtClock, fmtUtc, fmtOffset, fmtMph, toMs } from "./format.js";

export const TYPE_ORDER = ["hard_brake", "hard_accel", "harsh_turn", "stop", "bump"];

const COLORS = {
  hard_brake: "#ff6b6b",
  hard_accel: "#ffd166",
  harsh_turn: "#4dabf7",
  stop: "#adb5bd",
  bump: "#69db7c",
};

export function typeColor(t) { return COLORS[t] || "#9fb0c0"; }
export function typeLabel(t) { return String(t || "").replaceAll("_", " "); }

const SRC = "events-src";
const LAYER = "events-layer";

// How an event's details read in words.
const DETAIL_LABELS = {
  speed_mph: ["speed", v => fmtMph(v)],
  a_long_mps2: ["braking / accel", v => `${Number(v).toFixed(2)} m/s²`],
  a_lat_mps2: ["sideways", v => `${Number(v).toFixed(2)} m/s²`],
  yaw_rate_rad_s: ["turn rate", v => `${(Number(v) * 180 / Math.PI).toFixed(1)} °/s`],
  duration_s: ["stopped for", v => `${Math.round(Number(v))} s`],
  max_mag: ["peak", v => `${Number(v).toFixed(2)} g`],
  thr: ["threshold", v => `${Number(v).toFixed(2)} g`],
};

export function initEvents({ map, state, els }) {
  let data = EMPTY_FC;

  const colorExpr = ["match", ["get", "event_type"], ...TYPE_ORDER.flatMap(t => [t, COLORS[t]]), "#9fb0c0"];

  function visibleFilter() {
    const types = TYPE_ORDER.filter(t => state.isTypeVisible(t));
    return ["in", ["get", "event_type"], ["literal", types]];
  }

  const install = () => {
    removeLayers(map, [LAYER], SRC);
    map.addSource(SRC, { type: "geojson", data });
    map.addLayer({
      id: LAYER, type: "circle", source: SRC,
      filter: visibleFilter(),
      paint: {
        "circle-radius": ["interpolate", ["linear"], ["coalesce", ["get", "sev_norm"], 0], 0, 4, 1, 10],
        "circle-stroke-width": 1.5,
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
    const ev = state.events.find(x => x.event_id === f.properties.event_id);
    if (ev) focus(ev);
  });
  map.on("mouseenter", LAYER, () => { map.getCanvas().style.cursor = "pointer"; });
  map.on("mouseleave", LAYER, () => { map.getCanvas().style.cursor = ""; });

  function setData(fc) {
    data = fc;
    const src = map.getSource(SRC);
    if (src) src.setData(fc);
  }

  /** Jump to an event and open its before/after view. */
  function focus(ev) {
    const ms = toMs(ev.ts_utc);
    if (ms == null) return;
    state.selectEvent(ev);
    state.setCursor(ms, "events");
    if (ev.lat != null && ev.lon != null) map.easeTo({ center: [ev.lon, ev.lat], duration: 250 });
  }

  // --- Chips ---

  function renderChips() {
    const counts = {};
    for (const e of state.events) counts[e.event_type] = (counts[e.event_type] || 0) + 1;
    els.chips.innerHTML = TYPE_ORDER.map(t => {
      const c = typeColor(t);
      const on = state.isTypeVisible(t);
      return `<button type="button" class="chip${on ? " on" : ""}" data-type="${t}" style="--chip:${c}">
        ${escapeHtml(typeLabel(t))} <span class="n">${counts[t] || 0}</span></button>`;
    }).join("");
  }

  els.chips.addEventListener("click", e => {
    const b = e.target.closest(".chip[data-type]");
    if (!b) return;
    const t = b.dataset.type;
    state.setTypeVisible(t, !state.isTypeVisible(t));
  });

  // --- List ---

  function renderList() {
    const sorted = state.visibleEvents().sort((a, b) => {
      const ta = toMs(a.ts_utc) ?? 0, tb = toMs(b.ts_utc) ?? 0;
      if (ta !== tb) return ta - tb;
      return TYPE_ORDER.indexOf(a.event_type) - TYPE_ORDER.indexOf(b.event_type);
    });
    els.count.textContent = state.events.length ? String(state.events.length) : "";
    els.list.innerHTML = sorted.map(e => {
      // A stop's severity is how long it lasted; the others are accelerations.
      const sev = e.severity == null ? "—"
        : e.event_type === "stop" ? `${Math.round(Number(e.severity))} s`
        : `sev ${Number(e.severity).toFixed(2)}`;
      const c = typeColor(e.event_type);
      const sel = state.selectedEvent?.event_id === e.event_id ? " selected" : "";
      return `
        <div class="ev${sel}" data-id="${escapeHtml(e.event_id)}">
          <span class="dot" style="background:${c}"></span>
          <span class="type" style="color:${c}">${escapeHtml(typeLabel(e.event_type))}</span>
          <span class="ts t mono" title="${fmtUtc(e.ts_utc)}">${escapeHtml(fmtClock(e.ts_utc, { seconds: true }))}</span>
          <span class="sev muted">${sev}</span>
        </div>`;
    }).join("") || `<div class="small" style="padding:8px 4px;">No events to show.</div>`;
  }

  els.list.addEventListener("click", e => {
    const node = e.target.closest(".ev[data-id]");
    if (!node) return;
    const ev = state.events.find(x => x.event_id === node.dataset.id);
    if (ev) focus(ev);
  });

  // --- Before / after view ---

  function frameCard(index, eventMs, label) {
    const f = state.frames[index];
    if (!f?.url) return `<div class="ba-card empty"><div class="small">no still ${label}</div></div>`;
    const ms = state.frameMs[index];
    return `
      <div class="ba-card" data-index="${index}">
        <img src="${mediaUrl(f.url)}" alt="" />
        <div class="cap"><span>${label}</span><span class="mono">${escapeHtml(fmtOffset(ms, eventMs))}</span></div>
      </div>`;
  }

  function renderFocus() {
    const ev = state.selectedEvent;
    if (!ev) { els.focus.hidden = true; els.focus.innerHTML = ""; return; }
    const ms = toMs(ev.ts_utc);
    const c = typeColor(ev.event_type);
    const before = state.frameIndexBefore(ms);
    const after = state.frameIndexAfter(ms);
    const details = Object.entries(ev.details || {})
      .filter(([k]) => DETAIL_LABELS[k])
      .map(([k, v]) => `<span><b>${DETAIL_LABELS[k][0]}</b> ${escapeHtml(DETAIL_LABELS[k][1](v))}</span>`)
      .join("");
    els.focus.hidden = false;
    els.focus.innerHTML = `
      <div class="ba-head">
        <span class="type" style="color:${c}">${escapeHtml(typeLabel(ev.event_type))}</span>
        <span class="t mono" title="${fmtUtc(ev.ts_utc)}">${escapeHtml(fmtClock(ev.ts_utc, { seconds: true }))}</span>
        <button type="button" class="ba-close" title="Close">×</button>
      </div>
      <div class="ba-stills">
        ${frameCard(before, ms, "before")}
        ${frameCard(after === before ? state.frameIndexAfter(ms + 1) : after, ms, "after")}
      </div>
      <div class="ba-details small">${details || "—"}</div>`;
  }

  els.focus.addEventListener("click", e => {
    if (e.target.closest(".ba-close")) { state.selectEvent(null); return; }
    const card = e.target.closest(".ba-card[data-index]");
    if (card) state.setCursor(state.frameMs[Number(card.dataset.index)], "events");
  });

  // --- State ---

  state.addEventListener("load", () => {
    const maxSev = {};
    for (const e of state.events) {
      if (e.severity != null) maxSev[e.event_type] = Math.max(maxSev[e.event_type] || 0, e.severity);
    }
    renderChips();
    renderList();
    renderFocus();
    setData({
      type: "FeatureCollection",
      features: state.events
        .filter(e => e.lon != null && e.lat != null)
        .map(e => ({
          type: "Feature",
          geometry: { type: "Point", coordinates: [e.lon, e.lat] },
          properties: {
            event_id: e.event_id, event_type: e.event_type,
            sev_norm: (e.severity != null && maxSev[e.event_type]) ? e.severity / maxSev[e.event_type] : 0,
          },
        })),
    });
  });

  state.addEventListener("clear", () => {
    els.list.innerHTML = "";
    els.count.textContent = "";
    renderFocus();
    setData(EMPTY_FC);
  });

  state.addEventListener("eventfilter", () => {
    renderChips();
    renderList();
    if (map.getLayer(LAYER)) map.setFilter(LAYER, visibleFilter());
  });

  state.addEventListener("eventselect", () => {
    renderFocus();
    renderList();
  });

  renderChips();
  return { focus };
}
