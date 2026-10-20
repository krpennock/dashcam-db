// Entry point for the viewer page: wires the modules together, loads a drive,
// and keeps the address bar pointing at the moment on screen.

import { renderNav } from "./nav.js";
import { apiGet, toast } from "./api.js";
import { escapeHtml, fmtClock, fmtUtc, fmtLocalFull, timeHtml, fmtDurationClock, fmtMi, fmtMph, toMs } from "./format.js";
import { createMap } from "./basemap.js";
import { DriveState } from "./state.js";
import { initTrack } from "./track.js";
import { initStoryboard } from "./storyboard.js";
import { initEvents, TYPE_ORDER } from "./events.js";
import { initChart } from "./timeline.js";
import { initDrives } from "./drives.js";

renderNav("viewer", "Dashcam Viewer");

const $ = id => document.getElementById(id);

const state = new DriveState();
const map = createMap("map");

initTrack(map, state);

initStoryboard({
  map, state,
  els: {
    count: $("storyCount"), status: $("storyStatus"), grid: $("thumbGrid"),
    previewEmpty: $("storyPreviewEmpty"), previewImg: $("storyPreviewImg"),
    previewTs: $("storyPreviewTs"), openLink: $("storyOpenLink"), reloadBtn: $("storyReloadBtn"),
  },
  onReload: () => {
    const sid = state.session?.drive_session_id;
    if (sid) loadDrive(sid, { t: state.cursorMs }).catch(err => toast(err.message));
  },
});

initEvents({
  map, state,
  els: { list: $("eventList"), count: $("eventCount"), toggleMap: $("toggleMapEvents") },
});

initChart({ el: $("chart"), state, toggleEvents: $("toggleChartEvents") });

const drives = initDrives({
  els: {
    vehicle: $("vehicleFilter"), from: $("dateFrom"), to: $("dateTo"),
    body: $("sessionTableBody"), table: document.querySelector(".session-table"), status: $("sessionStatus"),
  },
  onSelect: sid => loadDrive(sid).catch(err => toast(err.message)),
});

// --- Session card and summary card ---

const elSessionMeta = $("sessionMeta");

// A start time that was not measured from satellites is worth flagging, quietly.
function approxTimeBadge(tc) {
  if (!tc || !String(tc).startsWith("filename")) return "";
  const why = tc === "filename_estimated_clock_offset"
    ? "Camera clock, corrected using nearby drives"
    : "Camera clock — too few satellite fixes to measure the time";
  return ` <span class="badge" title="${escapeHtml(why)} (${escapeHtml(tc)})">approximate time</span>`;
}

function setSessionMeta(s) {
  const sync = s.time_sync || {};
  elSessionMeta.innerHTML = `
    <div><b>Session:</b> <span class="small mono">${escapeHtml(s.drive_session_id)}</span></div>
    <div><b>Vehicle:</b> <span class="small">${escapeHtml(s.vehicle_tag || "—")}</span></div>
    <div><b>Start:</b> <span class="small">${timeHtml(s.start_ts_utc)}</span>${approxTimeBadge(s.time_confidence)}</div>
    <div><b>End:</b> <span class="small">${timeHtml(s.end_ts_utc)}</span></div>
    <div><b>time_sync:</b> <span class="small">slope=${escapeHtml(sync.slope ?? "—")} p95_ms=${escapeHtml(sync.residual_p95_ms ?? "—")}</span></div>
  `;
}

function setSummaryCard(sum) {
  const set = (id, v) => { $(id).textContent = v; };
  if (!sum) {
    set("summaryUpdated", "—");
    set("sumDistance", "—"); set("sumDuration", "—"); set("sumMaxSpeed", "—"); set("sumAvgSpeed", "—");
    $("sumCounts").innerHTML = `<span class="badge">Summary not found (run refresh_derived)</span>`;
    return;
  }
  $("summaryUpdated").textContent = sum.updated_at ? `updated ${fmtLocalFull(sum.updated_at)}` : "—";
  $("summaryUpdated").title = sum.updated_at ? fmtUtc(sum.updated_at) : "";
  set("sumDistance", fmtMi(sum.distance_mi, 2));
  set("sumDuration", fmtDurationClock(sum.duration_s));
  set("sumMaxSpeed", fmtMph(sum.max_speed_mph, 1));
  set("sumAvgSpeed", `${fmtMph(sum.avg_speed_mph, 1)} / ${fmtMph(sum.moving_avg_speed_mph, 1)}`);
  $("sumCounts").innerHTML = `
    <div><b>Stops:</b> ${sum.stops_count ?? 0} &nbsp; <b>Bumps:</b> ${sum.bump_count ?? 0}</div>
    <div><b>Hard brake:</b> ${sum.hard_brake_count ?? 0} &nbsp; <b>Hard accel:</b> ${sum.hard_accel_count ?? 0} &nbsp; <b>Harsh turn:</b> ${sum.harsh_turn_count ?? 0}</div>
    ${sum.params ? `
      <details class="paramsDetails">
        <summary>params</summary>
        <pre class="paramsBox">${escapeHtml(JSON.stringify(sum.params, null, 2))}</pre>
      </details>` : ""}
  `;
}

// --- Scrub slider and the "at this moment" panel ---

const slider = $("scrubSlider");
const elScrubIso = $("scrubIso");
const elGnssLine = $("nearGnss");
const elAccelLine = $("nearAccel");
const elClipLine = $("nearClip");

slider.addEventListener("input", () => state.setCursor(Number(slider.value), "scrub"));

function enableScrub() {
  slider.min = String(state.startMs);
  slider.max = String(state.endMs);
  slider.step = "200";
  slider.value = String(state.startMs);
  slider.disabled = !(state.startMs != null && state.endMs != null && state.endMs > state.startMs);
}

function updateNearestInfo(ms) {
  const g = state.nearestGnss(ms);
  const a = state.nearestAccel(ms);
  elGnssLine.innerHTML = g
    ? `${timeHtml(g.ts_utc, { full: false })}  (${g.lat.toFixed(6)}, ${g.lon.toFixed(6)})  ${fmtMph(g.speed_mph)}`
    : "—";
  elAccelLine.innerHTML = a && a.a_mag_raw != null
    ? `${timeHtml(a.ts_utc, { full: false })}  mag=${a.a_mag_raw.toFixed(2)}  ax=${a.ax.toFixed(2)} ay=${a.ay.toFixed(2)} az=${a.az.toFixed(2)}`
    : "—";
}

// The clip name is the one thing the browser cannot work out itself; ask for
// it once the cursor has stopped moving.
let clipTimer = null;
let clipToken = 0;
function scheduleClipLookup(ms) {
  if (clipTimer) clearTimeout(clipTimer);
  const token = ++clipToken;
  const sid = state.session?.drive_session_id;
  clipTimer = setTimeout(async () => {
    clipTimer = null;
    try {
      const data = await apiGet(`/sessions/${sid}/nearest`, { ts: new Date(ms).toISOString() });
      if (token !== clipToken || state.session?.drive_session_id !== sid) return;
      const c = data.clip;
      elClipLine.innerHTML = c
        ? `<span class="mono">${escapeHtml(c.clip_name)}</span>  (${fmtClock(c.start_ts_utc, { seconds: true })} → ${fmtClock(c.end_ts_utc, { seconds: true })})`
        : "—";
    } catch (err) {
      if (token === clipToken) elClipLine.textContent = "—";
      console.warn("clip lookup failed", err);
    }
  }, 300);
}

// Keep the address bar shareable: ?session=<id>&t=<moment>.
let urlTimer = null;
function scheduleUrlUpdate() {
  if (urlTimer) clearTimeout(urlTimer);
  urlTimer = setTimeout(() => {
    urlTimer = null;
    const sid = state.session?.drive_session_id;
    if (!sid || state.cursorMs == null) return;
    const p = new URLSearchParams();
    p.set("session", sid);
    p.set("t", fmtUtc(state.cursorMs));
    history.replaceState(null, "", `?${p.toString()}`);
  }, 500);
}

state.addEventListener("cursorchange", e => {
  const { ms, source } = e.detail;
  if (source !== "scrub") slider.value = String(ms);
  elScrubIso.textContent = fmtClock(ms, { seconds: true });
  elScrubIso.title = fmtUtc(ms);
  updateNearestInfo(ms);
  scheduleClipLookup(ms);
  scheduleUrlUpdate();
});

state.addEventListener("clear", () => {
  slider.disabled = true;
  elScrubIso.textContent = "—";
  elGnssLine.textContent = "—";
  elAccelLine.textContent = "—";
  elClipLine.textContent = "—";
});

// --- Loading a drive ---

let loadToken = 0;

async function loadDrive(sid, { t = null } = {}) {
  const token = ++loadToken;
  state.clear();
  elSessionMeta.innerHTML = `<div><b>Session:</b> <span class="small">loading…</span></div>`;

  const [session, summary, gnss, frames, events, accel] = await Promise.all([
    apiGet(`/sessions/${sid}`),
    apiGet(`/sessions/${sid}/summary`).catch(err => { if (err.status === 404) return null; throw err; }),
    apiGet(`/sessions/${sid}/gnss`, { limit: 200000 }),
    apiGet(`/sessions/${sid}/storyboard`, { limit: 5000 }).catch(err => { console.warn("storyboard", err); return []; }),
    apiGet(`/sessions/${sid}/events`, { types: TYPE_ORDER.join(","), limit: 5000 }),
    apiGet(`/sessions/${sid}/accel`, { limit: 200000 }),
  ]);
  if (token !== loadToken) return; // a newer drive was picked meanwhile

  state.load({ session, summary, gnss, frames, events, accel });
  setSessionMeta(session);
  setSummaryCard(summary);
  enableScrub();
  state.setCursor(t ?? state.startMs, "load");
}

// --- Start ---

const params = new URLSearchParams(location.search);
const initialSid = params.get("session");
const initialT = toMs(params.get("t"));

drives.init()
  .then(() => {
    if (!initialSid) return;
    drives.highlight(initialSid);
    return loadDrive(initialSid, { t: initialT });
  })
  .catch(err => toast(err.message));