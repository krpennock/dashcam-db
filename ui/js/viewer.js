// Entry point for the viewer page: wires the modules together, loads a drive,
// and keeps the address bar pointing at the moment on screen.

import { renderNav } from "./nav.js";
import { apiGet, toast } from "./api.js";
import { escapeHtml, fmtClock, fmtUtc, fmtLocalFull, fmtDurationShort, fmtMi, fmtMph, fmtDeg, toMs } from "./format.js";
import { createMap } from "./basemap.js";
import { DriveState } from "./state.js";
import { initTrack } from "./track.js";
import { initStoryboard } from "./storyboard.js";
import { initEvents, TYPE_ORDER, typeColor, typeLabel } from "./events.js";
import { initTimeline } from "./timeline.js";
import { initDrives } from "./drives.js";
import { initDrawer } from "./drawer.js";

renderNav("viewer", "Dashcam Viewer");

const $ = id => document.getElementById(id);

const state = new DriveState();
const map = createMap("map");

// For poking at the page from the console or a headless check.
window.__dashcam = { map, state };

let story; // assigned below; the track's hover handlers call into it
const track = initTrack(map, state, {
  onHover: (ms, point) => story?.hoverAt(ms, point),
  onHoverEnd: () => story?.hideHover(),
});

story = initStoryboard({
  map, state, track,
  els: {
    strip: $("filmstrip"), count: $("stillCount"),
    pip: $("pip"), pipImg: $("pipImg"), pipTime: $("pipTime"), pipSpeed: $("pipSpeed"), pipOpen: $("pipOpen"),
    hoverCard: $("hoverCard"), hoverImg: $("hoverImg"), hoverCap: $("hoverCap"),
    btnFirst: $("btnFirst"), btnPrev: $("btnPrev"), btnPlay: $("btnPlay"), btnNext: $("btnNext"), btnLast: $("btnLast"),
    rate: $("rate"),
  },
});

initEvents({
  map, state,
  els: { chips: $("eventChips"), list: $("eventList"), count: $("eventCount"), focus: $("eventFocus") },
});

const timeline = initTimeline({ el: $("chart"), state });

const drives = initDrives({
  root: $("tab-drives"),
  onSelect: sid => loadDrive(sid).catch(err => toast(err.message)),
  getBounds: () => map.getBounds(),
  onChange: () => scheduleUrlUpdate(),
});

const drawer = initDrawer({ app: $("app"), onResize: () => { map.resize(); timeline.resize(); } });
state.addEventListener("eventselect", e => { if (e.detail.event) { drawer.setOpen(true); drawer.showTab("events"); } });

// --- Summary strip ---

const elStrip = $("summaryStrip");

function approxTimeBadge(tc) {
  if (!tc || !String(tc).startsWith("filename")) return "";
  const why = tc === "filename_estimated_clock_offset"
    ? "Camera clock, corrected using nearby drives"
    : "Camera clock — too few satellite fixes to measure the time";
  return `<span class="badge warn" title="${escapeHtml(why)} (${escapeHtml(tc)})">approximate time</span>`;
}

function setStrip(s, sum) {
  const counts = sum ? [
    ["hard_brake", sum.hard_brake_count], ["hard_accel", sum.hard_accel_count], ["harsh_turn", sum.harsh_turn_count],
    ["stop", sum.stops_count], ["bump", sum.bump_count],
  ].filter(([, n]) => n > 0).map(([t, n]) =>
    `<span class="count" style="--chip:${typeColor(t)}">${n}× ${escapeHtml(typeLabel(t))}</span>`
  ).join("") : "";
  const sync = s.time_sync || {};
  const idTitle = `session ${s.drive_session_id}\nstart ${fmtUtc(s.start_ts_utc)}\nend ${fmtUtc(s.end_ts_utc)}\n` +
    `time_sync slope=${sync.slope ?? "—"} p95_ms=${sync.residual_p95_ms ?? "—"}` +
    (sum?.updated_at ? `\nsummary updated ${fmtUtc(sum.updated_at)}` : "");
  elStrip.innerHTML = `
    <span class="veh">${escapeHtml(s.vehicle_tag || "—")}</span>
    <span class="when t" title="${fmtUtc(s.start_ts_utc)}">${escapeHtml(fmtLocalFull(s.start_ts_utc))}</span>
    ${approxTimeBadge(s.time_confidence)}
    <span class="stat"><b>${escapeHtml(fmtMi(sum?.distance_mi, 1))}</b></span>
    <span class="stat"><b>${escapeHtml(fmtDurationShort(sum?.duration_s))}</b></span>
    <span class="stat">max <b>${escapeHtml(fmtMph(sum?.max_speed_mph))}</b></span>
    <span class="stat">avg <b>${escapeHtml(fmtMph(sum?.avg_speed_mph))}</b> moving <b>${escapeHtml(fmtMph(sum?.moving_avg_speed_mph))}</b></span>
    ${counts}
    ${sum ? "" : `<span class="badge">no summary (run refresh_derived)</span>`}
    <span class="spacer"></span>
    <span class="id mono muted" title="${escapeHtml(idTitle)}">${escapeHtml(s.drive_session_id.slice(0, 8))}</span>
  `;
}

// --- "Now" line: what the sensors said at the cursor ---

const elNow = $("nowLine");
let clipTimer = null;
let clipToken = 0;
let clipText = "";

function renderNow(ms) {
  const g = state.nearestGnss(ms);
  const a = state.nearestAccel(ms);
  const parts = [`<span class="t" title="${fmtUtc(ms)}">${escapeHtml(fmtClock(ms, { seconds: true }))}</span>`];
  if (g) parts.push(escapeHtml(fmtMph(g.speed_mph)), `heading ${escapeHtml(fmtDeg(g.course_deg))}`);
  if (a?.a_mag_raw != null) parts.push(`${a.a_mag_raw.toFixed(2)} g`);
  if (clipText) parts.push(`<span class="mono">${escapeHtml(clipText)}</span>`);
  elNow.innerHTML = parts.join(`<span class="sep">·</span>`);
}

// The clip name is the one thing the browser cannot work out itself; ask for
// it once the cursor has stopped moving.
function scheduleClipLookup(ms) {
  if (clipTimer) clearTimeout(clipTimer);
  const token = ++clipToken;
  const sid = state.session?.drive_session_id;
  clipTimer = setTimeout(async () => {
    clipTimer = null;
    try {
      const data = await apiGet(`/sessions/${sid}/nearest`, { ts: new Date(ms).toISOString() });
      if (token !== clipToken || state.session?.drive_session_id !== sid) return;
      clipText = data.clip ? data.clip.clip_name : "";
      renderNow(state.cursorMs);
    } catch (err) {
      console.warn("clip lookup failed", err);
    }
  }, 300);
}

// Keep the address bar shareable: ?session=<id>&t=<moment> plus the list filters.
let urlTimer = null;
function scheduleUrlUpdate() {
  if (urlTimer) clearTimeout(urlTimer);
  urlTimer = setTimeout(() => {
    urlTimer = null;
    const p = drives.toQuery(new URLSearchParams());
    const sid = state.session?.drive_session_id;
    if (sid && state.cursorMs != null) {
      p.set("session", sid);
      p.set("t", fmtUtc(state.cursorMs));
    }
    const q = p.toString();
    history.replaceState(null, "", q ? `?${q}` : location.pathname);
  }, 500);
}

state.addEventListener("cursorchange", e => {
  renderNow(e.detail.ms);
  scheduleClipLookup(e.detail.ms);
  scheduleUrlUpdate();
});

state.addEventListener("clear", () => {
  clipText = "";
  elNow.textContent = "—";
  elStrip.innerHTML = `<span class="muted">Loading…</span>`;
});

// --- Loading a drive ---

let loadToken = 0;

async function loadDrive(sid, { t = null } = {}) {
  const token = ++loadToken;
  state.clear();

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
  setStrip(session, summary);
  state.setCursor(t ?? state.startMs, "load");
}

// --- Start ---

const params = new URLSearchParams(location.search);
const initialSid = params.get("session");
const initialT = toMs(params.get("t"));

drives.init(params)
  .then(() => {
    if (!initialSid) return;
    drives.highlight(initialSid);
    return loadDrive(initialSid, { t: initialT });
  })
  .catch(err => toast(err.message));
