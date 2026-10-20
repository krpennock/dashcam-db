// One place for how times, distances and speeds are written.
//
// Every page shows local time (the cars live in one time zone) and keeps the
// UTC instant in a tooltip, so a timestamp can still be matched against the
// database or a clip name.

export const TZ = "America/Chicago";

const dtfCache = new Map();
function dtf(opts) {
  const key = JSON.stringify(opts);
  let f = dtfCache.get(key);
  if (!f) {
    f = new Intl.DateTimeFormat("en-US", { timeZone: TZ, ...opts });
    dtfCache.set(key, f);
  }
  return f;
}

/** Milliseconds since the epoch for an ISO string, or null. Numbers pass through. */
export function toMs(v) {
  if (typeof v === "number") return Number.isFinite(v) ? v : null;
  if (!v) return null;
  const ms = Date.parse(v);
  return Number.isFinite(ms) ? ms : null;
}

/** "6:12p" or, with seconds, "6:12:44p". */
export function fmtClock(v, { seconds = false } = {}) {
  const ms = toMs(v);
  if (ms == null) return "—";
  const s = dtf({
    hour: "numeric", minute: "2-digit",
    ...(seconds ? { second: "2-digit" } : {}),
    hour12: true,
  }).format(new Date(ms));
  return s.replace(" AM", "a").replace(" PM", "p");
}

/** "Oct 19 6:12p": the drive-list style. */
export function fmtLocal(v) {
  const ms = toMs(v);
  if (ms == null) return "—";
  return `${dtf({ month: "short", day: "numeric" }).format(new Date(ms))} ${fmtClock(ms)}`;
}

/** "Oct 19, 2026 6:12:44p": for a single moment shown on its own. */
export function fmtLocalFull(v) {
  const ms = toMs(v);
  if (ms == null) return "—";
  return `${dtf({ month: "short", day: "numeric", year: "numeric" }).format(new Date(ms))} ${fmtClock(ms, { seconds: true })}`;
}

/** The UTC instant as ISO with a Z and no milliseconds: what goes in tooltips. */
export function fmtUtc(v) {
  const ms = toMs(v);
  if (ms == null) return "—";
  return new Date(ms).toISOString().replace(/\.\d{3}Z$/, "Z");
}

/** An HTML span: local time shown, UTC on hover. */
export function timeHtml(v, { seconds = true, full = true } = {}) {
  const ms = toMs(v);
  if (ms == null) return "—";
  const shown = full ? fmtLocalFull(ms) : fmtClock(ms, { seconds });
  return `<span class="t" title="${fmtUtc(ms)}">${escapeHtml(shown)}</span>`;
}

/** "41m" or "1h 5m". */
export function fmtDurationShort(sec) {
  if (sec == null) return "—";
  const m = Math.round(sec / 60);
  if (m < 60) return `${m}m`;
  return `${Math.floor(m / 60)}h ${m % 60}m`;
}

/** "00:41:12". */
export function fmtDurationClock(sec) {
  if (sec == null) return "—";
  const s = Math.max(0, Math.floor(sec));
  const hh = String(Math.floor(s / 3600)).padStart(2, "0");
  const mm = String(Math.floor((s % 3600) / 60)).padStart(2, "0");
  const ss = String(s % 60).padStart(2, "0");
  return `${hh}:${mm}:${ss}`;
}

export function fmtMi(mi, digits = 1) {
  return mi == null ? "—" : `${Number(mi).toFixed(digits)} mi`;
}

export function fmtMph(v, digits = 0) {
  return v == null || Number.isNaN(Number(v)) ? "—" : `${Number(v).toFixed(digits)} mph`;
}

export function escapeHtml(s) {
  return String(s ?? "")
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#039;");
}

// Plotly knows nothing about time zones: a zone-less "YYYY-MM-DD HH:MM:SS.sss"
// string is drawn exactly as written. So the local wall-clock string is built
// here. The zone offset is looked up once per hour of data rather than once
// per sample, which keeps a 50 000-point series fast and still follows a
// daylight-saving change (they happen on the hour).
const offsetCache = new Map();
function zoneOffsetMs(ms) {
  const hour = Math.floor(ms / 3600000);
  let off = offsetCache.get(hour);
  if (off === undefined) {
    const parts = dtf({
      year: "numeric", month: "2-digit", day: "2-digit",
      hour: "2-digit", minute: "2-digit", second: "2-digit", hour12: false,
    }).formatToParts(new Date(hour * 3600000));
    const get = t => Number(parts.find(p => p.type === t).value);
    let h = get("hour");
    if (h === 24) h = 0;
    const asUtc = Date.UTC(get("year"), get("month") - 1, get("day"), h, get("minute"), get("second"));
    off = asUtc - hour * 3600000;
    offsetCache.set(hour, off);
  }
  return off;
}

/** Local wall-clock string for Plotly's date axis. */
export function toPlotlyLocal(v) {
  const ms = toMs(v);
  if (ms == null) return null;
  return new Date(ms + zoneOffsetMs(ms)).toISOString().replace("T", " ").replace("Z", "");
}
