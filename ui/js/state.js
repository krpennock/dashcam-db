// What the viewer knows about the drive on screen, and the one "current
// moment" that the map, the stills and the chart all follow.
//
// Everything a scrub needs is loaded once: the GPS track, the stills, the
// events and the accelerometer series. Finding the sample nearest a moment is
// a binary search over timestamps, so moving the cursor costs nothing.

import { toMs } from "./format.js";

/** First index whose value is >= x in a sorted array. */
export function lowerBound(arr, x) {
  let lo = 0, hi = arr.length;
  while (lo < hi) {
    const mid = (lo + hi) >> 1;
    if (arr[mid] < x) lo = mid + 1; else hi = mid;
  }
  return lo;
}

/** Index of the value closest to x in a sorted array, or -1 when empty. */
export function nearestIndex(arr, x) {
  if (!arr.length) return -1;
  const i = lowerBound(arr, x);
  if (i <= 0) return 0;
  if (i >= arr.length) return arr.length - 1;
  return (x - arr[i - 1]) <= (arr[i] - x) ? i - 1 : i;
}

export class DriveState extends EventTarget {
  constructor() {
    super();
    this._reset();
  }

  _reset() {
    this.session = null;
    this.summary = null;
    this.gnss = []; this.gnssMs = [];
    this.frames = []; this.frameMs = [];
    this.events = [];
    this.accel = []; this.accelMs = [];
    this.startMs = null;
    this.endMs = null;
    this.cursorMs = null;
  }

  get loaded() { return this.session != null; }

  clear() {
    this._reset();
    this.dispatchEvent(new CustomEvent("clear"));
  }

  load({ session, summary = null, gnss = [], frames = [], events = [], accel = [] }) {
    const stamps = rows => rows.map(r => toMs(r.ts_utc));
    this.session = session;
    this.summary = summary;
    this.gnss = gnss.filter(r => r.ts_utc && r.lat != null && r.lon != null);
    this.gnssMs = stamps(this.gnss);
    this.frames = frames.filter(f => f.ts_utc);
    this.frameMs = stamps(this.frames);
    this.events = events.filter(e => e.ts_utc);
    this.accel = accel.filter(r => r.ts_utc);
    this.accelMs = stamps(this.accel);
    this.startMs = toMs(session.start_ts_utc) ?? this.gnssMs[0] ?? null;
    this.endMs = toMs(session.end_ts_utc) ?? this.gnssMs[this.gnssMs.length - 1] ?? null;
    this.cursorMs = this.startMs;
    this.dispatchEvent(new CustomEvent("load"));
  }

  /** Move the current moment. `source` says who moved it, so that widget can skip echoing it. */
  setCursor(ms, source = "unknown") {
    if (!this.loaded || !Number.isFinite(ms)) return;
    if (this.startMs != null) ms = Math.max(this.startMs, ms);
    if (this.endMs != null) ms = Math.min(this.endMs, ms);
    this.cursorMs = ms;
    this.dispatchEvent(new CustomEvent("cursorchange", {
      detail: { ms, iso: new Date(ms).toISOString(), source },
    }));
  }

  nearestGnss(ms) {
    const i = nearestIndex(this.gnssMs, ms);
    return i < 0 ? null : this.gnss[i];
  }

  nearestAccel(ms) {
    const i = nearestIndex(this.accelMs, ms);
    return i < 0 ? null : this.accel[i];
  }

  nearestFrameIndex(ms) {
    return nearestIndex(this.frameMs, ms);
  }

  nearestFrame(ms) {
    const i = this.nearestFrameIndex(ms);
    return i < 0 ? null : this.frames[i];
  }
}
