// The drive's route on the map, coloured by speed, with a heading arrow at
// the cursor and a ring that marks whichever still is being hovered.
//
// The route is built from the GPS samples already loaded for scrubbing: one
// short line per pair of consecutive samples, carrying that moment's speed
// and time, so the colour ramp and hover lookups need nothing else.

import { onStyleReady, removeLayers, EMPTY_FC } from "./basemap.js";

const SRC = "track-src";
const CASING = "track-casing";
const LINE = "track-line";
const CUR_SRC = "cursor-src";
const CUR_DOT = "cursor-dot";
const CUR_ARROW = "cursor-arrow";
const RING_SRC = "ring-src";
const RING = "ring-layer";
const ARROW_IMAGE = "cursor-arrow-img";

/** mph -> colour, shared with the legend in the page. */
export const SPEED_STOPS = [0, "#4dabf7", 20, "#2ec4b6", 40, "#ffd166", 60, "#ff8c42"];

function arrowImage(size = 44) {
  const c = document.createElement("canvas");
  c.width = c.height = size;
  const ctx = c.getContext("2d");
  const m = size / 2;
  ctx.beginPath();
  ctx.moveTo(m, size * 0.08);
  ctx.lineTo(size * 0.82, size * 0.88);
  ctx.lineTo(m, size * 0.68);
  ctx.lineTo(size * 0.18, size * 0.88);
  ctx.closePath();
  ctx.fillStyle = "#ffffff";
  ctx.fill();
  ctx.lineWidth = size * 0.07;
  ctx.strokeStyle = "#0b0f14";
  ctx.lineJoin = "round";
  ctx.stroke();
  const img = ctx.getImageData(0, 0, size, size);
  return { width: size, height: size, data: img.data };
}

export function initTrack(map, state, { onHover, onHoverEnd } = {}) {
  let data = EMPTY_FC;
  let cursor = EMPTY_FC;
  let ring = EMPTY_FC;

  const install = () => {
    removeLayers(map, [LINE, CASING], SRC);
    removeLayers(map, [CUR_ARROW, CUR_DOT], CUR_SRC);
    removeLayers(map, [RING], RING_SRC);
    if (!map.hasImage(ARROW_IMAGE)) map.addImage(ARROW_IMAGE, arrowImage(), { pixelRatio: 2 });

    map.addSource(SRC, { type: "geojson", data });
    map.addLayer({
      id: CASING, type: "line", source: SRC,
      layout: { "line-join": "round", "line-cap": "round" },
      paint: { "line-color": "#0b0f14", "line-width": 7, "line-opacity": 0.6 },
    });
    map.addLayer({
      id: LINE, type: "line", source: SRC,
      layout: { "line-join": "round", "line-cap": "round" },
      paint: {
        "line-width": 4,
        "line-color": ["interpolate", ["linear"], ["coalesce", ["get", "speed"], 0], ...SPEED_STOPS],
      },
    });

    map.addSource(RING_SRC, { type: "geojson", data: ring });
    map.addLayer({
      id: RING, type: "circle", source: RING_SRC,
      paint: { "circle-radius": 11, "circle-opacity": 0, "circle-stroke-width": 2.5, "circle-stroke-color": "#ffffff" },
    });

    map.addSource(CUR_SRC, { type: "geojson", data: cursor });
    map.addLayer({
      id: CUR_DOT, type: "circle", source: CUR_SRC,
      filter: ["!", ["get", "hasCourse"]],
      paint: { "circle-radius": 7, "circle-color": "#ffffff", "circle-stroke-width": 2, "circle-stroke-color": "#0b0f14" },
    });
    map.addLayer({
      id: CUR_ARROW, type: "symbol", source: CUR_SRC,
      filter: ["get", "hasCourse"],
      layout: {
        "icon-image": ARROW_IMAGE,
        "icon-size": 1,
        "icon-rotate": ["get", "course"],
        "icon-rotation-alignment": "map",
        "icon-allow-overlap": true,
        "icon-ignore-placement": true,
      },
    });
  };
  onStyleReady(map, install);

  if (onHover) {
    map.on("mousemove", LINE, e => {
      const f = e.features?.[0];
      if (f) onHover(Number(f.properties.ts), e.point);
    });
    map.on("mouseleave", LINE, () => onHoverEnd && onHoverEnd());
  }

  const setSource = (id, fc) => { const s = map.getSource(id); if (s) s.setData(fc); };

  function fit() {
    if (!state.gnss.length) return;
    let minLon = Infinity, maxLon = -Infinity, minLat = Infinity, maxLat = -Infinity;
    for (const g of state.gnss) {
      if (g.lon < minLon) minLon = g.lon;
      if (g.lon > maxLon) maxLon = g.lon;
      if (g.lat < minLat) minLat = g.lat;
      if (g.lat > maxLat) maxLat = g.lat;
    }
    map.fitBounds([[minLon, minLat], [maxLon, maxLat]], { padding: 48, duration: 400 });
  }

  function setCursorPoint(g) {
    if (!g) { cursor = EMPTY_FC; setSource(CUR_SRC, cursor); return; }
    const hasCourse = g.course_deg != null && (g.speed_mph ?? 0) >= 2;
    cursor = {
      type: "FeatureCollection",
      features: [{
        type: "Feature",
        geometry: { type: "Point", coordinates: [g.lon, g.lat] },
        properties: { hasCourse, course: hasCourse ? g.course_deg : 0 },
      }],
    };
    setSource(CUR_SRC, cursor);
  }

  /** Mark a spot (the hovered still), or clear with null. */
  function setRing(lon, lat) {
    ring = (lon == null || lat == null) ? EMPTY_FC : {
      type: "FeatureCollection",
      features: [{ type: "Feature", geometry: { type: "Point", coordinates: [lon, lat] }, properties: {} }],
    };
    setSource(RING_SRC, ring);
  }

  state.addEventListener("load", () => {
    const g = state.gnss, ms = state.gnssMs;
    const features = [];
    for (let i = 1; i < g.length; i++) {
      const a = g[i - 1], b = g[i];
      const speed = (a.speed_mph != null && b.speed_mph != null) ? (a.speed_mph + b.speed_mph) / 2 : (b.speed_mph ?? a.speed_mph ?? null);
      features.push({
        type: "Feature",
        geometry: { type: "LineString", coordinates: [[a.lon, a.lat], [b.lon, b.lat]] },
        properties: { speed, ts: ms[i] },
      });
    }
    data = { type: "FeatureCollection", features };
    setSource(SRC, data);
    setRing(null);
    fit();
    setCursorPoint(g[0] || null);
  });

  state.addEventListener("clear", () => {
    data = EMPTY_FC;
    setSource(SRC, data);
    setCursorPoint(null);
    setRing(null);
  });

  state.addEventListener("cursorchange", e => {
    setCursorPoint(state.nearestGnss(e.detail.ms));
  });

  return { fit, setRing };
}
