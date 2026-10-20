// The drive's route on the map and the marker that follows the cursor.
//
// The route is built from the GPS samples already loaded for scrubbing, so
// no separate track request is needed.

import { onStyleReady, removeLayers, EMPTY_FC } from "./basemap.js";

const SRC = "track-src";
const CASING = "track-casing";
const LINE = "track-line";

export function initTrack(map, state) {
  const marker = new maplibregl.Marker({ draggable: false });
  let data = EMPTY_FC;
  let onMap = false;

  const install = () => {
    removeLayers(map, [LINE, CASING], SRC);
    map.addSource(SRC, { type: "geojson", data });
    map.addLayer({
      id: CASING, type: "line", source: SRC,
      layout: { "line-join": "round", "line-cap": "round" },
      paint: { "line-color": "#0b0f14", "line-width": 6, "line-opacity": 0.55 },
    });
    map.addLayer({
      id: LINE, type: "line", source: SRC,
      layout: { "line-join": "round", "line-cap": "round" },
      paint: { "line-color": "#4dabf7", "line-width": 3.5 },
    });
  };
  onStyleReady(map, install);

  function setData(fc) {
    data = fc;
    const src = map.getSource(SRC);
    if (src) src.setData(fc);
  }

  function fit() {
    const coords = data.features[0]?.geometry?.coordinates || [];
    if (coords.length < 2) return;
    let minLon = Infinity, maxLon = -Infinity, minLat = Infinity, maxLat = -Infinity;
    for (const [lon, lat] of coords) {
      if (lon < minLon) minLon = lon;
      if (lon > maxLon) maxLon = lon;
      if (lat < minLat) minLat = lat;
      if (lat > maxLat) maxLat = lat;
    }
    map.fitBounds([[minLon, minLat], [maxLon, maxLat]], { padding: 40, duration: 400 });
  }

  state.addEventListener("load", () => {
    const coords = state.gnss.map(g => [g.lon, g.lat]);
    setData(coords.length >= 2
      ? { type: "FeatureCollection", features: [{ type: "Feature", geometry: { type: "LineString", coordinates: coords }, properties: {} }] }
      : EMPTY_FC);
    fit();
    if (coords.length) {
      marker.setLngLat(coords[0]);
      if (!onMap) { marker.addTo(map); onMap = true; }
    }
  });

  state.addEventListener("clear", () => {
    setData(EMPTY_FC);
    if (onMap) { marker.remove(); onMap = false; }
  });

  state.addEventListener("cursorchange", e => {
    const g = state.nearestGnss(e.detail.ms);
    if (g) marker.setLngLat([g.lon, g.lat]);
  });

  return { marker, fit };
}
