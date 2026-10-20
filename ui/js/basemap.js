// The map itself: basemap styles, the switch between them, and a way for the
// other modules to put their layers back whenever the style changes.
//
// MapLibre throws away every source and layer on setStyle(). Each module that
// draws on the map registers an installer with onStyleReady(); it runs now if
// the style is ready and again after every style change, so a basemap switch
// never loses the track, the events or the stills.

const OSM_ATTR = '&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> contributors';

// Both styles take their label fonts from the same place, so a layer that
// draws text (the cluster counts) can name one font and work on either.
export const GLYPHS = "https://tiles.openfreemap.org/fonts/{fontstack}/{range}.pbf";
export const LABEL_FONT = ["Noto Sans Regular"];

function rasterStyle(tiles, attribution) {
  return {
    version: 8,
    glyphs: GLYPHS,
    sources: {
      base: { type: "raster", tiles, tileSize: 256, maxzoom: 19, attribution },
    },
    layers: [{ id: "base", type: "raster", source: "base" }],
  };
}

export const BASEMAPS = {
  // OpenFreeMap: vector tiles, free, no key, OpenStreetMap data. The style
  // carries its own attribution.
  dark: {
    label: "Dark",
    style: "https://tiles.openfreemap.org/styles/dark",
  },
  streets: {
    label: "Streets",
    style: rasterStyle(["https://tile.openstreetmap.org/{z}/{x}/{y}.png"], OSM_ATTR),
  },
};

const STORAGE_KEY = "dashcam.basemap";

export function currentBasemap() {
  try {
    const v = localStorage.getItem(STORAGE_KEY);
    if (v && v in BASEMAPS) return v;
  } catch { /* storage blocked */ }
  return "dark";
}

export function setBasemap(map, name) {
  if (!(name in BASEMAPS) || name === map.__basemap) return;
  map.__basemap = name;
  try { localStorage.setItem(STORAGE_KEY, name); } catch { /* storage blocked */ }
  // diff:false makes MapLibre load the new style from scratch, which is the
  // only path that fires "style.load" and so re-runs the layer installers.
  map.setStyle(BASEMAPS[name].style, { diff: false });
}

class BasemapControl {
  onAdd(map) {
    this._map = map;
    const box = document.createElement("div");
    box.className = "maplibregl-ctrl maplibregl-ctrl-group";
    const btn = document.createElement("button");
    btn.type = "button";
    btn.className = "basemap-btn";
    btn.title = "Switch basemap (dark / streets)";
    btn.setAttribute("aria-label", "Switch basemap");
    btn.textContent = "◐";
    btn.addEventListener("click", () => {
      setBasemap(map, currentBasemap() === "dark" ? "streets" : "dark");
    });
    box.appendChild(btn);
    this._box = box;
    return box;
  }
  onRemove() {
    this._box.remove();
    this._map = null;
  }
}

function runSafely(fn) {
  try { fn(); } catch (e) { console.error("map layer installer failed", e); }
}

export function createMap(container, { center = [-97.12, 33.16], zoom = 11 } = {}) {
  const name = currentBasemap();
  const map = new maplibregl.Map({
    container,
    style: BASEMAPS[name].style,
    center,
    zoom,
  });
  map.__basemap = name;
  map.__installers = [];
  map.on("style.load", () => {
    for (const fn of map.__installers) runSafely(fn);
  });
  // The OpenFreeMap style names a pattern its sprite does not carry; supply a
  // blank one rather than let it warn on every load.
  map.on("styleimagemissing", e => {
    if (!map.hasImage(e.id)) map.addImage(e.id, { width: 1, height: 1, data: new Uint8Array(4) });
  });
  map.addControl(new maplibregl.NavigationControl(), "top-right");
  map.addControl(new BasemapControl(), "top-right");
  return map;
}

/** Register a function that (re)adds sources and layers. It must be safe to run more than once. */
export function onStyleReady(map, install) {
  map.__installers.push(install);
  if (map.isStyleLoaded()) runSafely(install);
  return () => {
    const i = map.__installers.indexOf(install);
    if (i >= 0) map.__installers.splice(i, 1);
  };
}

/** Remove layers and a source if they exist, so an installer can start clean. */
export function removeLayers(map, layerIds, sourceId) {
  for (const id of layerIds) if (map.getLayer(id)) map.removeLayer(id);
  if (sourceId && map.getSource(sourceId)) map.removeSource(sourceId);
}

export const EMPTY_FC = Object.freeze({ type: "FeatureCollection", features: [] });
