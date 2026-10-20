// The stills: clustered dots on the map, the thumbnail grid, and the preview
// that follows the cursor.

import { onStyleReady, removeLayers, EMPTY_FC, LABEL_FONT } from "./basemap.js";
import { mediaUrl } from "./api.js";
import { escapeHtml, fmtClock, fmtUtc, fmtMph } from "./format.js";

const SRC = "story-src";
const CLUSTERS = "story-clusters";
const COUNTS = "story-cluster-count";
const POINTS = "story-points";

/** Scroll `node` into view inside `container` without moving anything else on the page. */
function scrollWithin(container, node) {
  const top = node.offsetTop - container.offsetTop;
  const bottom = top + node.offsetHeight;
  if (top < container.scrollTop) container.scrollTop = top;
  else if (bottom > container.scrollTop + container.clientHeight) container.scrollTop = bottom - container.clientHeight;
}

export function initStoryboard({ map, state, els, onReload }) {
  let data = EMPTY_FC;
  let popup = null;
  let activeIndex = -1;

  const install = () => {
    removeLayers(map, [POINTS, COUNTS, CLUSTERS], SRC);
    map.addSource(SRC, {
      type: "geojson", data,
      cluster: true, clusterRadius: 40, clusterMaxZoom: 15,
    });
    map.addLayer({
      id: CLUSTERS, type: "circle", source: SRC,
      filter: ["has", "point_count"],
      paint: {
        "circle-color": "#365070",
        "circle-opacity": 0.85,
        "circle-stroke-width": 1,
        "circle-stroke-color": "#9fb0c0",
        "circle-radius": ["step", ["get", "point_count"], 11, 10, 14, 50, 18],
      },
    });
    map.addLayer({
      id: COUNTS, type: "symbol", source: SRC,
      filter: ["has", "point_count"],
      layout: {
        "text-field": ["get", "point_count_abbreviated"],
        "text-font": LABEL_FONT,
        "text-size": 11,
      },
      paint: { "text-color": "#e6edf3" },
    });
    map.addLayer({
      id: POINTS, type: "circle", source: SRC,
      filter: ["!", ["has", "point_count"]],
      paint: {
        "circle-radius": 4,
        "circle-color": "#9fb0c0",
        "circle-opacity": 0.9,
        "circle-stroke-width": 1,
        "circle-stroke-color": "#0b0f14",
      },
    });
  };
  onStyleReady(map, install);

  // Layer event handlers are keyed by layer id, so they survive the layer
  // being removed and re-added on a basemap switch.
  map.on("click", CLUSTERS, e => {
    const f = e.features?.[0];
    const src = map.getSource(SRC);
    if (!f || !src) return;
    src.getClusterExpansionZoom(f.properties.cluster_id, (err, zoom) => {
      if (err) return;
      map.easeTo({ center: f.geometry.coordinates, zoom });
    });
  });
  map.on("click", POINTS, e => {
    const f = e.features?.[0];
    if (!f) return;
    select(Number(f.properties.index), { popup: true });
  });
  for (const id of [CLUSTERS, POINTS]) {
    map.on("mouseenter", id, () => { map.getCanvas().style.cursor = "pointer"; });
    map.on("mouseleave", id, () => { map.getCanvas().style.cursor = ""; });
  }

  function setData(fc) {
    data = fc;
    const src = map.getSource(SRC);
    if (src) src.setData(fc);
  }

  function select(index, { popup: showPopup = false } = {}) {
    const f = state.frames[index];
    if (!f) return;
    state.setCursor(state.frameMs[index], "storyboard");
    if (showPopup) showPopupFor(f);
  }

  function showPopupFor(f) {
    if (popup) { popup.remove(); popup = null; }
    if (f.lon == null || f.lat == null || !f.url) return;
    const html = `
      <div class="story-popup">
        <div class="t mono" title="${fmtUtc(f.ts_utc)}">${escapeHtml(fmtClock(f.ts_utc, { seconds: true }))}</div>
        <div class="muted">${escapeHtml(fmtMph(f.speed_mph))}</div>
        <img src="${mediaUrl(f.url)}" alt="" />
      </div>`;
    popup = new maplibregl.Popup({ closeButton: true, closeOnClick: true, maxWidth: "270px" })
      .setLngLat([f.lon, f.lat])
      .setHTML(html)
      .addTo(map);
    map.easeTo({ center: [f.lon, f.lat], duration: 200 });
  }

  function setPreview(f) {
    if (!f?.url) {
      els.previewEmpty.style.display = "block";
      els.previewImg.style.display = "none";
      els.previewImg.removeAttribute("src");
      els.previewTs.textContent = "—";
      els.previewTs.removeAttribute("title");
      els.openLink.style.display = "none";
      return;
    }
    const url = mediaUrl(f.url);
    els.previewEmpty.style.display = "none";
    els.previewImg.style.display = "block";
    if (els.previewImg.getAttribute("src") !== url) els.previewImg.src = url;
    els.previewTs.textContent = `${fmtClock(f.ts_utc, { seconds: true })} · ${fmtMph(f.speed_mph)}`;
    els.previewTs.title = fmtUtc(f.ts_utc);
    els.openLink.style.display = "inline";
    els.openLink.href = url;
  }

  function highlight(index) {
    if (index === activeIndex) return;
    els.grid.querySelector(".thumb.active")?.classList.remove("active");
    const node = els.grid.children[index];
    if (node) {
      node.classList.add("active");
      scrollWithin(els.grid, node);
    }
    activeIndex = index;
  }

  function renderGrid() {
    els.grid.innerHTML = state.frames.map((f, i) => `
      <div class="thumb" data-index="${i}">
        <img loading="lazy" src="${mediaUrl(f.url)}" alt="" />
        <div class="cap" title="${fmtUtc(f.ts_utc)}">${escapeHtml(fmtClock(f.ts_utc, { seconds: true }))}</div>
      </div>`).join("");
  }

  els.grid.addEventListener("click", e => {
    const node = e.target.closest(".thumb[data-index]");
    if (!node) return;
    select(Number(node.dataset.index), { popup: true });
  });

  if (els.reloadBtn && onReload) {
    els.reloadBtn.addEventListener("click", () => onReload());
  }

  state.addEventListener("load", () => {
    activeIndex = -1;
    const n = state.frames.length;
    els.count.textContent = `(${n})`;
    els.status.textContent = n ? "loaded" : "no storyboard frames for this session";
    if (els.reloadBtn) els.reloadBtn.disabled = false;
    renderGrid();
    setData({
      type: "FeatureCollection",
      features: state.frames
        .map((f, i) => ({ f, i }))
        .filter(({ f }) => f.lon != null && f.lat != null && f.url)
        .map(({ f, i }) => ({
          type: "Feature",
          geometry: { type: "Point", coordinates: [Number(f.lon), Number(f.lat)] },
          properties: { index: i },
        })),
    });
    setPreview(state.frames[0] || null);
    if (n) highlight(0);
  });

  state.addEventListener("clear", () => {
    if (popup) { popup.remove(); popup = null; }
    activeIndex = -1;
    setData(EMPTY_FC);
    els.grid.innerHTML = "";
    els.count.textContent = "—";
    els.status.textContent = "—";
    if (els.reloadBtn) els.reloadBtn.disabled = true;
    setPreview(null);
  });

  state.addEventListener("cursorchange", e => {
    const i = state.nearestFrameIndex(e.detail.ms);
    if (i < 0) return;
    highlight(i);
    setPreview(state.frames[i]);
  });

  return { select };
}
