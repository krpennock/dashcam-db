// The stills: a filmstrip with playback under the map, a picture-in-picture
// of the current still on the map, hover previews, and clustered dots.

import { onStyleReady, removeLayers, EMPTY_FC, LABEL_FONT } from "./basemap.js";
import { mediaUrl } from "./api.js";
import { escapeHtml, fmtClock, fmtUtc, fmtMph } from "./format.js";

const SRC = "story-src";
const CLUSTERS = "story-clusters";
const COUNTS = "story-cluster-count";
const POINTS = "story-points";

const RATE_MS = { 1: 500, 2: 250, 4: 125 };

function scrollIntoStrip(strip, node) {
  const left = node.offsetLeft - strip.offsetLeft;
  const right = left + node.offsetWidth;
  const want = left - (strip.clientWidth - node.offsetWidth) / 2;
  if (left < strip.scrollLeft || right > strip.scrollLeft + strip.clientWidth) {
    strip.scrollTo({ left: Math.max(0, want), behavior: "smooth" });
  }
}

export function initStoryboard({ map, state, els, track }) {
  let data = EMPTY_FC;
  let activeIndex = -1;
  let timer = null;

  // --- Map dots ---

  const install = () => {
    removeLayers(map, [POINTS, COUNTS, CLUSTERS], SRC);
    map.addSource(SRC, { type: "geojson", data, cluster: true, clusterRadius: 40, clusterMaxZoom: 15 });
    map.addLayer({
      id: CLUSTERS, type: "circle", source: SRC,
      filter: ["has", "point_count"],
      paint: {
        "circle-color": "#365070", "circle-opacity": 0.85,
        "circle-stroke-width": 1, "circle-stroke-color": "#9fb0c0",
        "circle-radius": ["step", ["get", "point_count"], 11, 10, 14, 50, 18],
      },
    });
    map.addLayer({
      id: COUNTS, type: "symbol", source: SRC,
      filter: ["has", "point_count"],
      layout: { "text-field": ["get", "point_count_abbreviated"], "text-font": LABEL_FONT, "text-size": 11 },
      paint: { "text-color": "#e6edf3" },
    });
    map.addLayer({
      id: POINTS, type: "circle", source: SRC,
      filter: ["!", ["has", "point_count"]],
      paint: {
        "circle-radius": 4, "circle-color": "#9fb0c0", "circle-opacity": 0.9,
        "circle-stroke-width": 1, "circle-stroke-color": "#0b0f14",
      },
    });
  };
  onStyleReady(map, install);

  map.on("click", CLUSTERS, e => {
    const f = e.features?.[0];
    const src = map.getSource(SRC);
    if (!f || !src) return;
    src.getClusterExpansionZoom(f.properties.cluster_id, (err, zoom) => {
      if (!err) map.easeTo({ center: f.geometry.coordinates, zoom });
    });
  });
  map.on("click", POINTS, e => {
    const f = e.features?.[0];
    if (f) select(Number(f.properties.index), "storyboard");
  });
  map.on("mousemove", POINTS, e => {
    const f = e.features?.[0];
    if (f) showHover(Number(f.properties.index), e.point);
  });
  map.on("mouseleave", POINTS, hideHover);
  for (const id of [CLUSTERS, POINTS]) {
    map.on("mouseenter", id, () => { map.getCanvas().style.cursor = "pointer"; });
    map.on("mouseleave", id, () => { map.getCanvas().style.cursor = ""; });
  }

  function setData(fc) {
    data = fc;
    const src = map.getSource(SRC);
    if (src) src.setData(fc);
  }

  // --- Selection and playback ---

  function select(index, source = "storyboard") {
    if (index < 0 || index >= state.frames.length) return;
    state.setCursor(state.frameMs[index], source);
  }

  function step(delta) {
    stop();
    const base = activeIndex < 0 ? 0 : activeIndex;
    select(Math.max(0, Math.min(state.frames.length - 1, base + delta)), "transport");
  }

  function play() {
    if (timer || !state.frames.length) return;
    if (activeIndex >= state.frames.length - 1) select(0, "transport");
    els.btnPlay.textContent = "❚❚";
    els.btnPlay.title = "Pause (Space)";
    const tick = () => {
      const next = activeIndex + 1;
      if (next >= state.frames.length) { stop(); return; }
      select(next, "playback");
      timer = setTimeout(tick, RATE_MS[els.rate.value] || 500);
    };
    timer = setTimeout(tick, RATE_MS[els.rate.value] || 500);
  }

  function stop() {
    if (timer) { clearTimeout(timer); timer = null; }
    els.btnPlay.textContent = "▶";
    els.btnPlay.title = "Play (Space)";
  }

  function toggle() { if (timer) stop(); else play(); }

  els.btnPlay.addEventListener("click", toggle);
  els.btnPrev.addEventListener("click", () => step(-1));
  els.btnNext.addEventListener("click", () => step(1));
  els.btnFirst.addEventListener("click", () => { stop(); select(0, "transport"); });
  els.btnLast.addEventListener("click", () => { stop(); select(state.frames.length - 1, "transport"); });

  document.addEventListener("keydown", e => {
    const tag = (e.target?.tagName || "").toLowerCase();
    if (tag === "input" || tag === "select" || tag === "textarea") return;
    if (e.ctrlKey || e.metaKey || e.altKey) return;
    switch (e.key) {
      case " ": e.preventDefault(); toggle(); break;
      case "ArrowLeft": e.preventDefault(); step(-1); break;
      case "ArrowRight": e.preventDefault(); step(1); break;
      case "Home": e.preventDefault(); stop(); select(0, "transport"); break;
      case "End": e.preventDefault(); stop(); select(state.frames.length - 1, "transport"); break;
      default: return;
    }
  });

  // --- Picture in picture ---

  function setPip(f) {
    if (!f?.url) { els.pip.hidden = true; return; }
    const url = mediaUrl(f.url);
    els.pip.hidden = false;
    if (els.pipImg.getAttribute("src") !== url) els.pipImg.src = url;
    els.pipTime.textContent = fmtClock(f.ts_utc, { seconds: true });
    els.pipTime.title = fmtUtc(f.ts_utc);
    els.pipSpeed.textContent = fmtMph(f.speed_mph);
    els.pipOpen.href = url;
  }

  // --- Hover card (over the map) ---

  function showHover(index, point) {
    const f = state.frames[index];
    if (!f?.url) { hideHover(); return; }
    const url = mediaUrl(f.url);
    if (els.hoverImg.getAttribute("src") !== url) els.hoverImg.src = url;
    els.hoverCap.textContent = `${fmtClock(f.ts_utc, { seconds: true })} · ${fmtMph(f.speed_mph)}`;
    els.hoverCard.hidden = false;
    const box = els.hoverCard.parentElement.getBoundingClientRect();
    const w = els.hoverCard.offsetWidth || 260, h = els.hoverCard.offsetHeight || 180;
    let x = point.x + 16, y = point.y + 16;
    if (x + w > box.width - 8) x = point.x - w - 16;
    if (y + h > box.height - 8) y = point.y - h - 16;
    els.hoverCard.style.transform = `translate(${Math.max(0, x)}px, ${Math.max(0, y)}px)`;
  }

  function hideHover() { els.hoverCard.hidden = true; }

  /** For the route: the still nearest a moment, shown at a map pixel. */
  function hoverAt(ms, point) {
    const i = state.nearestFrameIndex(ms);
    if (i >= 0) showHover(i, point);
  }

  // --- Filmstrip ---

  function highlight(index) {
    if (index === activeIndex) return;
    els.strip.querySelector(".frame.active")?.classList.remove("active");
    const node = els.strip.children[index];
    if (node) {
      node.classList.add("active");
      scrollIntoStrip(els.strip, node);
    }
    activeIndex = index;
  }

  function renderStrip() {
    els.strip.innerHTML = state.frames.map((f, i) => `
      <div class="frame" data-index="${i}" title="${fmtUtc(f.ts_utc)}">
        <img loading="lazy" src="${mediaUrl(f.url)}" alt="" />
        <div class="cap">${escapeHtml(fmtClock(f.ts_utc, { seconds: true }))}</div>
      </div>`).join("");
  }

  els.strip.addEventListener("click", e => {
    const node = e.target.closest(".frame[data-index]");
    if (node) { stop(); select(Number(node.dataset.index), "filmstrip"); }
  });
  els.strip.addEventListener("mouseover", e => {
    const node = e.target.closest(".frame[data-index]");
    if (!node) return;
    const f = state.frames[Number(node.dataset.index)];
    if (f) track.setRing(f.lon, f.lat);
  });
  els.strip.addEventListener("mouseleave", () => track.setRing(null, null));

  // --- State ---

  state.addEventListener("load", () => {
    stop();
    activeIndex = -1;
    const n = state.frames.length;
    els.count.textContent = n ? `${n} stills` : "no stills";
    renderStrip();
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
    for (const b of [els.btnPlay, els.btnPrev, els.btnNext, els.btnFirst, els.btnLast]) b.disabled = !n;
    setPip(null);
  });

  state.addEventListener("clear", () => {
    stop();
    activeIndex = -1;
    setData(EMPTY_FC);
    els.strip.innerHTML = "";
    els.count.textContent = "—";
    setPip(null);
    hideHover();
    for (const b of [els.btnPlay, els.btnPrev, els.btnNext, els.btnFirst, els.btnLast]) b.disabled = true;
  });

  state.addEventListener("cursorchange", e => {
    const i = state.nearestFrameIndex(e.detail.ms);
    if (i < 0) return;
    highlight(i);
    setPip(state.frames[i]);
  });

  return { select, hoverAt, hideHover, stop };
}
