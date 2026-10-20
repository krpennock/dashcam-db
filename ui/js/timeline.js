// The timeline band under the filmstrip: speed and accelerometer magnitude
// against time, event ticks, and the cursor. Click or drag on it to seek.

import { toPlotlyLocal, fromPlotlyLocal } from "./format.js";
import { typeColor } from "./events.js";

const CONFIG = { displayModeBar: false, responsive: true, doubleClick: false, scrollZoom: false };

function baseLayout() {
  return {
    margin: { l: 44, r: 44, t: 10, b: 32 },
    paper_bgcolor: "rgba(0,0,0,0)",
    plot_bgcolor: "rgba(0,0,0,0)",
    font: { color: "#9fb0c0", size: 11 },
    xaxis: { gridcolor: "#253244", zerolinecolor: "#253244", fixedrange: true },
    yaxis: { title: { text: "mph", standoff: 6 }, gridcolor: "#253244", zerolinecolor: "#253244", rangemode: "tozero", fixedrange: true },
    yaxis2: { title: { text: "g", standoff: 6 }, overlaying: "y", side: "right", showgrid: false, rangemode: "tozero", fixedrange: true },
    showlegend: false,
    hovermode: "x",
    dragmode: false,
    shapes: [],
  };
}

export function initTimeline({ el, state }) {
  let ready = false;
  let eventShapes = [];
  let cursorX = null;
  let drawTimer = null;
  let dragging = false;

  const readyPromise = Plotly.newPlot(el, [], baseLayout(), CONFIG).then(() => { ready = true; });

  function shapes() {
    const out = eventShapes.filter(s => state.isTypeVisible(s.__type));
    if (cursorX) {
      out.push({
        type: "line", xref: "x", yref: "paper",
        x0: cursorX, x1: cursorX, y0: 0, y1: 1,
        line: { width: 2, color: "#e6edf3" },
      });
    }
    return out;
  }

  /** Several cursor moves inside 80 ms become one redraw. */
  function scheduleDraw() {
    if (drawTimer) return;
    drawTimer = setTimeout(() => {
      drawTimer = null;
      if (ready) Plotly.relayout(el, { shapes: shapes() }).catch(console.error);
    }, 80);
  }

  function seekTo(x) {
    const ms = fromPlotlyLocal(x);
    if (ms != null) state.setCursor(ms, "timeline");
  }

  readyPromise.then(() => {
    el.on("plotly_click", ev => { const p = ev.points?.[0]; if (p) seekTo(p.x); });
    el.on("plotly_hover", ev => { const p = ev.points?.[0]; if (dragging && p) seekTo(p.x); });
  });
  el.addEventListener("mousedown", e => { if (e.button === 0) dragging = true; });
  window.addEventListener("mouseup", () => { dragging = false; });

  state.addEventListener("load", async () => {
    await readyPromise;
    const xs = state.gnssMs.map(toPlotlyLocal);
    const xa = state.accelMs.map(toPlotlyLocal);
    const traces = [
      {
        x: xs, y: state.gnss.map(g => g.speed_mph), type: "scattergl", mode: "lines", name: "speed",
        line: { color: "#69db7c", width: 1.5 }, hovertemplate: "%{y:.0f} mph<extra></extra>",
      },
      {
        x: xa, y: state.accel.map(a => a.a_mag_raw), type: "scattergl", mode: "lines", name: "accel",
        yaxis: "y2", line: { color: "#4dabf7", width: 1 }, opacity: 0.6, hovertemplate: "%{y:.2f} g<extra></extra>",
      },
    ];
    eventShapes = state.events.map(e => ({
      __type: e.event_type,
      type: "line", xref: "x", yref: "paper",
      x0: toPlotlyLocal(e.ts_utc), x1: toPlotlyLocal(e.ts_utc), y0: 0, y1: 1,
      line: { width: 1.5, dash: "dot", color: typeColor(e.event_type) },
    }));
    cursorX = toPlotlyLocal(state.cursorMs);
    const layout = baseLayout();
    if (state.startMs != null && state.endMs != null) {
      layout.xaxis.range = [toPlotlyLocal(state.startMs), toPlotlyLocal(state.endMs)];
      layout.xaxis.autorange = false;
    }
    layout.shapes = shapes();
    await Plotly.react(el, traces, layout, CONFIG);
  });

  state.addEventListener("clear", async () => {
    await readyPromise;
    eventShapes = [];
    cursorX = null;
    await Plotly.react(el, [], baseLayout(), CONFIG);
  });

  state.addEventListener("cursorchange", e => {
    cursorX = toPlotlyLocal(e.detail.ms);
    scheduleDraw();
  });

  state.addEventListener("eventfilter", scheduleDraw);

  return {
    resize() { if (ready) Plotly.Plots.resize(el); },
  };
}
