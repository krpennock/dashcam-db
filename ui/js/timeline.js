// The accelerometer chart under the map, with the cursor and event ticks.

import { toPlotlyLocal } from "./format.js";
import { typeColor } from "./events.js";

const CONFIG = { displayModeBar: false, responsive: true };

function baseLayout() {
  return {
    margin: { l: 50, r: 20, t: 16, b: 45 },
    paper_bgcolor: "rgba(0,0,0,0)",
    plot_bgcolor: "rgba(0,0,0,0)",
    font: { color: "#9fb0c0", size: 11 },
    xaxis: { title: { text: "Time (local)" }, gridcolor: "#253244", zerolinecolor: "#253244" },
    yaxis: { title: { text: "Accel (g)" }, gridcolor: "#253244", zerolinecolor: "#253244" },
    legend: { bgcolor: "rgba(0,0,0,0)" },
    showlegend: true,
    shapes: [],
  };
}

export function initChart({ el, state, toggleEvents }) {
  let ready = false;
  let eventShapes = [];
  let cursorX = null;
  let drawTimer = null;

  const readyPromise = Plotly.newPlot(el, [], baseLayout(), CONFIG).then(() => { ready = true; });

  function shapes() {
    const out = toggleEvents.checked ? [...eventShapes] : [];
    if (cursorX) {
      out.push({
        type: "line", xref: "x", yref: "paper",
        x0: cursorX, x1: cursorX, y0: 0, y1: 1,
        line: { width: 2, color: "#e6edf3" },
      });
    }
    return out;
  }

  /** Redraw the shapes soon. Several cursor moves inside 80 ms become one redraw. */
  function scheduleDraw() {
    if (drawTimer) return;
    drawTimer = setTimeout(() => {
      drawTimer = null;
      if (ready) Plotly.relayout(el, { shapes: shapes() }).catch(console.error);
    }, 80);
  }

  toggleEvents.addEventListener("change", scheduleDraw);

  state.addEventListener("load", async () => {
    await readyPromise;
    const x = state.accelMs.map(toPlotlyLocal);
    const col = k => state.accel.map(p => p[k]);
    const traces = [
      { x, y: col("a_mag_raw"), mode: "lines", name: "magnitude", type: "scattergl", line: { color: "#4dabf7", width: 1 } },
      { x, y: col("ax"), mode: "lines", name: "ax", type: "scattergl", visible: "legendonly", line: { width: 1 } },
      { x, y: col("ay"), mode: "lines", name: "ay", type: "scattergl", visible: "legendonly", line: { width: 1 } },
      { x, y: col("az"), mode: "lines", name: "az", type: "scattergl", visible: "legendonly", line: { width: 1 } },
    ];
    eventShapes = state.events.map(e => ({
      type: "line", xref: "x", yref: "paper",
      x0: toPlotlyLocal(e.ts_utc), x1: toPlotlyLocal(e.ts_utc), y0: 0, y1: 1,
      line: { width: 1, dash: "dot", color: typeColor(e.event_type) },
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
}
