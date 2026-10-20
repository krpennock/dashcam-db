// The list of drives and its filters.

import { apiGet, toast } from "./api.js";
import { fmtLocal, fmtUtc, fmtDurationShort, fmtMi, escapeHtml } from "./format.js";

export function initDrives({ els, onSelect }) {
  let all = [];
  let sortCol = "start_ts_utc";
  let sortAsc = false; // newest first
  let activeSid = null;

  function sorted() {
    return [...all].sort((a, b) => {
      let va = a[sortCol], vb = b[sortCol];
      if (va == null && vb == null) return 0;
      if (va == null) return 1;
      if (vb == null) return -1;
      if (typeof va === "string") { va = va.toLowerCase(); vb = String(vb).toLowerCase(); }
      const cmp = va < vb ? -1 : va > vb ? 1 : 0;
      return sortAsc ? cmp : -cmp;
    });
  }

  function render() {
    const list = sorted();
    els.body.innerHTML = list.map(s => `
      <tr data-sid="${s.drive_session_id}"${s.drive_session_id === activeSid ? ' class="active"' : ""}>
        <td>${escapeHtml(s.vehicle_tag || "—")}</td>
        <td title="${fmtUtc(s.start_ts_utc)}">${fmtLocal(s.start_ts_utc)}</td>
        <td>${fmtDurationShort(s.duration_s)}</td>
        <td>${fmtMi(s.distance_mi)}</td>
      </tr>`).join("");
    els.status.textContent = `${list.length} drive${list.length === 1 ? "" : "s"}`;
  }

  async function refresh() {
    const params = { limit: 5000, vehicle: els.vehicle.value };
    if (els.from.value) params.after = `${els.from.value}T00:00:00Z`;
    if (els.to.value) params.before = `${els.to.value}T23:59:59Z`;
    els.status.textContent = "Loading…";
    all = await apiGet("/sessions", params);
    render();
  }

  async function loadVehicles() {
    try {
      const vehicles = await apiGet("/vehicles");
      els.vehicle.innerHTML = `<option value="">All</option>` +
        vehicles.map(v => `<option value="${escapeHtml(v)}">${escapeHtml(v)}</option>`).join("");
    } catch { /* keep "All" */ }
  }

  function highlight(sid) {
    activeSid = sid;
    for (const tr of els.body.querySelectorAll("tr")) {
      tr.classList.toggle("active", tr.dataset.sid === sid);
    }
    const tr = els.body.querySelector(`tr.active`);
    if (tr) tr.scrollIntoView({ block: "nearest" });
  }

  els.body.addEventListener("click", e => {
    const tr = e.target.closest("tr[data-sid]");
    if (!tr) return;
    highlight(tr.dataset.sid);
    onSelect(tr.dataset.sid);
  });

  for (const th of els.table.querySelectorAll("th[data-col]")) {
    th.addEventListener("click", () => {
      const col = th.dataset.col;
      if (sortCol === col) sortAsc = !sortAsc; else { sortCol = col; sortAsc = true; }
      render();
    });
  }

  const onFilter = () => refresh().catch(err => toast(err.message));
  els.vehicle.addEventListener("change", onFilter);
  els.from.addEventListener("change", onFilter);
  els.to.addEventListener("change", onFilter);

  return {
    refresh,
    highlight,
    async init() {
      await loadVehicles();
      await refresh();
    },
  };
}
