// The collapsible side panel and its tabs, shared by the pages that have one.

const KEY = "dashcam.drawer";

export function initDrawer({ app, onResize }) {
  const toggle = app.querySelector("#drawerToggle");
  const expand = app.querySelector("#drawerExpand");
  const tabs = app.querySelector(".drawer-tabs");

  function setOpen(open) {
    app.classList.toggle("collapsed", !open);
    try { localStorage.setItem(KEY, open ? "open" : "closed"); } catch { /* storage blocked */ }
    if (onResize) requestAnimationFrame(onResize);
  }

  function showTab(name) {
    for (const b of app.querySelectorAll(".drawer-tabs .tab")) b.classList.toggle("active", b.dataset.tab === name);
    for (const p of app.querySelectorAll(".tab-panel")) p.classList.toggle("active", p.id === `tab-${name}`);
  }

  toggle?.addEventListener("click", () => setOpen(false));
  expand?.addEventListener("click", () => setOpen(true));
  tabs?.addEventListener("click", e => {
    const b = e.target.closest(".tab[data-tab]");
    if (b) showTab(b.dataset.tab);
  });
  try { if (localStorage.getItem(KEY) === "closed") setOpen(false); } catch { /* storage blocked */ }

  return { setOpen, showTab };
}
