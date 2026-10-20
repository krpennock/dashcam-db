// The header every page shares.

export const VERSION = "v0.4";

const PAGES = [
  { id: "viewer", label: "Viewer", href: "index.html" },
  { id: "overview", label: "Overview", href: "overview.html" },
  { id: "imports", label: "Imports", href: "imports.html" },
];

/** Fill the page's <header>. `status` is the text on the right; pages may update #statusLine later. */
export function renderNav(current, title, { status = `API: ${location.origin}/api` } = {}) {
  const header = document.querySelector("header");
  if (!header) return;
  const links = PAGES.map(p =>
    p.id === current
      ? `<a href="${p.href}" class="current" aria-current="page">${p.label}</a>`
      : `<a href="${p.href}">${p.label}</a>`
  ).join(`<span class="sep">|</span>`);
  header.innerHTML = `
    <div class="title">${title} <span class="pill">${VERSION}</span></div>
    <nav class="nav">${links}</nav>
    <div class="muted" id="statusLine">${status}</div>
  `;
}
