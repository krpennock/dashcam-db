// Talking to the API, and telling the user when that fails.

export const API = "/api";

export class ApiError extends Error {
  constructor(status, path, detail) {
    super(detail ? `${status} for ${path}: ${detail}` : `${status} for ${path}`);
    this.status = status;
    this.path = path;
    this.detail = detail;
  }
}

/** Query string from an object; null, undefined and empty values are left out. */
export function qs(params) {
  if (!params) return "";
  const u = new URLSearchParams();
  for (const [k, v] of Object.entries(params)) {
    if (v == null || v === "") continue;
    u.set(k, String(v));
  }
  const s = u.toString();
  return s ? `?${s}` : "";
}

export async function apiGet(path, params) {
  const r = await fetch(API + path + qs(params));
  if (!r.ok) {
    let detail = "";
    try {
      const j = await r.json();
      detail = typeof j?.detail === "string" ? j.detail : "";
    } catch { /* not JSON */ }
    throw new ApiError(r.status, path, detail);
  }
  return r.json();
}

/** A storyboard still's URL. The API hands out "/media/..."; it is served behind the API prefix. */
export function mediaUrl(rel) {
  return rel ? API + rel : "";
}

/** Show a message at the bottom of the page. Errors stay until dismissed. */
export function toast(message, { kind = "error", timeoutMs = kind === "error" ? 0 : 5000 } = {}) {
  let host = document.getElementById("toasts");
  if (!host) {
    host = document.createElement("div");
    host.id = "toasts";
    document.body.appendChild(host);
  }
  const el = document.createElement("div");
  el.className = `toast toast-${kind}`;
  const text = document.createElement("span");
  text.textContent = message;
  const close = document.createElement("button");
  close.type = "button";
  close.textContent = "×";
  close.title = "Dismiss";
  close.onclick = () => el.remove();
  el.append(text, close);
  host.appendChild(el);
  if (timeoutMs > 0) setTimeout(() => el.remove(), timeoutMs);
  return el;
}
