// @ts-check
// What every module of the studio's page shares: the session's token, the API, and building nodes.
// Every value the page shows comes from a job or a source, so it is set as text, never as HTML: a
// script slipped into a sampled value would run dtpipe with the person's own access.

const TOKEN_KEY = "dtpipe-studio-token";
export const $ = (id) => document.getElementById(id);

// The token arrives in the fragment, which no request carries, and leaves the address bar at once.
export const token = (() => {
  const fromUrl = location.hash.slice(1);
  if (fromUrl) {
    sessionStorage.setItem(TOKEN_KEY, fromUrl);
    history.replaceState(null, "", location.pathname);
    return fromUrl;
  }
  return sessionStorage.getItem(TOKEN_KEY) || "";
})();

export function forgetToken() { sessionStorage.removeItem(TOKEN_KEY); }

// An element: props set as attributes ("class" as its class, "on…" as listeners, a boolean as a
// present-or-absent attribute), children appended as nodes or as text.
export function el(tag, props = {}, ...children) {
  const node = document.createElement(tag);
  for (const [k, v] of Object.entries(props)) {
    if (v === undefined || v === null || v === false) continue;
    if (k === "class") node.className = v;
    else if (k.startsWith("on")) node.addEventListener(k.slice(2), v);
    else if (k === "value") node.value = v;
    else node.setAttribute(k, v === true ? "" : v);
  }
  append(node, children);
  return node;
}

function append(node, children) {
  for (const c of children.flat(Infinity)) if (c != null && c !== false) node.append(c instanceof Node ? c : String(c));
}

// Replaces a node's children: lists are flattened, which replaceChildren alone would turn into text.
export function fill(node, ...children) {
  node.replaceChildren();
  append(node, children);
  return node;
}

const SVG = "http://www.w3.org/2000/svg";
export function svg(tag, attrs = {}) {
  const node = document.createElementNS(SVG, tag);
  for (const [k, v] of Object.entries(attrs)) node.setAttribute(k, v);
  return node;
}

export async function api(path, body, method) {
  const response = await fetch(path, {
    method: method || (body === undefined ? "GET" : "POST"),
    headers: { "X-Dtpipe-Studio": token, "Content-Type": "application/json" },
    body: body === undefined ? undefined : JSON.stringify(body),
  });
  const text = await response.text();
  const data = text ? JSON.parse(text) : {};
  if (!response.ok) {
    throw Object.assign(new Error(data.error || `${response.status} ${response.statusText}`), { status: response.status, data });
  }
  return data;
}

/** The element an event was aimed at: the page's listeners all sit on elements. @param {Event} e @returns {any} */
export const targetOf = (e) => /** @type {any} */ (e.target);

export function status(text, kind = "") {
  $("status").textContent = text;
  $("status").className = `pill ${kind}`;
}
