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

/**
 * What a drawer tab draws when it has actions: `body` scrolls, `actions` (the buttons that act on it, never a text) sit in a bar
 * under it that is always in view, and `fill` lets one element of the body take the height left (the editor, a table).
 * @param {any} body @param {any[] | null} actions @param {{ fill?: boolean }} [options]
 */
export function tabOf(body, actions, options = {}) {
  return { tab: true, body, actions: (actions || []).filter(Boolean), fill: !!options.fill };
}

/** Puts a tip beside its anchor, kept in the window: the tip is fixed so no scrolling panel clips it. @param {Element} anchor @param {HTMLElement} tip */
export function placeTip(anchor, tip) {
  const box = anchor.getBoundingClientRect();
  const left = Math.max(8, Math.min(box.left - 12, window.innerWidth - tip.offsetWidth - 8));
  const below = box.bottom + 6;
  const top = below + tip.offsetHeight > window.innerHeight - 8 ? Math.max(8, box.top - tip.offsetHeight - 6) : below;
  tip.style.left = `${left}px`;
  tip.style.top = `${top}px`;
}

let tipSeq = 0;
/** @type {HTMLElement | null} */
let activeTip = null;
/** @type {HTMLElement | null} */
let pinnedTip = null;
/** @type {ReturnType<typeof setTimeout> | null} */
let hideTimer = null;
let pointerOpening = false;
let ignoreFocusScrollUntil = 0;

/** @param {HTMLElement} trigger @returns {HTMLElement | null} */
const tipOf = (trigger) => trigger.querySelector(".info-text");

/** @param {HTMLElement} trigger @param {boolean} [pin] */
function openTip(trigger, pin = false) {
  const tip = tipOf(trigger);
  if (!tip) return;
  clearTimeout(hideTimer);
  hideTimer = null;
  if (activeTip && activeTip !== trigger) closeTip(activeTip);
  activeTip = trigger;
  if (pin) pinnedTip = trigger;
  trigger.setAttribute("aria-expanded", "true");
  placeTip(trigger, tip);
}

/** @param {HTMLElement | null} [trigger] */
function closeTip(trigger = activeTip) {
  clearTimeout(hideTimer);
  hideTimer = null;
  ignoreFocusScrollUntil = 0;
  if (!trigger) return;
  trigger.setAttribute("aria-expanded", "false");
  if (activeTip === trigger) activeTip = null;
  if (pinnedTip === trigger) pinnedTip = null;
}

/** @param {HTMLElement} trigger */
function scheduleClose(trigger) {
  clearTimeout(hideTimer);
  hideTimer = setTimeout(() => {
    if (activeTip === trigger && pinnedTip !== trigger && !trigger.matches(":focus-within")) closeTip(trigger);
  }, 150);
}

document.addEventListener("pointerover", (e) => {
  if (e.pointerType === "touch") return;
  const target = e.target instanceof Element ? e.target : null;
  const trigger = /** @type {HTMLElement | null} */ (target?.closest(".info") ?? null);
  if (trigger) openTip(trigger, false);
});

document.addEventListener("pointerout", (e) => {
  if (e.pointerType === "touch" || !activeTip) return;
  const related = e.relatedTarget instanceof Node ? e.relatedTarget : null;
  if (related && activeTip.contains(related)) return;
  scheduleClose(activeTip);
});

document.addEventListener("pointerdown", (e) => {
  const target = e.target instanceof Element ? e.target : null;
  if (target?.closest(".info") && !target.closest(".info-text")) {
    pointerOpening = true;
    setTimeout(() => { pointerOpening = false; }, 0);
  }
});

document.addEventListener("focusin", (e) => {
  if (pointerOpening) return;
  const target = e.target instanceof Element ? e.target : null;
  const trigger = /** @type {HTMLElement | null} */ (target?.closest(".info") ?? null);
  if (trigger) {
    ignoreFocusScrollUntil = performance.now() + 80;
    openTip(trigger, false);
  }
});

document.addEventListener("focusout", (e) => {
  if (!activeTip || pinnedTip === activeTip) return;
  const related = e.relatedTarget instanceof Node ? e.relatedTarget : null;
  if (related && activeTip.contains(related)) return;
  closeTip(activeTip);
});

document.addEventListener("click", (e) => {
  pointerOpening = false;
  const target = e.target instanceof Element ? e.target : null;
  if (target?.closest(".info-text")) return;
  const trigger = /** @type {HTMLElement | null} */ (target?.closest(".info") ?? null);
  if (trigger) {
    if (pinnedTip === trigger || (e.detail === 0 && activeTip === trigger)) closeTip(trigger);
    else openTip(trigger, true);
  } else if (activeTip) {
    closeTip(activeTip);
  }
});

document.addEventListener("keydown", (e) => {
  if (e.key === "Escape" && activeTip) {
    e.preventDefault();
    e.stopPropagation();
    closeTip(activeTip);
  }
}, true);

document.addEventListener("scroll", (e) => {
  if (!activeTip) return;
  const target = e.target instanceof Node ? e.target : null;
  if (target instanceof Element && target.closest(".info-text")) return;
  if (performance.now() < ignoreFocusScrollUntil && target?.contains(activeTip)) {
    const tip = tipOf(activeTip);
    if (tip) placeTip(activeTip, tip);
    return;
  }
  closeTip(activeTip);
}, { capture: true, passive: true });

window.addEventListener("resize", () => { if (activeTip) closeTip(activeTip); });

/**
 * A ⓘ right after the text it explains: its tooltip opens on hover, focus, or click and dismisses on Escape or scroll.
 * @param {string} text @param {string} [label]
 */
export function info(text, label = "More information") {
  const tipId = `info-tip-${++tipSeq}`;
  const tip = el("span", { id: tipId, class: "info-text", role: "tooltip" }, text);
  const glyph = el("span", { "aria-hidden": "true" }, "ⓘ");
  return el("button", {
    type: "button",
    class: "info",
    "aria-label": label,
    "aria-expanded": "false",
    "aria-describedby": tipId,
  }, glyph, tip);
}

const SVG = "http://www.w3.org/2000/svg";
export function svg(tag, attrs = {}) {
  const node = document.createElementNS(SVG, tag);
  for (const [k, v] of Object.entries(attrs)) node.setAttribute(k, v);
  return node;
}

// Said when a request got no answer at all: the studio is stopped, or its address gone. That is the link's failure, never the
// content's: every module that shows an error's message shows this one, and the header keeps saying it until an answer comes.
export const STUDIO_SILENT = "The studio does not answer: it may have stopped. Start it again and reload this page.";
const doubts = [];

/** Called when the coordinator or what hangs on it failed for a request: the link's status is worth reading again. @param {() => void} listener */
export function onLinkDoubt(listener) { doubts.push(listener); }

function studioAnswered(answered) {
  const note = $("studio-link");
  if (!note) return;
  note.hidden = answered;
  if (!answered) fill(note, el("span", { class: "dot bad" }), " studio: not answering");
}

/** @param {string} path @param {any} [body] @param {string} [method] @param {AbortSignal} [signal] aborting it abandons the request, which the server reads as a stop */
export async function api(path, body, method, signal) {
  let response;
  try {
    response = await fetch(path, {
      signal,
      method: method || (body === undefined ? "GET" : "POST"),
      headers: { "X-Dtpipe-Studio": token, "Content-Type": "application/json" },
      body: body === undefined ? undefined : JSON.stringify(body),
    });
  } catch (e) {
    // An abandoned request is the page's own doing; a request that never reached an answer is the transport's.
    if (e?.name === "AbortError") throw e;
    studioAnswered(false);
    throw Object.assign(new Error(STUDIO_SILENT), { transport: true });
  }
  studioAnswered(true);
  const text = await response.text();
  const data = text ? JSON.parse(text) : {};
  if (!response.ok) {
    // A 502 of the coordinator's routes: what the link last met is in its status.
    if (response.status === 502 && !path.startsWith("/api/agent")) for (const listener of doubts) listener();
    throw Object.assign(new Error(data.error || `${response.status} ${response.statusText}`), { status: response.status, data });
  }
  return data;
}

/** The element an event was aimed at: the page's listeners all sit on elements. @param {Event} e @returns {any} */
export const targetOf = (e) => /** @type {any} */ (e.target);

/** A count with its noun, in the plural unless the count is one. @param {number} n @param {string} word */
/**
 * A version without its build metadata: "1.17.0+<commit>" reads "1.17.0". A version that differs
 * from `other` only by that metadata keeps a short commit so the two never read alike.
 * @param {string | undefined} version
 * @param {string} [other]
 */
export function tagOf(version, other) {
  if (!version) return "";
  const [tag, build] = version.split("+");
  return other && other !== version && other.split("+")[0] === tag && build ? `${tag}+${build.slice(0, 7)}` : tag;
}
export const plural = (n, word) => `${n} ${word}${n === 1 ? "" : "s"}`;
export function status(text, kind = "") {
  $("status").textContent = text;
  $("status").className = `pill ${kind}`;
}
