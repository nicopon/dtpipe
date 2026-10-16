// @ts-check
// The page is drawn by zone: a zone is a part of the page with its own renderer, which reads the state and
// rebuilds its nodes. A change names the zones it touches with `invalidate`; the renderers run once, in a
// fixed order, when the task that asked is done. A zone that holds what the person is doing (the inspector's
// focused field, the drawer's scroll) is invalidated only by a change that makes it untrue, never by its own
// events: that is what keeps a caret in place while the job is composed behind it.
//
// Drawn outside this mechanism, because they are gestures and messages and not state: the card under the
// pointer while it is dragged and the highlight of the card it selects (redrawing the card would drop the drag),
// the wire being drawn, a pane's size while its handle moves, and the status line.

/** @typedef {"palette" | "canvas" | "inspector" | "drawer" | "bar" | "jobs" | "coordinator"} Zone */

/** @type {Zone[]} */
const ORDER = ["palette", "canvas", "inspector", "drawer", "bar", "jobs", "coordinator"];

/** @type {Map<Zone, Array<() => void>>} */
const renderers = new Map();
/** @type {Set<Zone>} */
const wanted = new Set();
/** @type {Array<() => void>} */
const afterwards = [];
let scheduled = false;

/** Adds work that follows every drawing, once all the zones asked for are drawn. @param {() => void} work */
export function afterFlush(work) { afterwards.push(work); }

/** Adds a renderer to a zone; a zone may have several, the designer's and the host's. @param {Zone} zone @param {() => void} render */
export function onZone(zone, render) {
  const list = renderers.get(zone) ?? [];
  list.push(render);
  renderers.set(zone, list);
}

/** Asks for zones to be drawn once the current task is done. @param {...Zone} zones */
export function invalidate(...zones) {
  for (const zone of zones) wanted.add(zone);
  if (!scheduled) {
    scheduled = true;
    queueMicrotask(flush);
  }
}

/** Draws the zones asked for now. Code that reads the nodes it just asked for calls it first. */
export function flush() {
  scheduled = false;
  const zones = ORDER.filter((zone) => wanted.has(zone));
  wanted.clear();
  let failure = null;
  for (const zone of zones) {
    for (const render of renderers.get(zone) ?? []) {
      // One zone failing leaves the others drawn.
      try { render(); } catch (e) { failure ??= e; }
    }
  }
  for (const work of afterwards) {
    try { work(); } catch (e) { failure ??= e; }
  }
  if (failure) throw failure;
}
