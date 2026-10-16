// @ts-check
// The studio's designer: a job of the folder as cards, one per source, step and sink. The server
// composes the job (POST /api/design/compose) on every change and takes a job apart into cards
// (POST /api/design/decompose); this page never writes YAML itself. Built with DOM calls only: the
// page's policy forbids inline script and style, and every value is set as text.

import { $, api, el, fill, status, svg, targetOf } from "./dom.js";
import { KEPT_TABS } from "./prefs.js";
import { flush, invalidate, onZone } from "./render.js";
import { sequence, state } from "./state.js";

const CARD_W = 210;
const CARD_H = 114;
const PORT_IN_Y = 34;
const PORT_REF_Y = 66;

// The transformer types the page offers, each with the shape its YAML takes (REFERENCE.md,
// "Transformer YAML reference"). dtpipe validates the result: a sample runs it.
const TRANSFORMERS = {
  fake: { hint: "mappings: column → faker path (name.lastName); options: locale, seed, seed-column, seed-row, skip-null", mapping: "column", value: "dataset.method" },
  null: { hint: "mappings: column → (empty): the column becomes null", mapping: "column", value: "" },
  overwrite: { hint: "mappings: column → constant value", mapping: "column", value: "value" },
  mask: { hint: "mappings: column → pattern ('#' keeps a character, anything else replaces it)", mapping: "column", value: "###****" },
  format: { hint: "mappings: column → .NET composite format over other columns, \"{A} {B}\"", mapping: "column", value: "{A} {B}" },
  compute: { hint: "mappings: column → JavaScript expression over row", mapping: "column", value: "row.a + row.b" },
  filter: { hint: "mappings: JavaScript boolean expression → (empty)", mapping: "expression", value: "" },
  expand: { hint: "mappings: JavaScript expression returning an array of objects → (empty)", mapping: "expression", value: "" },
  window: { hint: "options: count, script", mapping: "line", value: "" },
  project: { hint: "options: project \"a,b\", drop \"c\", rename \"Old:New\"", mapping: null, value: null },
};

const KIND_LABEL = { source: "source", sink: "sink", transform: "transform", sql: "sql", merge: "merge", branch: "branch" };

// The panels' sizes, kept across visits.
const SIZE_LIMITS = { pal: [160, 420], insp: [240, 640], drawer: [100, 640] };

const cards = state.cards;
let host = null;
let hooks = {};
const composing = sequence();
const opening = sequence();

onZone("palette", renderPalette);
onZone("canvas", renderCanvas);
onZone("inspector", renderInspector);
onZone("drawer", renderDrawer);
onZone("bar", renderToolbarState);
let composeTimer = null;

/**
 * Builds the designer in `root`. The host gives what only it knows: `toolbar` (the bar's buttons), `onComposed`,
 * `cardTools`, `cardBadge` and `cardTool` (what a card shows and does), `previewView` (the Preview tab) and `extraTabs`.
 */
export function mountDesigner(root, options = {}) {
  host = root;
  hooks = options;
  document.addEventListener("keydown", onKey);
}

/**
 * Opens a job: its text taken apart into cards by the server, with the layout it was saved with. A job not on disk opens
 * as unsaved changes. `kept` is how the page was left on it: the tab and the card, where they are still there.
 * @param {string} yaml @param {any} layout @param {boolean} [unsaved] @param {{ tab?: string, card?: string } | null} [kept]
 */
export async function openJob(yaml, layout, unsaved = false, kept = null) {
  const ticket = opening.begin();
  const answer = await api("/api/design/decompose", { yaml, layout: layout || null });
  // Another job was asked for meanwhile: its cards are the ones to show.
  if (opening.stale(ticket)) return false;
  cards.model = normalize(answer.model);
  cards.model.layout = Object.fromEntries(Object.entries(cards.model.layout).filter(([a]) => stepOf(a)));
  if (cards.model.steps.some((s) => !cards.model.layout[s.alias])) autoLayout(false);
  cards.result = answer;
  cards.selected = kept?.card && stepOf(kept.card) ? { alias: kept.card } : null;
  cards.zoom = null; cards.dirty = false;
  // A job that does not compose opens on its problems, whatever was kept.
  state.view.drawer = cards.result.errors?.length ? "problems" : kept?.tab && KEPT_TABS.includes(kept.tab) ? kept.tab : "yaml";
  renderShell();
  await compose();
  if (!opening.stale(ticket)) cards.dirty = unsaved;
  return !opening.stale(ticket);
}

/** Starts from no card at all. */
export async function openBlank(title) {
  opening.cancel();
  cards.model = normalize({ title, steps: [], layout: {} });
  cards.result = null; cards.selected = null; cards.zoom = null; cards.dirty = true; state.view.drawer = "yaml";
  renderShell();
  await compose();
}

/** The coordinator's nodes and bricks: the palette offers them, the inspector describes them. */
export function setCatalog(next) {
  state.catalog = { nodes: next?.nodes || [], bricks: next?.bricks || [], studioVersion: next?.studioVersion || "" };
  invalidate("palette");
  if (cards.model) invalidate("canvas", "inspector");
}

const brickOf = (key) => state.catalog.bricks.find((b) => b.key === key);

/** Each brick card, by the branch of the composed job that runs it: a sample takes its rows from its node. */
export function brickBranches() {
  if (!cards.model) return [];
  return cards.model.steps.filter((s) => s.brick).map((s) => ({
    branch: cards.result?.steps?.find((i) => i.alias === s.alias)?.branch || s.alias,
    key: s.brick,
  }));
}

/**
 * Where a card's output can be watched: the branch of the composed job that runs it, and the place on it. A source
 * is watched at its read, a step at the end of its branch; a sink holds nothing the branch does not already show.
 */
export function cardWatch(alias) {
  const step = cards.model && stepOf(alias);
  if (!step || step.kind === "sink") return null;
  return { branch: cards.result?.steps?.find((i) => i.alias === alias)?.branch || alias, at: step.kind === "source" ? "read" : "end" };
}

/** The branch of the composed job that runs a card, whatever its kind: the page reads a sink's failure there. */
export const cardBranch = (alias) => (cards.model && stepOf(alias) ? cards.result?.steps?.find((i) => i.alias === alias)?.branch || alias : null);

/** Selects a card and shows its rows in the Preview panel. */
export function focusCard(alias) {
  if (!cards.model || !stepOf(alias)) return;
  cards.selected = { alias };
  state.view.drawer = "preview";
  invalidate("canvas", "inspector", "drawer");
}

/** The sizes the panes were left at; a value out of range is clamped when they are applied. @param {Record<string, number>} sizes */
export function restoreSizes(sizes) {
  for (const name of Object.keys(SIZE_LIMITS)) if (Number.isFinite(sizes[name])) state.view.sizes[name] = sizes[name];
  applySizes();
}

export function markSaved() { cards.dirty = false; invalidate("bar"); }

function normalize(m) {
  return {
    title: m.title || "Untitled pipeline", description: m.description || "", directives: m.directives || [],
    steps: (m.steps || []).map((s) => ({ ...s, from: s.from || [], ref: s.ref || [] })),
    layout: m.layout || {},
  };
}

// ---------------------------------------------------------------- composing

function scheduleCompose() {
  clearTimeout(composeTimer);
  composeTimer = setTimeout(compose, 200);
}

export async function compose() {
  clearTimeout(composeTimer);
  const seq = composing.begin();
  try {
    const answer = await api("/api/design/compose", cards.model);
    if (composing.stale(seq)) return cards.result;
    cards.result = answer;
  } catch (e) {
    if (!composing.stale(seq)) cards.result = { yaml: "", steps: [], errors: [e.message], warnings: [] };
  }
  invalidate("canvas", "drawer", "bar");
  hooks.onComposed?.(cards.result);
  return cards.result;
}

function touch({ recompose = true, inspector = false, canvas = true } = {}) {
  cards.dirty = true;
  if (canvas) invalidate("canvas");
  if (inspector) invalidate("inspector");
  invalidate("bar");
  if (recompose) scheduleCompose();
}

// ---------------------------------------------------------------- model edits

const stepOf = (alias) => cards.model.steps.find((s) => s.alias === alias);
const isEndpoint = (step) => step.kind === "source" || step.kind === "sink";

function freshAlias(base) {
  let alias = String(base).replace(/[^A-Za-z0-9_-]/g, "_");
  if (!/^[A-Za-z]/.test(alias)) alias = "s_" + alias;
  let candidate = alias;
  for (let i = 2; stepOf(candidate); i++) candidate = `${alias}${i}`;
  return candidate;
}

function addStep(kind, component, x, y, brick) {
  const alias = freshAlias(brick ? brick.replace(/^.*\//, "") : component || kind);
  const step = { alias, kind, from: [], ref: [] };
  if (brick) step.brick = brick;
  // A local endpoint starts as its component's prefix: the person completes the address.
  if (kind === "source" && !brick) step.yaml = `input: "${component}:"\n`;
  if (kind === "sink" && !brick) step.yaml = `output: "${component}:"\n`;
  if (kind === "transform") step.transformers = [];
  if (kind === "sql") step.query = "SELECT *\nFROM input";
  cards.model.steps.push(step);
  cards.model.layout[alias] = { x: Math.max(10, Math.round(x)), y: Math.max(10, Math.round(y)) };
  cards.selected = { alias };
  touch({ inspector: true });
}

function removeStep(alias) {
  cards.model.steps = cards.model.steps.filter((s) => s.alias !== alias);
  for (const s of cards.model.steps) { s.from = s.from.filter((a) => a !== alias); s.ref = s.ref.filter((a) => a !== alias); }
  delete cards.model.layout[alias];
  if (cards.zoom === alias) cards.zoom = null;
  cards.selected = null;
  touch({ inspector: true });
  applyView();
}

function renameStep(from, to) {
  to = to.trim();
  if (to === from) return;
  if (!/^[A-Za-z][A-Za-z0-9_-]*$/.test(to)) { status(`'${to}' is not a usable alias: a letter first, then letters, digits, '_' or '-'.`, "bad"); invalidate("inspector"); return; }
  if (stepOf(to)) { status(`A step is already named '${to}'.`, "bad"); invalidate("inspector"); return; }
  stepOf(from).alias = to;
  for (const s of cards.model.steps) {
    s.from = s.from.map((a) => (a === from ? to : a));
    s.ref = s.ref.map((a) => (a === from ? to : a));
    if (s.kind === "sql" && s.query) s.query = s.query.replace(new RegExp(`\\b${from}\\b`, "g"), to);
  }
  cards.model.layout[to] = cards.model.layout[from];
  delete cards.model.layout[from];
  if (cards.zoom === from) cards.zoom = to;
  cards.selected = { alias: to };
  touch({ inspector: true });
}

function connect(fromAlias, toAlias, port) {
  const target = stepOf(toAlias);
  if (!target || fromAlias === toAlias) return;
  if (target.kind === "source") { status("A source reads its own input: it takes no other.", "bad"); return; }
  if (port === "ref") {
    if (!target.ref.includes(fromAlias) && !target.from.includes(fromAlias)) target.ref.push(fromAlias);
  } else if (target.kind === "merge" || target.kind === "branch") {
    if (!target.from.includes(fromAlias)) target.from.push(fromAlias);
  } else {
    target.from = [fromAlias];
    target.ref = target.ref.filter((a) => a !== fromAlias);
  }
  touch({ inspector: cards.selected?.alias === toAlias });
}

function disconnect(edge) {
  const target = stepOf(edge.to);
  if (!target) return;
  if (edge.port === "ref") target.ref = target.ref.filter((a) => a !== edge.from);
  else target.from = target.from.filter((a) => a !== edge.from);
  cards.selected = null;
  touch({ inspector: true });
}

function autoLayout(render = true) {
  const byAlias = Object.fromEntries(cards.model.steps.map((s) => [s.alias, s]));
  const depth = {};
  const depthOf = (s, seen = new Set()) => {
    if (depth[s.alias] !== undefined) return depth[s.alias];
    if (seen.has(s.alias)) return 0;
    seen.add(s.alias);
    const d = [...s.from, ...s.ref].filter((a) => byAlias[a]).reduce((m, a) => Math.max(m, depthOf(byAlias[a], seen) + 1), 0);
    return (depth[s.alias] = d);
  };
  cards.model.steps.forEach((s) => depthOf(s));
  const rows = {};
  for (const s of cards.model.steps) {
    const d = depth[s.alias];
    const row = rows[d] = (rows[d] ?? -1) + 1;
    cards.model.layout[s.alias] = { x: 30 + d * (CARD_W + 70), y: 30 + row * (CARD_H + 36) };
  }
  if (render) touch({ recompose: false });
}

// ---------------------------------------------------------------- zoom

// A Transform step opens as the linear chain of its transformers; every edit goes through the same model.
function enterZoom(alias) {
  const step = stepOf(alias);
  if (step?.kind !== "transform") return;
  cards.zoom = alias;
  cards.selected = { alias, t: step.transformers?.length ? 0 : undefined };
  applyView();
}

function exitZoom() {
  if (!cards.zoom) return;
  cards.selected = { alias: cards.zoom };
  cards.zoom = null;
  applyView();
}

// The canvas shows the cards, or the chain of the zoomed card.
function applyView() {
  invalidate("canvas", "inspector");
}

function addTransformer(step) {
  (step.transformers ||= []).push({ type: "project", options: {} });
  if (cards.zoom) cards.selected = { alias: step.alias, t: step.transformers.length - 1 };
  touch({ inspector: true });
}

function removeTransformer(step, i) {
  step.transformers.splice(i, 1);
  if (cards.zoom) cards.selected = { alias: step.alias, t: step.transformers.length ? Math.min(i, step.transformers.length - 1) : undefined };
}

function transformerSummary(t) {
  const parts = [...Object.keys(t.mappings || {}), ...Object.entries(t.options || {}).map(([k, v]) => `${k}: ${v ?? ""}`)];
  return parts.join(", ").slice(0, 70) || "nothing set yet";
}

function renderChain() {
  const view = $("d-chain");
  const step = cards.zoom ? stepOf(cards.zoom) : null;
  if (!view || !step) return;
  const link = () => el("span", { class: "chain-link" });
  const items = (step.transformers || []).flatMap((t, i) => [
    link(),
    el("div", { class: `chain-item ${cards.selected?.t === i ? "selected" : ""}`, "data-t": i, tabindex: "0", title: "Select to edit it in the inspector" },
      el("div", { class: "chain-type" }, el("span", { class: "idx" }, i + 1), t.type),
      el("div", { class: "chain-sub" }, transformerSummary(t))),
  ]);
  fill(view,
    el("nav", { class: "crumbs" },
      el("a", { href: "#", "data-crumb": true, title: "Back to the job (Esc)" }, cards.model.title), el("span", { class: "sep" }, "›"), el("b", {}, step.alias)),
    el("div", { class: "chain" },
      el("div", { class: "chain-end" }, el("span", { class: "kind" }, "input"),
        step.from.length ? step.from.map((a) => el("code", {}, a)) : el("span", { class: "hint" }, "not wired")),
      items,
      link(),
      el("button", { class: "chain-add", "data-chain-add": true, title: "Add a transformer at the end of the chain" }, "+ transformer"),
      link(),
      el("div", { class: "chain-end" }, el("span", { class: "kind" }, "output"), el("code", {}, step.alias))),
    step.transformers?.length ? null : el("p", { class: "hint" }, "No transformer yet: the step passes its input through unchanged."));
}

// ---------------------------------------------------------------- rendering

function renderShell() {
  const seg = (id, label, title) => el("button", { id, title }, label);
  fill(host,
    el("div", { class: "designer-bar" },
      el("input", { id: "d-title", class: "title-input", value: cards.model.title, "aria-label": "Title" }),
      el("input", { id: "d-description", class: "grow", value: cards.model.description, placeholder: "What this job does", "aria-label": "Description" }),
      el("span", { id: "d-dirty", class: "pill warn", hidden: true }, "unsaved"),
      el("span", { class: "actions" },
        seg("d-layout", "Arrange", "Arrange the cards in columns, sources first"),
        hooks.toolbar?.() || null)),
    el("div", { class: "designer-body" },
      el("aside", { class: "palette card", id: "d-palette" }),
      el("div", { class: "canvas card", id: "d-canvas" },
        el("div", { class: "canvas-inner", id: "d-inner" }, svg("svg", { id: "d-svg" })),
        el("div", { class: "chain-view", id: "d-chain", hidden: true })),
      el("aside", { class: "inspector card", id: "d-inspector" }),
      el("div", { class: "split split-col split-pal", "data-split": "pal", role: "separator", "aria-orientation": "vertical", "aria-label": "Resize the palette", tabindex: "0" }),
      el("div", { class: "split split-col split-insp", "data-split": "insp", role: "separator", "aria-orientation": "vertical", "aria-label": "Resize the inspector", tabindex: "0" })),
    el("div", { class: "split split-row", "data-split": "drawer", role: "separator", "aria-orientation": "horizontal", "aria-label": "Resize the drawer", tabindex: "0" }),
    el("div", { class: "drawer card", id: "d-drawer" }));

  $("d-title").addEventListener("input", (e) => { cards.model.title = targetOf(e).value; touch({ canvas: !!cards.zoom }); });
  $("d-description").addEventListener("input", (e) => { cards.model.description = targetOf(e).value; touch({ canvas: false }); });
  $("d-layout").addEventListener("click", () => autoLayout());
  applySizes();
  bindSplits();
  bindCanvas();
  bindChain();
  bindPalette();
  bindInspector();
  bindDrawer();
  invalidate("palette", "canvas", "inspector", "drawer", "bar");
  flush();
}

function applySizes() {
  for (const [name, [lo, hi]] of Object.entries(SIZE_LIMITS)) {
    state.view.sizes[name] = Math.min(hi, Math.max(lo, Math.round(+state.view.sizes[name] || lo)));
    host.style.setProperty(name === "pal" ? "--pal-w" : name === "insp" ? "--insp-w" : "--drawer-h", `${state.view.sizes[name]}px`);
  }
}

function resize(name, value, persist) {
  state.view.sizes[name] = value;
  applySizes();
  // The sizes are kept after a drawing (prefs.js): asking for one is how a finished move is noticed.
  if (persist) invalidate("bar");
}

// A handle is dragged with the pointer, or moved with the arrow keys; the size is clamped to its limits.
function bindSplits() {
  const body = host.querySelector(".designer-body");
  for (const handle of host.querySelectorAll(".split")) {
    const name = handle.dataset.split;
    const target = (e) => {
      if (name === "pal") return e.clientX - body.getBoundingClientRect().left - 6;
      if (name === "insp") return body.getBoundingClientRect().right - e.clientX - 6;
      return host.getBoundingClientRect().bottom - e.clientY - 12;
    };
    handle.addEventListener("pointerdown", (e) => { handle.setPointerCapture(e.pointerId); handle.classList.add("dragging"); e.preventDefault(); });
    handle.addEventListener("pointermove", (e) => { if (handle.hasPointerCapture(e.pointerId)) resize(name, target(e), false); });
    const end = (e) => { if (handle.hasPointerCapture(e.pointerId)) { handle.releasePointerCapture(e.pointerId); handle.classList.remove("dragging"); resize(name, state.view.sizes[name], true); } };
    handle.addEventListener("pointerup", end);
    handle.addEventListener("pointercancel", end);
    handle.addEventListener("keydown", (e) => {
      const grow = { pal: "ArrowRight", insp: "ArrowLeft", drawer: "ArrowUp" }[name];
      const shrink = { pal: "ArrowLeft", insp: "ArrowRight", drawer: "ArrowDown" }[name];
      if (e.key !== grow && e.key !== shrink) return;
      resize(name, state.view.sizes[name] + (e.key === grow ? 16 : -16), true);
      e.preventDefault();
    });
  }
}

function renderToolbarState() {
  const flag = $("d-dirty");
  if (flag) flag.hidden = !cards.dirty;
  }

function renderPalette() {
  const view = $("d-palette");
  if (!view) return;
  const item = (kind, label, hint, component) => el("div",
    { class: `pal-item kind-${kind}`, draggable: "true", "data-kind": kind, "data-component": component, title: hint },
    el("span", {}, label), el("button", { class: "small ghost", "data-add": true, title: "Add to the canvas" }, "+"));
  const endpoints = (list, kind) => list.length
    ? list.map((c) => item(kind, c.name, `${c.category} · ${kind === "source" ? "reads" : "writes"} through the ${c.name} component`, c.name))
    : el("p", { class: "hint" }, "none");
  const brickItem = (b) => {
    const node = el("div",
      { class: `pal-item kind-${b.kind}${b.online ? "" : " offline"}`, draggable: "true", "data-kind": b.kind, "data-brick": b.key,
        title: `${b.kind} brick of ${b.node} · version ${b.version}${b.online ? "" : " · its node is offline"}${b.description ? "\n" + b.description : ""}` },
      el("span", {}, b.title || b.id), el("button", { class: "small ghost", "data-add": true, title: "Add to the canvas" }, "+"));
    return node;
  };
  const nodes = state.catalog.nodes.filter((n) => state.catalog.bricks.some((b) => b.node === n.name));
  const bricks = state.catalog.bricks.length ? [
    el("h3", {}, "Bricks ", el("span", { class: "hint" }, "of the coordinator's nodes")),
    nodes.map((n) => [
      el("h4", { class: n.online ? "" : "offline" }, n.name, n.online ? "" : " (offline)"),
      state.catalog.bricks.filter((b) => b.node === n.name).map(brickItem),
    ]),
  ] : null;
  fill(view,
    bricks,
    el("h3", {}, "Sources"), endpoints(state.components.readers, "source"),
    el("h3", {}, "Steps"),
    item("transform", "Transform", "An ordered list of transformers over one input"),
    item("sql", "SQL", "A DuckDB query over one main input, joined to others as ref"),
    item("merge", "Merge", "UNION ALL of several inputs with the same schema"),
    el("h3", {}, "Sinks"), endpoints(state.components.writers, "sink"),
    el("p", { class: "hint" }, "Drag a component or a step onto the canvas, then wire an output port (right) to an input port (left). Drag a wire's end off its input port to move it or, dropped in the void, to remove it."),
    versions());
}

// This studio's dtpipe and each node's: a difference is shown, never refused, since the coordinator
// checks a pipeline again when it is published.
function versions() {
  if (!state.catalog.nodes.length) return null;
  const differ = state.catalog.nodes.filter((n) => n.dtpipeVersion !== state.catalog.studioVersion);
  return [
    el("h3", {}, "Versions"),
    el("ul", { class: "versions" },
      el("li", {}, "this studio: ", el("code", {}, state.catalog.studioVersion || "unknown")),
      state.catalog.nodes.map((n) => el("li", { class: n.dtpipeVersion === state.catalog.studioVersion ? "" : "warn" },
        `${n.name}: `, el("code", {}, n.dtpipeVersion || "unknown")))),
    differ.length ? el("p", { class: "hint warn" }, `${differ.length} node(s) run another dtpipe: a sample here may differ from a run there. The coordinator checks a pipeline again when it is published.`) : null,
  ];
}

// What a local endpoint names: its input or output, read from its own YAML for the card's subtitle.
function endpointOf(step) {
  const match = /^(?:input|output):\s*(.*)$/m.exec(step.yaml || "");
  return match ? match[1].replace(/^["']|["']$/g, "") : "";
}

function cardInfo(step) {
  const info = cards.result?.steps?.find((s) => s.alias === step.alias);
  let subtitle = "";
  const brick = step.brick ? brickOf(step.brick) : null;
  if (brick) subtitle = `${brick.title || brick.id} · ${brick.node}`;
  else if (isEndpoint(step)) subtitle = step.brick || endpointOf(step) || `no ${step.kind === "source" ? "input" : "output"} yet`;
  else if (step.kind === "transform") subtitle = step.transformers?.length ? step.transformers.map((t) => t.type).join(" → ") : "no transformer yet";
  else if (step.kind === "sql") subtitle = (step.query || "").replace(/\s+/g, " ").slice(0, 60);
  else if (step.kind === "merge") subtitle = `UNION ALL of ${step.from.length} input(s)`;
  else subtitle = "branch, as written";
  // A card the server joined into another branch says which: the job dtpipe runs has that branch.
  const branch = info?.branch && info.branch !== step.alias ? `in branch ${info.branch}` : "";
  return { subtitle, branch };
}

function renderCanvas() {
  const inner = $("d-inner");
  if (!inner || !cards.model) return;
  inner.hidden = !!cards.zoom;
  $("d-chain").hidden = !cards.zoom;
  $("d-palette").classList.toggle("dimmed", !!cards.zoom);
  if (cards.zoom) { renderChain(); return; }
  const problems = new Set((cards.result?.errors || []).flatMap((e) => [...e.matchAll(/'([^']+)'/g)].map((m) => m[1])));
  inner.querySelectorAll(".dcard, .canvas-empty").forEach((c) => c.remove());
  let width = 900, height = 520;
  for (const step of cards.model.steps) {
    const pos = cards.model.layout[step.alias] || { x: 20, y: 20 };
    const { subtitle, branch } = cardInfo(step);
    const card = el("div", {
      class: `dcard kind-${step.kind} ${cards.selected?.alias === step.alias ? "selected" : ""} ${problems.has(step.alias) ? "problem" : ""}`,
      "data-alias": step.alias,
    },
      el("div", { class: "dcard-head" }, el("span", { class: "kind" }, KIND_LABEL[step.kind] || step.kind), el("span", { class: "alias" }, step.alias)),
      step.kind === "transform" ? el("button", { class: "dcard-zoom", title: "Zoom into the transformers (or double-click the card)", "aria-label": `Zoom into ${step.alias}` }, "⌕") : null,
      hooks.cardTools?.(step.alias) || null,
      el("div", { class: "dcard-sub" }, subtitle),
      hooks.cardBadge?.(step.alias) || null,
      el("div", { class: "dcard-branch" }, branch),
      step.kind !== "source" ? el("span", { class: "port in", "data-port": "in", title: step.kind === "merge" ? "inputs (several)" : "input" }) : null,
      step.kind === "sql" ? [el("span", { class: "port ref", "data-port": "ref", title: "joined inputs (ref)" }), el("span", { class: "port-label ref" }, "ref")] : null,
      step.kind !== "sink" ? el("span", { class: "port out", "data-port": "out", title: "output: drag to an input" }) : null);
    card.style.left = `${pos.x}px`;
    card.style.top = `${pos.y}px`;
    inner.appendChild(card);
    width = Math.max(width, pos.x + CARD_W + 200);
    height = Math.max(height, pos.y + CARD_H + 160);
  }
  inner.style.width = `${width}px`;
  inner.style.height = `${height}px`;
  drawEdges();
  if (!cards.model.steps.length) inner.appendChild(el("p", { class: "canvas-empty hint" }, "Drag sources, steps and sinks here from the palette."));
}

function portPoint(alias, port) {
  const pos = cards.model.layout[alias] || { x: 0, y: 0 };
  if (port === "out") return { x: pos.x + CARD_W, y: pos.y + PORT_IN_Y };
  return { x: pos.x, y: pos.y + (port === "ref" ? PORT_REF_Y : PORT_IN_Y) };
}

const curve = (a, b) => { const m = Math.max(40, (b.x - a.x) / 2); return `M${a.x},${a.y} C${a.x + m},${a.y} ${b.x - m},${b.y} ${b.x},${b.y}`; };

function drawEdges(temp) {
  const canvasSvg = $("d-svg");
  const inner = $("d-inner");
  if (!canvasSvg) return;
  canvasSvg.setAttribute("width", String(inner.offsetWidth));
  canvasSvg.setAttribute("height", String(inner.offsetHeight));
  const parts = [];
  let selectedHit = null;
  for (const step of cards.model.steps) {
    for (const [port, list] of [["in", step.from], ["ref", step.ref]]) {
      for (const from of list) {
        if (!stepOf(from)) continue;
        const isSel = cards.selected?.edge && cards.selected.edge.from === from && cards.selected.edge.to === step.alias && cards.selected.edge.port === port;
        const d = curve(portPoint(from, "out"), portPoint(step.alias, port));
        const hit = svg("path", { class: "dedge-hit", "data-from": from, "data-to": step.alias, "data-port": port, d });
        parts.push(svg("path", { class: `dedge ${port} ${isSel ? "selected" : ""}`, d }), hit);
        if (isSel) selectedHit = hit;
      }
    }
  }
  if (temp) parts.push(svg("path", { class: "dedge temp", d: curve(temp.a, temp.b) }));
  fill(canvasSvg, ...parts);

  // The selected wire carries its own delete button, at its middle.
  inner.querySelector(".edge-delete")?.remove();
  if (selectedHit && !temp) {
    const mid = selectedHit.getPointAtLength(selectedHit.getTotalLength() / 2);
    const button = el("button", { class: "edge-delete", title: "Remove this wire (Delete)" }, "×");
    button.style.left = `${mid.x}px`;
    button.style.top = `${mid.y}px`;
    inner.appendChild(button);
  }
}

// ---------------------------------------------------------------- canvas interaction

function canvasPoint(e) {
  const rect = $("d-inner").getBoundingClientRect();
  return { x: e.clientX - rect.left, y: e.clientY - rect.top };
}

function bindCanvas() {
  const inner = $("d-inner");
  let drag = null;

  inner.addEventListener("pointerdown", (e) => {
    const port = targetOf(e).closest(".port.out");
    const inPort = targetOf(e).closest(".port.in, .port.ref");
    const card = targetOf(e).closest(".dcard");
    const edge = targetOf(e).closest(".dedge-hit");
    const tool = targetOf(e).closest("[data-card-tool]");
    if (tool && card) {
      hooks.cardTool?.(card.dataset.alias, tool.dataset.cardTool);
      e.preventDefault();
      return;
    }
    if (targetOf(e).closest(".dcard-zoom") && card) {
      enterZoom(card.dataset.alias);
      e.preventDefault();
      return;
    }
    if (targetOf(e).closest(".edge-delete")) {
      disconnect(cards.selected.edge);
      e.preventDefault();
      return;
    }
    if (inPort && card) {
      // A wire is moved or removed by its input end: picked up here, dropped on another input,
      // or dropped anywhere else to remove it.
      const target = stepOf(card.dataset.alias);
      const portName = inPort.dataset.port === "ref" ? "ref" : "in";
      const list = portName === "ref" ? target.ref : target.from;
      if (list.length) {
        const from = list[list.length - 1];
        list.splice(list.length - 1, 1);
        drag = { type: "link", from, picked: { to: target.alias, port: portName } };
        cards.selected = null;
        inner.setPointerCapture(e.pointerId);
        drawEdges({ a: portPoint(from, "out"), b: canvasPoint(e) });
        e.preventDefault();
      }
      return;
    }
    if (port && card) {
      drag = { type: "link", from: card.dataset.alias };
      inner.setPointerCapture(e.pointerId);
      e.preventDefault();
    } else if (card && !targetOf(e).closest(".port")) {
      const pos = cards.model.layout[card.dataset.alias];
      const p = canvasPoint(e);
      drag = { type: "move", alias: card.dataset.alias, dx: p.x - pos.x, dy: p.y - pos.y, moved: false };
      if (cards.selected?.alias !== card.dataset.alias) {
        cards.selected = { alias: card.dataset.alias };
        invalidate("inspector");
        inner.querySelectorAll(".dcard").forEach((c) => c.classList.toggle("selected", c === card));
        drawEdges();
        // Clicking a card shows what it delivers, unless the person is reading another tab.
        if (state.view.drawer === "yaml") state.view.drawer = "preview";
        if (state.view.drawer === "preview") invalidate("drawer");
      }
      inner.setPointerCapture(e.pointerId);
    } else if (edge) {
      cards.selected = { edge: { from: edge.dataset.from, to: edge.dataset.to, port: edge.dataset.port } };
      invalidate("canvas", "inspector");
    } else if (targetOf(e) === inner || targetOf(e).id === "d-svg") {
      cards.selected = null;
      invalidate("canvas", "inspector");
      if (state.view.drawer === "preview") invalidate("drawer");
    }
  });

  inner.addEventListener("pointermove", (e) => {
    if (!drag) return;
    const p = canvasPoint(e);
    if (drag.type === "link") drawEdges({ a: portPoint(drag.from, "out"), b: p });
    else {
      const pos = { x: Math.max(0, Math.round(p.x - drag.dx)), y: Math.max(0, Math.round(p.y - drag.dy)) };
      cards.model.layout[drag.alias] = pos;
      drag.moved = true;
      const card = /** @type {HTMLElement} */ (inner.querySelector(`.dcard[data-alias="${CSS.escape(drag.alias)}"]`));
      card.style.left = `${pos.x}px`;
      card.style.top = `${pos.y}px`;
      drawEdges();
    }
  });

  inner.addEventListener("pointerup", (e) => {
    if (!drag) return;
    const d = drag;
    drag = null;
    if (d.type === "link") {
      const target = document.elementFromPoint(e.clientX, e.clientY);
      const port = /** @type {HTMLElement | null | undefined} */ (target?.closest(".port.in, .port.ref"));
      const card = /** @type {HTMLElement | null | undefined} */ (target?.closest(".dcard"));
      if (card) {
        // Dropped on a SQL card but beside its ports: the first wire is its main input, the next ones joins.
        const step = stepOf(card.dataset.alias);
        const fallback = step?.kind === "sql" && step.from.length && !step.from.includes(d.from) ? "ref" : "in";
        connect(d.from, card.dataset.alias, port ? port.dataset.port : fallback);
      } else if (d.picked) {
        touch({ inspector: true });
        status(`Wire ${d.from} → ${d.picked.to} removed.`);
      } else drawEdges();
    } else if (d.moved) {
      cards.dirty = true;
      invalidate("bar", "canvas");
    }
  });

  inner.addEventListener("dblclick", (e) => {
    const card = targetOf(e).closest(".dcard");
    if (card && !targetOf(e).closest(".port")) enterZoom(card.dataset.alias);
  });

  const canvas = $("d-canvas");
  canvas.addEventListener("dragover", (e) => { e.preventDefault(); e.dataTransfer.dropEffect = "copy"; });
  canvas.addEventListener("drop", (e) => {
    e.preventDefault();
    if (cards.zoom) return;
    let item;
    try { item = JSON.parse(e.dataTransfer.getData("application/json") || "null"); } catch { item = null; }
    if (!item) return;
    const p = canvasPoint(e);
    addStep(item.kind, item.component, p.x - CARD_W / 2, p.y - 20, item.brick);
  });
}

function bindChain() {
  const view = $("d-chain");
  const select = (item) => {
    cards.selected = { alias: cards.zoom, t: +item.dataset.t };
    invalidate("canvas", "inspector");
  };
  view.addEventListener("click", (e) => {
    if (targetOf(e).closest("[data-crumb]")) { e.preventDefault(); exitZoom(); return; }
    if (targetOf(e).closest("[data-chain-add]")) { addTransformer(stepOf(cards.zoom)); return; }
    const item = targetOf(e).closest(".chain-item");
    if (item) select(item);
    else if (targetOf(e) === view) { cards.selected = { alias: cards.zoom }; invalidate("canvas", "inspector"); }
  });
  view.addEventListener("keydown", (e) => {
    const item = targetOf(e).closest(".chain-item");
    if (item && (e.key === "Enter" || e.key === " ")) { select(item); e.preventDefault(); }
  });
}

function bindPalette() {
  const view = $("d-palette");
  view.addEventListener("dragstart", (e) => {
    const item = targetOf(e).closest(".pal-item");
    if (!item) return;
    e.dataTransfer.setData("application/json", JSON.stringify({ kind: item.dataset.kind, component: item.dataset.component || null, brick: item.dataset.brick || null }));
    e.dataTransfer.effectAllowed = "copy";
  });
  view.addEventListener("click", (e) => {
    if (!targetOf(e).closest("[data-add]")) return;
    const item = targetOf(e).closest(".pal-item");
    // Placed after the cards already there, in the column its kind usually takes.
    const column = { source: 0, transform: 1, sql: 1, merge: 1, sink: 2 }[item.dataset.kind] ?? 1;
    const inColumn = cards.model.steps.filter((s) => (cards.model.layout[s.alias]?.x ?? 0) >= 30 + column * (CARD_W + 70) - 20 && (cards.model.layout[s.alias]?.x ?? 0) < 30 + (column + 1) * (CARD_W + 70) - 20);
    addStep(item.dataset.kind, item.dataset.component || null, 30 + column * (CARD_W + 70), 30 + inColumn.length * (CARD_H + 36), item.dataset.brick || null);
  });
}

function onKey(e) {
  if (!cards.model || !host?.isConnected) return;
  if (["INPUT", "TEXTAREA", "SELECT"].includes(document.activeElement?.tagName)) return;
  if (cards.zoom) {
    // Zoomed, Delete removes the selected transformer, never the step.
    if (e.key === "Escape") { exitZoom(); e.preventDefault(); }
    else if ((e.key === "Delete" || e.key === "Backspace") && cards.selected?.t !== undefined) {
      removeTransformer(stepOf(cards.zoom), cards.selected.t);
      touch({ inspector: true });
      e.preventDefault();
    }
    return;
  }
  if (e.key === "Delete" || e.key === "Backspace") {
    if (cards.selected?.alias) { removeStep(cards.selected.alias); e.preventDefault(); }
    else if (cards.selected?.edge) { disconnect(cards.selected.edge); e.preventDefault(); }
  }
}

// ---------------------------------------------------------------- inspector

function kvRows(obj, section, index, placeholders) {
  return el("div", { class: "kv", "data-section": section, "data-index": index },
    Object.entries(obj || {}).map(([k, v], row) => el("div", { class: "kv-row", "data-row": row },
      el("input", { class: "mono", "data-kv": "key", value: k, placeholder: placeholders[0] }),
      el("input", { class: "mono", "data-kv": "value", value: v ?? "", placeholder: placeholders[1] }),
      el("button", { class: "small ghost", "data-kv-remove": true, title: "Remove" }, "×"))),
    el("button", { class: "small ghost", "data-kv-add": true }, `+ ${section === "mappings" ? "mapping" : "option"}`));
}

function inputsList(step) {
  const item = (alias, port) => el("li", {}, el("code", {}, alias), port === "ref" ? [" ", el("span", { class: "chip" }, "ref")] : null, " ",
    el("button", { class: "small ghost", "data-unwire": alias, "data-port": port, title: "Disconnect" }, "×"));
  const items = [...step.from.map((a) => item(a, "in")), ...step.ref.map((a) => item(a, "ref"))];
  return [el("h3", {}, "Inputs"), items.length ? el("ul", { class: "inputs" }, items) : el("p", { class: "hint" }, "Not wired yet: drag from another card's output port.")];
}

// One transformer's editor: its type, order buttons, mappings and options. `data-index` is its place in the step.
function transformerEditor(step, t, i) {
  const spec = TRANSFORMERS[t.type] || { hint: "", mapping: "key", value: "value" };
  const types = Object.keys(TRANSFORMERS).concat(TRANSFORMERS[t.type] ? [] : [t.type]);
  return el("div", { class: "transformer", "data-index": i },
    el("div", { class: "row" },
      el("select", { "data-ttype": true }, types.map((k) => el("option", { selected: k === t.type }, k))),
      el("span", { class: "actions" },
        el("button", { class: "small ghost", "data-tmove": "-1", disabled: i === 0 }, "↑"),
        el("button", { class: "small ghost", "data-tmove": "1", disabled: i === step.transformers.length - 1 }, "↓"),
        el("button", { class: "small ghost danger", "data-tremove": true }, "×"))),
    el("p", { class: "hint" }, spec.hint),
    spec.mapping !== null || t.mappings ? [el("div", { class: "sub" }, "mappings"), kvRows(t.mappings, "mappings", i, [spec.mapping || "key", spec.value ?? "value"])] : null,
    el("div", { class: "sub" }, "options"), kvRows(t.options, "options", i, ["option", "value"]));
}

function renderInspector() {
  const view = $("d-inspector");
  if (!view) return;
  if (cards.selected?.edge) {
    const e = cards.selected.edge;
    fill(view, el("h2", {}, "Connection"),
      el("p", {}, el("code", {}, e.from), " → ", el("code", {}, e.to), e.port === "ref" ? " (ref)" : ""),
      el("button", { class: "danger", "data-unwire": e.from, "data-port": e.port, "data-to": e.to }, "Disconnect"),
      el("p", { class: "hint" }, "Or press Delete, or click the × on the wire. To move it, drag its end off the input port and drop it on another input; dropped anywhere else, it is removed."));
    return;
  }
  const step = cards.selected?.alias ? stepOf(cards.selected.alias) : null;
  if (!step) {
    const errors = cards.result?.errors?.length || 0;
    fill(view, el("h2", {}, "Job"),
      el("p", { class: "hint" }, `${cards.model.steps.length} card(s). `, errors ? el("span", { class: "bad" }, `${errors} problem(s)`) : "Composes cleanly."),
      el("p", { class: "hint" }, "Select a card to edit it. A source and a sink of their own carry their endpoint as YAML; the server joins a source, its transform and its sink back into one branch when nothing else reads between them."),
      cards.model.directives.length ? [el("h3", {}, "Header directives"), el("ul", { class: "inputs" }, cards.model.directives.map((d) => el("li", {}, el("code", {}, d))))] : null);
    return;
  }
  if (cards.zoom && step.alias === cards.zoom) {
    const t = cards.selected.t !== undefined ? step.transformers?.[cards.selected.t] : null;
    fill(view, el("div", { class: "row" }, el("h2", {}, t ? `Transformer ${cards.selected.t + 1}` : "Transformers")),
      t ? transformerEditor(step, t, cards.selected.t) : el("p", { class: "hint" }, "Select a transformer in the chain to edit it, or add one."));
    return;
  }
  let body;
  if (isEndpoint(step) && !step.brick) {
    body = [step.kind === "sink" ? inputsList(step) : null,
      el("h3", {}, step.kind === "source" ? "Its reader" : "Its writer"),
      el("textarea", { id: "i-yaml", class: "mono", rows: "10", spellcheck: "false", value: step.yaml || "" }),
      el("p", { class: "hint" }, step.kind === "source"
        ? "input, the keys that bound or sample what it reads (limit, cursor, sampling-rate…), and its reader's provider-options blocks."
        : "output, the keys that govern what it writes (prefix, checkpoint, contract-save), and its writer's provider-options blocks.")];
  } else if (isEndpoint(step)) {
    body = [step.kind === "sink" ? inputsList(step) : null, brickView(step)];
  } else if (step.kind === "transform") {
    body = [inputsList(step), el("h3", {}, "Transformers ", el("span", { class: "hint" }, "applied in order")),
      (step.transformers || []).map((t, i) => transformerEditor(step, t, i)),
      el("button", { "data-tadd": true }, "+ transformer")];
  } else if (step.kind === "sql") {
    body = [inputsList(step), el("h3", {}, "Query ", el("span", { class: "hint" }, "DuckDB; each input is a table named by its alias")),
      el("textarea", { id: "i-query", class: "mono", rows: "10", spellcheck: "false", value: step.query || "" })];
  } else if (step.kind === "merge") {
    body = [inputsList(step), el("p", { class: "hint" }, "UNION ALL: every input must carry the same columns, with the same types. dtpipe converts nothing on the way.")];
  } else {
    body = [inputsList(step), el("h3", {}, "Branch, as written"),
      el("textarea", { id: "i-yaml", class: "mono", rows: "12", spellcheck: "false", value: step.yaml || "" }),
      el("p", { class: "hint" }, "A branch no card describes: kept exactly as dtpipe reads it.")];
  }
  fill(view,
    el("div", { class: "row" }, el("h2", {}, KIND_LABEL[step.kind] || step.kind), el("button", { class: "small ghost danger", "data-remove-step": true }, "Delete card")),
    el("label", { class: "field" }, "Alias ", el("input", { id: "i-alias", class: "mono", value: step.alias })),
    body);
}

// A brick card: what the coordinator's catalog says of it. Its branch is its node's, never edited here.
function brickView(step) {
  const brick = brickOf(step.brick);
  if (!brick) return [el("p", {}, el("code", {}, step.brick)), el("p", { class: "hint warn" }, "Not in the catalog read last: sign in to the coordinator to read it.")];
  return [
    el("p", {}, el("b", {}, brick.title || brick.id), " ", el("span", { class: "hint" }, `${brick.kind} brick`)),
    brick.description ? el("p", { class: "hint" }, brick.description) : null,
    el("table", { class: "vars" }, el("tbody", {},
      el("tr", {}, el("td", {}, "node"), el("td", {}, brick.node, brick.online ? "" : " (offline)")),
      el("tr", {}, el("td", {}, "brick"), el("td", {}, el("code", {}, brick.key))),
      el("tr", {}, el("td", {}, "version"), el("td", {}, el("code", {}, brick.version))))),
    brick.kind === "source" ? [
      el("h3", {}, "Columns"),
      brick.schema?.length
        ? el("table", { class: "vars" }, el("tbody", {}, brick.schema.map((c) => el("tr", {}, el("td", {}, el("code", {}, c.name)), el("td", {}, c.type, c.isNullable ? "" : " not null")))))
        : el("p", { class: "hint" }, brick.schemaError || "Its node read no schema."),
    ] : null,
    el("p", { class: "hint" }, step.kind === "source"
      ? "A sample reads its rows from its node, through the coordinator."
      : "A sample writes nothing to it: its rows end in a throwaway file."),
  ];
}

function bindInspector() {
  const view = $("d-inspector");
  const current = () => (cards.selected?.alias ? stepOf(cards.selected.alias) : null);

  view.addEventListener("change", (e) => {
    const step = current();
    if (targetOf(e).id === "i-alias" && step) renameStep(step.alias, targetOf(e).value);
    else if (targetOf(e).matches("[data-ttype]") && step) {
      const i = +targetOf(e).closest(".transformer").dataset.index;
      const t = step.transformers[i];
      t.type = targetOf(e).value;
      if (TRANSFORMERS[t.type]?.mapping === null) delete t.mappings;
      touch({ inspector: true });
    }
  });

  view.addEventListener("input", (e) => {
    const step = current();
    if (!step) return;
    if (targetOf(e).id === "i-query") { step.query = targetOf(e).value; touch(); }
    else if (targetOf(e).id === "i-yaml") { step.yaml = targetOf(e).value; touch(); }
    else if (targetOf(e).dataset.kv) {
      const kv = targetOf(e).closest(".kv");
      const t = step.transformers[+kv.dataset.index];
      const section = kv.dataset.section;
      const rows = [...kv.querySelectorAll(".kv-row")].map((r) => [r.querySelector('[data-kv="key"]').value, r.querySelector('[data-kv="value"]').value]);
      const obj = {};
      for (const [k, v] of rows) if (k.trim()) obj[k.trim()] = v === "" ? null : v;
      t[section] = obj;
      touch();
    }
  });

  view.addEventListener("click", (e) => {
    const b = targetOf(e).closest("button");
    if (!b) return;
    const step = current();
    if (b.dataset.unwire !== undefined) { disconnect({ from: b.dataset.unwire, to: b.dataset.to || step.alias, port: b.dataset.port }); return; }
    if (!step) return;
    if (b.dataset.removeStep !== undefined) { removeStep(step.alias); return; }
    if (b.dataset.tadd !== undefined) { addTransformer(step); return; }
    const box = b.closest(".transformer");
    if (!box) return;
    const i = +box.dataset.index;
    if (b.dataset.tremove !== undefined) removeTransformer(step, i);
    else if (b.dataset.tmove) {
      const j = i + +b.dataset.tmove;
      [step.transformers[i], step.transformers[j]] = [step.transformers[j], step.transformers[i]];
      if (cards.zoom) cards.selected = { alias: step.alias, t: j };
    } else if (b.dataset.kvAdd !== undefined) {
      const section = b.closest(".kv").dataset.section;
      const t = step.transformers[i];
      t[section] = { ...(t[section] || {}), "": null };
      // An empty key is kept on screen only: the row is written once it has a name.
      invalidate("inspector");
      flush();
      const rows = view.querySelectorAll(`.transformer[data-index="${i}"] .kv[data-section="${section}"] .kv-row`);
      rows[rows.length - 1]?.querySelector("input")?.focus();
      delete t[section][""];
      return;
    } else if (b.dataset.kvRemove !== undefined) {
      const row = b.closest(".kv-row");
      const section = b.closest(".kv").dataset.section;
      const key = row.querySelector('[data-kv="key"]').value.trim();
      delete step.transformers[i][section]?.[key];
    }
    touch({ inspector: true });
  });
}

// ---------------------------------------------------------------- drawer: YAML, problems, preview

function renderDrawer() {
  const view = $("d-drawer");
  if (!view || !cards.result) return;
  const problems = (cards.result.errors?.length || 0) + (cards.result.warnings?.length || 0);
  const tab = (name, label, extra) => el("button", { class: state.view.drawer === name ? "active" : "", "data-tab": name }, label, extra || null);
  const tabs = el("div", { class: "tabs" },
    tab("yaml", "YAML"),
    tab("problems", "Problems ", problems ? el("span", { class: "count" }, problems) : null),
    tab("edit", "Edit as YAML"),
    tab("preview", "Preview"),
    (hooks.extraTabs || []).map((t) => tab(t.name, t.label, t.badge?.())),
    el("span", { class: "drawer-status" }, cards.result.errors?.length ? el("span", { class: "bad" }, `${cards.result.errors.length} error(s)`) : el("span", { class: "good" }, "composes")));
  let body;
  if (state.view.drawer === "yaml") {
    body = [el("div", { class: "row" }, el("button", { "data-copy": true }, "Copy"),
      el("span", { class: "hint" }, "The job the server composed: a plain dtpipe job, runnable whole with dtpipe --job.")),
    el("pre", { id: "d-yaml" }, cards.result.yaml)];
  } else if (state.view.drawer === "problems") {
    body = [cards.result.errors?.length ? el("ul", { class: "msgs bad" }, cards.result.errors.map((m) => el("li", {}, m))) : null,
      cards.result.warnings?.length ? el("ul", { class: "msgs warn" }, cards.result.warnings.map((m) => el("li", {}, m))) : null,
      problems ? null : el("p", { class: "hint" }, "Nothing to report.")];
  } else if (state.view.drawer === "edit") {
    body = [el("textarea", { id: "d-edit", class: "mono", rows: "16", spellcheck: "false", value: cards.result.yaml }),
      el("div", { class: "row" }, el("button", { "data-apply": true }, "Apply"),
        el("span", { class: "hint" }, "The job is taken apart into cards again; cards that keep their alias keep their place."))];
  } else if (state.view.drawer === "preview") {
    body = hooks.previewView?.(cards.selected?.alias || null) || null;
  } else {
    body = (hooks.extraTabs || []).find((t) => t.name === state.view.drawer)?.render() || null;
  }
  fill(view, tabs, el("div", { class: "drawer-body" }, body));
}

export function showTab(name) {
  state.view.drawer = name;
  invalidate("drawer");
}

function bindDrawer() {
  $("d-drawer").addEventListener("click", async (e) => {
    const b = targetOf(e).closest("button");
    if (!b) return;
    if (b.dataset.tab) showTab(b.dataset.tab);
    else if (b.dataset.copy !== undefined) {
      try { await navigator.clipboard.writeText(cards.result.yaml); status("YAML copied."); }
      catch { window.getSelection().selectAllChildren($("d-yaml")); status("Selected: copy it with your keyboard."); }
    } else if (b.dataset.apply !== undefined) {
      try {
        const ticket = opening.begin();
        const answer = await api("/api/design/decompose", { yaml: /** @type {HTMLTextAreaElement} */ ($("d-edit")).value, layout: cards.model.layout });
        if (opening.stale(ticket)) return;
        const layout = cards.model.layout;
        cards.model = normalize(answer.model);
        cards.model.layout = Object.fromEntries(Object.entries(layout).filter(([a]) => stepOf(a)));
        if (cards.model.steps.some((s) => !cards.model.layout[s.alias])) autoLayout(false);
        cards.selected = null;
        cards.zoom = null;
        state.view.drawer = "yaml";
        cards.dirty = true;
        renderShell();
        await compose();
      } catch (err) { status(err.message, "bad"); }
    }
  });
}

// ---------------------------------------------------------------- sample tables

export function stageTable(stage) {
  const columns = stage.columns || [];
  return el("table", {},
    el("thead", {}, el("tr", {}, columns.map((c) => el("th", {}, c.Name ?? c.name, el("small", {}, c.type))))),
    el("tbody", {}, (stage.sample || []).map((row) => el("tr", {},
      row.map((v) => (v === null ? el("td", { class: "null" }, "null") : el("td", {}, v)))))));
}
