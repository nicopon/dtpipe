// @ts-check
// The studio's designer: a job of the folder as cards, one per source, step and sink. The server
// composes the job (POST /api/design/compose) on every change and takes a job apart into cards
// (POST /api/design/decompose); this page never writes YAML itself. Built with DOM calls only: the
// page's policy forbids inline script and style, and every value is set as text.

import { $, api, el, fill, info, plural, status, svg, tabOf, tagOf, targetOf } from "./dom.js";
import { KEPT_TABS } from "./prefs.js";
import { flush, invalidate, onZone } from "./render.js";
import { sequence, state } from "./state.js";

const CARD_W = 210;
const CARD_H = 114;
// A wire ends on a port's centre. The ports are positioned from the card's padding box (below its
// 4px top border) in studio.css: .port.in/.out top 27px and .port.ref top 59px, each 14px tall.
const PORT_IN_Y = 4 + 27 + 7;
const PORT_REF_Y = 4 + 59 + 7;

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

const KIND_LABEL = { source: "source", sink: "output", transform: "transform", sql: "sql", merge: "merge", branch: "branch" };

// The panels' sizes, kept across visits.
const SIZE_LIMITS = { pal: [160, 420], insp: [240, 640], drawer: [210, 640], agent: [260, 720] };
const SIZE_VARS = { pal: "--pal-w", insp: "--insp-w", drawer: "--drawer-h", agent: "--agent-w" };
// The drawer's floor holds its tabs, a few lines and its action bar; the canvas keeps a floor of its own. The layout reads both
// (`--drawer-min`, `--canvas-min`), so a window too short for the size asked shrinks the drawer, never its actions out of sight.
const CANVAS_MIN = 120;

const cards = state.cards;
let host = null;
let hooks = {};
const composing = sequence();
const opening = sequence();
const reading = sequence();

onZone("palette", renderPalette);
onZone("canvas", renderCanvas);
onZone("inspector", renderInspector);
onZone("drawer", renderDrawer);
onZone("bar", renderToolbarState);
let composeTimer = null;

/**
 * Builds the designer in `root`. The host gives what only it knows: `toolbar` (the bar's buttons), `onComposed`,
 * `cardTools`, `cardBadge` and `cardTool` (what a card shows and does), `previewView` (the Preview tab), `publicationProblems`
 * (the second verdict of Problems) and `extraTabs`.
 */
export function mountDesigner(root, options = {}) {
  host = root;
  hooks = options;
  document.addEventListener("keydown", onKey);
}

/**
 * Opens a job: its text taken apart into cards by the server, with the layout it was saved with. A job not on disk opens
 * as unsaved changes. `kept` is how the page was left on it: the tab and the card, where they are still there. `replacing`
 * names a text that replaces the open job's cards (a plan, a file the agent saved), which Undo puts back; any other open
 * starts the history afresh.
 * @param {string} yaml @param {any} layout @param {boolean} [unsaved] @param {{ tab?: string, card?: string } | null} [kept]
 * @param {string | null} [replacing]
 */
export async function openJob(yaml, layout, unsaved = false, kept = null, replacing = null) {
  const ticket = opening.begin();
  reading.cancel();
  state.brickRows = null;
  const answer = await api("/api/design/decompose", { yaml, layout: layout || null });
  // Another job was asked for meanwhile: its cards are the ones to show.
  if (opening.stale(ticket)) return false;
  const show = () => {
    const model = normalize(answer.model);
    // A text with no header comment says nothing of the job's header: the steps it replaces keep theirs.
    if (replacing && cards.model && !model.header) model.header = cards.model.header;
    putCards(model, answer, kept);
  };
  if (replacing && cards.model) remember(replacing, show);
  else { cards.history = []; show(); }
  await compose();
  if (!opening.stale(ticket)) cards.dirty = unsaved;
  return !opening.stale(ticket);
}

/**
 * Puts a model on the cards and draws them, the one step opening a job and undoing share. `kept` is the tab and the card to
 * keep, where they are still there; `tab`, when given, stays the drawer's tab whatever it is.
 * @param {Model} model @param {Composed | null} result @param {{ tab?: string, card?: string } | null} kept @param {string | null} [tab]
 */
function putCards(model, result, kept, tab = null) {
  cards.model = model;
  cards.model.layout = Object.fromEntries(Object.entries(cards.model.layout).filter(([a]) => stepOf(a)));
  if (cards.model.steps.some((s) => !cards.model.layout[s.alias])) autoLayout(false);
  cards.result = result;
  cards.selected = kept?.card && stepOf(kept.card) ? { alias: kept.card } : null;
  cards.zoom = null; cards.dirty = false; cards.text = null;
  // A job that does not compose opens on its problems, whatever was kept.
  state.view.drawer = tab ?? (cards.result?.errors?.length ? "problems" : kept?.tab && KEPT_TABS.includes(kept.tab) ? kept.tab : "yaml");
  renderShell();
}

// ---------------------------------------------------------------- the cards' history

const HISTORY_LIMIT = 20;

/** A value as text with its keys sorted, so two models the server and the inspector built alike read alike. @param {any} value */
const canonical = (value) => JSON.stringify(value, (_, v) => (v && typeof v === "object" && !Array.isArray(v)
  ? Object.fromEntries(Object.entries(v).sort(([a], [b]) => (a < b ? -1 : a > b ? 1 : 0))) : v));

/** What a model holds besides where its cards sit. @param {Model} model */
const fingerprint = ({ layout, ...rest }) => canonical(rest);

/**
 * Runs `change`, which removes or replaces cards, after keeping the cards as they were: Undo puts them back. The history lives
 * in the page only (never saved, never sent), holds the last few changes, and an open of another job empties it.
 * @param {string} label what Undo undoes, as its title says it @param {() => void} change
 */
function remember(label, change) {
  const entry = { label, model: structuredClone(cards.model), dirty: cards.dirty, hash: state.job?.hash ?? null, after: "" };
  change();
  entry.after = fingerprint(/** @type {Model} */ (cards.model));
  cards.history = [...cards.history.slice(1 - HISTORY_LIMIT), entry];
  invalidate("bar");
}

/** The change Undo would undo, and whether it can: only while the cards are as that change left them, so no edit made since is lost. */
function undoable() {
  const entry = cards.history.at(-1);
  return { entry, ready: !!entry && !!cards.model && fingerprint(cards.model) === entry.after };
}

/**
 * Puts back the cards the last change removed or replaced, through the step an open takes. They are unsaved unless they were
 * saved when the change came and the file has not been written since.
 */
export async function undo() {
  const { entry, ready } = undoable();
  if (!entry || !ready || editingText()) return;
  cards.history = cards.history.slice(0, -1);
  const ticket = opening.begin();
  reading.cancel();
  state.brickRows = null;
  putCards(entry.model, null, { card: cards.selected?.alias }, state.view.drawer);
  invalidate("agent");
  await compose();
  if (!opening.stale(ticket)) cards.dirty = entry.dirty || entry.hash !== (state.job?.hash ?? null);
  invalidate("bar");
}

/**
 * The cards `after` adds, changes and removes against `before`, by alias, where they sit left aside. Both models should come
 * from the server's decomposition, so that a card reads alike on both sides.
 * @param {any} before @param {any} after
 */
export function cardChanges(before, after) {
  const of = (/** @type {any} */ m) => new Map((m?.steps || []).map((/** @type {any} */ s) => [s.alias, canonical(s)]));
  const was = of(before), now = of(after);
  return {
    added: [...now.keys()].filter((a) => !was.has(a)),
    changed: [...now.keys()].filter((a) => was.has(a) && was.get(a) !== now.get(a)),
    removed: [...was.keys()].filter((a) => !now.has(a)),
  };
}

/** Starts from no step at all. */
export async function openBlank() {
  opening.cancel();
  cards.history = [];
  cards.model = normalize({ header: null, steps: [], layout: {} });
  cards.result = null; cards.selected = null; cards.zoom = null; cards.dirty = true; cards.text = null; state.view.drawer = "yaml";
  renderShell();
  await compose();
}

/** Closes the open job and clears any pending design requests. */
export function closeDesigner() {
  opening.cancel();
  composing.cancel();
  reading.cancel();
  clearTimeout(composeTimer);
  state.brickRows = null;
  Object.assign(cards, { model: null, result: null, selected: null, zoom: null, dirty: false, history: [], text: null });
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

/** Whether the YAML tab holds a text the cards do not: they are held until it is applied or reverted. */
export const editingText = () => cards.text !== null && cards.text !== (cards.result?.yaml ?? "");

/** Whether leaving the job loses what the person did: cards not saved, or a YAML text not applied. */
export const holdsEdits = () => cards.dirty || editingText();

function normalize(m) {
  return {
    header: m.header || null,
    steps: (m.steps || []).map((s) => ({ ...s, from: s.from || [], ref: s.ref || [], comment: s.comment || null })),
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
    // A studio that does not answer composed nothing: the cards are not wrong and the last verdict stays, but what asked
    // (a save, a preview) is told no verdict came.
    if (e.transport) { if (!composing.stale(seq)) status(e.message, "bad"); return { ...cards.result, unanswered: true }; }
    if (!composing.stale(seq)) cards.result = { yaml: "", steps: [], errors: [e.message], warnings: [] };
  }
  invalidate("canvas", "drawer", "bar");
  // With no wire and no step selected the inspector shows the job's summary; skip redrawing while typing in its comments.
  if (!cards.selected?.edge && !(cards.selected?.alias && cards.model && stepOf(cards.selected.alias)) && document.activeElement?.id !== "i-header")
    invalidate("inspector");
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
  if (kind === "sql") step.query = starterQuery(null);
  cards.model.steps.push(step);
  cards.model.layout[alias] = { x: Math.max(10, Math.round(x)), y: Math.max(10, Math.round(y)) };
  cards.selected = { alias };
  touch({ inspector: true });
}

function removeStep(alias) {
  remember(`deleting step '${alias}'`, () => {
    cards.model.steps = cards.model.steps.filter((s) => s.alias !== alias);
    for (const s of cards.model.steps) { s.from = s.from.filter((a) => a !== alias); s.ref = s.ref.filter((a) => a !== alias); }
    delete cards.model.layout[alias];
  });
  if (cards.zoom === alias) cards.zoom = null;
  cards.selected = null;
  touch({ inspector: true });
  applyView();
}

function renameStep(from, to) {
  to = to.trim();
  if (to === from) return;
  if (!/^[A-Za-z][A-Za-z0-9_-]*$/.test(to)) { status(`'${to}' is not a valid alias: letter first, then letters, digits, _ or -.`, "bad"); invalidate("inspector"); return; }
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

/**
 * The query a new SQL card starts with: it reads its main input by that input's alias, which is the table the SQL
 * processor exposes. Until one is wired it names a placeholder, which composing refuses for want of an input anyway.
 * @param {string | null} from
 */
const starterQuery = (from) => `SELECT *\nFROM ${from || "input"}`;

/** Wires `fromAlias`'s output into `toAlias`: the one write both the drag and the inspector's Connect make. Answers whether it wired. */
function connect(fromAlias, toAlias, port) {
  const target = stepOf(toAlias);
  if (!target || fromAlias === toAlias) return false;
  if (target.kind === "source") { status("A source reads its own input: it takes no other.", "bad"); return false; }
  if (port === "ref") {
    if (!target.ref.includes(fromAlias) && !target.from.includes(fromAlias)) target.ref.push(fromAlias);
  } else if (target.kind === "merge" || target.kind === "branch") {
    if (!target.from.includes(fromAlias)) target.from.push(fromAlias);
  } else {
    // A query nobody has edited follows the input it reads; one the person wrote is theirs.
    if (target.kind === "sql" && target.query === starterQuery(target.from[0] ?? null)) target.query = starterQuery(fromAlias);
    target.from = [fromAlias];
    target.ref = target.ref.filter((a) => a !== fromAlias);
  }
  touch({ inspector: cards.selected?.alias === toAlias });
  status(`Wired ${fromAlias} → ${toAlias}${port === "ref" ? " as a join" : ""}.`);
  return true;
}

function disconnect(edge) {
  const target = stepOf(edge.to);
  if (!target) return;
  const fromInspector = !!document.activeElement && !!$("d-inspector")?.contains(document.activeElement);
  if (edge.port === "ref") target.ref = target.ref.filter((a) => a !== edge.from);
  else target.from = target.from.filter((a) => a !== edge.from);
  // A wire selected goes with its selection; a card whose input is removed stays chosen, to be wired again.
  if (cards.selected?.edge) cards.selected = null;
  touch({ inspector: true });
  status(`Wire ${edge.from} → ${edge.to} removed.`);
  if (fromInspector) focusAfterDraw("#i-connect-from", `.dcard[data-alias="${CSS.escape(edge.to)}"]`);
}

/** Puts the focus on the first of these that exists once the zones are drawn: a control the person was on is gone when its zone is. */
function focusAfterDraw(...selectors) {
  flush();
  for (const selector of selectors) {
    const node = document.querySelector(selector);
    if (node) { /** @type {HTMLElement} */ (node).focus(); return; }
  }
}

/** Chooses a card: marks it in place (the node under the pointer or the focus is never redrawn), shows its inspector and what it delivers. @param {string} alias */
function selectCard(alias) {
  if (cards.selected?.alias === alias) return;
  cards.selected = { alias };
  invalidate("inspector");
  for (const c of $("d-inner").querySelectorAll(".dcard")) {
    const chosen = c.dataset.alias === alias;
    c.classList.toggle("selected", chosen);
    if (chosen) c.setAttribute("aria-current", "true"); else c.removeAttribute("aria-current");
  }
  drawEdges();
  // Choosing a card shows what it delivers, unless the person is reading another tab.
  if (state.view.drawer === "yaml") state.view.drawer = "preview";
  if (state.view.drawer === "preview") invalidate("drawer");
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
      el("a", { href: "#", "data-crumb": true, title: "Back to the job (Esc)" }, state.job?.path || "Job"), el("span", { class: "sep" }, "›"), el("b", {}, step.alias)),
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
      el("span", { id: "d-dirty", class: "pill warn", hidden: true }, "unsaved"),
      el("span", { class: "actions" },
        seg("d-undo", "Undo", "Nothing to undo"),
        seg("d-layout", "Arrange", "Arrange the steps in columns, sources first"),
        hooks.toolbar?.() || null)),
    el("div", { class: "designer-body" },
      el("div", { class: "canvas card", id: "d-canvas" },
        el("div", { class: "canvas-inner", id: "d-inner" }, svg("svg", { id: "d-svg" })),
        el("div", { class: "chain-view", id: "d-chain", hidden: true })),
      el("aside", { class: "inspector card", id: "d-inspector" }),
      el("div", { class: "split split-col split-insp", "data-split": "insp", role: "separator", "aria-orientation": "vertical", "aria-label": "Resize the inspector", tabindex: "0" }),
      el("p", { id: "d-held", class: "held-note", hidden: true }, "The steps are held: the YAML tab has a text not applied. ",
        el("button", { class: "small", onclick: () => showTab("yaml") }, "Show the YAML"))),
    el("div", { class: "split split-row", "data-split": "drawer", role: "separator", "aria-orientation": "horizontal", "aria-label": "Resize the drawer", tabindex: "0" }),
    el("div", { class: "drawer card", id: "d-drawer" }));
  // The palette lives in the page's side panel (Steps), a new one for each job like the rest of the designer.
  fill($("side-steps-view"), el("div", { class: "palette", id: "d-palette" }));

  $("d-layout").addEventListener("click", () => autoLayout());
  $("d-undo").addEventListener("click", () => { undo().catch((e) => status(e.message, "bad")); });
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

/** Applies every size to the element that lays its pane out: the side panel's and the agent's widths belong to the page's grid, not the designer's. */
export function applySizes() {
  for (const [name, [lo, hi]] of Object.entries(SIZE_LIMITS)) {
    state.view.sizes[name] = Math.min(hi, Math.max(lo, Math.round(+state.view.sizes[name] || lo)));
    (name === "agent" || name === "pal" ? document.querySelector("main") : host)?.style.setProperty(SIZE_VARS[name], `${state.view.sizes[name]}px`);
  }
  host?.style.setProperty("--drawer-min", `${SIZE_LIMITS.drawer[0]}px`);
  host?.style.setProperty("--canvas-min", `${CANVAS_MIN}px`);
}

/**
 * The tallest the drawer can be now: what it and the canvas share, less the canvas's floor. Measured, so the gaps and the bar
 * are never restated here. Null when the drawer is not laid out (a window narrow enough to stack the panels).
 */
function drawerRoom() {
  const drawer = $("d-drawer");
  const body = host?.querySelector(".designer-body");
  return drawer && body && drawer.offsetHeight ? drawer.offsetHeight + body.offsetHeight - CANVAS_MIN : null;
}

/** The size a pane shows now: the drawer's is shrunk by the layout when the window is too short for what was asked. */
const shownSize = (name) => (name === "drawer" ? $("d-drawer")?.offsetHeight || state.view.sizes.drawer : state.view.sizes[name]);

function resize(name, value, persist) {
  if (name === "drawer") value = Math.min(value, drawerRoom() ?? value);
  state.view.sizes[name] = value;
  applySizes();
  // The sizes are kept after a drawing (prefs.js): asking for one is how a finished move is noticed.
  if (persist) invalidate("bar");
}

// The designer's handles; the side panel's and the agent's lie in their own panels, which bind them through bindSplit.
function bindSplits() {
  const body = host.querySelector(".designer-body");
  for (const handle of host.querySelectorAll(".split")) {
    const name = handle.dataset.split;
    bindSplit(/** @type {HTMLElement} */ (handle), name, (e) => {
      if (name === "insp") return body.getBoundingClientRect().right - e.clientX - 6;
      return host.getBoundingClientRect().bottom - e.clientY - 12;
    });
  }
}

const SPLIT_KEYS = { pal: ["ArrowRight", "ArrowLeft"], insp: ["ArrowLeft", "ArrowRight"], drawer: ["ArrowUp", "ArrowDown"], agent: ["ArrowLeft", "ArrowRight"] };

/**
 * A handle dragged with the pointer, or moved with the arrow keys; the size is clamped to its limits.
 * @param {HTMLElement} handle @param {string} name @param {(e: PointerEvent) => number} target the size the pointer asks for
 */
export function bindSplit(handle, name, target) {
  handle.addEventListener("pointerdown", (e) => { handle.setPointerCapture(e.pointerId); handle.classList.add("dragging"); e.preventDefault(); });
  handle.addEventListener("pointermove", (e) => { if (handle.hasPointerCapture(e.pointerId)) resize(name, target(e), false); });
  const end = (e) => { if (handle.hasPointerCapture(e.pointerId)) { handle.releasePointerCapture(e.pointerId); handle.classList.remove("dragging"); resize(name, state.view.sizes[name], true); } };
  handle.addEventListener("pointerup", end);
  handle.addEventListener("pointercancel", end);
  handle.addEventListener("keydown", (e) => {
    const [grow, shrink] = SPLIT_KEYS[name];
    if (e.key !== grow && e.key !== shrink) return;
    resize(name, shownSize(name) + (e.key === grow ? 16 : -16), true);
    e.preventDefault();
  });
}

function renderToolbarState() {
  const flag = $("d-dirty");
  if (flag) flag.hidden = !cards.dirty;
  const button = /** @type {HTMLButtonElement | null} */ ($("d-undo"));
  if (!button) return;
  const { entry, ready } = undoable();
  button.disabled = !ready;
  button.title = !entry ? "Nothing to undo"
    : ready ? `Undo ${entry.label}`
    : `Undo ${entry.label}: the steps changed since, and undoing would lose that change`;
}

function renderPalette() {
  const view = $("d-palette");
  // A panel that does not show the palette leaves it as it is: opening Steps draws it again.
  if (!view || view.closest("[hidden]")) return;
  const item = (kind, label, hint, component) => el("div",
    { class: `pal-item kind-${kind}`, draggable: "true", "data-kind": kind, "data-component": component, title: hint },
    el("span", {}, label), el("button", { class: "small ghost", "data-add": true, "aria-label": `Add ${label} to the canvas`, title: "Add to the canvas" }, "+"));
  const endpoints = (list, kind) => list.length
    ? list.map((c) => item(kind, c.name, `${c.category} · ${kind === "source" ? "reads" : "writes"} through the ${c.name} component`, c.name))
    : el("p", { class: "hint" }, "none");
  const brickItem = (b) => {
    const node = el("div",
      { class: `pal-item kind-${b.kind}${b.online ? "" : " offline"}`, draggable: "true", "data-kind": b.kind, "data-brick": b.key,
        title: `${b.kind} brick of ${b.node} · version ${b.version}${b.online ? "" : " · its node is offline"}${b.description ? "\n" + b.description : ""}` },
      el("span", {}, b.title || b.id), el("button", { class: "small ghost", "data-add": true, "aria-label": `Add ${b.title || b.id} to the canvas`, title: "Add to the canvas" }, "+"));
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
    el("h3", {}, "Outputs"), endpoints(state.components.writers, "sink"),
    el("p", { class: "hint" }, "Wire output (right) → input (left). ", info("Drag a wire end to another input to move it, or off the ports to remove it.")),
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
      el("li", {}, "this studio: ", el("code", { title: state.catalog.studioVersion }, tagOf(state.catalog.studioVersion) || "unknown")),
      state.catalog.nodes.map((n) => el("li", { class: n.dtpipeVersion === state.catalog.studioVersion ? "" : "warn" },
        `${n.name}: `, el("code", { title: n.dtpipeVersion || "" }, tagOf(n.dtpipeVersion, state.catalog.studioVersion) || "unknown")))),
    differ.length ? el("p", { class: "hint warn" }, `⚠ ${plural(differ.length, "node")} on another dtpipe version: samples may differ.`) : null,
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
  else if (step.kind === "merge") subtitle = `UNION ALL of ${plural(step.from.length, "input")}`;
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
  // The cards are drawn again; a card or a tool of one that holds the focus gets it back.
  const held = document.activeElement?.closest?.(".dcard") ? document.activeElement : null;
  const heldAt = held ? { alias: held.closest(".dcard").dataset.alias, tool: held.dataset.cardTool ?? null, self: held.classList.contains("dcard") } : null;
  inner.querySelectorAll(".dcard, .canvas-empty").forEach((c) => c.remove());
  let width = 900, height = 520;
  for (const step of cards.model.steps) {
    const pos = cards.model.layout[step.alias] || { x: 20, y: 20 };
    const { subtitle, branch } = cardInfo(step);
    const card = el("div", {
      class: `dcard kind-${step.kind} ${cards.selected?.alias === step.alias ? "selected" : ""} ${problems.has(step.alias) ? "problem" : ""}`,
      "data-alias": step.alias, tabindex: "0", role: "group", "aria-label": `${KIND_LABEL[step.kind] || step.kind} ${step.alias}`,
      "aria-current": cards.selected?.alias === step.alias ? "true" : null,
    },
      el("div", { class: "dcard-head" }, el("span", { class: "kind" }, KIND_LABEL[step.kind] || step.kind), el("span", { class: "alias" }, step.alias)),
      step.kind === "transform" ? el("button", { class: "dcard-zoom", title: "Zoom into the transformers (or double-click the step)", "aria-label": `Zoom into ${step.alias}` }, "⌕") : null,
      hooks.cardTools?.(step.alias) || null,
      el("div", { class: "dcard-sub" }, subtitle),
      hooks.cardBadge?.(step.alias) || null,
      el("div", { class: "dcard-branch", title: branch || null }, branch),
      step.kind !== "source" ? el("span", { class: "port in", "data-port": "in", title: step.kind === "merge" ? "inputs (several)" : "input" }) : null,
      step.kind === "sql" ? [el("span", { class: "port ref", "data-port": "ref", title: "joined inputs (ref)" }), el("span", { class: "port-label ref" }, "ref")] : null,
      step.kind !== "sink" ? el("span", { class: "port out", "data-port": "out", title: "output: drag to an input" }) : null);
    card.style.left = `${pos.x}px`;
    card.style.top = `${pos.y}px`;
    inner.appendChild(card);
    width = Math.max(width, pos.x + CARD_W + 200);
    height = Math.max(height, pos.y + CARD_H + 160);
  }
  if (heldAt && (heldAt.self || heldAt.tool)) {
    const card = `.dcard[data-alias="${CSS.escape(heldAt.alias)}"]`;
    /** @type {HTMLElement | null} */ (inner.querySelector(heldAt.self ? card : `${card} [data-card-tool="${CSS.escape(heldAt.tool)}"]`))?.focus();
  }
  inner.style.width = `${width}px`;
  inner.style.height = `${height}px`;
  drawEdges();
  if (!cards.model.steps.length) inner.appendChild(el("p", { class: "canvas-empty hint" }, "Drag sources, steps and outputs here from Steps, on the left."));
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
    const button = el("button", { class: "edge-delete", "aria-label": "Remove this wire", title: "Remove this wire (Delete)" }, "×");
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
      selectCard(card.dataset.alias);
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

  // A card is chosen with Enter or Space, as a click chooses it; its wires are the inspector's.
  inner.addEventListener("keydown", (e) => {
    const card = targetOf(e);
    if (!card.classList?.contains("dcard") || (e.key !== "Enter" && e.key !== " ")) return;
    e.preventDefault();
    selectCard(card.dataset.alias);
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
  if (!cards.model || !host?.isConnected || editingText()) return;
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
  const names = (section === "mappings" ? [`Mapping ${placeholders[0]}`, `Mapping ${placeholders[1]}`] : ["Option name", "Option value"]).map((n) => `${n} of transformer ${index + 1}`);
  return el("div", { class: "kv", "data-section": section, "data-index": index },
    Object.entries(obj || {}).map(([k, v], row) => el("div", { class: "kv-row", "data-row": row },
      el("input", { class: "mono", "data-kv": "key", "aria-label": names[0], value: k, placeholder: placeholders[0] }),
      el("input", { class: "mono", "data-kv": "value", "aria-label": names[1], value: v ?? "", placeholder: placeholders[1] }),
      el("button", { class: "small ghost", "data-kv-remove": true, "aria-label": `Remove this ${section === "mappings" ? "mapping" : "option"} of transformer ${index + 1}`, title: "Remove" }, "×"))),
    el("button", { class: "small ghost", "data-kv-add": true }, `+ ${section === "mappings" ? "mapping" : "option"}`));
}

function inputsList(step) {
  const item = (alias, port) => el("li", {}, el("code", {}, alias), port === "ref" ? [" ", el("span", { class: "chip" }, "ref")] : null, " ",
    el("button", { class: "small ghost", "data-unwire": alias, "data-port": port, "aria-label": `Disconnect ${alias} from ${step.alias}`, title: "Disconnect" }, "×"));
  const items = [...step.from.map((a) => item(a, "in")), ...step.ref.map((a) => item(a, "ref"))];
  return [el("h3", {}, "Inputs"), items.length ? el("ul", { class: "inputs" }, items) : el("p", { class: "hint" }, "Not wired: drag from an output port, or connect below."), connectControls(step)];
}

// The keyboard's way to wire a card: the choice of an input and a button, which make the wire the drag makes.
function connectControls(step) {
  const others = cards.model.steps.filter((s) => s.alias !== step.alias && s.kind !== "sink");
  const option = (port, s) => el("option", { value: `${port}:${s.alias}` }, s.alias);
  const asInput = others.filter((s) => !step.from.includes(s.alias));
  const groups = step.kind === "sql"
    ? [el("optgroup", { label: "Main input" }, asInput.map((s) => option("in", s))),
      el("optgroup", { label: "Joined (ref)" }, asInput.filter((s) => !step.ref.includes(s.alias)).map((s) => option("ref", s)))]
    : asInput.map((s) => option("in", s));
  if (!asInput.length) return null;
  return el("div", { class: "row connect" },
    el("select", { id: "i-connect-from", "aria-label": `Step to connect to ${step.alias}` }, el("option", { value: "" }, "Connect an input…"), groups),
    el("button", { id: "i-connect", class: "small", "data-connect": true, "aria-label": `Connect the chosen step to ${step.alias}` }, "Connect"));
}

function connectChosen(step) {
  const chosen = $("i-connect-from")?.value ?? "";
  if (!chosen) { status("Choose the step to connect first.", "warn"); return; }
  const colon = chosen.indexOf(":");
  if (connect(chosen.slice(colon + 1), step.alias, chosen.slice(0, colon))) focusAfterDraw("#i-connect-from", `.dcard[data-alias="${CSS.escape(step.alias)}"]`);
}

// One transformer's editor: its type, order buttons, mappings and options. `data-index` is its place in the step.
function transformerEditor(step, t, i) {
  const spec = TRANSFORMERS[t.type] || { hint: "", mapping: "key", value: "value" };
  const types = Object.keys(TRANSFORMERS).concat(TRANSFORMERS[t.type] ? [] : [t.type]);
  return el("div", { class: "transformer", "data-index": i },
    el("div", { class: "row" },
      el("select", { "data-ttype": true, "aria-label": `Type of transformer ${i + 1}` }, types.map((k) => el("option", { selected: k === t.type }, k))),
      el("span", { class: "actions" },
        el("button", { class: "small ghost", "data-tmove": "-1", "aria-label": `Move transformer ${i + 1} up`, disabled: i === 0 }, "↑"),
        el("button", { class: "small ghost", "data-tmove": "1", "aria-label": `Move transformer ${i + 1} down`, disabled: i === step.transformers.length - 1 }, "↓"),
        el("button", { class: "small ghost danger", "data-tremove": true, "aria-label": `Remove transformer ${i + 1}` }, "×"))),
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
      el("p", { class: "hint" }, "Delete · × on the wire · drag its end to another input to move it."));
    return;
  }
  const step = cards.selected?.alias ? stepOf(cards.selected.alias) : null;
  if (!step) {
    // The composition's verdict is the YAML tab's light, never repeated here.
    fill(view, el("h2", {}, "Job"),
      el("p", { class: "hint" }, `${plural(cards.model.steps.length, "step")}.`),
      el("p", { class: "hint" }, "Select a step to edit it."),
      el("h3", {}, "Comments"),
      el("textarea", {
        id: "i-header",
        "aria-label": "Job header comments",
        class: "mono",
        rows: "5",
        spellcheck: "true",
        placeholder: "Optional # comments at the top of the YAML file",
        value: cards.model.header || "",
      }));
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
      el("h3", {}, step.kind === "source" ? "Its reader" : "Its output"),
      el("textarea", { id: "i-yaml", "aria-label": `${step.kind === "source" ? "Reader" : "Output"} of ${step.alias}, as YAML`, class: "mono", rows: "10", spellcheck: "false", value: step.yaml || "" }),
      el("p", { class: "hint" }, step.kind === "source"
        ? "Input keys: bounds and sampling (limit, cursor, sampling-rate…), reader options."
        : "Output keys: prefix, checkpoint, contract-save…, writer options.")];
  } else if (isEndpoint(step)) {
    body = [step.kind === "sink" ? inputsList(step) : null, brickView(step)];
  } else if (step.kind === "transform") {
    body = [inputsList(step), el("h3", {}, "Transformers ", el("span", { class: "hint" }, "applied in order")),
      (step.transformers || []).map((t, i) => transformerEditor(step, t, i)),
      el("button", { "data-tadd": true }, "+ transformer")];
  } else if (step.kind === "sql") {
    body = [inputsList(step), el("h3", {}, "Query ", el("span", { class: "hint" }, "DuckDB; each input is a table named by its alias")),
      el("textarea", { id: "i-query", "aria-label": `Query of ${step.alias}`, class: "mono", rows: "10", spellcheck: "false", value: step.query || "" })];
  } else if (step.kind === "merge") {
    body = [inputsList(step), el("p", { class: "hint" }, "UNION ALL: inputs need the same columns and types.")];
  } else {
    body = [inputsList(step), el("h3", {}, "Branch, as written"),
      el("textarea", { id: "i-yaml", "aria-label": `Branch ${step.alias}, as written`, class: "mono", rows: "12", spellcheck: "false", value: step.yaml || "" }),
      el("p", { class: "hint" }, "A branch no step describes: kept exactly as dtpipe reads it.")];
  }
  fill(view,
    el("div", { class: "row" }, el("h2", {}, KIND_LABEL[step.kind] || step.kind), el("button", { class: "small ghost danger", "data-remove-step": true }, "Delete step")),
    el("label", { class: "field" }, "Alias ", el("input", { id: "i-alias", class: "mono", value: step.alias })),
    body,
    el("h3", {}, "Comments"),
    el("textarea", {
      id: "i-comment",
      "aria-label": `Comments for ${step.alias}`,
      class: "mono",
      rows: "3",
      spellcheck: "true",
      placeholder: "Optional # comments written under this branch",
      value: step.comment || "",
    }));
}

// A brick card: what the coordinator's catalog says of it. Its branch is its node's, never edited here.
function brickView(step) {
  const brick = brickOf(step.brick);
  if (!brick) return [el("p", {}, el("code", {}, step.brick)), el("p", { class: "hint warn" }, "Not in the last catalog: sign in to refresh.")];
  return [
    el("p", {}, el("b", {}, brick.title || brick.id), " ", el("span", { class: "hint" }, `${brick.kind} brick`)),
    brick.description ? el("p", { class: "hint" }, brick.description) : null,
    el("table", { class: "vars" }, el("tbody", {},
      el("tr", {}, el("td", {}, "node"), el("td", {}, brick.node, brick.online ? "" : " (offline)")),
      el("tr", {}, el("td", {}, "brick"), el("td", {}, el("code", {}, brick.key))),
      el("tr", {}, el("td", {}, "version"), el("td", {}, el("code", {}, brick.version))))),
    brickRowsView(brick),
    el("p", { class: "hint" }, step.kind === "source"
      ? "A sample reads its rows from its node, through the coordinator."
      : "Samples write a temporary file."),
  ];
}

// What a brick holds is read by its node when a person asks, never kept in the catalog: its columns depend on its rows.
function brickRowsView(brick) {
  const read = state.brickRows?.key === brick.key ? state.brickRows : null;
  const title = brick.kind === "source" ? "Rows" : "What it holds now";
  const button = el("button", { class: "small ghost", "data-read-brick": brick.key, disabled: read?.phase === "loading" }, read ? "Read again" : "Read its rows");
  if (!read) return [el("h3", {}, title), el("p", {}, button), el("p", { class: "hint" }, `${brick.node} reads the first rows of the brick: nothing is kept.`)];
  if (read.phase === "loading") return [el("h3", {}, title), el("p", {}, button), el("p", { class: "hint" }, `Reading on ${brick.node}…`)];
  if (read.phase === "failed") return [el("h3", {}, title), el("p", {}, button), el("p", { class: "hint warn" }, read.error)];
  return [
    el("h3", {}, title),
    el("p", {}, button),
    read.columns?.length
      ? el("table", { class: "vars" }, el("tbody", {}, read.columns.map((c) => el("tr", {}, el("td", {}, el("code", {}, c.name)), el("td", {}, c.type, c.isNullable ? "" : " not null")))))
      : null,
    read.csv ? el("pre", { class: "mono" }, read.csv) : null,
    el("p", { class: "hint" }, `First rows, read by ${brick.node} just now.`),
  ];
}

async function readBrick(key) {
  const ticket = reading.begin();
  state.brickRows = { key, phase: "loading" };
  invalidate("inspector");
  try {
    const answer = await api("/api/bricks/preview", { key });
    if (reading.stale(ticket)) return;
    state.brickRows = { key, phase: "done", csv: answer.csv, columns: answer.columns };
  } catch (e) {
    if (reading.stale(ticket)) return;
    state.brickRows = { key, phase: "failed", error: e.message };
  }
  invalidate("inspector");
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
    if (targetOf(e).id === "i-header") {
      if (!cards.model) return;
      cards.model.header = targetOf(e).value || null;
      touch({ canvas: false });
      return;
    }
    const step = current();
    if (!step) return;
    if (targetOf(e).id === "i-comment") { step.comment = targetOf(e).value || null; touch({ canvas: false }); }
    else if (targetOf(e).id === "i-query") { step.query = targetOf(e).value; touch(); }
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
    if (b.dataset.readBrick !== undefined) { readBrick(b.dataset.readBrick); return; }
    if (b.dataset.unwire !== undefined) { disconnect({ from: b.dataset.unwire, to: b.dataset.to || step.alias, port: b.dataset.port }); return; }
    if (!step) return;
    if (b.dataset.connect !== undefined) { connectChosen(step); return; }
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
  const errors = cards.result.errors?.length || 0;
  const warnings = cards.result.warnings?.length || 0;
  const problems = errors + warnings;
  const tab = (name, label, ...extra) => el("button", { class: state.view.drawer === name ? "active" : "", "data-tab": name }, label, ...extra);
  const tabs = el("div", { class: "tabs" },
    // The light and its name are drawn by drawHold, which typing calls too.
    el("button", { id: "d-yaml-tab", class: state.view.drawer === "yaml" ? "active" : "", "data-tab": "yaml" }),
    tab("problems", "Problems ",
      errors ? el("span", { class: "count", title: plural(errors, "error") }, errors) : null,
      warnings ? el("span", { class: "count warn", title: plural(warnings, "warning") }, warnings) : null),
    tab("preview", "Preview"),
    (hooks.extraTabs || []).map((t) => tab(t.name, t.label, t.badge?.())));
  // A redraw while the person types keeps the text (it is state) and gives the editor back its caret.
  const typing = document.activeElement?.id === "d-edit" ? /** @type {HTMLTextAreaElement} */ (document.activeElement) : null;
  const caret = typing ? { start: typing.selectionStart, end: typing.selectionEnd, top: typing.scrollTop } : null;
  // A tab is its content, which scrolls, and its actions, which never do: both come from `tabOf`, or a tab with no action gives its content alone.
  let drawn;
  if (state.view.drawer === "yaml") {
    drawn = tabOf([el("div", { class: "row" }, el("span", { id: "d-verdict" })),
      el("textarea", { id: "d-edit", "aria-label": "The job, as YAML", class: "mono yaml-edit", spellcheck: "false", value: cards.text ?? cards.result.yaml })],
    [el("button", { "data-copy": true, title: "Copy the text shown" }, "Copy"),
      el("button", { id: "d-apply", "data-apply": true, title: "Rebuild the steps from this text (steps keep their place by alias)" }, "Apply"),
      el("button", { id: "d-revert", "data-revert": true, title: "Show the job the steps compose again, and free the steps" }, "Revert")],
    { fill: true });
  } else if (state.view.drawer === "problems") {
    // Two verdicts, never mixed: what the composition says of a run here, and what a publication would refuse (the host's).
    const publication = hooks.publicationProblems?.() ?? tabOf(null, null);
    drawn = tabOf([el("h3", {}, "Will not run here"),
      errors ? [el("h4", {}, `Errors (${errors})`), el("ul", { class: "msgs bad" }, cards.result.errors.map((m) => el("li", {}, m)))] : null,
      warnings ? [el("h4", {}, `Warnings (${warnings})`), el("ul", { class: "msgs warn" }, cards.result.warnings.map((m) => el("li", {}, m)))] : null,
      problems ? null : el("p", { class: "hint" }, "Nothing to report."),
      publication.body], publication.actions);
  } else if (state.view.drawer === "preview") {
    drawn = hooks.previewView?.(cards.selected?.alias || null) || null;
  } else {
    drawn = (hooks.extraTabs || []).find((t) => t.name === state.view.drawer)?.render() || null;
  }
  const { body, actions, fill: fills } = drawn?.tab ? drawn : { body: drawn, actions: null, fill: false };
  fill(view, tabs,
    el("div", { class: fills ? "drawer-body fill" : "drawer-body" }, body),
    actions?.length ? el("div", { class: "drawer-actions" }, actions) : null);
  drawHold();
  const editor = /** @type {HTMLTextAreaElement | null} */ ($("d-edit"));
  if (caret && editor) { editor.focus(); editor.setSelectionRange(caret.start, caret.end); editor.scrollTop = caret.top; }
}

// What the steps hold while the YAML tab has a text they do not: these are greyed and take no input.
const HELD = ["d-canvas", "d-inspector", "d-palette", "d-undo", "d-layout"];

/**
 * The verdict the YAML tab's light gives: the composition's on the cards, unless the tab holds a text not applied, of which
 * the light says nothing more (a verdict on the cards would vouch for a text not on screen).
 */
function verdict() {
  if (editingText()) return { tone: "held", short: "not applied", words: "Not applied: apply or revert the text." };
  const errors = cards.result?.errors?.length || 0;
  const warnings = cards.result?.warnings?.length || 0;
  if (errors) return { tone: "bad", short: plural(errors, "error"), words: `${plural(errors, "error")}: the steps do not compose into a job.` };
  if (warnings) return { tone: "warn", short: plural(warnings, "warning"), words: `Composes, with ${plural(warnings, "warning")}.` };
  return { tone: "good", short: "composes", words: "Composes cleanly: a plain dtpipe job, runnable whole with dtpipe --job." };
}

/**
 * Draws in place what the YAML tab's text decides: the tab's light and its name, the verdict, Apply and Revert, and the steps
 * held. Typing calls it instead of redrawing the drawer, which would take the editor's caret.
 */
function drawHold() {
  const held = editingText();
  for (const id of HELD) {
    const node = $(id);
    if (!node) continue;
    node.toggleAttribute("inert", held);
    node.classList.toggle("held", held);
  }
  const note = $("d-held");
  if (note) note.hidden = !held;
  const v = verdict();
  const light = () => el("span", { class: `dot ${v.tone}`, "aria-hidden": "true" });
  const tabButton = $("d-yaml-tab");
  // The light is no name on its own: the tab's name carries its words.
  if (tabButton) { fill(tabButton, light(), " YAML"); tabButton.setAttribute("aria-label", `YAML, ${v.short}`); tabButton.title = v.words; }
  const line = $("d-verdict");
  const problems = !held && ((cards.result?.errors?.length || 0) + (cards.result?.warnings?.length || 0)) > 0;
  if (line) fill(line, light(), " ", el("span", { class: v.tone === "held" ? "hint" : v.tone }, v.words),
    problems ? [" ", el("button", { class: "ghost small", "data-tab": "problems" }, "See Problems")] : null);
  for (const id of ["d-apply", "d-revert"]) {
    const button = /** @type {HTMLButtonElement | null} */ ($(id));
    if (button) button.disabled = !held;
  }
}

export function showTab(name) {
  state.view.drawer = name;
  invalidate("drawer");
}

function bindDrawer() {
  $("d-drawer").addEventListener("input", (e) => {
    const t = targetOf(e);
    if (t.id !== "d-edit") return;
    // A text typed back to the composed one is no text: kept, it would come back stale once the cards compose another.
    cards.text = t.value === (cards.result?.yaml ?? "") ? null : t.value;
    drawHold();
  });
  $("d-drawer").addEventListener("click", async (e) => {
    const b = targetOf(e).closest("button");
    if (!b) return;
    if (b.dataset.tab) showTab(b.dataset.tab);
    else if (b.dataset.copy !== undefined) {
      try { await navigator.clipboard.writeText(cards.text ?? cards.result.yaml); status("YAML copied."); }
      catch { /** @type {HTMLTextAreaElement} */ ($("d-edit")).select(); status("Selected: copy it with your keyboard."); }
    } else if (b.dataset.revert !== undefined) {
      cards.text = null;
      invalidate("drawer");
      status("the YAML is the steps' again");
    } else if (b.dataset.apply !== undefined) {
      try {
        const ticket = opening.begin();
        const answer = await api("/api/design/decompose", { yaml: cards.text ?? cards.result.yaml, layout: cards.model.layout });
        if (opening.stale(ticket)) return;
        // The text replaces every card: Undo puts back the cards it replaced.
        remember("applying the YAML", () => {
          const layout = cards.model.layout;
          cards.model = normalize(answer.model);
          cards.model.layout = Object.fromEntries(Object.entries(layout).filter(([a]) => stepOf(a)));
          if (cards.model.steps.some((s) => !cards.model.layout[s.alias])) autoLayout(false);
          cards.selected = null;
          cards.zoom = null;
          cards.text = null;
          state.view.drawer = "yaml";
          cards.dirty = true;
        });
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
