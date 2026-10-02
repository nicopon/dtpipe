// Designer: assemble a pipeline from the nodes' bricks and the runner's steps, one card per
// branch. The coordinator composes the job (POST /api/design/compose) on every change; this page
// never writes YAML itself.
import { $, api, esc, nodeColor, store, toast } from "./core.js";

const CARD_W = 210;
const CARD_H = 96;
const PORT_IN_Y = 34;
const PORT_REF_Y = 66;

// The transformer types the page offers, each with the shape its YAML takes (REFERENCE.md,
// "Transformer YAML reference"). dtpipe validates the result: Validate runs it.
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

let host = null;
let id = null;
let model = null;          // { title, description, check, steps, layout }
let result = null;         // the last compose answer
let selected = null;       // { alias } or { edge: { from, to, port } }
let dirty = false;
let validation = null;     // { ok, dryRun, errors }
let drawer = "yaml";       // yaml | problems | edit
let composeSeq = 0;
let composeTimer = null;

export function mount(el, params) {
  host = el;
  id = params[0] || null;
  model = null; result = null; selected = null; dirty = false; validation = null; drawer = "yaml";
  if (!id) { renderPicker(); return; }
  load().catch((e) => { host.innerHTML = `<p class="mismatch">${esc(e.message)}</p>`; });
}

export function leave() {
  if (dirty && !confirm(`${id} has unsaved changes. Leave the designer anyway?`)) {
    history.replaceState(null, "", `#/designer/${encodeURIComponent(id)}`);
    return false;
  }
  document.removeEventListener("keydown", onKey);
  return true;
}

export function dirtyState() { return dirty; }
export { dirtyState as dirty };

export function update(kind) {
  if (!model) return;
  if (kind === "bricks" || kind === "all") { renderPalette(); scheduleCompose(); }
}

// ---------------------------------------------------------------- loading

async function renderPicker() {
  const entries = await api("/api/library");
  host.innerHTML = `
    <div class="view-head"><h1>Designer</h1><p class="hint">Open a pipeline, or start one from the Library.</p></div>
    <section class="card picker">${entries.map((e) => `<a href="#/designer/${encodeURIComponent(e.id)}"><b>${esc(e.title)}</b><span class="hint mono">${esc(e.id)}</span></a>`).join("")}</section>`;
}

async function load() {
  if (store.draft?.id === id) {
    model = normalize(store.draft.model);
    store.draft = null;
    dirty = true;
  } else {
    const doc = await api(`/api/library/${encodeURIComponent(id)}`);
    const decomposed = await api("/api/design/decompose", { yaml: doc.yaml, layout: doc.layout });
    model = normalize(decomposed.model);
  }
  if (model.steps.some((s) => !model.layout[s.alias])) autoLayout(false);
  renderShell();
  await compose();
}

function normalize(m) {
  return {
    title: m.title || "Untitled pipeline", description: m.description || "", check: m.check || null,
    steps: (m.steps || []).map((s) => ({ ...s, from: s.from || [], ref: s.ref || [] })),
    layout: m.layout || {},
  };
}

// ---------------------------------------------------------------- composing

function scheduleCompose() {
  clearTimeout(composeTimer);
  composeTimer = setTimeout(compose, 200);
}

async function compose() {
  const seq = ++composeSeq;
  try {
    const answer = await api("/api/design/compose", model);
    if (seq !== composeSeq) return;
    result = answer;
  } catch (e) {
    if (seq === composeSeq) result = { yaml: "", steps: [], errors: [e.message], warnings: [] };
  }
  renderCanvas();
  renderDrawer();
  renderToolbarState();
}

function touch({ recompose = true, inspector = false, canvas = true } = {}) {
  dirty = true;
  validation = null;
  if (canvas) renderCanvas();
  if (inspector) renderInspector();
  renderToolbarState();
  if (recompose) scheduleCompose();
}

// ---------------------------------------------------------------- model edits

const stepOf = (alias) => model.steps.find((s) => s.alias === alias);
const runnerName = () => store.nodes.find((n) => n.role === "Runner")?.name || "runner";
const brickOf = (key) => store.bricks.find((b) => b.key === key);

function freshAlias(base) {
  let alias = String(base).replace(/[^A-Za-z0-9_-]/g, "_");
  if (!/^[A-Za-z]/.test(alias)) alias = "s_" + alias;
  let candidate = alias;
  for (let i = 2; stepOf(candidate); i++) candidate = `${alias}${i}`;
  return candidate;
}

function addStep(kind, brickKey, x, y) {
  const brick = brickKey ? brickOf(brickKey) : null;
  const alias = freshAlias(brick ? brick.id : kind);
  const step = { alias, kind, from: [], ref: [] };
  if (brick) step.brick = brick.key;
  if (kind === "transform") step.transformers = [];
  if (kind === "sql") step.query = "SELECT *\nFROM input";
  model.steps.push(step);
  model.layout[alias] = { x: Math.max(10, Math.round(x)), y: Math.max(10, Math.round(y)) };
  selected = { alias };
  touch({ inspector: true });
}

function removeStep(alias) {
  model.steps = model.steps.filter((s) => s.alias !== alias);
  for (const s of model.steps) { s.from = s.from.filter((a) => a !== alias); s.ref = s.ref.filter((a) => a !== alias); }
  delete model.layout[alias];
  selected = null;
  touch({ inspector: true });
}

function renameStep(from, to) {
  to = to.trim();
  if (to === from) return;
  if (!/^[A-Za-z][A-Za-z0-9_-]*$/.test(to)) { toast(`'${to}' is not a usable alias: a letter first, then letters, digits, '_' or '-'.`); renderInspector(); return; }
  if (stepOf(to)) { toast(`A step is already named '${to}'.`); renderInspector(); return; }
  stepOf(from).alias = to;
  for (const s of model.steps) {
    s.from = s.from.map((a) => (a === from ? to : a));
    s.ref = s.ref.map((a) => (a === from ? to : a));
    if (s.kind === "sql" && s.query) s.query = s.query.replace(new RegExp(`\\b${from}\\b`, "g"), to);
  }
  model.layout[to] = model.layout[from];
  delete model.layout[from];
  selected = { alias: to };
  touch({ inspector: true });
}

function connect(fromAlias, toAlias, port) {
  const target = stepOf(toAlias);
  if (!target || fromAlias === toAlias) return;
  if (target.kind === "source") { toast("A source reads its own database: it takes no input."); return; }
  if (port === "ref") {
    if (!target.ref.includes(fromAlias) && !target.from.includes(fromAlias)) target.ref.push(fromAlias);
  } else if (target.kind === "merge" || target.kind === "branch") {
    if (!target.from.includes(fromAlias)) target.from.push(fromAlias);
  } else {
    target.from = [fromAlias];
    target.ref = target.ref.filter((a) => a !== fromAlias);
  }
  touch({ inspector: selected?.alias === toAlias });
}

function disconnect(edge) {
  const target = stepOf(edge.to);
  if (!target) return;
  if (edge.port === "ref") target.ref = target.ref.filter((a) => a !== edge.from);
  else target.from = target.from.filter((a) => a !== edge.from);
  selected = null;
  touch({ inspector: true });
}

function autoLayout(render = true) {
  const byAlias = Object.fromEntries(model.steps.map((s) => [s.alias, s]));
  const depth = {};
  const depthOf = (s, seen = new Set()) => {
    if (depth[s.alias] !== undefined) return depth[s.alias];
    if (seen.has(s.alias)) return 0;
    seen.add(s.alias);
    const d = [...s.from, ...s.ref].filter((a) => byAlias[a]).reduce((m, a) => Math.max(m, depthOf(byAlias[a], seen) + 1), 0);
    return (depth[s.alias] = d);
  };
  model.steps.forEach((s) => depthOf(s));
  const rows = {};
  for (const s of model.steps) {
    const d = depth[s.alias];
    const row = rows[d] = (rows[d] ?? -1) + 1;
    model.layout[s.alias] = { x: 30 + d * (CARD_W + 70), y: 30 + row * (CARD_H + 36) };
  }
  if (render) touch({ recompose: false });
}

// ---------------------------------------------------------------- rendering

function renderShell() {
  host.innerHTML = `
    <div class="designer">
      <div class="designer-bar card">
        <span class="mono muted">${esc(id)}</span>
        <input id="d-title" class="title-input" value="${esc(model.title)}" aria-label="Title">
        <input id="d-description" class="grow" value="${esc(model.description)}" placeholder="What this pipeline does" aria-label="Description">
        <span id="d-dirty" class="chip warn" hidden>unsaved</span>
        <span class="actions">
          <button id="d-layout" class="ghost" title="Arrange the cards in columns, sources first">Arrange</button>
          <button id="d-validate" title="dtpipe runs the whole job over one row per source, writers neutralised">Validate</button>
          <button id="d-save">Save…</button>
          <button id="d-distribute" class="primary">Distribute →</button>
        </span>
      </div>
      <div class="designer-body">
        <aside class="palette card" id="d-palette"></aside>
        <div class="canvas card" id="d-canvas"><div class="canvas-inner" id="d-inner"><svg id="d-svg"></svg></div></div>
        <aside class="inspector card" id="d-inspector"></aside>
      </div>
      <div class="drawer card" id="d-drawer"></div>
    </div>`;

  $("d-title").addEventListener("input", (e) => { model.title = e.target.value; touch({ canvas: false }); });
  $("d-description").addEventListener("input", (e) => { model.description = e.target.value; touch({ canvas: false }); });
  $("d-layout").addEventListener("click", () => autoLayout());
  $("d-validate").addEventListener("click", validate);
  $("d-save").addEventListener("click", () => save().catch((e) => toast(e.message)));
  $("d-distribute").addEventListener("click", distribute);
  bindCanvas();
  bindPalette();
  bindInspector();
  bindDrawer();
  document.removeEventListener("keydown", onKey);
  document.addEventListener("keydown", onKey);
  renderPalette();
  renderInspector();
  renderCanvas();
  renderDrawer();
  renderToolbarState();
}

function renderToolbarState() {
  const flag = $("d-dirty");
  if (flag) flag.hidden = !dirty;
}

function renderPalette() {
  const el = $("d-palette");
  if (!el) return;
  const group = (kind) => {
    const byNode = {};
    for (const b of store.bricks.filter((b) => b.kind === kind)) (byNode[b.node] ||= []).push(b);
    return Object.entries(byNode).map(([node, bricks]) => `
      <div class="pal-node" style="--node-color:${nodeColor(node)}"><span class="dot on" style="background:var(--node-color)"></span>${esc(node)}</div>
      ${bricks.map((b) => `<div class="pal-item brick" draggable="true" data-kind="${kind === "Source" ? "source" : "sink"}" data-brick="${esc(b.key)}" style="--node-color:${nodeColor(node)}" title="${esc(b.description || b.title)}">
        <span>${esc(b.title)}</span><button class="small ghost" data-add title="Add to the canvas">+</button></div>`).join("")}`).join("")
      || `<p class="hint">none offered</p>`;
  };
  const step = (kind, label, hint) => `<div class="pal-item step" draggable="true" data-kind="${kind}" title="${esc(hint)}" style="--node-color:${nodeColor(runnerName())}"><span>${label}</span><button class="small ghost" data-add title="Add to the canvas">+</button></div>`;
  el.innerHTML = `
    <h3>Sources</h3>${group("Source")}
    <h3>Steps <span class="hint">on the runner</span></h3>
    ${step("transform", "Transform", "An ordered list of transformers over one input")}
    ${step("sql", "SQL", "A DuckDB query over one main input, joined to others as ref")}
    ${step("merge", "Merge", "UNION ALL of several inputs with the same schema")}
    <h3>Sinks</h3>${group("Sink")}
    <p class="hint">Drag a brick or a step onto the canvas, then wire an output port (right) to an input port (left). Drag a wire's end off its input port to move it or, dropped in the void, to remove it.</p>`;
}

function cardInfo(step) {
  const info = result?.steps?.find((s) => s.alias === step.alias);
  const brick = step.brick ? brickOf(step.brick) : null;
  const node = step.kind === "source" || step.kind === "sink" ? (brick?.node || info?.node || step.brick?.split("/")[0]) : runnerName();
  let subtitle = "";
  if (step.kind === "source" || step.kind === "sink") subtitle = brick?.title || `${step.brick} (not offered)`;
  else if (step.kind === "transform") subtitle = step.transformers?.length ? step.transformers.map((t) => t.type).join(" → ") : "no transformer yet";
  else if (step.kind === "sql") subtitle = (step.query || "").replace(/\s+/g, " ").slice(0, 60);
  else if (step.kind === "merge") subtitle = `UNION ALL of ${step.from.length} input(s)`;
  else subtitle = "raw branch (as written)";
  return { node, subtitle, missing: (step.kind === "source" || step.kind === "sink") && !brick };
}

const KIND_LABEL = { source: "source", sink: "sink", transform: "transform", sql: "sql", merge: "merge", branch: "branch" };

function renderCanvas() {
  const inner = $("d-inner");
  if (!inner || !model) return;
  const problems = new Set((result?.errors || []).flatMap((e) => [...e.matchAll(/'([^']+)'/g)].map((m) => m[1])));
  inner.querySelectorAll(".dcard").forEach((c) => c.remove());
  let width = 900, height = 520;
  for (const step of model.steps) {
    const pos = model.layout[step.alias] || { x: 20, y: 20 };
    const { node, subtitle, missing } = cardInfo(step);
    const card = document.createElement("div");
    card.className = `dcard kind-${step.kind} ${selected?.alias === step.alias ? "selected" : ""} ${problems.has(step.alias) || missing ? "problem" : ""}`;
    card.dataset.alias = step.alias;
    card.style.left = `${pos.x}px`;
    card.style.top = `${pos.y}px`;
    card.style.setProperty("--node-color", nodeColor(node));
    card.innerHTML = `
      <div class="dcard-head"><span class="kind">${KIND_LABEL[step.kind] || step.kind}</span><span class="alias">${esc(step.alias)}</span></div>
      <div class="dcard-sub">${esc(subtitle)}</div>
      <div class="dcard-node"><span class="dot on" style="background:var(--node-color)"></span>${esc(node || "?")}</div>
      ${step.kind !== "source" ? `<span class="port in" data-port="in" title="${step.kind === "merge" ? "inputs (several)" : "input"}"></span>` : ""}
      ${step.kind === "sql" ? `<span class="port ref" data-port="ref" title="joined inputs (ref)"></span><span class="port-label ref">ref</span>` : ""}
      ${step.kind !== "sink" ? `<span class="port out" data-port="out" title="output: drag to an input"></span>` : ""}`;
    inner.appendChild(card);
    width = Math.max(width, pos.x + CARD_W + 200);
    height = Math.max(height, pos.y + CARD_H + 160);
  }
  inner.style.width = `${width}px`;
  inner.style.height = `${height}px`;
  drawEdges();
  const empty = inner.querySelector(".canvas-empty");
  if (!model.steps.length && !empty) inner.insertAdjacentHTML("beforeend", `<p class="canvas-empty hint">Drag bricks and steps here from the palette.</p>`);
  if (model.steps.length && empty) empty.remove();
}

function portPoint(alias, port) {
  const pos = model.layout[alias] || { x: 0, y: 0 };
  if (port === "out") return { x: pos.x + CARD_W, y: pos.y + PORT_IN_Y };
  return { x: pos.x, y: pos.y + (port === "ref" ? PORT_REF_Y : PORT_IN_Y) };
}

const curve = (a, b) => { const m = Math.max(40, (b.x - a.x) / 2); return `M${a.x},${a.y} C${a.x + m},${a.y} ${b.x - m},${b.y} ${b.x},${b.y}`; };

function drawEdges(temp) {
  const svg = $("d-svg");
  const inner = $("d-inner");
  if (!svg) return;
  svg.setAttribute("width", inner.offsetWidth);
  svg.setAttribute("height", inner.offsetHeight);
  const parts = [];
  for (const step of model.steps) {
    for (const [port, list] of [["in", step.from], ["ref", step.ref]]) {
      for (const from of list) {
        if (!stepOf(from)) continue;
        const isSel = selected?.edge && selected.edge.from === from && selected.edge.to === step.alias && selected.edge.port === port;
        const d = curve(portPoint(from, "out"), portPoint(step.alias, port));
        parts.push(`<path class="dedge ${port} ${isSel ? "selected" : ""}" d="${d}"/><path class="dedge-hit" data-from="${esc(from)}" data-to="${esc(step.alias)}" data-port="${port}" d="${d}"/>`);
      }
    }
  }
  if (temp) parts.push(`<path class="dedge temp" d="${curve(temp.a, temp.b)}"/>`);
  svg.innerHTML = parts.join("");

  // The selected wire carries its own delete button, at its middle.
  inner.querySelector(".edge-delete")?.remove();
  const sel = selected?.edge && svg.querySelector(`.dedge-hit[data-from="${CSS.escape(selected.edge.from)}"][data-to="${CSS.escape(selected.edge.to)}"][data-port="${selected.edge.port}"]`);
  if (sel && !temp) {
    const mid = sel.getPointAtLength(sel.getTotalLength() / 2);
    inner.insertAdjacentHTML("beforeend", `<button class="edge-delete" style="left:${mid.x}px;top:${mid.y}px" title="Remove this wire (Delete)">×</button>`);
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
    const port = e.target.closest(".port.out");
    const inPort = e.target.closest(".port.in, .port.ref");
    const card = e.target.closest(".dcard");
    const edge = e.target.closest(".dedge-hit");
    if (e.target.closest(".edge-delete")) {
      disconnect(selected.edge);
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
        selected = null;
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
    } else if (card && !e.target.closest(".port")) {
      const pos = model.layout[card.dataset.alias];
      const p = canvasPoint(e);
      drag = { type: "move", alias: card.dataset.alias, dx: p.x - pos.x, dy: p.y - pos.y, moved: false };
      if (selected?.alias !== card.dataset.alias) { selected = { alias: card.dataset.alias }; renderInspector(); inner.querySelectorAll(".dcard").forEach((c) => c.classList.toggle("selected", c === card)); drawEdges(); }
      inner.setPointerCapture(e.pointerId);
    } else if (edge) {
      selected = { edge: { from: edge.dataset.from, to: edge.dataset.to, port: edge.dataset.port } };
      renderCanvas();
      renderInspector();
    } else if (e.target === inner || e.target.id === "d-svg") {
      selected = null;
      renderCanvas();
      renderInspector();
    }
  });

  inner.addEventListener("pointermove", (e) => {
    if (!drag) return;
    const p = canvasPoint(e);
    if (drag.type === "link") drawEdges({ a: portPoint(drag.from, "out"), b: p });
    else {
      const pos = { x: Math.max(0, Math.round(p.x - drag.dx)), y: Math.max(0, Math.round(p.y - drag.dy)) };
      model.layout[drag.alias] = pos;
      drag.moved = true;
      const card = inner.querySelector(`.dcard[data-alias="${CSS.escape(drag.alias)}"]`);
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
      const port = target?.closest(".port.in, .port.ref");
      const card = target?.closest(".dcard");
      if (card) {
        // Dropped on a SQL card but beside its ports: the first wire is its main input, the next ones joins.
        const step = stepOf(card.dataset.alias);
        const fallback = step?.kind === "sql" && step.from.length && !step.from.includes(d.from) ? "ref" : "in";
        connect(d.from, card.dataset.alias, port ? port.dataset.port : fallback);
      } else if (d.picked) {
        touch({ inspector: true });
        toast(`Wire ${d.from} → ${d.picked.to} removed.`, "info");
      } else drawEdges();
    } else if (d.moved) {
      dirty = true;
      renderToolbarState();
      renderCanvas();
    }
  });

  const canvas = $("d-canvas");
  canvas.addEventListener("dragover", (e) => { e.preventDefault(); e.dataTransfer.dropEffect = "copy"; });
  canvas.addEventListener("drop", (e) => {
    e.preventDefault();
    let item;
    try { item = JSON.parse(e.dataTransfer.getData("application/json") || "null"); } catch { item = null; }
    if (!item) return;
    const p = canvasPoint(e);
    addStep(item.kind, item.brick, p.x - CARD_W / 2, p.y - 20);
  });
}

function bindPalette() {
  const el = $("d-palette");
  el.addEventListener("dragstart", (e) => {
    const item = e.target.closest(".pal-item");
    if (!item) return;
    e.dataTransfer.setData("application/json", JSON.stringify({ kind: item.dataset.kind, brick: item.dataset.brick || null }));
    e.dataTransfer.effectAllowed = "copy";
  });
  el.addEventListener("click", (e) => {
    if (!e.target.closest("[data-add]")) return;
    const item = e.target.closest(".pal-item");
    // Placed after the cards already there, in the column its kind usually takes.
    const column = { source: 0, transform: 1, sql: 1, merge: 1, sink: 2 }[item.dataset.kind] ?? 1;
    const inColumn = model.steps.filter((s) => (model.layout[s.alias]?.x ?? 0) >= 30 + column * (CARD_W + 70) - 20 && (model.layout[s.alias]?.x ?? 0) < 30 + (column + 1) * (CARD_W + 70) - 20);
    addStep(item.dataset.kind, item.dataset.brick || null, 30 + column * (CARD_W + 70), 30 + inColumn.length * (CARD_H + 36));
  });
}

function onKey(e) {
  if (!model || !host?.isConnected) return;
  if (["INPUT", "TEXTAREA", "SELECT"].includes(document.activeElement?.tagName)) return;
  if (e.key === "Delete" || e.key === "Backspace") {
    if (selected?.alias) { removeStep(selected.alias); e.preventDefault(); }
    else if (selected?.edge) { disconnect(selected.edge); e.preventDefault(); }
  }
}

// ---------------------------------------------------------------- inspector

function kvRows(obj, section, index, placeholders) {
  const entries = Object.entries(obj || {});
  return `
    <div class="kv" data-section="${section}" data-index="${index}">
      ${entries.map(([k, v], row) => `<div class="kv-row" data-row="${row}">
        <input class="mono" data-kv="key" value="${esc(k)}" placeholder="${esc(placeholders[0])}">
        <input class="mono" data-kv="value" value="${esc(v ?? "")}" placeholder="${esc(placeholders[1])}">
        <button class="small ghost" data-kv-remove title="Remove">×</button></div>`).join("")}
      <button class="small ghost" data-kv-add>+ ${section === "mappings" ? "mapping" : "option"}</button>
    </div>`;
}

function inputsList(step) {
  const item = (alias, port) => `<li><code>${esc(alias)}</code>${port === "ref" ? ` <span class="chip">ref</span>` : ""} <button class="small ghost" data-unwire="${esc(alias)}" data-port="${port}" title="Disconnect">×</button></li>`;
  const list = [...step.from.map((a) => item(a, "in")), ...step.ref.map((a) => item(a, "ref"))].join("");
  return `<h3>Inputs</h3>${list ? `<ul class="inputs">${list}</ul>` : `<p class="hint">Not wired yet: drag from another card's output port.</p>`}`;
}

function columnsOf(alias, seen = new Set()) {
  const step = stepOf(alias);
  if (!step || seen.has(alias)) return null;
  seen.add(alias);
  if (step.kind === "source") return brickOf(step.brick)?.schema || null;
  // A transform's output is not known without running it; a pass-through keeps its input's columns.
  if (step.kind === "transform" && !step.transformers?.length && step.from[0]) return columnsOf(step.from[0], seen);
  return null;
}

function renderInspector() {
  const el = $("d-inspector");
  if (!el) return;
  if (selected?.edge) {
    const e = selected.edge;
    el.innerHTML = `<h2>Connection</h2><p><code>${esc(e.from)}</code> → <code>${esc(e.to)}</code>${e.port === "ref" ? " (ref)" : ""}</p>
      <button class="danger" data-unwire="${esc(e.from)}" data-port="${e.port}" data-to="${esc(e.to)}">Disconnect</button>
      <p class="hint">Or press Delete, or click the × on the wire. To move it, drag its end off <code>${esc(e.to)}</code>'s input port and drop it on another input; dropped anywhere else, it is removed.</p>`;
    return;
  }
  const step = selected?.alias ? stepOf(selected.alias) : null;
  if (!step) {
    const errors = result?.errors?.length || 0;
    el.innerHTML = `<h2>Pipeline</h2>
      <p class="hint">${model.steps.length} step(s). ${errors ? `<span class="mismatch">${errors} problem(s)</span>` : "Composes cleanly."}</p>
      <label class="field">Check query <span class="hint">(<code>lab-check</code>, run by <code>./lab.sh smoke</code> on the warehouse)</span>
        <input id="i-check" class="mono" value="${esc(model.check || "")}"></label>
      <p class="hint">Select a card to edit it. A card's colour is the node it runs on: a brick on its own node, every other step on the runner.</p>`;
    return;
  }
  const brick = step.brick ? brickOf(step.brick) : null;
  let body = "";
  if (step.kind === "source" || step.kind === "sink") {
    body = brick ? `
      <p><b>${esc(brick.title)}</b></p>
      <p class="hint">${esc(brick.description || "")}</p>
      <p><span class="chip" style="color:${nodeColor(brick.node)}">${esc(brick.node)}</span> <code class="muted">${esc(brick.key)} · ${esc(brick.version)}</code></p>
      ${brick.schema?.length ? `<h3>Columns</h3><div class="cols">${brick.schema.map((c) => `<code title="${esc(c.type)}">${esc(c.name)}</code>`).join(" ")}</div>` : ""}
      <details><summary>What the node runs</summary><pre>${esc(brick.yaml)}</pre></details>
      <p class="hint">A brick is defined by its node's owner: only its alias is yours.</p>`
      : `<p class="mismatch">No online node offers ${esc(step.brick)}.</p>`;
    if (step.kind === "sink") body = inputsList(step) + body;
  } else if (step.kind === "transform") {
    body = inputsList(step) + `<h3>Transformers <span class="hint">applied in order</span></h3>
      ${(step.transformers || []).map((t, i) => {
        const spec = TRANSFORMERS[t.type] || { hint: "", mapping: "key", value: "value" };
        return `<div class="transformer" data-index="${i}">
          <div class="row"><select data-ttype>${Object.keys(TRANSFORMERS).concat(TRANSFORMERS[t.type] ? [] : [t.type]).map((k) => `<option ${k === t.type ? "selected" : ""}>${esc(k)}</option>`).join("")}</select>
          <span class="actions"><button class="small ghost" data-tmove="-1" ${i === 0 ? "disabled" : ""}>↑</button><button class="small ghost" data-tmove="1" ${i === step.transformers.length - 1 ? "disabled" : ""}>↓</button><button class="small ghost danger" data-tremove>×</button></span></div>
          <p class="hint">${esc(spec.hint)}</p>
          ${spec.mapping !== null || t.mappings ? `<div class="sub">mappings</div>${kvRows(t.mappings, "mappings", i, [spec.mapping || "key", spec.value ?? "value"])}` : ""}
          <div class="sub">options</div>${kvRows(t.options, "options", i, ["option", "value"])}
        </div>`;
      }).join("")}
      <button data-tadd>+ transformer</button>`;
    const cols = step.from[0] ? columnsOf(step.from[0]) : null;
    if (cols) body += `<h3>Input columns</h3><div class="cols">${cols.map((c) => `<code title="${esc(c.type)}">${esc(c.name)}</code>`).join(" ")}</div>`;
  } else if (step.kind === "sql") {
    const inputs = [...step.from, ...step.ref];
    body = inputsList(step) + `<h3>Query <span class="hint">DuckDB; each input is a table named by its alias</span></h3>
      <textarea id="i-query" class="mono" rows="10" spellcheck="false">${esc(step.query || "")}</textarea>
      ${inputs.map((a) => { const cols = columnsOf(a); return `<div class="hint"><code>${esc(a)}</code>: ${cols ? cols.map((c) => esc(c.name)).join(", ") : "columns known once it runs"}</div>`; }).join("")}`;
  } else if (step.kind === "merge") {
    body = inputsList(step) + `<p class="hint">UNION ALL: every input must carry the same columns, with the same types. dtpipe converts nothing on the way.</p>`;
  } else {
    body = inputsList(step) + `<h3>Branch, as written</h3><textarea id="i-yaml" class="mono" rows="12" spellcheck="false">${esc(step.yaml || "")}</textarea>
      <p class="hint">A branch no card describes: kept exactly, and placed on the runner.</p>`;
  }
  el.innerHTML = `
    <div class="row"><h2>${esc(KIND_LABEL[step.kind] || step.kind)}</h2><button class="small ghost danger" data-remove-step>Delete step</button></div>
    <label class="field">Alias <input id="i-alias" class="mono" value="${esc(step.alias)}"></label>
    ${body}`;
}

function bindInspector() {
  const el = $("d-inspector");
  const current = () => (selected?.alias ? stepOf(selected.alias) : null);

  el.addEventListener("change", (e) => {
    const step = current();
    if (e.target.id === "i-alias" && step) renameStep(step.alias, e.target.value);
    else if (e.target.matches("[data-ttype]") && step) {
      const i = +e.target.closest(".transformer").dataset.index;
      const t = step.transformers[i];
      t.type = e.target.value;
      if (TRANSFORMERS[t.type]?.mapping === null) delete t.mappings;
      touch({ inspector: true });
    }
  });

  el.addEventListener("input", (e) => {
    const step = current();
    if (e.target.id === "i-check") { model.check = e.target.value || null; touch({ canvas: false }); return; }
    if (!step) return;
    if (e.target.id === "i-query") { step.query = e.target.value; touch(); }
    else if (e.target.id === "i-yaml") { step.yaml = e.target.value; touch(); }
    else if (e.target.dataset.kv) {
      const kv = e.target.closest(".kv");
      const t = step.transformers[+kv.dataset.index];
      const section = kv.dataset.section;
      const rows = [...kv.querySelectorAll(".kv-row")].map((r) => [r.querySelector('[data-kv="key"]').value, r.querySelector('[data-kv="value"]').value]);
      const obj = {};
      for (const [k, v] of rows) if (k.trim()) obj[k.trim()] = v === "" ? null : v;
      t[section] = obj;
      touch();
    }
  });

  el.addEventListener("click", (e) => {
    const b = e.target.closest("button");
    if (!b) return;
    const step = current();
    if (b.dataset.unwire !== undefined) { disconnect({ from: b.dataset.unwire, to: b.dataset.to || step.alias, port: b.dataset.port }); return; }
    if (!step) return;
    if (b.dataset.removeStep !== undefined) { removeStep(step.alias); return; }
    if (b.dataset.tadd !== undefined) { (step.transformers ||= []).push({ type: "project", options: {} }); touch({ inspector: true }); return; }
    const box = b.closest(".transformer");
    if (!box) {
      if (b.dataset.kvAdd !== undefined) return;
      return;
    }
    const i = +box.dataset.index;
    if (b.dataset.tremove !== undefined) step.transformers.splice(i, 1);
    else if (b.dataset.tmove) {
      const j = i + +b.dataset.tmove;
      [step.transformers[i], step.transformers[j]] = [step.transformers[j], step.transformers[i]];
    } else if (b.dataset.kvAdd !== undefined) {
      const section = b.closest(".kv").dataset.section;
      const t = step.transformers[i];
      t[section] = { ...(t[section] || {}), "": null };
      // An empty key is kept on screen only: the row is written once it has a name.
      renderInspector();
      const rows = el.querySelectorAll(`.transformer[data-index="${i}"] .kv[data-section="${section}"] .kv-row`);
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

// ---------------------------------------------------------------- drawer: YAML and problems

function renderDrawer() {
  const el = $("d-drawer");
  if (!el || !result) return;
  const problems = (result.errors?.length || 0) + (result.warnings?.length || 0) + (validation && !validation.ok ? 1 : 0);
  const tabs = `<div class="tabs">
      <button class="${drawer === "yaml" ? "active" : ""}" data-tab="yaml">YAML</button>
      <button class="${drawer === "problems" ? "active" : ""}" data-tab="problems">Problems ${problems ? `<span class="count">${problems}</span>` : ""}</button>
      <button class="${drawer === "edit" ? "active" : ""}" data-tab="edit">Edit as YAML</button>
      <span class="drawer-status">${result.errors?.length ? `<span class="mismatch">${result.errors.length} error(s)</span>` : `<span class="ok-line">composes</span>`}${validation ? (validation.ok ? ` · <span class="ok-line">dtpipe accepts it</span>` : ` · <span class="mismatch">dtpipe refuses it</span>`) : ""}</span>
    </div>`;
  let body = "";
  if (drawer === "yaml") {
    body = `<div class="row"><button data-copy>Copy</button><span class="hint">The job the coordinator composed: a plain dtpipe job, runnable whole with <code>dtpipe --job</code>.</span></div><pre id="d-yaml">${esc(result.yaml)}</pre>`;
  } else if (drawer === "problems") {
    body = `${result.errors?.length ? `<ul class="msgs errors">${result.errors.map((m) => `<li>${esc(m)}</li>`).join("")}</ul>` : ""}
      ${result.warnings?.length ? `<ul class="msgs">${result.warnings.map((m) => `<li>${esc(m)}</li>`).join("")}</ul>` : ""}
      ${validation?.dryRun ? `<h3>dtpipe --dry-run</h3><pre class="mismatch">${esc(validation.dryRun)}</pre>` : ""}
      ${!problems ? `<p class="hint">Nothing to report.${validation ? "" : " Validate to have dtpipe run it over one row per source."}</p>` : ""}`;
  } else {
    body = `<textarea id="d-edit" class="mono" rows="16" spellcheck="false">${esc(result.yaml)}</textarea>
      <div class="row"><button data-apply>Apply</button><span class="hint">The job is taken apart into cards again; cards that keep their alias keep their place.</span></div>`;
  }
  el.innerHTML = tabs + body;
}

function bindDrawer() {
  $("d-drawer").addEventListener("click", async (e) => {
    const b = e.target.closest("button");
    if (!b) return;
    if (b.dataset.tab) { drawer = b.dataset.tab; renderDrawer(); }
    else if (b.dataset.copy !== undefined) {
      try { await navigator.clipboard.writeText(result.yaml); toast("YAML copied.", "info"); }
      catch { const sel = window.getSelection(); sel.selectAllChildren($("d-yaml")); toast("Selected: copy it with your keyboard.", "info"); }
    } else if (b.dataset.apply !== undefined) {
      try {
        const answer = await api("/api/design/decompose", { yaml: $("d-edit").value, layout: model.layout });
        const layout = model.layout;
        model = normalize(answer.model);
        model.layout = Object.fromEntries(Object.entries(layout).filter(([a]) => stepOf(a)));
        if (model.steps.some((s) => !model.layout[s.alias])) autoLayout(false);
        selected = null;
        drawer = "yaml";
        dirty = true;
        renderShell();
        await compose();
      } catch (err) { toast(err.message); }
    }
  });
}

// ---------------------------------------------------------------- actions

async function validate() {
  $("d-validate").disabled = true;
  $("d-validate").textContent = "Validating…";
  try {
    validation = await api("/api/design/validate", model);
    drawer = validation.ok && !result?.errors?.length ? drawer : "problems";
    toast(validation.ok ? "dtpipe accepts the job." : "dtpipe refuses the job: see Problems.", validation.ok ? "info" : "error");
  } catch (e) { toast(e.message); }
  $("d-validate").disabled = false;
  $("d-validate").textContent = "Validate";
  renderDrawer();
}

async function save() {
  await compose();
  if (result.errors?.length && !confirm(`${result.errors.length} problem(s) remain. Save anyway?`)) return false;
  const message = prompt("Describe this change (the commit message):", dirty ? `Update ${id}` : `Save ${id}`);
  if (message === null) return false;
  const answer = await api(`/api/library/${encodeURIComponent(id)}`, { yaml: result.yaml, layout: model.layout, message }, "PUT");
  dirty = false;
  renderToolbarState();
  toast(`Saved as ${String(answer.commit || "").slice(0, 8)}.`, "info");
  return true;
}

async function distribute() {
  try {
    if (dirty) {
      if (!confirm("Save before distributing? The plan is computed from the saved pipeline.")) return;
      if (!(await save())) return;
    }
    location.hash = `#/distribution/${encodeURIComponent(id)}`;
  } catch (e) { toast(e.message); }
}
