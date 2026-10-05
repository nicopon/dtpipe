// Nodes: every node and runner, the bricks each offers with their schema and a preview read by
// the node itself, the fragments it hosts, and the flow matrix.
import { api, csvTable, esc, nodeColor, store } from "./core.js";

let host = null;
const open = new Set();      // brick keys whose details are unfolded
const previews = {};         // brick key -> html

export function mount(el) {
  host = el;
  host.addEventListener("click", onClick);
  render();
}

export function update(kind) {
  if (["all", "nodes", "bricks", "deployments"].includes(kind)) render();
}

function brickRow(b) {
  const unfolded = open.has(b.key);
  const schema = b.schema?.length
    ? `<table class="schema"><tr><th>column</th><th>type</th><th>null</th></tr>${b.schema.map((c) => `<tr><td>${esc(c.name)}</td><td>${esc(c.type)}</td><td>${c.isNullable ? "yes" : "no"}</td></tr>`).join("")}</table>`
    : b.schemaError ? `<p class="mismatch">schema: ${esc(b.schemaError)}</p>` : b.kind === "Sink" ? `<p class="hint">A sink declares where it writes; its table's content shows in the preview.</p>` : "";
  return `
    <div class="brick ${b.kind.toLowerCase()}" data-key="${esc(b.key)}">
      <div class="brick-line">
        <span class="kind-badge ${b.kind.toLowerCase()}">${b.kind === "Source" ? "source" : "sink"}</span>
        <span class="brick-title">${esc(b.title)}</span>
      </div>
      <div class="brick-meta">
        <code class="muted" title="id · version: the hash of what the node runs">${esc(b.id)} · ${esc(b.version)}</code>
        <span class="actions">
          <button class="small ghost" data-toggle="${esc(b.key)}">${unfolded ? "hide" : "details"}</button>
          <button class="small ghost" data-preview="${esc(b.key)}">preview</button>
        </span>
      </div>
      ${b.description ? `<div class="hint">${esc(b.description)}</div>` : ""}
      ${unfolded ? `<div class="brick-details">${schema}<pre>${esc(b.yaml)}</pre></div>` : ""}
      ${previews[b.key] ? `<div class="brick-preview">${previews[b.key]}</div>` : ""}
    </div>`;
}

function render() {
  if (!host) return;
  const cards = store.nodes.map((n) => {
    const bricks = store.bricks.filter((b) => b.node === n.name);
    const fragments = n.fragments;
    return `
      <section class="card node-card ${n.online ? "" : "offline"}" style="--node-color:${nodeColor(n.name)}">
        <div class="node-head">
          <span class="node-name"><span class="dot ${n.online ? "on" : ""}"></span>${esc(n.name)}</span>
          <span><span class="chip">${esc(n.role === "Runner" ? "runner" : "data")}</span> <span class="chip">group ${esc(n.group)}</span>${n.sandbox && n.role !== "Runner" ? ` <span class="chip" title="Started with LAB_NODE_SANDBOX: this node runs any reader or writer, not only its bricks.">sandbox</span>` : ""}</span>
        </div>
        <p class="hint">${esc(n.description)}</p>
        ${n.datasets.map((d) => `<div class="dataset"><code>\${{${esc(d.variable)}}}</code> ${esc(d.engine)} · ${esc(d.description)}</div>`).join("")}
        ${n.role === "Runner" ? `<p class="hint">Carries every step between the bricks: transformers, SQL, merges.</p>` : ""}
        ${bricks.length ? `<h3>Bricks</h3>${bricks.map(brickRow).join("")}` : n.role === "Runner" ? "" : `<p class="hint">No brick offered.</p>`}
        <h3>Fragments hosted (${fragments.length})</h3>
        ${fragments.map((f) => `
          <div class="frag-line"><span>${esc(f.fragment)}${f.instance !== "main" ? ` <span class="chip">${esc(f.instance)}</span>` : ""}</span>
          <span class="state ${esc(f.state)}">${esc(f.state)}${f.exitCode != null ? ` ${f.exitCode}` : ""}</span></div>`).join("") || `<p class="hint">none</p>`}
      </section>`;
  }).join("");

  const groups = Object.keys(store.matrix);
  const matrix = `
    <table class="matrix">
      <tr><th>from \\ to</th>${groups.map((g) => `<th>${esc(g)}</th>`).join("")}</tr>
      ${groups.map((g) => `<tr><th>${esc(g)}</th>${groups.map((t) => `<td class="${store.matrix[g].includes(t) ? "yes" : "no"}">${store.matrix[g].includes(t) ? "✓" : "·"}</td>`).join("")}</tr>`).join("")}
    </table>`;

  host.innerHTML = `
    <div class="view-head"><h1>Nodes</h1><p class="hint">What each node offers. A brick is a read or a write its owner preconfigured; the designer assembles pipelines from them.</p></div>
    <div class="node-grid">${cards || `<p class="hint">No node connected yet. Start them with <code>./lab.sh up</code>.</p>`}</div>
    <section class="card"><div class="card-head"><h2>Flow matrix</h2><a href="#/access">edit in Access →</a></div><p class="hint">Which node group may send to which: the hub checks it on every transfer, the planner on every plan. A node's group comes from its token.</p>${matrix}</section>`;
}

async function onClick(e) {
  const toggle = e.target.closest("[data-toggle]");
  const preview = e.target.closest("[data-preview]");
  if (toggle) {
    const key = toggle.dataset.toggle;
    open.has(key) ? open.delete(key) : open.add(key);
    render();
  }
  if (preview) {
    const key = preview.dataset.preview;
    if (previews[key]) { delete previews[key]; render(); return; }
    const [node, id] = key.split("/");
    previews[key] = `<p class="hint">Reading on ${esc(node)}…</p>`;
    render();
    try {
      const { csv } = await api(`/api/bricks/${encodeURIComponent(node)}/${encodeURIComponent(id)}/preview`, {});
      previews[key] = csvTable(csv) + `<p class="hint">First rows, read by ${esc(node)}.</p>`;
    } catch (err) {
      previews[key] = `<p class="mismatch">${esc(err.message)}</p>`;
    }
    render();
  }
}
