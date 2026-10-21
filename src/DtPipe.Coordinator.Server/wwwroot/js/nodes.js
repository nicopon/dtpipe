// Nodes: every node and runner, the bricks each offers by the reference a job makes to them, a preview read by
// the node itself when asked, the replicas that carry it, and the flow matrix.
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
      ${unfolded ? `<div class="brick-details"><p class="hint">A job names this brick by the reference below; its node holds the rest.</p><pre>${esc(b.yaml)}</pre></div>` : ""}
      ${previews[b.key] ? `<div class="brick-preview">${previews[b.key]}</div>` : ""}
    </div>`;
}

function render() {
  if (!host) return;
  const cards = store.nodes.map((n) => {
    const bricks = store.bricks.filter((b) => b.node === n.name);
    const replicas = n.replicas;
    return `
      <section class="card node-card ${n.online ? "" : "offline"}" style="--node-color:${nodeColor(n.name)}">
        <div class="node-head">
          <span class="node-name"><span class="dot ${n.online ? "on" : ""}"></span>${esc(n.name)}</span>
          <span><span class="chip">${esc(n.role === "Runner" ? "runner" : "data")}</span> <span class="chip">${n.groups.length > 1 ? "groups" : "group"} ${esc(n.groups.join(" + "))}</span>${n.capabilities.includes("raw-fragments") && n.role !== "Runner" ? ` <span class="chip" title="This host accepts a fragment that is no brick of its own: its owner configured it so (the lab's sandbox mode).">raw fragments</span>` : ""}</span>
        </div>
        <p class="hint">${esc(n.description)}</p>
        ${n.datasets.map((d) => `<div class="dataset"><code>\${{${esc(d.variable)}}}</code> ${esc(d.description)}</div>`).join("")}
        ${n.role === "Runner" ? `<p class="hint">Carries every step between the bricks: transformers, SQL, merges.</p>` : ""}
        ${bricks.length ? `<h3>Bricks</h3>${bricks.map(brickRow).join("")}` : n.role === "Runner" ? "" : `<p class="hint">No brick offered.</p>`}
        ${replicas.length ? `<h3 title="A replica is one host carrying this node's identity: a run is placed on one of them that can run its fragment.">Replicas (${replicas.filter((r) => r.connected).length}/${replicas.length})</h3>
        ${n.replicasDisagree ? `<p class="hint mismatch">The replicas do not offer the same bricks: a rolling update is under way, and a run goes to a replica that composes its plan.</p>` : ""}
        ${replicas.map((r) => `
          <div class="frag-line" title="${esc(r.origin ?? "")}"><span><code>${esc(r.id)}</code>${r.dtpipeVersion ? ` <span class="chip">dtpipe ${esc(r.dtpipeVersion)}</span>` : ""} <span class="hint">${esc(r.origin ?? "")}</span></span>
          <span class="state ${r.connected ? "Registered" : "Failed"}">${r.connected ? "connected" : "away"}${r.runs ? ` · ${r.runs} run${r.runs > 1 ? "s" : ""}` : ""}</span></div>`).join("")}`
          : `<p class="hint">No replica of this node has announced itself.</p>`}
      </section>`;
  }).join("");

  const groups = Object.keys(store.matrix);
  const matrix = `
    <table class="matrix">
      <tr><th>from \\ to</th>${groups.map((g) => `<th>${esc(g)}</th>`).join("")}</tr>
      ${groups.map((g) => `<tr><th>${esc(g)}</th>${groups.map((t) => `<td class="${store.matrix[g].includes(t) ? "yes" : "no"}">${store.matrix[g].includes(t) ? "✓" : "·"}</td>`).join("")}</tr>`).join("")}
    </table>`;

  host.innerHTML = `
    <div class="view-head"><h1>Nodes</h1><p class="hint">What each node offers. A brick is a read or a write its owner preconfigured; dtpipe studio assembles pipelines from them.</p></div>
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
      const { csv, columns } = await api(`/api/bricks/${encodeURIComponent(node)}/${encodeURIComponent(id)}/preview`, {});
      previews[key] = csvTable(csv, 200, columns) + `<p class="hint">First rows, read by ${esc(node)} through the brick's declared branch.</p>`;
    } catch (err) {
      previews[key] = `<p class="mismatch">${esc(err.message)}</p>`;
    }
    render();
  }
}
