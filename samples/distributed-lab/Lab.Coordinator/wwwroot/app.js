// The Lab view: a thin client over /api. The coordinator plans, deploys and judges; this page only
// draws what it returns and sends the user's cuts and placement back. Other pipelines may be
// deployed and run from the coordinator's own views meanwhile: this page follows only the one it
// deployed.
"use strict";

const state = {
  catalog: [],
  matrix: {},
  nodes: [],
  pipeline: null,        // { id, title, description, yaml, cuts, placement } as proposed
  cuts: [],
  placement: {},
  pins: {},
  plan: null,            // last /api/plan answer for the current layout
  deployed: null,        // the plan the coordinator holds
  deployState: "",
  run: null,
  history: [],
  bytes: { total: 0, activeTransfers: 0 },
  logs: [],
};

const $ = (id) => document.getElementById(id);
const esc = (s) => String(s ?? "").replace(/[&<>"']/g, (c) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[c]));
const fmt = (n) => (n ?? 0).toLocaleString("en-US");
const nodeColor = (node) => `var(--${node}, var(--muted))`;
const fragmentOf = (plan, node) => `${plan.pipelineId}@${node}`;
// A run's fragments all share the pipeline id; the node is what tells them apart on screen.
// A cause that is a sentence (a refused transfer, a row-count mismatch) is shown whole.
const short = (fragment) => {
  const text = String(fragment ?? "");
  return text.includes(" ") ? text : text.split("@").pop();
};

async function api(path, body) {
  const init = body === undefined ? {} : { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify(body) };
  const response = await fetch(path, init);
  const text = await response.text();
  const data = text ? JSON.parse(text) : {};
  if (!response.ok) throw new Error(data.error || `${response.status} ${response.statusText}`);
  return data;
}

function toast(message) {
  addLog({ node: "lab", level: "Error", message });
  alert(message);
}

// ---------------------------------------------------------------- pipeline selection and planning

function planBody() {
  return { pipelineId: state.pipeline.id, yaml: state.pipeline.yaml, cuts: state.cuts, placement: state.placement };
}

function signature(plan) {
  if (!plan) return "";
  return JSON.stringify((plan.fragments || []).map((f) => [f.name, f.version]).sort());
}

const isDeployedLayout = () => !!state.deployed && !!state.plan && state.plan.fragments.length > 0 && signature(state.plan) === signature(state.deployed);
const isRunning = () => ["running", "queued"].includes(state.run?.state);

let planSeq = 0;
let planTimer = null;
function replan(immediate = false) {
  clearTimeout(planTimer);
  const work = async () => {
    const seq = ++planSeq;
    $("graph").classList.add("busy");
    try {
      const plan = await api("/api/plan", planBody());
      if (seq !== planSeq) return;
      state.plan = plan;
    } catch (e) {
      if (seq === planSeq) state.plan = { units: [], fragments: [], edges: [], errors: [e.message], warnings: [] };
    }
    $("graph").classList.remove("busy");
    renderAll();
  };
  if (immediate) work(); else planTimer = setTimeout(work, 150);
}

function selectPipeline(entry, layout) {
  state.pipeline = { ...entry };
  state.cuts = structuredClone(layout?.cuts ?? entry.cuts ?? []);
  state.placement = { ...(layout?.placement ?? entry.placement ?? {}) };
  state.pins = {};
  $("pipeline-select").value = entry.id;
  $("pipeline-description").textContent = entry.description || "";
  $("yaml-text").value = entry.yaml;
  replan(true);
}

function renderCatalog() {
  const select = $("pipeline-select");
  select.innerHTML = state.catalog.map((e) => `<option value="${esc(e.id)}">${esc(e.title)}</option>`).join("")
    + `<option value="__custom">Custom job…</option>`;
}

// ---------------------------------------------------------------- nodes and matrix

function renderNodes() {
  $("nodes").innerHTML = state.nodes.map((n) => `
    <div class="node ${n.online ? "" : "offline"}" style="--node-color:${nodeColor(n.name)}">
      <div class="node-head">
        <span class="node-name"><span class="dot ${n.online ? "on" : ""}"></span>${esc(n.name)}</span>
        <span class="chip">${esc(n.group)}</span>
      </div>
      <div class="hint" style="margin:2px 0">${esc(n.description)}</div>
      ${n.datasets.map((d) => `<div class="dataset"><code>\${{${esc(d.variable)}}}</code> ${esc(d.engine)} · ${esc(d.description)}</div>`).join("")}
      ${n.fragments.map((f) => `
        <div class="frag">
          <div class="frag-line">
            <span>${esc(f.fragment)}${f.instance !== "main" ? ` <span class="chip">${esc(f.instance)}</span>` : ""}</span>
            <span class="state ${esc(f.state)}">${esc(f.state)}${f.exitCode != null ? ` ${f.exitCode}` : ""}</span>
          </div>
          <div class="msg">v ${esc((f.version || "").slice(0, 10))}${f.message ? ` · ${esc(f.message)}` : ""}</div>
        </div>`).join("")}
    </div>`).join("") || `<p class="hint">No node connected yet. Start them with <code>./lab.sh up</code>.</p>`;

  const filter = $("log-filter");
  const current = filter.value;
  filter.innerHTML = `<option value="">all nodes</option>` + state.nodes.map((n) => `<option>${esc(n.name)}</option>`).join("") + `<option>lab</option>`;
  filter.value = current;

  const datasets = state.nodes.flatMap((n) => n.datasets.map((d) => ({ ...d, node: n.name })));
  const qs = $("query-dataset");
  if (qs.options.length !== datasets.length) {
    const previous = qs.value;
    qs.innerHTML = datasets.map((d) => `<option value="${esc(d.variable)}" data-engine="${esc(d.engine)}">${esc(d.node)} · ${esc(d.variable)}</option>`).join("");
    qs.value = previous || datasets.find((d) => d.variable.includes("WAREHOUSE"))?.variable || datasets[0]?.variable || "";
    defaultQuery();
  }
}

function renderMatrix() {
  $("matrix").innerHTML = Object.entries(state.matrix).map(([group, targets]) =>
    `<div class="matrix-row"><b>${esc(group)}</b> → ${targets.length ? targets.map(esc).join(", ") : `<span class="none">nobody</span>`}</div>`).join("");
}

// ---------------------------------------------------------------- graph

const COL_WIDTH = 250;
const COL_GAP = 90;
const ROW_GAP = 22;

function renderGraph() {
  const host = $("graph");
  const plan = state.plan;
  if (!plan || plan.units.length === 0) {
    host.innerHTML = `<p class="hint">${plan ? "Nothing to draw." : "Planning…"}</p>`;
    return;
  }
  const units = plan.units;
  const byId = Object.fromEntries(units.map((u) => [u.id, u]));
  const tails = new Set(units.filter((u) => u.isHead).map((u) => u.yamlKey));

  // Column = longest path from a source.
  const depth = {};
  const depthOf = (u, seen = new Set()) => {
    if (depth[u.id] !== undefined) return depth[u.id];
    if (seen.has(u.id)) return 0;
    seen.add(u.id);
    const d = u.dependsOn.filter((x) => byId[x]).reduce((m, x) => Math.max(m, depthOf(byId[x], seen) + 1), 0);
    return (depth[u.id] = d);
  };
  units.forEach((u) => depthOf(u));

  const nodeOptions = (u) => `<option value="">— node —</option>` + state.nodes.map((n) =>
    `<option value="${esc(n.name)}" ${u.node === n.name ? "selected" : ""} style="color:${nodeColor(n.name)}">${esc(n.name)} · ${esc(n.group)}</option>`).join("");

  const cards = units.map((u) => {
    const b = u.branch;
    const isTail = tails.has(u.id);
    const cut = state.cuts.find((c) => c.branch === u.yamlKey);
    // Slots exist on an uncut source branch, and on a head (to move or remove its cut).
    const canCut = (b.cuttable && !isTail && !u.isHead) || u.isHead;
    const slot = (index) => {
      if (!canCut) return "";
      const active = u.isHead && cut && cut.at === index;
      const title = active ? "Remove this cut" : `Cut ${u.yamlKey} after this stage (dtpipe split --at ${index})`;
      return `<div class="cut-slot ${active ? "active" : ""}"><button data-cut="${esc(u.yamlKey)}" data-at="${index}" title="${esc(title)}">✂</button></div>`;
    };
    const title = u.isHead ? `${esc(u.yamlKey)} <small>head</small>` : isTail ? `${esc(u.id)} <small>tail</small>` : esc(u.id);
    const rows = [];
    const optionRows = (predicate) => {
      const lines = b.options.filter(predicate);
      return lines.length ? `<div class="stage">${lines.map((o) => `<span class="opt">${esc(o)}</span>`).join("")}</div>` : "";
    };
    const isReader = (o) => o.split(".")[0].endsWith("-reader") || o.split(".")[0] === (b.input || "").split(":")[0];
    const isProcessor = (o) => b.processor && o.split(".")[0] === b.processor;
    if (b.input) rows.push(`<div class="stage io"><span class="k">in</span>${esc(b.input)}</div>`);
    if (b.input) rows.push(optionRows(isReader));
    if (b.from.length || b.ref.length) rows.push(`<div class="stage io"><span class="k">from</span>${esc(b.from.join(", "))}${b.ref.length ? ` <span class="k">ref</span>${esc(b.ref.join(", "))}` : ""}</div>`);
    if (b.input) rows.push(slot(0));
    b.transformers.forEach((t, i) => {
      rows.push(`<div class="stage"><span class="k">${esc(t.type)}</span>${esc(t.summary)}</div>`);
      rows.push(slot(i + 1));
    });
    if (b.processor) rows.push(`<div class="stage"><span class="k">${esc(b.processor)}</span>${esc(b.options.filter((o) => o.startsWith(b.processor + ".")).map((o) => o.split(" = ").slice(1).join(" = ")).join(" ") || "")}</div>`);
    rows.push(optionRows((o) => !isProcessor(o) && !(b.input && isReader(o))));
    if (b.output) rows.push(`<div class="stage io"><span class="k">out</span>${esc(b.output)}</div>`);
    return `
      <div class="unit ${u.node ? "" : "unplaced"}" data-unit="${esc(u.id)}" style="--node-color:${u.node ? nodeColor(u.node) : "var(--bad)"}; left:${depth[u.id] * (COL_WIDTH + COL_GAP)}px; top:0">
        <div class="unit-head"><span class="unit-title" title="${esc(u.id)}">${title}</span></div>
        <div class="unit-place"><select data-place="${esc(u.id)}" aria-label="Node for ${esc(u.id)}">${nodeOptions(u)}</select></div>
        ${rows.join("")}
      </div>`;
  }).join("");

  host.innerHTML = `<div class="graph-inner">${cards}<svg></svg></div>`;
  const inner = host.firstElementChild;

  // Stack each column, then size the canvas.
  const columnY = {};
  let height = 0;
  for (const u of units) {
    const el = inner.querySelector(`[data-unit="${CSS.escape(u.id)}"]`);
    const y = columnY[depth[u.id]] ?? 0;
    el.style.top = `${y}px`;
    columnY[depth[u.id]] = y + el.offsetHeight + ROW_GAP;
    height = Math.max(height, y + el.offsetHeight);
  }
  const width = (Math.max(...Object.values(depth)) + 1) * (COL_WIDTH + COL_GAP) - COL_GAP;
  inner.style.width = `${width}px`;
  inner.style.height = `${height + 10}px`;

  drawEdges(inner, units, byId);
}

function edgeRunCounts(plan, producerFragment, consumerFragment, consumerAlias) {
  if (!state.run?.result || !isDeployedLayout() || state.run.planId !== state.deployed?.id) return null;
  return state.run.result.edgeCounts.find((e) =>
    e.producerFragment === producerFragment && e.consumerFragment === consumerFragment && e.consumerAlias === consumerAlias) || null;
}

function drawEdges(inner, units, byId) {
  const svg = inner.querySelector("svg");
  svg.setAttribute("width", inner.offsetWidth);
  svg.setAttribute("height", inner.offsetHeight);
  const plan = state.plan;
  const box = (id) => {
    const el = inner.querySelector(`[data-unit="${CSS.escape(id)}"]`);
    return { x: el.offsetLeft, y: el.offsetTop, w: el.offsetWidth, h: el.offsetHeight };
  };
  const parts = [];
  for (const u of units) {
    u.dependsOn.forEach((dep, i) => {
      const p = byId[dep];
      if (!p) return;
      const a = box(p.id), b = box(u.id);
      const x1 = a.x + a.w, y1 = a.y + Math.min(a.h / 2, 40);
      const x2 = b.x, y2 = b.y + 30 + i * 18;
      const mid = (x1 + x2) / 2;
      const crossing = p.node && u.node && p.node !== u.node;
      const alias = dep.endsWith("^") ? u.yamlKey : dep;
      let kind = "memory";
      let label = "";
      let labelClass = "";
      if (crossing) {
        const pf = fragmentOf(plan, p.node), cf = fragmentOf(plan, u.node);
        const planned = plan.edges.find((e) => e.producerFragment === pf && e.consumerFragment === cf && e.consumerAlias === alias);
        kind = planned && !planned.allowed ? "forbidden" : "network";
        label = kind === "forbidden" ? "refused" : alias;
        const counts = edgeRunCounts(plan, pf, cf, alias);
        if (counts) {
          label = `${alias} · ${fmt(counts.sent)} → ${fmt(counts.received)}`;
          labelClass = counts.sent != null && counts.sent === counts.received ? "ok" : "bad";
        }
      }
      const flowing = kind === "network" && isRunning() && isDeployedLayout() ? "flowing" : "";
      parts.push(`<path class="edge ${kind} ${flowing}" d="M${x1},${y1} C${mid},${y1} ${mid},${y2} ${x2},${y2}"/>`);
      if (label) parts.push(`<text class="edge-label ${labelClass}" x="${x1 + 8}" y="${y1 - 6}">${esc(label)}</text>`);
    });
  }
  svg.innerHTML = parts.join("");
}

// ---------------------------------------------------------------- plan panel

function renderPlan() {
  const plan = state.plan;
  const host = $("plan");
  if (!plan) { host.innerHTML = ""; return; }
  const deployedHere = isDeployedLayout();
  const running = isRunning();
  const ready = state.deployState === "ready";

  const parts = [];
  if (plan.errors.length) parts.push(`<ul class="msgs errors">${plan.errors.map((e) => `<li>${esc(e)}</li>`).join("")}</ul>`);
  if (plan.flowRejection) parts.push(`<ul class="msgs errors"><li>${esc(plan.flowRejection)}</li></ul>`);
  if (plan.warnings?.length) parts.push(`<ul class="msgs">${plan.warnings.map((e) => `<li>${esc(e)}</li>`).join("")}</ul>`);
  if (plan.deployable) {
    parts.push(`<p class="ok-line">${plan.fragments.length} fragments, ${plan.edges.length} arrow edge(s)` +
      (deployedHere ? (ready ? " — deployed, every fragment registered." : ` — deployed (${esc(state.deployState)}).`) : " — not deployed yet.") + `</p>`);
  }

  for (const f of plan.fragments) {
    const inbound = f.edges.filter((e) => e.direction === "Inbound").map((e) => e.alias);
    const outbound = f.edges.filter((e) => e.direction === "Outbound").map((e) => e.alias);
    parts.push(`
      <div class="frag-card" style="--node-color:${nodeColor(f.node)}">
        <div class="row">
          <b>${esc(f.name)}</b>
          <span class="actions">
            <button class="small danger ghost" data-kill="${esc(f.name)}" ${deployedHere && running ? "" : "disabled"} title="Kill this fragment's dtpipe child, as a crashed host would">kill</button>
            <button class="small ghost" data-variant="${esc(f.name)}" ${deployedHere && !running && ready ? "" : "disabled"} title="Deploy a second instance whose job differs by a comment: admission then refuses the run as misaligned">+ misaligned instance</button>
          </span>
        </div>
        <div class="edges">group ${esc(f.group)} · in: ${inbound.map(esc).join(", ") || "—"} · out: ${outbound.map(esc).join(", ") || "—"} · version <code>${esc(f.version.slice(0, 12))}</code></div>
        <details><summary>job and pin</summary>
          <pre>${esc(f.yaml)}</pre>
          <div class="row" style="margin-top:6px"><span class="hint">Pin this run to version</span>
            <input data-pin="${esc(f.name)}" placeholder="latest" value="${esc(state.pins[f.name] || "")}" class="mono"></div>
        </details>
      </div>`);
  }
  host.innerHTML = parts.join("");

  $("deploy-btn").disabled = !plan.deployable || running;
  $("deploy-btn").textContent = deployedHere ? "Redeploy" : "Deploy";
  $("run-btn").disabled = !(deployedHere && ready && !running);
  $("run-btn").title = deployedHere ? "" : "Deploy this layout first";
  $("cancel-btn").disabled = !running;
}

// ---------------------------------------------------------------- run panel

function renderRun() {
  const run = state.run;
  const host = $("run");
  if (!run) { host.innerHTML = `<p class="hint">No run yet.</p>`; }
  else {
    const r = run.result;
    const elapsed = run.finishedAt ? ((new Date(run.finishedAt) - new Date(run.startedAt)) / 1000).toFixed(1) + " s" : "";
    const rows = (r?.edgeCounts || []).map((e) => `
      <tr><td>${esc(short(e.producerFragment))}.${esc(e.producerAlias)}</td><td>${esc(short(e.consumerFragment))}.${esc(e.consumerAlias)}</td>
      <td class="num">${e.sent == null ? "?" : fmt(e.sent)}</td><td class="num ${e.sent === e.received && e.sent != null ? "" : "mismatch"}">${e.received == null ? "?" : fmt(e.received)}</td></tr>`).join("");
    const reports = r ? Object.values(r.reports).map((p) => `
      <tr><td>${esc(short(p.fragment))}</td><td class="num">${p.exitCode}</td><td>${esc(p.origin ?? "")}</td><td style="white-space:normal">${esc(p.firstFault ?? "")}</td></tr>`).join("") : "";
    host.innerHTML = `
      <div class="outcome ${esc(run.state)}">${esc(run.runId)} · ${esc(run.state)} ${elapsed ? `<span class="hint">${elapsed}</span>` : ""}</div>
      ${run.refusal ? `<p class="mismatch">${esc(run.refusal)}</p>` : ""}
      ${r?.cause ? `<p><b>Cause:</b> ${esc(short(r.cause))}${r.causeIsUnresponsive ? " (unresponsive)" : ""}</p>` : ""}
      ${r?.consequences?.length ? `<p><b>Consequences:</b> ${r.consequences.map((c) => esc(short(c))).join(", ")}</p>` : ""}
      ${rows ? `<div class="table-wrap"><table><tr><th>from</th><th>to</th><th>sent</th><th>received</th></tr>${rows}</table></div>` : ""}
      ${reports ? `<h3>Exit reports</h3><div class="table-wrap"><table><tr><th>fragment</th><th>exit</th><th>origin</th><th>first fault</th></tr>${reports}</table></div>` : ""}`;
  }
  $("history").innerHTML = state.history.map((h) => `
    <div class="history-item"><span class="outcome ${esc(h.state)}" style="font-size:12.5px">${esc(h.state)}</span>
    <span>${esc(h.runId)}</span><span class="muted">${new Date(h.startedAt).toLocaleTimeString()}</span>
    <span class="muted">${esc(h.planId)}</span></div>`).join("") || `<p class="hint">—</p>`;
}

function renderStatus() {
  const pill = $("deploy-state");
  const labels = { deploying: "deploying…", rearming: "re-arming fragments…", ready: "deployed · ready", failed: "deployment failed", variant: "deployed · extra instance" };
  pill.textContent = isRunning() ? `${state.run.runId} running` : (state.deployed ? `${state.deployed.pipelineId}: ${labels[state.deployState] || state.deployState || "deployed"}` : "nothing deployed");
  pill.className = "pill " + (isRunning() || ["deploying", "rearming"].includes(state.deployState) ? "busy" : state.deployState === "failed" ? "off" : state.deployed ? "on" : "");
  const b = state.bytes.total;
  const human = b > 1 << 30 ? (b / (1 << 30)).toFixed(2) + " GiB" : b > 1 << 20 ? (b / (1 << 20)).toFixed(1) + " MiB" : b > 1024 ? (b / 1024).toFixed(0) + " KiB" : b + " B";
  $("bytes").textContent = `hub ${human}${state.bytes.activeTransfers > 0 ? ` · ${state.bytes.activeTransfers} transfer(s)` : ""}`;
}

function renderAll() {
  renderGraph();
  renderPlan();
  renderRun();
  renderStatus();
}

// ---------------------------------------------------------------- log

function addLog(line) {
  if (/:\s*$/.test(line.message)) return; // a label whose value was empty, e.g. a silent child's stderr
  state.logs.push({ ...line, at: new Date() });
  if (state.logs.length > 1500) state.logs.splice(0, state.logs.length - 1500);
  scheduleLogRender();
}

let logTimer = null;
function scheduleLogRender() {
  if (logTimer) return;
  logTimer = setTimeout(() => { logTimer = null; renderLog(); }, 200);
}

function renderLog() {
  const host = $("log");
  const node = $("log-filter").value;
  const debug = $("log-debug").checked;
  const stick = host.scrollTop + host.clientHeight >= host.scrollHeight - 20;
  host.innerHTML = state.logs
    .filter((l) => (!node || l.node === node) && (debug || l.level !== "Debug"))
    .slice(-500)
    .map((l) => `<div class="${esc(l.level)}"><span class="t">${l.at.toLocaleTimeString()}</span> <span class="n" style="color:${nodeColor(l.node)}">${esc(l.node)}</span>${l.fragment ? ` <span class="t">${esc(l.fragment)}</span>` : ""} ${esc(l.message)}</div>`)
    .join("");
  if (stick) host.scrollTop = host.scrollHeight;
}

// ---------------------------------------------------------------- live events

function connectEvents() {
  const source = new EventSource("/api/events");
  source.onopen = () => { $("conn").textContent = "coordinator connected"; $("conn").className = "pill on"; };
  source.onerror = () => { $("conn").textContent = "reconnecting…"; $("conn").className = "pill off"; };
  source.onmessage = (message) => {
    const { type, data } = JSON.parse(message.data);
    switch (type) {
      case "nodes":
        state.nodes = data;
        renderNodes();
        break;
      case "state":
        state.deployed = data.plan;
        state.run = data.run;
        if (data.plan) state.deployState = "ready";
        restoreDeployedLayout();
        renderAll();
        break;
      case "deploy":
        if (state.deployed && data.pipelineId !== state.deployed.pipelineId) break;
        state.deployState = data.state;
        if (data.state === "failed") addLog({ node: "lab", level: "Error", message: `deployment: ${data.message}` });
        renderPlan();
        renderStatus();
        break;
      case "run":
        if (state.deployed && data.pipelineId && data.pipelineId !== state.deployed.pipelineId) break;
        state.run = data;
        if (data.state !== "running") {
          state.history = [data, ...state.history.filter((h) => h.runId !== data.runId)].slice(0, 20);
          addLog({ node: "lab", level: data.state === "succeeded" ? "Information" : "Warning", message: `${data.runId} ${data.state}${data.refusal ? `: ${data.refusal}` : ""}` });
        }
        renderAll();
        break;
      case "fragment":
        addLog({ node: data.node, fragment: data.fragment, level: data.state === "Failed" ? "Warning" : "Information", message: `${data.state}${data.exitCode != null ? ` (exit ${data.exitCode})` : ""}${data.message ? ` · ${data.message}` : ""}` });
        break;
      case "log":
        addLog({ node: data.node, fragment: data.fragment, level: data.level, message: data.message });
        break;
      case "bytes":
        state.bytes = data;
        renderStatus();
        break;
    }
  };
}

let restored = false;
function restoreDeployedLayout() {
  // On a fresh page, show the deployed plan rather than the first catalog entry.
  if (restored || !state.deployed || !state.catalog.length) return;
  const entry = state.catalog.find((e) => e.id === state.deployed.pipelineId);
  if (!entry) return;
  restored = true;
  const placement = Object.fromEntries(state.deployed.units.map((u) => [u.id, u.node]));
  selectPipeline(entry, { cuts: state.deployed.cuts, placement });
}

// ---------------------------------------------------------------- query

function defaultQuery() {
  const option = $("query-dataset").selectedOptions[0];
  if (!option) return;
  $("query-sql").value = option.dataset.engine === "sqlite"
    ? "SELECT name FROM sqlite_master WHERE type = 'table'"
    : "SELECT table_name, estimated_size AS rows FROM duckdb_tables()";
}

function parseCsv(text) {
  const rows = [];
  let row = [], field = "", quoted = false;
  for (let i = 0; i < text.length; i++) {
    const c = text[i];
    if (quoted) {
      if (c === '"' && text[i + 1] === '"') { field += '"'; i++; }
      else if (c === '"') quoted = false;
      else field += c;
    } else if (c === '"') quoted = true;
    else if (c === ",") { row.push(field); field = ""; }
    else if (c === "\n") { row.push(field.replace(/\r$/, "")); rows.push(row); row = []; field = ""; }
    else field += c;
  }
  if (field || row.length) { row.push(field.replace(/\r$/, "")); rows.push(row); }
  return rows;
}

async function runQuery() {
  const host = $("query-result");
  host.innerHTML = `<p class="hint">Querying…</p>`;
  try {
    const { csv } = await api("/api/query", { variable: $("query-dataset").value, sql: $("query-sql").value });
    const [header, ...rows] = parseCsv(csv);
    if (!header) { host.innerHTML = `<p class="hint">No rows.</p>`; return; }
    host.innerHTML = `<table><tr>${header.map((h) => `<th>${esc(h)}</th>`).join("")}</tr>${rows.map((r) => `<tr>${r.map((v) => `<td>${esc(v)}</td>`).join("")}</tr>`).join("")}</table>
      <p class="hint">${rows.length} row(s)${rows.length >= 200 ? ", first 200 only" : ""}.</p>`;
  } catch (e) {
    host.innerHTML = `<p class="mismatch" style="white-space:pre-wrap">${esc(e.message)}</p>`;
  }
}

// ---------------------------------------------------------------- command line

function splitArgs(line) {
  const args = [];
  let current = "", quote = null, has = false;
  for (let i = 0; i < line.length; i++) {
    const c = line[i];
    if (quote) {
      if (c === quote) quote = null;
      else if (c === "\\" && quote === '"' && i + 1 < line.length) current += line[++i];
      else current += c;
    } else if (c === "'" || c === '"') { quote = c; has = true; }
    else if (/\s/.test(c)) { if (has || current) { args.push(current); current = ""; has = false; } }
    else if (c === "\\" && i + 1 < line.length) { current += line[++i]; has = true; }
    else { current += c; has = true; }
  }
  if (has || current) args.push(current);
  if (args[0] === "dtpipe") args.shift();
  return args;
}

// ---------------------------------------------------------------- wiring

function useCustomYaml(yaml, title) {
  selectPipeline({ id: "custom", title, description: "A job typed on this page.", yaml, cuts: [], placement: {} });
  $("pipeline-select").value = "__custom";
}

function bind() {
  $("pipeline-select").addEventListener("change", (e) => {
    if (e.target.value === "__custom") {
      $("yaml-editor").hidden = false;
      useCustomYaml(state.pipeline?.yaml || "", "Custom job");
      return;
    }
    selectPipeline(state.catalog.find((x) => x.id === e.target.value));
  });
  $("toggle-yaml").addEventListener("click", () => { $("yaml-editor").hidden = !$("yaml-editor").hidden; });
  $("toggle-command").addEventListener("click", () => { $("command-editor").hidden = !$("command-editor").hidden; });
  $("reset-layout").addEventListener("click", () => {
    const entry = state.catalog.find((x) => x.id === state.pipeline?.id);
    if (entry) selectPipeline(entry);
    else { state.cuts = []; state.placement = {}; replan(); }
  });
  $("apply-yaml").addEventListener("click", () => useCustomYaml($("yaml-text").value, "Custom job"));
  $("apply-command").addEventListener("click", async () => {
    try {
      const { yaml } = await api("/api/pipelines/from-command", { args: splitArgs($("command-text").value) });
      $("yaml-text").value = yaml;
      $("yaml-editor").hidden = false;
      useCustomYaml(yaml, "From a command line");
    } catch (e) { toast(e.message); }
  });

  $("graph").addEventListener("change", (e) => {
    const unit = e.target.dataset.place;
    if (unit === undefined) return;
    if (e.target.value) state.placement[unit] = e.target.value;
    else delete state.placement[unit];
    replan();
  });
  $("graph").addEventListener("click", (e) => {
    const button = e.target.closest("button[data-cut]");
    if (!button) return;
    const branch = button.dataset.cut;
    const at = Number(button.dataset.at);
    const existing = state.cuts.find((c) => c.branch === branch);
    state.cuts = state.cuts.filter((c) => c.branch !== branch);
    if (!existing || existing.at !== at) state.cuts.push({ branch, at });
    if (!state.cuts.some((c) => c.branch === branch)) delete state.placement[branch + "^"];
    replan();
  });

  $("plan").addEventListener("input", (e) => {
    const fragment = e.target.dataset.pin;
    if (fragment !== undefined) state.pins[fragment] = e.target.value.trim();
  });
  $("plan").addEventListener("click", async (e) => {
    const kill = e.target.closest("button[data-kill]");
    const variant = e.target.closest("button[data-variant]");
    try {
      if (kill) await api(`/api/fragments/${encodeURIComponent(kill.dataset.kill)}/kill`, {});
      if (variant) await api(`/api/fragments/${encodeURIComponent(variant.dataset.variant)}/variant`, {});
    } catch (err) { toast(err.message); }
  });

  $("deploy-btn").addEventListener("click", async () => {
    $("deploy-btn").disabled = true;
    state.deployState = "deploying";
    renderStatus();
    try {
      const plan = await api("/api/deploy", planBody());
      state.deployed = plan;
      state.plan = plan;
      state.pins = {};
    } catch (e) { toast(e.message); }
    renderAll();
  });
  $("run-btn").addEventListener("click", async () => {
    const pins = Object.fromEntries(Object.entries(state.pins).filter(([, v]) => v));
    try { state.run = await api("/api/runs", { pins, pipelineId: state.deployed?.pipelineId }); renderAll(); } catch (e) { toast(e.message); }
  });
  $("cancel-btn").addEventListener("click", () => api("/api/runs/cancel", { runId: state.run?.runId }).catch((e) => toast(e.message)));

  $("query-dataset").addEventListener("change", defaultQuery);
  $("query-btn").addEventListener("click", runQuery);
  $("query-sql").addEventListener("keydown", (e) => { if (e.key === "Enter") runQuery(); });
  $("log-filter").addEventListener("change", renderLog);
  $("log-debug").addEventListener("change", renderLog);
  $("log-clear").addEventListener("click", () => { state.logs = []; renderLog(); });
  window.addEventListener("resize", () => renderGraph());
}

async function init() {
  bind();
  const [catalog, matrix, snapshot] = await Promise.all([api("/api/pipelines"), api("/api/flow-matrix"), api("/api/state")]);
  state.catalog = catalog;
  state.matrix = matrix.groups;
  state.history = snapshot.history || [];
  state.deployed = snapshot.plan;
  state.run = snapshot.run;
  if (snapshot.plan) state.deployState = "ready";
  renderCatalog();
  renderMatrix();
  restoreDeployedLayout();
  if (!state.pipeline && catalog.length) selectPipeline(catalog[0]);
  // ?static renders once without the live stream (a headless browser waits on it forever).
  if (!new URLSearchParams(location.search).has("static")) connectEvents();
  else state.nodes = await api("/api/nodes"), renderNodes();
}

window.addEventListener("error", (e) => addLog({ node: "lab", level: "Error", message: `page: ${e.message}` }));
window.addEventListener("unhandledrejection", (e) => addLog({ node: "lab", level: "Error", message: `page: ${e.reason?.stack || e.reason}` }));
init().catch((e) => toast(`The lab page failed to start: ${e.message}`));
