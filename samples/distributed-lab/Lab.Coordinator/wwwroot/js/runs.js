// Runs: what is deployed, the run in flight and the queue behind it, the journal of finished runs,
// the live events of every node, and a look into a node's database.
import { $, api, csvTable, duration, esc, fmt, nodeColor, shortNode, store, toast } from "./core.js";

let host = null;
let openRun = null;          // runId whose report is unfolded
let filter = "";             // pipeline id filter of the journal
let logNode = "";
let logDebug = false;
let ticker = null;
let logTimer = null;

export function mount(el) {
  host = el;
  host.innerHTML = `
    <div class="view-head"><h1>Runs</h1><p class="hint">The coordinator runs one pipeline at a time: a run waits in the queue until the one in flight ends and its own fragments are registered.</p></div>
    <section class="card"><h2>Deployments</h2><div id="r-deployments"></div></section>
    <section class="card"><h2>In flight and queued</h2><div id="r-queue"></div></section>
    <section class="card"><div class="card-head"><h2>Journal</h2><select id="r-filter"></select></div><div id="r-journal"></div></section>
    <div class="grid2">
      <section class="card"><div class="card-head"><h2>Events</h2>
        <div class="actions"><select id="r-lognode"></select><label class="check"><input type="checkbox" id="r-debug"> child stderr</label><button id="r-clear" class="ghost small">Clear</button></div></div>
        <div id="r-log" class="log mono"></div></section>
      <section class="card"><h2>Look into a node's database</h2>
        <div class="row"><select id="r-dataset"></select><input id="r-sql" class="mono grow" spellcheck="false"><button id="r-query">Query</button></div>
        <div id="r-result"></div></section>
    </div>`;
  host.addEventListener("click", onClick);
  $("r-filter").addEventListener("change", (e) => { filter = e.target.value; renderJournal(); });
  $("r-lognode").addEventListener("change", (e) => { logNode = e.target.value; renderLog(); });
  $("r-debug").addEventListener("change", (e) => { logDebug = e.target.checked; renderLog(); });
  $("r-clear").addEventListener("click", () => { store.logs = []; renderLog(); });
  $("r-dataset").addEventListener("change", defaultQuery);
  $("r-query").addEventListener("click", runQuery);
  $("r-sql").addEventListener("keydown", (e) => { if (e.key === "Enter") runQuery(); });
  clearInterval(ticker);
  ticker = setInterval(() => { if (host?.isConnected) renderQueue(); else clearInterval(ticker); }, 1000);
  renderAll();
}

export function update(kind) {
  if (kind === "logs") { clearTimeout(logTimer); logTimer = setTimeout(renderLog, 200); return; }
  if (["all", "deployments", "runs", "nodes"].includes(kind)) renderAll();
}

function renderAll() {
  renderDeployments();
  renderQueue();
  renderJournal();
  renderSelectors();
  renderLog();
}

function renderDeployments() {
  const el = $("r-deployments");
  if (!el) return;
  el.innerHTML = store.deployments.length ? `<table>
    <tr><th>pipeline</th><th>from</th><th>state</th><th>fragments</th><th>edges</th><th>queued</th><th></th></tr>
    ${store.deployments.map((d) => `<tr>
      <td>${d.origin === "library" ? `<a href="#/distribution/${encodeURIComponent(d.pipelineId)}">${esc(d.pipelineId)}</a>` : esc(d.pipelineId)}</td>
      <td>${esc(d.origin === "lab" ? "Lab view" : "library")}</td>
      <td><span class="chip ${d.state === "ready" ? "ok" : d.state === "failed" ? "bad" : "warn"}" title="${esc(d.message || "")}">${esc(d.state)}</span></td>
      <td class="wrap">${d.fragments.map((f) => `<span class="chip" style="color:${nodeColor(f.node)}" title="${esc(f.name)} · ${esc(f.version.slice(0, 12))}">${esc(f.node)}</span>`).join(" ")}</td>
      <td class="num">${d.edges}</td><td class="num">${d.queued || ""}</td>
      <td class="actions-cell"><button class="small" data-run="${esc(d.pipelineId)}" ${d.state === "failed" ? "disabled" : ""}>Run</button>
        <button class="small ghost danger" data-undeploy="${esc(d.pipelineId)}" ${d.runningRunId || d.queued ? "disabled" : ""}>Undeploy</button></td>
    </tr>`).join("")}</table>` : `<p class="hint">Nothing deployed. Deploy a pipeline from its Distribution page.</p>`;
}

function renderQueue() {
  const el = $("r-queue");
  if (!el) return;
  el.innerHTML = store.queue.length ? store.queue.map((r) => {
    const since = r.state === "running" ? `${((Date.now() - new Date(r.startedAt)) / 1000).toFixed(0)} s` : `queued ${((Date.now() - new Date(r.queuedAt)) / 1000).toFixed(0)} s ago`;
    return `<div class="queue-item"><span class="outcome-chip ${esc(r.state)}">${esc(r.state)}</span> <b>${esc(r.pipelineId)}</b> <code>${esc(r.runId)}</code> <span class="hint">${since}</span>
      <button class="small ghost danger" data-cancel="${esc(r.runId)}">Cancel</button></div>`;
  }).join("") : `<p class="hint">Idle.</p>`;
}

function renderSelectors() {
  const f = $("r-filter");
  const ids = [...new Set(store.runs.map((r) => r.pipelineId).filter(Boolean))].sort();
  f.innerHTML = `<option value="">every pipeline</option>` + ids.map((i) => `<option ${i === filter ? "selected" : ""}>${esc(i)}</option>`).join("");
  const n = $("r-lognode");
  n.innerHTML = `<option value="">every node</option>` + [...store.nodes.map((x) => x.name), "coordinator"].map((x) => `<option ${x === logNode ? "selected" : ""}>${esc(x)}</option>`).join("");
  const datasets = store.nodes.flatMap((node) => node.datasets.map((d) => ({ ...d, node: node.name })));
  const qs = $("r-dataset");
  if (qs.options.length !== datasets.length) {
    const previous = qs.value;
    qs.innerHTML = datasets.map((d) => `<option value="${esc(d.variable)}" data-engine="${esc(d.engine)}">${esc(d.node)} · ${esc(d.variable)}</option>`).join("");
    qs.value = previous || datasets.find((d) => d.variable.includes("WAREHOUSE"))?.variable || datasets[0]?.variable || "";
    defaultQuery();
  }
}

function report(r) {
  const res = r.result;
  const edges = (res?.edgeCounts || []).map((e) => `<tr><td>${esc(shortNode(e.producerFragment))}.${esc(e.producerAlias)}</td><td>${esc(shortNode(e.consumerFragment))}.${esc(e.consumerAlias)}</td>
    <td class="num">${e.sent == null ? "?" : fmt(e.sent)}</td><td class="num ${e.sent === e.received && e.sent != null ? "" : "mismatch"}">${e.received == null ? "?" : fmt(e.received)}</td></tr>`).join("");
  const exits = res ? Object.values(res.reports).map((p) => `<tr><td>${esc(shortNode(p.fragment))}</td><td class="num">${p.exitCode}</td><td>${esc(p.origin ?? "")}</td><td class="wrap">${esc(p.firstFault ?? "")}</td></tr>`).join("") : "";
  return `<div class="run-report">
    ${r.refusal ? `<p class="mismatch">${esc(r.refusal)}</p>` : ""}
    ${res?.cause ? `<p><b>Cause:</b> ${esc(shortNode(res.cause))}${res.causeIsUnresponsive ? " (unresponsive)" : ""}</p>` : ""}
    ${res?.consequences?.length ? `<p><b>Consequences:</b> ${res.consequences.map((c) => esc(shortNode(c))).join(", ")}</p>` : ""}
    ${edges ? `<table><tr><th>from</th><th>to</th><th>sent</th><th>received</th></tr>${edges}</table>` : ""}
    ${exits ? `<h3>Exit reports</h3><table><tr><th>fragment</th><th>exit</th><th>origin</th><th>first fault</th></tr>${exits}</table>` : ""}
    ${r.pins && Object.keys(r.pins).length ? `<p class="hint">Pinned: ${esc(JSON.stringify(r.pins))}</p>` : ""}
  </div>`;
}

function renderJournal() {
  const el = $("r-journal");
  if (!el) return;
  const runs = store.runs.filter((r) => !filter || r.pipelineId === filter).slice(0, 60);
  el.innerHTML = runs.length ? `<table class="journal">
    <tr><th>run</th><th>pipeline</th><th>outcome</th><th>started</th><th>duration</th><th>rows crossed</th><th>cause</th></tr>
    ${runs.map((r) => {
      const rows = r.result?.edgeCounts?.reduce((s, e) => s + (e.received ?? 0), 0);
      return `<tr class="clickable" data-open="${esc(r.runId)}"><td><code>${esc(r.runId)}</code></td><td>${esc(r.pipelineId ?? r.planId)}</td>
        <td><span class="outcome-chip ${esc(r.state)}">${esc(r.state)}</span></td><td>${esc(new Date(r.startedAt).toLocaleTimeString())}</td>
        <td class="num">${esc(duration(r))}</td><td class="num">${rows ? fmt(rows) : ""}</td><td class="wrap">${esc(r.result?.cause ? shortNode(r.result.cause) : r.refusal ? r.refusal.slice(0, 80) : "")}</td></tr>
        ${openRun === r.runId ? `<tr><td colspan="7">${report(r)}</td></tr>` : ""}`;
    }).join("")}</table>` : `<p class="hint">No run yet.</p>`;
}

function renderLog() {
  const el = $("r-log");
  if (!el) return;
  const stick = el.scrollTop + el.clientHeight >= el.scrollHeight - 20;
  el.innerHTML = store.logs
    .filter((l) => (!logNode || l.node === logNode) && (logDebug || l.level !== "Debug"))
    .slice(-500)
    .map((l) => `<div class="${esc(l.level)}"><span class="t">${l.at.toLocaleTimeString()}</span> <span class="n" style="color:${nodeColor(l.node)}">${esc(l.node)}</span>${l.fragment ? ` <span class="t">${esc(l.fragment)}</span>` : ""} ${esc(l.message)}</div>`)
    .join("");
  if (stick) el.scrollTop = el.scrollHeight;
}

function defaultQuery() {
  const option = $("r-dataset").selectedOptions[0];
  if (!option) return;
  $("r-sql").value = option.dataset.engine === "sqlite"
    ? "SELECT name FROM sqlite_master WHERE type = 'table'"
    : "SELECT table_name, estimated_size AS rows FROM duckdb_tables()";
}

async function runQuery() {
  const el = $("r-result");
  el.innerHTML = `<p class="hint">Querying…</p>`;
  try {
    const { csv } = await api("/api/query", { variable: $("r-dataset").value, sql: $("r-sql").value });
    el.innerHTML = csvTable(csv);
  } catch (e) {
    el.innerHTML = `<p class="mismatch" style="white-space:pre-wrap">${esc(e.message)}</p>`;
  }
}

async function onClick(e) {
  const row = e.target.closest("[data-open]");
  const b = e.target.closest("button");
  try {
    if (b?.dataset.run) { const run = await api("/api/runs", { pipelineId: b.dataset.run }); toast(`${run.runId} ${run.state}.`, "info"); }
    else if (b?.dataset.undeploy) await api(`/api/deployments/${encodeURIComponent(b.dataset.undeploy)}`, undefined, "DELETE");
    else if (b?.dataset.cancel) await api("/api/runs/cancel", { runId: b.dataset.cancel });
    else if (row) { openRun = openRun === row.dataset.open ? null : row.dataset.open; renderJournal(); }
  } catch (err) {
    toast(err.message);
  }
}
