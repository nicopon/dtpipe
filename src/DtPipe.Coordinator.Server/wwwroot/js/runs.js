// Runs: what is deployed, the runs in flight and the queue behind them, and the journal of finished runs.
import { $, ROWS_CROSSED_HINT, api, duration, endNotes, esc, fmt, nodeColor, plural, cursorsOf, ranPlan, rowsCrossed, runStateLabel, shortNode, store, toast } from "./core.js";

let host = null;
let openRun = null;          // runId whose report is unfolded
let filter = "";             // pipeline id filter of the journal
let filtered = [];           // the runs of that pipeline, as the server lists them
let ticker = null;

export function mount(el) {
  host = el;
  host.innerHTML = `
    <div class="view-head"><h1>Runs</h1><p class="hint">Runs that share no data node are in flight together; a run that shares one waits its turn, in the order runs came, until that run ends and its own fragments are registered again. The runner never counts.</p></div>
    <section class="card"><h2>Deployments</h2><div id="r-deployments"></div></section>
    <section class="card"><h2>In flight and queued</h2><div id="r-queue"></div></section>
    <section class="card"><div class="card-head"><h2>Runs log</h2><select id="r-filter"></select></div><div id="r-journal"></div></section>`;
  host.addEventListener("click", onClick);
  $("r-filter").addEventListener("change", (e) => { filter = e.target.value; filtered = []; renderJournal(); loadFiltered(); });
  clearInterval(ticker);
  ticker = setInterval(() => { if (host?.isConnected) renderQueue(); else clearInterval(ticker); }, 1000);
  renderAll();
  loadFiltered();
}

export function update(kind) {
  if (["all", "deployments", "runs", "nodes"].includes(kind)) renderAll();
  if (kind === "runs") loadFiltered();
}

// The journal of one pipeline is the server's, not a cut of the page's own latest runs.
async function loadFiltered() {
  const asked = filter;
  if (!asked) return;
  try {
    const runs = await api(`/api/runs?pipeline=${encodeURIComponent(asked)}`);
    if (asked === filter) { filtered = runs; renderJournal(); }
  } catch (err) {
    toast(err.message);
  }
}

function renderAll() {
  renderDeployments();
  renderQueue();
  renderJournal();
  renderSelectors();
}

function renderDeployments() {
  const el = $("r-deployments");
  if (!el) return;
  el.innerHTML = store.deployments.length ? `<table>
    <tr><th>pipeline</th><th>from</th><th>state</th><th>fragments</th><th>edges</th><th>queued</th><th></th></tr>
    ${store.deployments.map((d) => `<tr>
      <td>${d.origin === "library" ? `<a href="#/library/${encodeURIComponent(d.pipelineId)}">${esc(d.pipelineId)}</a>` : esc(d.pipelineId)}</td>
      <td>${esc(d.origin)}</td>
      <td><span class="chip ${d.state === "ready" ? "ok" : d.state === "failed" ? "bad" : "warn"}" title="${esc(d.message || "")}">${esc(d.state)}</span>${d.commit ? ` <span class="hint mono" title="The commit it is armed at">${esc(d.commit.slice(0, 8))}</span>` : ""}</td>
      <td class="wrap">${d.fragments.length ? "" : (d.nodes || []).map((n) => `<span class="chip" style="color:${nodeColor(n)}" title="Bound by this deployment; planned at each launch">${esc(n)}</span>`).join(" ")}${d.fragments.map((f) => `<span class="chip" style="color:${nodeColor(f.node)}" title="${esc(f.name)} · ${esc(f.version.slice(0, 12))} · ${plural(f.replicas ?? 0, "replica")} could run it now">${esc(f.node)}${f.replicas != null ? ` · ${f.replicas}` : ""}</span>`).join(" ")}</td>
      <td class="num">${d.edges}</td><td class="num">${d.queued || ""}</td>
      <td class="actions-cell"><button class="small" data-needs="designer" data-run="${esc(d.pipelineId)}" ${d.state === "failed" ? "disabled" : ""}>Run</button>
        <button class="small ghost danger" data-needs="designer" data-undeploy="${esc(d.pipelineId)}" ${d.runningRunId || d.queued ? "disabled" : ""}>Undeploy</button></td>
    </tr>`).join("")}</table>` : `<p class="hint">Nothing deployed. Deploy a pipeline from the Library.</p>`;
}

function renderQueue() {
  const el = $("r-queue");
  if (!el) return;
  el.innerHTML = store.queue.length ? store.queue.map((r) => {
    const since = r.state === "running" ? `${((Date.now() - new Date(r.startedAt)) / 1000).toFixed(0)} s` : `queued ${((Date.now() - new Date(r.queuedAt)) / 1000).toFixed(0)} s ago`;
    return `<div class="queue-item"><span class="outcome-chip ${esc(r.state)}">${esc(runStateLabel(r.state))}</span> <b>${esc(r.pipelineId)}</b> <code>${esc(r.runId)}</code> <span class="hint">${since}${r.launchedBy ? ` · by ${esc(r.launchedBy)}` : ""}</span>
      <button class="small ghost danger" data-needs="designer" data-cancel="${esc(r.runId)}">Stop run</button>${r.state === "running" ? awayNotes(r) : ""}</div>`;
  }).join("") : `<p class="hint">Idle.</p>`;
}

// A node of a running run whose host dropped its connection: the run is kept while the coordinator waits for the host to
// announce again, and the page says since when and until when, rather than leaving "running" unexplained.
function awayNotes(run) {
  const nodes = new Set((run.plan?.fragments || []).map((f) => f.node));
  const seconds = (from, to) => Math.max(0, Math.round((to - from) / 1000));
  return store.nodes.filter((n) => nodes.has(n.name)).flatMap((n) => (n.replicas || []).filter((r) => !r.connected && r.awaySince).map((r) =>
    `<p class="hint mismatch">Node ${esc(n.name)} lost its connection ${seconds(new Date(r.awaySince), Date.now())} s ago: the coordinator waits ${seconds(Date.now(), new Date(r.givenUpAt))} s more for its host to return before it gives the node up.</p>`)).join("");
}

function renderSelectors() {
  const f = $("r-filter");
  const ids = [...store.last.keys()].sort();
  f.innerHTML = `<option value="">every pipeline</option>` + ids.map((i) => `<option ${i === filter ? "selected" : ""}>${esc(i)}</option>`).join("");
}

function report(r) {
  const res = r.result;
  const edges = (res?.edgeCounts || []).map((e) => `<tr><td>${esc(shortNode(e.producerFragment))}.${esc(e.producerAlias)}</td><td>${esc(shortNode(e.consumerFragment))}.${esc(e.consumerAlias)}</td>
    <td class="num">${e.sent == null ? "?" : fmt(e.sent)}</td><td class="num ${e.sent === e.received && e.sent != null ? "" : "mismatch"}">${e.received == null ? "?" : fmt(e.received)}</td></tr>`).join("");
  const exits = res ? Object.values(res.reports).map((p) => `<tr><td>${esc(shortNode(p.fragment))}</td><td class="num">${p.exitCode}</td><td>${esc(p.origin ?? "")}</td><td class="wrap">${esc(p.firstFault ?? "")}</td></tr>`).join("") : "";
  return `<div class="run-report">
    ${endNotes(r)}
    ${res?.consequences?.length ? `<p><b>Consequences:</b> ${res.consequences.map((c) => esc(shortNode(c))).join(", ")}</p>` : ""}
    ${edges ? `<table><tr><th>from</th><th>to</th><th>sent</th><th>received</th></tr>${edges}</table>` : ""}
    ${exits ? `<h3>Exit reports</h3><table><tr><th>fragment</th><th>exit</th><th>origin</th><th>first fault</th></tr>${exits}</table>` : ""}
    ${cursorsOf(r)}
    ${ranPlan(r.plan)}
    ${res?.replicas && Object.keys(res.replicas).length ? `<p class="hint">Ran on: ${Object.entries(res.replicas).map(([fragment, replica]) => `${esc(shortNode(fragment))} on <code title="${esc(replica.origin ?? "")}">${esc(replica.id)}</code>`).join(", ")}</p>` : ""}
  </div>`;
}

function renderJournal() {
  const el = $("r-journal");
  if (!el) return;
  const runs = (filter ? filtered : store.runs).slice(0, 60);
  el.innerHTML = runs.length ? `<table class="journal">
    <tr><th>run</th><th>pipeline</th><th>outcome</th><th>by</th><th>started</th><th>duration</th><th title="${esc(ROWS_CROSSED_HINT)}">rows crossed</th><th>cause</th></tr>
    ${runs.map((r) => {
      const rows = rowsCrossed(r);
      return `<tr class="clickable" data-open="${esc(r.runId)}"><td><code>${esc(r.runId)}</code></td><td>${esc(r.pipelineId ?? r.planId)}</td>
        <td><span class="outcome-chip ${esc(r.state)}">${esc(runStateLabel(r.state))}</span></td><td class="wrap">${esc(r.launchedBy ?? "")}</td><td>${esc(new Date(r.startedAt).toLocaleTimeString())}</td>
        <td class="num">${esc(duration(r))}</td><td class="num">${rows ? fmt(rows) : ""}</td><td class="wrap">${esc(r.reason ?? (r.result?.cause ? shortNode(r.result.cause) : ""))}</td></tr>
        ${openRun === r.runId ? `<tr><td colspan="8">${report(r)}</td></tr>` : ""}`;
    }).join("")}</table>` : `<p class="hint">No run yet.</p>`;
}

async function onClick(e) {
  const row = e.target.closest("[data-open]");
  const b = e.target.closest("button");
  try {
    if (b?.dataset.run) { const run = await api("/api/runs", { pipelineId: b.dataset.run }); toast(`${run.runId} ${runStateLabel(run.state)}.`, "info"); }
    else if (b?.dataset.undeploy) await api(`/api/deployments/${encodeURIComponent(b.dataset.undeploy)}`, undefined, "DELETE");
    else if (b?.dataset.cancel) await api("/api/runs/cancel", { runId: b.dataset.cancel });
    else if (row) { openRun = openRun === row.dataset.open ? null : row.dataset.open; renderJournal(); }
  } catch (err) {
    toast(err.message);
  }
}
