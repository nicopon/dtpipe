// Runs: the runs in flight and the queue behind them, the journal of finished runs (filtered by pipeline and by state at the
// server, counted, paged), and the deployments that are no library pipeline's. A run opens its own page, `run.js`:
// the address `#/runs/<runId>` is this view's too, and it hands the page over.
import { $, ROWS_CROSSED_HINT, api, apiPage, causeKindChip, causeLine, duration, esc, fmt, info, nodeColor, plural, rowsCrossed, runHref, runStateLabel, store, toast } from "./core.js";
import * as runPage from "./run.js";

const PAGE = 50;       // what the journal asks for first and what Show more adds
const MAX_PAGE = 500;  // the most one request may ask for: the server's page bound
// The states a journaled run ends in, as the server writes them: a run in flight is in the queue, never in the journal.
const ENDED = ["succeeded", "failed", "cancelled", "refused", "error"];

let page = null;             // the page of one run, while the address names a run
let host = null;
let filter = { pipeline: "", state: "" };
let limit = PAGE;
let journal = { runs: [], total: 0 };
let loaded = false;          // the journal has been read once since the view opened
let asking = 0;              // the newest journal request: an older answer is dropped
let pipelines = "";          // the pipeline select's options, to redraw it only when they change
let ticker = null;

/** The query a journal request carries: the filters that are set, and how many runs the page holds. */
export function journalQuery(f, n) {
  const query = new URLSearchParams();
  if (f.pipeline) query.set("pipeline", f.pipeline);
  if (f.state) query.set("state", f.state);
  query.set("limit", String(n));
  return query.toString();
}

/** What the journal's count says: the runs the page holds of the runs that answer the filters. */
export const countLabel = (shown, total) => `${shown} of ${total}`;

/** How many runs the next page asks for, never more than the server gives. */
export const nextLimit = (n) => Math.min(n + PAGE, MAX_PAGE);

export function mount(el, params, query) {
  page = params.length ? runPage : null;
  if (page) { page.mount(el, params, query); return; }
  host = el;
  filter = { pipeline: query?.get("pipeline") ?? "", state: query?.get("state") ?? "" };
  limit = PAGE;
  journal = { runs: [], total: 0 };
  loaded = false;
  pipelines = "";
  host.innerHTML = `
    <div class="view-head"><h1>Runs</h1><p class="hint">Runs sharing a data node queue in order; others run together. ${info("A run waits until the earlier run that shares a data node ends. Runners never count. Open a run for its page.")}</p></div>
    <section class="card"><h2>In flight and queued</h2><div id="r-queue"></div></section>
    <section class="card"><div class="card-head"><h2>Journal <span id="r-count" class="hint"></span></h2>
      <div class="actions"><select id="r-filter" aria-label="Pipeline"></select><select id="r-state" aria-label="Outcome"></select></div></div>
      <div id="r-journal"></div><div id="r-more"></div></section>
    <div id="r-outside"></div>`;
  host.addEventListener("click", onClick);
  $("r-filter").addEventListener("change", (e) => { filter.pipeline = e.target.value; changed(); });
  $("r-state").addEventListener("change", (e) => { filter.state = e.target.value; changed(); });
  clearInterval(ticker);
  // Only the queue has a clock: its lines say how long a run has run or waited.
  ticker = setInterval(() => { if (!host?.isConnected) clearInterval(ticker); else if (store.queue.length) renderQueue(); }, 1000);
  renderQueue();
  renderOutside();
  renderSelectors();
  renderJournal();
  loadJournal();
}

export function leave() {
  if (page) { page.leave(); page = null; return; }
  clearInterval(ticker);
  asking++;
  host = null;
}

// Each event redraws what it touches: a node's event never rebuilds the journal or its selects.
export function update(kind) {
  if (page) { page.update(kind); return; }
  if (!host) return;
  if (kind === "all") { renderQueue(); renderOutside(); renderSelectors(); loadJournal(); }
  else if (kind === "nodes" || kind === "runs") renderQueue();
  else if (kind === "deployments") renderOutside();
  else if (kind === "journal") { renderSelectors(); loadJournal(); }
}

// A filter changed: the address says so, and the journal starts again from its first page.
function changed() {
  const query = new URLSearchParams();
  if (filter.pipeline) query.set("pipeline", filter.pipeline);
  if (filter.state) query.set("state", filter.state);
  history.replaceState(null, "", `#/runs${query.size ? `?${query}` : ""}`);
  limit = PAGE;
  loadJournal();
}

// The journal and its total are the server's: the runs that answer the filters, and how many there are before the page cuts them.
async function loadJournal() {
  const ticket = ++asking;
  try {
    const page = await apiPage(`/api/runs?${journalQuery(filter, limit)}`);
    if (ticket !== asking || !host?.isConnected) return;
    journal = { runs: page.items, total: page.total };
    loaded = true;
    renderJournal();
  } catch (err) {
    if (ticket === asking) toast(err.message);
  }
}

function renderQueue() {
  const el = $("r-queue");
  if (!el) return;
  el.innerHTML = store.queue.length ? store.queue.map((r) => {
    const since = r.state === "running" ? `${((Date.now() - new Date(r.startedAt)) / 1000).toFixed(0)} s` : `queued ${((Date.now() - new Date(r.queuedAt)) / 1000).toFixed(0)} s ago`;
    return `<div class="queue-item"><span class="outcome-chip ${esc(r.state)}">${esc(runStateLabel(r.state))}</span> <b>${esc(r.pipelineId)}</b> <a href="${esc(runHref(r.runId))}"><code>${esc(r.runId)}</code></a> <span class="hint">${since}${r.launchedBy ? ` · by ${esc(r.launchedBy)}` : ""}</span>
      <button class="small ghost danger" data-needs="designer" data-cancel="${esc(r.runId)}">Stop run</button>${r.state === "running" ? awayNotes(r) : ""}</div>`;
  }).join("") : `<p class="hint">Idle.</p>`;
}

// A node of a running run whose host dropped its connection: the run is kept while the coordinator waits for the host to
// announce again, and the page says since when and until when, rather than leaving "running" unexplained.
function awayNotes(run) {
  const nodes = new Set((run.plan?.fragments || []).map((f) => f.node));
  const seconds = (from, to) => Math.max(0, Math.round((to - from) / 1000));
  return store.nodes.filter((n) => nodes.has(n.name)).flatMap((n) => (n.replicas || []).filter((r) => !r.connected && r.awaySince).map((r) =>
    `<p class="hint mismatch">Node ${esc(n.name)} offline for ${seconds(new Date(r.awaySince), Date.now())} s: giving up in ${seconds(Date.now(), new Date(r.givenUpAt))} s.</p>`)).join("");
}

// The deployments that are no library pipeline's (a plan fixed by hand): a block that exists only while there is one.
function renderOutside() {
  const el = $("r-outside");
  if (!el) return;
  const outside = store.deployments.filter((d) => d.origin !== "library");
  el.innerHTML = outside.length ? `<section class="card"><h2>Deployments outside the library</h2>
    <p class="hint">Hand-fixed plans: not in the library, run and removed here.</p>
    <table>
    <tr><th>pipeline</th><th>from</th><th>state</th><th>fragments</th><th>edges</th><th>queued</th><th></th></tr>
    ${outside.map((d) => `<tr>
      <td>${esc(d.pipelineId)}</td>
      <td>${esc(d.origin)}</td>
      <td><span class="chip ${d.state === "ready" ? "ok" : d.state === "failed" ? "bad" : "warn"}" title="${esc(d.message || "")}">${esc(d.state)}</span>${d.commit ? ` <span class="hint mono" title="The commit it is armed at">${esc(d.commit.slice(0, 8))}</span>` : ""}</td>
      <td class="wrap">${d.fragments.length ? "" : (d.nodes || []).map((n) => `<span class="chip" style="color:${esc(nodeColor(n))}" title="Bound by this deployment; planned at each launch">${esc(n)}</span>`).join(" ")}${d.fragments.map((f) => `<span class="chip" style="color:${esc(nodeColor(f.node))}" title="${esc(f.name)} · ${esc(f.version.slice(0, 12))} · ${plural(f.replicas ?? 0, "replica")} could run it now">${esc(f.node)}${f.replicas != null ? ` · ${f.replicas}` : ""}</span>`).join(" ")}</td>
      <td class="num">${d.edges}</td><td class="num">${d.queued || ""}</td>
      <td class="actions-cell"><button class="small" data-needs="designer" data-run="${esc(d.pipelineId)}" ${d.state === "failed" ? "disabled" : ""}>Run</button>
        <button class="small ghost danger" data-needs="designer" data-undeploy="${esc(d.pipelineId)}" ${d.runningRunId || d.queued ? "disabled" : ""}>Undeploy</button></td>
    </tr>`).join("")}</table></section>` : "";
}

// The pipeline select holds the pipelines that have a journal; it is redrawn only when its options change.
function renderSelectors() {
  const f = $("r-filter");
  if (!f) return;
  const ids = [...new Set([...store.last.keys(), ...(filter.pipeline ? [filter.pipeline] : [])])].sort();
  const key = ids.join("\n");
  if (key === pipelines) return;
  pipelines = key;
  f.innerHTML = `<option value="">every pipeline</option>` + ids.map((i) => `<option ${i === filter.pipeline ? "selected" : ""}>${esc(i)}</option>`).join("");
}

// The outcome select never changes while the view is open.
function renderStates() {
  const s = $("r-state");
  if (s && !s.innerHTML) s.innerHTML = `<option value="">every outcome</option>` + ENDED.map((w) => `<option value="${w}" ${w === filter.state ? "selected" : ""}>${esc(runStateLabel(w))}</option>`).join("");
}

function renderJournal() {
  const el = $("r-journal");
  if (!el) return;
  renderStates();
  const { runs, total } = journal;
  $("r-count").textContent = loaded ? countLabel(runs.length, total) : "";
  el.innerHTML = runs.length ? `<table class="journal">
    <tr><th>run</th><th>pipeline</th><th>outcome</th><th>by</th><th>started</th><th>duration</th><th title="${esc(ROWS_CROSSED_HINT)}">rows moved</th><th>cause</th></tr>
    ${runs.map((r) => {
      const rows = rowsCrossed(r);
      const href = esc(runHref(r.runId));
      return `<tr class="clickable" data-href="${href}"><td><a href="${href}"><code>${esc(r.runId)}</code></a></td><td>${esc(r.pipelineId ?? r.planId)}</td>
        <td><span class="outcome-chip ${esc(r.state)}">${esc(runStateLabel(r.state))}</span></td><td class="wrap">${esc(r.launchedBy ?? "")}</td><td title="${esc(r.startedAt)}">${esc(new Date(r.startedAt).toLocaleString())}</td>
        <td class="num">${esc(duration(r))}</td><td class="num">${rows ? fmt(rows) : ""}</td><td class="wrap">${causeKindChip(r)}${esc(causeLine(r))}</td></tr>`;
    }).join("")}</table>` : `<p class="hint">${!loaded ? "Reading the journal…" : filter.pipeline || filter.state ? "No run answers these filters." : "No run yet."}</p>`;
  $("r-more").innerHTML = runs.length >= total ? ""
    : limit < MAX_PAGE ? `<p><button id="r-show-more" class="ghost small">Show more</button></p>`
      : `<p class="hint">Latest ${MAX_PAGE} runs: filter to reach others.</p>`;
}

async function onClick(e) {
  const b = e.target.closest("button");
  const row = e.target.closest("tr[data-href]");
  try {
    if (b?.id === "r-show-more") { limit = nextLimit(limit); await loadJournal(); }
    else if (b?.dataset.run) { const run = await api("/api/runs", { pipelineId: b.dataset.run }); toast(`${run.runId} ${runStateLabel(run.state)}.`, "info"); }
    else if (b?.dataset.undeploy) await api(`/api/deployments/${encodeURIComponent(b.dataset.undeploy)}`, undefined, "DELETE");
    else if (b?.dataset.cancel) await api("/api/runs/cancel", { runId: b.dataset.cancel });
    else if (row && !e.target.closest("a")) location.hash = row.dataset.href;
  } catch (err) {
    toast(err.message);
  }
}
