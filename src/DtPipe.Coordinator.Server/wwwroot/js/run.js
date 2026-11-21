// A run's own page, `#/runs/<runId>`: its outcome and report, the pipeline it ran, and the lines of the stream that name it.
// A run in flight is the queue's and follows it to its end; a run that is over is the server's, read once.
import { $, api, cursorsOf, duration, endNotes, esc, fmt, isActive, linesOfRun, protectionOf, protectionWord, ranPlan, runStateLabel, shortNode, store, toast } from "./core.js";
import { crumbs } from "./crumbs.js";
import { lineHtml } from "./events.js";

let host = null;
let id = "";
let run = null;             // the run, once found
let missing = false;        // the server holds no run of this id
let failure = "";           // what a read that was not a 404 said
let ticket = 0;             // the newest read: an older answer is dropped
let timer = null;

export function mount(el, params) {
  host = el;
  id = params[0] ?? "";
  run = null;
  missing = false;
  failure = "";
  host.innerHTML = `
    <div id="run-head" class="view-head"></div>
    <section class="card"><div id="run-report"></div></section>
    <section class="card"><div class="card-head"><h2>Events <span id="run-ev-count" class="hint"></span></h2><a href="#/events">all events →</a></div>
      <p class="hint">The lines this page holds that name this run, as its nodes wrote them: the ones that arrived while the page was open and those the stream replayed when it connected. A line that names no run is in Events only.</p>
      <div id="run-log" class="log mono"></div></section>`;
  host.addEventListener("click", onClick);
  drawHead();
  drawReport();
  drawEvents(true);
  load();
}

export function leave() {
  clearTimeout(timer);
  ticket++;
  host = null;
}

export function update(kind) {
  if (!host) return;
  if (kind === "logs") { clearTimeout(timer); timer = setTimeout(() => drawEvents(false), 200); }
  // A run in flight changes with the queue and, once over, is read from the server; a run that is over changes no more.
  else if (kind === "all" || (kind === "runs" && (!run || isActive(run)))) load();
}

async function load() {
  const mine = ++ticket;
  const live = store.queue.find((r) => r.runId === id);
  if (live) {
    run = live;
    missing = false;
    failure = "";
  } else {
    try {
      const found = await api(`/api/runs/${encodeURIComponent(id)}`);
      if (mine !== ticket) return;
      run = found;
      missing = false;
      failure = "";
    } catch (err) {
      if (mine !== ticket) return;
      if (err.status === 404) { run = null; missing = true; } else failure = err.message;
    }
  }
  drawHead();
  drawReport();
}

function drawHead() {
  const el = $("run-head");
  if (!el) return;
  if (!run) {
    el.innerHTML = crumbs([{ html: "Runs", href: "#/runs" }, { html: `Run <code>${esc(id)}</code>` }]);
    return;
  }
  // A run of a library pipeline is under it in the path; a run of a plan fixed by hand belongs to no pipeline.
  const path = run.pipelineId
    ? [{ html: "Library", href: "#/library" }, { html: `<span class="mono">${esc(run.pipelineId)}</span>`, href: `#/library/${esc(encodeURIComponent(run.pipelineId))}` }]
    : [{ html: "Runs", href: "#/runs" }];
  const facts = [
    run.pipelineId ? "" : `plan ${esc(run.planId)}`,
    run.launchedBy ? `by ${esc(run.launchedBy)}` : "",
    run.queuedAt ? `queued ${esc(new Date(run.queuedAt).toLocaleString())}` : "",
    run.state === "queued" ? "" : `started ${esc(new Date(run.startedAt).toLocaleString())}`,
    run.finishedAt ? `ended ${esc(new Date(run.finishedAt).toLocaleString())} · ${esc(duration(run))}` : "",
  ].filter(Boolean).join(" · ");
  el.innerHTML = `${crumbs([...path, { html: `Run <code>${esc(run.runId)}</code>` }], `<span class="outcome-chip ${esc(run.state)}">${esc(runStateLabel(run.state))}</span>`)}
    <p class="hint">${facts}</p>
    ${isActive(run) ? `<div class="actions"><button class="small ghost danger" data-needs="designer" data-cancel="${esc(run.runId)}">Stop run</button></div>` : ""}`;
}

function drawReport() {
  const el = $("run-report");
  if (!el) return;
  if (failure) el.innerHTML = `<p class="mismatch">${esc(failure)}</p>`;
  else if (missing) el.innerHTML = `<h2>No such run</h2><p>The coordinator holds no run <code>${esc(id)}</code>: it keeps the latest runs of each pipeline, so an older one is gone, or the id is wrong. <a href="#/runs">Back to the runs</a></p>`;
  else if (!run) el.innerHTML = `<p class="hint">Reading the run…</p>`;
  else el.innerHTML = report(run);
}

// What an end of an edge reported it applied, in the words its own client gave: a word per end, "unknown" for an end that reported none.
// The page draws no verdict from the two; the run's cause is the server's.
const protectionCell = (level) => `<td${protectionOf(level) ? "" : ` class="muted"`}>${esc(protectionWord(level))}</td>`;
const PROTECTION_HINT = "What this end's client reported it applied to the edge's transfer. Unknown: the end reported nothing.";

function report(r) {
  const res = r.result;
  const edges = (res?.edgeCounts || []).map((e) => `<tr><td>${esc(shortNode(e.producerFragment))}.${esc(e.producerAlias)}</td><td>${esc(shortNode(e.consumerFragment))}.${esc(e.consumerAlias)}</td>
    <td class="num">${e.sent == null ? "?" : fmt(e.sent)}</td><td class="num ${e.sent === e.received && e.sent != null ? "" : "mismatch"}">${e.received == null ? "?" : fmt(e.received)}</td>${protectionCell(e.producerProtection)}${protectionCell(e.consumerProtection)}</tr>`).join("");
  const exits = res ? Object.values(res.reports).map((p) => `<tr><td>${esc(shortNode(p.fragment))}</td><td class="num">${p.exitCode}</td><td>${esc(p.origin ?? "")}</td><td class="wrap">${esc(p.firstFault ?? "")}</td></tr>`).join("") : "";
  return `<div class="run-report">
    ${isActive(r) ? `<p class="hint">The run is ${esc(runStateLabel(r.state))}: its report appears here when it ends.</p>` : ""}
    ${endNotes(r)}
    ${res?.consequences?.length ? `<p><b>Consequences:</b> ${res.consequences.map((c) => esc(shortNode(c))).join(", ")}</p>` : ""}
    ${edges ? `<table><tr><th>from</th><th>to</th><th>sent</th><th>received</th><th title="${esc(PROTECTION_HINT)}">from end</th><th title="${esc(PROTECTION_HINT)}">to end</th></tr>${edges}</table>` : ""}
    ${exits ? `<h3>Exit reports</h3><table><tr><th>fragment</th><th>exit</th><th>origin</th><th>first fault</th></tr>${exits}</table>` : ""}
    ${cursorsOf(r)}
    ${ranPlan(r.plan)}
    ${res?.replicas && Object.keys(res.replicas).length ? `<p class="hint">Ran on: ${Object.entries(res.replicas).map(([fragment, replica]) => `${esc(shortNode(fragment))} on <code title="${esc(replica.origin ?? "")}">${esc(replica.id)}</code>`).join(", ")}</p>` : ""}
  </div>`;
}

// `force` puts the box at its newest line; otherwise it follows only when it already is there.
function drawEvents(force) {
  const el = $("run-log");
  if (!el) return;
  const lines = linesOfRun(id);
  const stick = force || el.scrollTop + el.clientHeight >= el.scrollHeight - 20;
  el.innerHTML = lines.map((l) => lineHtml(l, false)).join("");
  $("run-ev-count").textContent = `${lines.length} ${lines.length === 1 ? "line" : "lines"}`;
  if (stick) el.scrollTop = el.scrollHeight;
}

async function onClick(e) {
  const b = e.target.closest("button");
  try {
    if (b?.dataset.cancel) await api("/api/runs/cancel", { runId: b.dataset.cancel });
  } catch (err) {
    toast(err.message);
  }
}
