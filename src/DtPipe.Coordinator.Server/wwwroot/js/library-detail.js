// A library pipeline's page: the plan a launch would make now, which the coordinator computes with no help from the user (every
// brick on its node, every other step on a runner it picks at the launch), drawn in lanes with its actions beside it, its refusals,
// its fragments' jobs, the job's text read only, its latest runs and its versions. The node of each source and sink branch is the
// job's, shown and never chosen here; the runner is never named. Each brick's mode is chosen in its own box of the diagram; Deploy
// saves the modes as bindings and arms the pipeline at the commit the library holds, and the pipeline is then run from here. This
// page is the one place that deploys.
import { $, ROWS_CROSSED_HINT, api, apiPage, deploymentOf, duration, esc, fmt, isActive, lastRunOf, rowsCrossed, runHref, runStateLabel, shortNode, store, toast } from "./core.js";
import { crumbs } from "./crumbs.js";
import { deploymentChip, statusChip } from "./pipeline-status.js";
import { drawLanes, fragmentJobs, legendFor, planProblems } from "./placement.js";

let host = null;
let id = null;
let doc = null;
let plan = null;
let armable = false;     // whether Deploy can make the plan deployable: the server decides, never the page
let runner = null;       // the runner a launch would use now, if one passes
let edgeProtections = []; // the server's judgment of each edge of the plan, drawn and never made here; empty when the answer has none
let brickOfUnit = {};
let offers = {};          // alias -> the nodes that offer the brick of that branch
let draft = {};           // alias -> the node and mode drafted on the page, saved or not
let busy = false;
let journaled = null;     // the latest runs of this pipeline that are over, as the server journals them, once read
let journalError = "";    // what the read of them said when it failed
let asking = 0;           // the newest read of those runs: an older answer is dropped
let versionsOpen = false; // the Versions section is unfolded; its commits are read when it opens, never at a drawing
let commits = null;       // the pipeline's commits, newest first, once read
let versionsError = "";
let shown = null;         // { commit, yaml, diff }: the commit being looked at
let reading = 0;          // the newest read of the commits
let showing = 0;          // the newest read of one commit
// The reads of the job and of its plan share one ticket. `mount`, `leave` and each `load` take the next; a read of the plan alone,
// or of the job after a Deploy, keeps the one it found when it began, so a page left or a `load` begun after it retires it and it never
// retires a `load`. A read that is not its page's newest writes nothing and shows nothing, whatever its answer.
let reads = 0;
// The page on screen: `mount` and `leave` alone take the next. A click keeps the one it began on, and after an `await` draws on the
// page, and clears its `busy`, only while it is still the page's. `reads` cannot say it: every `load` takes one, the `load` that a
// save or a Deploy of the click's own makes included.
let visit = 0;

export function mount(el, params) {
  host = el;
  id = params[0];
  reads++; visit++;
  busy = false;
  doc = null; plan = null; edgeProtections = [];
  draft = {}; taken = sorted(draft);
  journaled = null; journalError = "";
  versionsOpen = false; commits = null; versionsError = ""; shown = null;
  host.addEventListener("click", onClick);
  host.addEventListener("change", onChange);
  load();
  loadRecent();
}

// An answer in flight when the page is left is dropped by its ticket, and `render` leaves a page with no host alone.
export function leave() {
  reads++; asking++; reading++; showing++; visit++;
  host = null;
}

export function update(kind) {
  if (kind === "library") load();
  // A change of the rights can change the plan (its groups, what the matrix allows between them), never the bindings drafted here.
  else if (kind === "rights" && doc) loadPlan(id, reads).then((kept) => kept && render(), (e) => toast(e.message));
  // A run that is over is in the journal: the runs listed are read again then, and drawn from the queue meanwhile.
  else if (kind === "journal") loadRecent();
  else if (["deployments", "runs", "nodes", "all", "health"].includes(kind)) render();
}

async function load() {
  const pipeline = id;
  const mine = ++reads;
  try {
    const read = await api(`/api/library/${encodeURIComponent(pipeline)}`);
    if (mine !== reads) return;
    doc = read;
    if (!(await loadPlan(pipeline, mine))) return;
    takeBindings();
  } catch (e) {
    if (mine === reads && host) host.innerHTML = `<p class="mismatch">${esc(e.message)}</p>`;
    return;
  }
  // A save adds a commit: an unfolded section reads them again.
  if (versionsOpen) await readCommits(++reading);
  if (mine === reads) render();
}

// Reads the plan of `pipeline` for the read that began at ticket `mine`. True when the plan is kept; false when that read is no
// longer its page's newest, whether it answered or failed.
async function loadPlan(pipeline, mine) {
  if (mine !== reads) return false;
  let answer;
  try {
    answer = await api(`/api/library/${encodeURIComponent(pipeline)}/plan`);
  } catch (e) {
    if (mine !== reads) return false;
    throw e;
  }
  if (mine !== reads) return false;
  plan = answer.plan;
  armable = !!answer.armable;
  runner = answer.runner;
  // A coordinator that predates the field, a plan it refuses and a plan with no edge all say nothing here.
  edgeProtections = Array.isArray(answer.edgeProtections) ? answer.edgeProtections : [];
  brickOfUnit = answer.bricks || {};
  offers = answer.offers || {};
  return true;
}

// A brick's node is the job's (`input: brick:<node>/<brick>`): the page shows it and never chooses it. The mode, how the brick may
// change after it was checked, is chosen in the Deploy step and saved with it. A strict binding's brick is resolved by the server from
// the job's text; one in another mode keeps its brick by id, sent back while the node is the same. A pin is the server's: a check writes it.
const MODES = [
  ["strict", "The branch must stay word for word the brick it was checked with"],
  ["contract", "The brick may change while the schema it produces or accepts stays the same"],
  ["compatible", "The brick may change while its consumers still work"],
  ["tolerant", "The brick may change as it likes"],
];
const draftOf = (bindings) => Object.fromEntries(Object.entries(bindings || {}).map(([alias, b]) => [alias, { node: b.node, mode: b.mode || "strict" }]));

const sorted = (o) => JSON.stringify(Object.entries(o).filter(([, v]) => v?.node).map(([a, v]) => [a, v.node, v.mode]).sort(([a], [b]) => (a < b ? -1 : 1)));
const bindingsDirty = () => sorted(draft) !== sorted(draftOf(doc.bindings));

// What the draft was last taken from, as `sorted` reads it. The draft is the person's own while it differs from that: a read of the
// job leaves it be, and takes the server's bindings otherwise. A save makes the draft what the server keeps, so the read it
// provokes takes the server's, whichever read answers last.
let taken = sorted({});
function takeBindings() {
  const server = draftOf(doc.bindings);
  if (sorted(draft) === taken) draft = server;
  taken = sorted(server);
}

// What the server keeps of a draft binding: its node and mode, and for a non-strict mode the brick it already holds on that node.
function bindingOf(alias, { node, mode }) {
  const kept = doc.bindings?.[alias];
  const brick = mode !== "strict" && kept?.node === node ? kept.brick : undefined;
  return { node, mode, ...(brick ? { brick } : {}) };
}


// A brick's mode, chosen in its own box of the diagram; the tooltip says what the mode allows and what the last check pinned.
// A mode that differs from the one the server keeps is marked until Deploy saves it.
function modeControl(alias) {
  if (!offers[alias]) return "";
  const mode = draft[alias]?.mode || "strict";
  const kept = doc.bindings?.[alias]?.mode || "strict";
  const pin = doc.bindings?.[alias]?.pin;
  const why = MODES.find(([m]) => m === mode)[1];
  const title = `${why}. ${pin ? `Checked at brick ${pin.version.slice(0, 12)}.` : "No check has pinned this brick yet."}`;
  const options = MODES.map(([m, hint]) => `<option value="${m}" title="${esc(hint)}" ${m === mode ? "selected" : ""}>${m}</option>`).join("");
  const problems = (doc.bindingProblems || []).filter((p) => p.alias === alias).map((p) => `<div class="mismatch">${esc(p.message)}</div>`).join("");
  return `<div class="unit-mode ${mode !== kept ? "changed" : ""}"><label>mode <select data-needs="designer" data-mode-alias="${esc(alias)}" title="${esc(title)}" ${busy ? "disabled" : ""}>${options}</select></label>${problems}</div>`;
}
// A binding the server keeps for an alias no branch of the job has: shown so it can be dropped.
function strayBindings() {
  const stray = Object.keys(draft).filter((a) => !offers[a]).sort();
  if (!stray.length) return "";
  const problems = doc.bindingProblems || [];
  const rows = stray.map((a) => `<li><span class="mono">${esc(a)}</span> binding kept for no brick branch of the job. ${problems.filter((p) => p.alias === a).map((p) => esc(p.message)).join(" ")}
    <button class="small ghost" data-needs="designer" data-drop="${esc(a)}" ${busy ? "disabled" : ""}>Remove</button></li>`).join("");
  return `<ul class="msgs">${rows}</ul>`;
}
function onChange(e) {
  const alias = e.target?.dataset?.modeAlias;
  if (alias === undefined || !offers[alias]) return;
  const mode = e.target.value;
  // A mode that is strict on a brick the library holds no binding for stays absent: nothing is saved for a brick never moved off strict.
  if (mode === "strict" && !doc.bindings?.[alias]) delete draft[alias];
  else draft[alias] = { node: offers[alias][0], mode };
  render();
}

function render() {
  if (!host || !doc || !plan) return;
  const deployment = deploymentOf(id);
  // A deployment runs the job it was armed at: it is this version when it is armed at the commit the library holds.
  const current = deployment && deployment.commit === doc.commit;
  const run = lastRunOf(id);
  const running = run?.state === "running";
  // Fragment names are stable per pipeline and node: the run's edges apply when they are the plan's.
  const edgeKey = (e) => `${e.producerFragment}|${e.producerAlias}|${e.consumerFragment}|${e.consumerAlias}`;
  const planEdges = new Set(plan.edges.map(edgeKey));
  const runMatches = !!run?.result && run.result.edgeCounts.length === planEdges.size && run.result.edgeCounts.every((e) => planEdges.has(edgeKey(e)));
  const canDeploy = armable && !isActive(run);
  const canRun = !!deployment && deployment.state !== "failed";
  const dirty = bindingsDirty();
  const upToDate = canRun && current && !dirty;
  const deployTitle = !armable ? "The plan cannot be deployed now"
    : isActive(run) ? "A run of this pipeline is queued or running"
    : dirty ? "Saves the modes, then deploys the version the library holds"
    : "Deploys the version the library holds";
  const entry = { ...doc, id };
  const hasBricks = Object.keys(offers).length > 0;
  host.innerHTML = `
    <div class="view-head">
      ${crumbs([{ html: "Library", href: "#/library" }, { html: `${esc(doc.yaml.split("\n")[0].replace(/^#\s*/, ""))} <span class="mono muted">${esc(id)}</span>` }])}
      <p class="hint">Planned at each launch: every brick on the node it is bound to, every other step on a runner chosen then. A source wired straight into an output crosses no runner. A launch of the version the library holds ${plan.deployable ? `would run${runner ? ` on ${esc(runner)}` : ""} now` : armable ? "would be refused until Deploy pins the bricks no check has pinned" : "would be refused now"}.</p>
    </div>
    ${planProblems(plan).length ? `<section class="card"><ul class="msgs errors">${planProblems(plan).map((e) => `<li>${esc(e)}</li>`).join("")}</ul></section>` : ""}
    ${missingEdgesCard()}
    <section class="card">
      <div class="card-head"><div class="card-title"><h2>Fragments and edges</h2>${deploymentChip(entry)} ${statusChip(entry)}</div>
        <span class="actions">
          ${dirty ? `<span class="chip warn" title="Deploy saves the modes you changed">modes changed</span>
          <button data-needs="designer" id="x-modes-reset" class="ghost" ${busy ? "disabled" : ""}>Reset</button>` : ""}
          ${deployment ? `<button data-needs="designer" id="x-undeploy" class="ghost danger" ${isActive(run) || busy ? "disabled" : ""}>Undeploy</button>` : ""}
          <button data-needs="designer" id="x-deploy" class="${upToDate ? "" : "primary"}" ${canDeploy && !busy ? "" : "disabled"} title="${esc(deployTitle)}">Deploy</button>
          <button data-needs="designer" id="x-run" class="${upToDate ? "primary" : ""}" ${canRun && !busy ? "" : "disabled"}>Run</button>
        </span></div>
      <div class="card-sub">${hasBricks ? `<span class="hint">A brick's mode says how it may change after it was checked; Deploy saves the modes.</span>` : "<span></span>"}${legendFor(plan, edgeProtections)}</div>
      <div class="lanes-head"><span>sources</span><span>runner</span><span>sinks</span></div>
      <div class="lanes" id="x-lanes"></div>
      ${strayBindings()}
      ${run?.result && !runMatches ? `<p class="hint">The last run's row counts are not drawn on the edges. It ran a plan with other edges than this one.</p>` : ""}
    </section>
    ${recentRunsCard()}
    <section class="card"><h2>Fragment jobs</h2>${fragmentJobs(plan)}</section>
    <section class="card"><div class="card-head"><h2>Job</h2><span class="hint">Read only: write it with <code>dtpipe studio</code> and publish it.</span></div>
    <pre id="x-job">${esc(doc.yaml)}</pre></section>
    ${versionsCard()}`;
  drawLanes($("x-lanes"), plan, brickOfUnit, { run, running, current: runMatches, extra: modeControl, edgeProtections });
}
// The edges between groups the plan needs and the flow matrix does not allow, read from the plan's refused edges: each group of
// the sender to each group of the receiver. An administrator allows them here, beside what the matrix allows already; anyone
// else is told who can.
function missingEdges() {
  const reaches = (from, to) => (store.matrix[from] || []).some((t) => t === "*" || t.toLowerCase() === to.toLowerCase());
  const allowed = (from, to) => reaches(from, to) || reaches("*", to);
  const pairs = plan.edges.filter((e) => !e.allowed).flatMap((e) => e.producerGroups.flatMap((from) => e.consumerGroups.map((to) => ({ from, to }))));
  const seen = new Set();
  return pairs.filter(({ from, to }) => !allowed(from, to) && !seen.has(`${from}\n${to}`) && seen.add(`${from}\n${to}`));
}

function missingEdgesCard() {
  const edges = missingEdges();
  if (!edges.length) return "";
  const admin = store.me?.roles?.includes("admin");
  return `<section class="card"><div class="card-head"><h2>Missing in the flow matrix</h2>
      ${admin ? `<span class="actions"><button id="x-allow-edges" class="primary" ${busy ? "disabled" : ""}>Allow ${edges.length > 1 ? "these edges" : "this edge"}</button></span>` : ""}</div>
    <ul class="msgs">${edges.map((e) => `<li><span class="mono">${esc(e.from)}</span> → <span class="mono">${esc(e.to)}</span></li>`).join("")}</ul>
    <p class="hint">${admin ? "Allowing adds these edges to the matrix and revokes nothing; the change is audited in Access." : "An administrator allows them, here or in Access."}</p></section>`;
}


const RECENT = 5;

/** The runs the page lists: those in flight or queued, in the queue's order, then the latest that are over; at most five. */
export function recentRuns(inFlight, over) {
  const seen = new Set();
  return [...inFlight, ...(over ?? [])].filter((r) => !seen.has(r.runId) && seen.add(r.runId)).slice(0, RECENT);
}

// The journal is the server's: its latest runs of this pipeline are read when the page opens and when a run ends.
async function loadRecent() {
  const mine = ++asking;
  try {
    const answer = await apiPage(`/api/runs?pipeline=${encodeURIComponent(id)}&limit=${RECENT}`);
    if (mine !== asking) return;
    journaled = answer.items;
    journalError = "";
  } catch (err) {
    if (mine !== asking) return;
    journalError = err.message;
  }
  render();
}

function recentRow(r) {
  const rows = rowsCrossed(r);
  const at = r.state === "queued" ? r.queuedAt : r.startedAt;
  const href = esc(runHref(r.runId));
  return `<tr><td><a href="${href}"><code>${esc(r.runId)}</code></a></td>
    <td><span class="outcome-chip ${esc(r.state)}">${esc(runStateLabel(r.state))}</span></td>
    <td title="${esc(at ?? "")}">${at ? esc(new Date(at).toLocaleString()) : ""}</td>
    <td class="num">${esc(duration(r))}</td><td class="num">${rows ? fmt(rows) : ""}</td>
    <td class="wrap">${esc(r.reason ?? (r.result?.cause ? shortNode(r.result.cause) : ""))}</td></tr>`;
}

// The latest runs of this pipeline, each linked to its page, which holds the report.
function recentRunsCard() {
  const rows = recentRuns(store.queue.filter((r) => r.pipelineId === id), journaled);
  const waiting = journaled === null && !journalError;
  return `<section class="card"><div class="card-head"><h2>Recent runs</h2><a href="#/runs?pipeline=${esc(encodeURIComponent(id))}">all runs →</a></div>
    ${rows.length ? `<table class="journal">
      <tr><th>run</th><th>outcome</th><th>started</th><th>duration</th><th title="${esc(ROWS_CROSSED_HINT)}">rows moved</th><th>cause</th></tr>
      ${rows.map(recentRow).join("")}</table>` : `<p class="hint">${waiting ? "Reading the runs…" : "No run yet."}</p>`}
    ${journalError ? `<p class="mismatch">${esc(journalError)}</p>` : ""}</section>`;
}

function diffHtml(diff) {
  return esc(diff || "(no change to this pipeline)").split("\n").map((l) =>
    l.startsWith("+") && !l.startsWith("+++") ? `<span class="add">${l}</span>` : l.startsWith("-") && !l.startsWith("---") ? `<span class="del">${l}</span>` : l).join("\n");
}

// The commits of the pipeline: what a commit changed and the job it left, and restoring one, which writes a new commit.
function versionsCard() {
  const list = (commits || []).map((c) => `
    <div class="commit ${shown?.commit === c.hash ? "active" : ""}">
      <code>${esc(c.hash.slice(0, 8))}</code> <span>${esc(c.message)}</span> <span class="hint">${esc(new Date(c.date).toLocaleString())}</span>
      <span class="actions"><button class="small ghost" data-show="${esc(c.hash)}">show</button>
      <button class="small ghost" data-needs="designer" data-restore="${esc(c.hash)}" ${busy ? "disabled" : ""}>restore</button></span>
    </div>`).join("");
  const panel = !versionsOpen ? ""
    : versionsError ? `<p class="mismatch">${esc(versionsError)}</p>`
    : commits === null ? `<p class="hint">Reading the versions…</p>`
    : `<div class="history-panel"><div>${list || `<p class="hint">No commit.</p>`}</div>
      ${shown ? `<div class="grid2"><div><h3>Change</h3><pre class="diff">${diffHtml(shown.diff)}</pre></div><div><h3>Job at ${esc(shown.commit.slice(0, 8))}</h3><pre>${esc(shown.yaml)}</pre></div></div>` : ""}</div>`;
  return `<section class="card"><div class="card-head"><h2>Versions</h2>
      <span class="actions"><button id="x-versions" class="small ghost">${versionsOpen ? "Hide versions" : "Show versions"}</button></span></div>
    <p class="hint">Each save is a commit: show what it changed and the job it left, or restore it.</p>
    ${panel}</section>`;
}

async function readCommits(mine) {
  try {
    const list = await api(`/api/library/${encodeURIComponent(id)}/history`);
    if (mine !== reading) return;
    commits = list;
    versionsError = "";
  } catch (err) {
    if (mine !== reading) return;
    versionsError = err.message;
  }
  render();
}

// Unfolding and looking at a commit only read: they leave the page as it is, and draw the section again.
async function onVersions(b) {
  try {
    if (b.id === "x-versions") {
      versionsOpen = !versionsOpen;
      shown = null;
      versionsError = "";
      commits = null;
      const mine = ++reading;
      render();
      if (versionsOpen) await readCommits(mine);
    } else {
      const commit = b.dataset.show;
      const mine = ++showing;
      const at = await api(`/api/library/${encodeURIComponent(id)}/at/${encodeURIComponent(commit)}`);
      if (mine !== showing) return;
      shown = { commit, ...at };
      render();
    }
  } catch (err) {
    toast(err.message);
  }
}

// The bindings drafted, as the server keeps them: saved with a check that writes the pins, then read again.
async function saveBindings(pipeline, page) {
  const bindings = Object.fromEntries(Object.entries(draft).filter(([, v]) => v?.node).map(([alias, v]) => [alias, bindingOf(alias, v)]));
  const saved = await api(`/api/library/${encodeURIComponent(pipeline)}/bindings`, { bindings, message: `Bind ${pipeline}` }, "PUT");
  if (page === visit) {
    taken = sorted(draft);
    await load();
  }
  return saved;
}

async function onClick(e) {
  const b = e.target.closest("button");
  if (!b || b.disabled) return;
  if (b.id === "x-versions" || b.dataset.show) return onVersions(b);
  if (b.id === "x-modes-reset") {
    draft = draftOf(doc.bindings);
    taken = sorted(draft);
    render();
    return;
  }
  // What a click does after its own request names the pipeline, the ticket and the page it began with: the request goes to the
  // pipeline it was made on, and the page is drawn on only if it is still that page's.
  const pipeline = id, mine = reads, page = visit;
  busy = true;
  render();
  try {
    if (b.dataset.drop) {
      delete draft[b.dataset.drop];
      const saved = await saveBindings(pipeline, page);
      toast(saved.problems?.length ? "Binding removed: some others do not hold yet." : "Binding removed.", "info");
    } else if (b.id === "x-deploy") {
      // The modes are saved first, so the deployment is armed with what the Deploy step showed: the save checks them and writes their pins.
      if (bindingsDirty()) await saveBindings(pipeline, page);
      toast(`Deploying ${pipeline}…`, "info");
      await api(`/api/library/${encodeURIComponent(pipeline)}/deploy`, {});
      toast(`${pipeline} deployed: every fragment registered.`, "info");
      const read = await api(`/api/library/${encodeURIComponent(pipeline)}`);
      if (mine === reads) doc = read;
    } else if (b.id === "x-allow-edges") {
      const edges = missingEdges();
      await api("/api/rights/matrix/edges", { edges });
      toast(`Allowed in the flow matrix: ${edges.map((e) => `${e.from} → ${e.to}`).join(", ")}.`, "info");
      await loadPlan(pipeline, mine);
    } else if (b.id === "x-run") {
      const run = await api("/api/runs", { pipelineId: pipeline });
      toast(`${run.runId} ${runStateLabel(run.state)}.`, "info");
    } else if (b.id === "x-undeploy") {
      await api(`/api/deployments/${encodeURIComponent(pipeline)}`, undefined, "DELETE");
    } else if (b.dataset.restore) {
      const commit = b.dataset.restore;
      const at = await api(`/api/library/${encodeURIComponent(pipeline)}/at/${encodeURIComponent(commit)}`);
      await api(`/api/library/${encodeURIComponent(pipeline)}`, { yaml: at.yaml, message: `Restore ${commit.slice(0, 8)}` }, "PUT");
      toast(`${pipeline} restored to ${commit.slice(0, 8)}.`, "info");
      if (page === visit) {
        shown = null;
        await load();
      }
    }
  } catch (err) {
    toast(err.message);
    // A refused Deploy is read again: the plan then shows what the matrix lacks now.
    if (b.id === "x-deploy") await loadPlan(pipeline, mine).catch(() => {});
  }
  // A click that ends on another page leaves that page's `busy` and drawing to it.
  if (page !== visit) return;
  busy = false;
  render();
}
