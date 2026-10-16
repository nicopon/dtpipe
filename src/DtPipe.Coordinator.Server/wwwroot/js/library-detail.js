// A library pipeline's page: the plan a launch would make now, which the coordinator computes with no help from the user (every
// brick on its node, every other step on a runner it picks at the launch), drawn in lanes, its refusals, its fragments'
// jobs, the job's text read only, and its last run with the plan that run ran. The node of each source and sink branch is
// chosen here, among the nodes that offer its brick; the runner is never named. The bindings are saved from here, and the
// pipeline is deployed (armed at the commit the library holds) and run.
import { $, api, deploymentOf, duration, esc, isActive, lastRunOf, ranPlan, shortNode, toast } from "./core.js";
import { drawLanes, fragmentJobs, legend, planProblems } from "./placement.js";

let host = null;
let id = null;
let doc = null;
let plan = null;
let runner = null;       // the runner a launch would use now, if one passes
let brickOfUnit = {};
let offers = {};          // alias -> the nodes that offer the brick of that branch
let draft = {};           // alias -> the node chosen on the page, saved or not
let busy = false;

export function mount(el, params) {
  host = el;
  id = params[0];
  doc = null; plan = null;
  host.addEventListener("click", onClick);
  host.addEventListener("change", onChange);
  load();
}

export function update(kind) {
  if (kind === "library") load();
  else if (["deployments", "runs", "nodes", "all"].includes(kind)) render();
}

async function load() {
  try {
    doc = await api(`/api/library/${encodeURIComponent(id)}`);
    const answer = await api(`/api/library/${encodeURIComponent(id)}/plan`);
    plan = answer.plan;
    runner = answer.runner;
    brickOfUnit = answer.bricks || {};
    offers = answer.offers || {};
    draft = nodesOf(doc.bindings);
  } catch (e) {
    host.innerHTML = `<p class="mismatch">${esc(e.message)}</p>`;
    return;
  }
  render();
}

// A binding is kept with its brick and mode; the page chooses only the node, and the server resolves the brick.
const nodesOf = (bindings) => Object.fromEntries(Object.entries(bindings || {}).map(([alias, b]) => [alias, b.node]));

const sorted = (o) => JSON.stringify(Object.entries(o).filter(([, v]) => v).sort(([a], [b]) => (a < b ? -1 : 1)));
const bindingsDirty = () => sorted(draft) !== sorted(nodesOf(doc.bindings));

function bindingsChip() {
  const problems = doc.bindingProblems || [];
  if (!problems.length) return "";
  const blocking = problems.some((p) => p.blocking);
  return `<span class="chip ${blocking ? "bad" : "warn"}" title="${esc(problems.map((p) => p.message).join(" "))}>bindings: ${problems.length} problem${problems.length > 1 ? "s" : ""}</span>`;
}

// One row per brick branch of the job, and one per binding kept for a branch that is none, so it can be dropped.
function bindingsCard() {
  const aliases = Object.keys(offers).sort();
  const stray = Object.keys(draft).filter((a) => !offers[a]).sort();
  if (!aliases.length && !stray.length) return "";
  const problems = doc.bindingProblems || [];
  const problemOf = (a) => problems.filter((p) => p.alias === a).map((p) => `<div class="mismatch">${esc(p.message)}</div>`).join("");
  const rows = aliases.map((a) => {
    const chosen = draft[a] || "";
    const nodes = offers[a].includes(chosen) || !chosen ? offers[a] : [...offers[a], chosen];
    return `<tr><td class="mono">${esc(a)}</td><td>
      <select data-alias="${esc(a)}" ${busy ? "disabled" : ""}>
        <option value="" ${chosen ? "" : "selected"}>choose a node</option>
        ${nodes.map((n) => `<option value="${esc(n)}" ${n === chosen ? "selected" : ""}>${esc(n)}${offers[a].includes(n) ? "" : " (does not offer it)"}</option>`).join("")}
      </select>${problemOf(a)}</td></tr>`;
  }).join("");
  const strays = stray.map((a) => `<tr><td class="mono">${esc(a)}</td><td>${problemOf(a) || `<span class="hint">no brick branch of this job</span>`}
      <button class="small ghost" data-drop="${esc(a)}" ${busy ? "disabled" : ""}>Remove</button></td></tr>`).join("");
  return `<section class="card"><div class="card-head"><h2>Bindings</h2>
      <span class="actions"><button id="x-bind" class="primary" ${bindingsDirty() && !busy ? "" : "disabled"}>Save bindings</button></span></div>
    <p class="hint">The node each source and sink runs on. Every other step runs on the runner, which is never named.</p>
    <table class="library">${rows}${strays}</table></section>`;
}

function onChange(e) {
  const alias = e.target?.dataset?.alias;
  if (alias === undefined) return;
  draft[alias] = e.target.value;
  render();
}

function render() {
  if (!host || !doc || !plan) return;
  const deployment = deploymentOf(id);
  // A deployment runs the job it was armed at: it is this version when it is armed at the commit the library holds.
  const current = deployment && deployment.commit === doc.commit;
  const run = lastRunOf(id);
  const running = run?.state === "running";
  // Fragment names are stable per pipeline and node: a run's edges apply when they are this plan's.
  const edgeKey = (e) => `${e.producerFragment}|${e.producerAlias}|${e.consumerFragment}|${e.consumerAlias}`;
  const planEdges = new Set(plan.edges.map(edgeKey));
  const runMatches = !!run?.result && run.result.edgeCounts.length === planEdges.size && run.result.edgeCounts.every((e) => planEdges.has(edgeKey(e)));
  const canDeploy = plan.deployable && !isActive(run);
  const canRun = deployment && deployment.state !== "failed";

  const status = [
    deployment ? `<span class="chip ${deployment.state === "ready" ? "ok" : deployment.state === "failed" ? "bad" : "warn"}">${current ? "deployed" : "an older version is deployed"}: ${esc(deployment.state)} @ ${esc((deployment.commit || "").slice(0, 8))}</span>` : `<span class="chip">not deployed</span>`,
    plan.deployable ? `<span class="chip ok">a launch would run${runner ? ` on ${esc(runner)}` : ""}</span>` : `<span class="chip bad">a launch would be refused</span>`,
    bindingsChip(),
  ].join(" ");

  host.innerHTML = `
    <div class="view-head">
      <h1>${esc(doc.yaml.split("\n")[0].replace(/^#\s*/, ""))} <span class="mono muted">${esc(id)}</span></h1>
      <p class="hint">Planned at each launch: every brick on the node it is bound to, every other step on a runner chosen then. A source wired straight into a sink crosses no runner.</p>
      <div class="row">${status}
        <span class="actions">
          <a class="button" href="#/library">← Library</a>
          <button id="x-deploy" ${canDeploy && !busy && !bindingsDirty() ? "" : "disabled"} title="${bindingsDirty() ? "Save the bindings first" : ""}">${current ? "Deploy again" : "Deploy"}</button>
          <button id="x-run" class="primary" ${canRun && !busy ? "" : "disabled"}>Run</button>
          ${deployment ? `<button id="x-undeploy" class="ghost danger" ${isActive(run) || busy ? "disabled" : ""}>Undeploy</button>` : ""}
        </span>
      </div>
    </div>
    ${bindingsCard()}
    ${planProblems(plan).length ? `<section class="card"><ul class="msgs errors">${planProblems(plan).map((e) => `<li>${esc(e)}</li>`).join("")}</ul></section>` : ""}
    <section class="card">
      <div class="card-head"><h2>Fragments and edges</h2>
        ${legend}</div>
      <div class="lanes-head"><span>sources</span><span>runner</span><span>sinks</span></div>
      <div class="lanes" id="x-lanes"></div>
    </section>
    ${runPanel(run, runMatches || !run?.result)}
    <section class="card"><h2>Fragment jobs</h2>${fragmentJobs(plan)}</section>
    <section class="card"><div class="card-head"><h2>Job</h2><span class="hint">Read only: write it with <code>dtpipe studio</code> and publish it.</span></div>
      <pre id="x-job">${esc(doc.yaml)}</pre></section>`;
  drawLanes($("x-lanes"), plan, brickOfUnit, { run, running, current: runMatches });
}

function runPanel(run, current) {
  if (!run) return "";
  const r = run.result;
  return `<section class="card"><div class="card-head"><h2>Last run</h2><a href="#/runs">all runs →</a></div>
    <div class="outcome ${esc(run.state)}">${esc(run.runId)} · ${esc(run.state)} <span class="hint">${esc(duration(run))}</span></div>
    ${current ? "" : `<p class="hint">It ran a plan with other edges than this one.</p>`}
    ${run.refusal ? `<p class="mismatch">${esc(run.refusal)}</p>` : ""}
    ${ranPlan(run.plan)}
    ${r?.cause ? `<p><b>Cause:</b> ${esc(shortNode(r.cause))}${r.causeIsUnresponsive ? " (unresponsive)" : ""}</p>` : ""}
    ${r?.consequences?.length ? `<p><b>Consequences:</b> ${r.consequences.map((c) => esc(shortNode(c))).join(", ")}</p>` : ""}
  </section>`;
}

async function onClick(e) {
  const b = e.target.closest("button");
  if (!b || b.disabled) return;
  busy = true;
  render();
  try {
    if (b.id === "x-bind" || b.dataset.drop) {
      if (b.dataset.drop) delete draft[b.dataset.drop];
      const bindings = Object.fromEntries(Object.entries(draft).filter(([, v]) => v).map(([alias, node]) => [alias, { node }]));
      const saved = await api(`/api/library/${encodeURIComponent(id)}/bindings`, { bindings, message: `Bind ${id}` }, "PUT");
      await load();
      toast(saved.problems?.length ? "Bindings saved: some do not hold yet." : "Bindings saved.", "info");
    } else if (b.id === "x-deploy") {
      toast(`Deploying ${id}…`, "info");
      await api(`/api/library/${encodeURIComponent(id)}/deploy`, {});
      toast(`${id} deployed: every fragment registered.`, "info");
      doc = await api(`/api/library/${encodeURIComponent(id)}`);
    } else if (b.id === "x-run") {
      const run = await api("/api/runs", { pipelineId: id });
      toast(`${run.runId} ${run.state}.`, "info");
    } else if (b.id === "x-undeploy") {
      await api(`/api/deployments/${encodeURIComponent(id)}`, undefined, "DELETE");
    }
  } catch (err) {
    toast(err.message);
  }
  busy = false;
  render();
}
