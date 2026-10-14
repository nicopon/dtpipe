// A library pipeline's page: the plan the coordinator computes with no help from the user (every
// brick on its node, every other step on the runner), drawn in lanes, its refusals, its fragments'
// jobs, the job's text read only, and its last run. The node of each source and sink branch is chosen here, among the nodes
// that offer its brick; the runner is never named. The placement, the plan, the deployment and the run are saved from here.
import { $, api, deploymentOf, duration, esc, isActive, lastRunOf, shortNode, toast } from "./core.js";
import { drawLanes, fragmentJobs, legend, planProblems } from "./placement.js";

let host = null;
let id = null;
let doc = null;
let plan = null;
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
    const answer = await api(`/api/library/${encodeURIComponent(id)}/distribute`, {});
    plan = answer.plan;
    brickOfUnit = answer.bricks || {};
    offers = answer.offers || {};
    draft = { ...(doc.placement || {}) };
  } catch (e) {
    host.innerHTML = `<p class="mismatch">${esc(e.message)}</p>`;
    return;
  }
  render();
}

// Two plans are the same when they place the same fragments, at the same versions, on nodes holding the same groups.
const fragmentKey = (f) => [f.name, f.version, [...f.groups].sort().join("+")];
const sameFragments = (a, b) => !!a && !!b &&
  JSON.stringify(a.fragments.map(fragmentKey).sort()) === JSON.stringify(b.fragments.map(fragmentKey).sort());

const sorted = (o) => JSON.stringify(Object.entries(o).filter(([, v]) => v).sort(([a], [b]) => (a < b ? -1 : 1)));
const placementDirty = () => sorted(draft) !== sorted(doc.placement || {});

function placementChip() {
  const problems = doc.placementProblems || [];
  if (!problems.length) return "";
  const blocking = problems.some((p) => p.blocking);
  return `<span class="chip ${blocking ? "bad" : "warn"}" title="${esc(problems.map((p) => p.message).join(" "))}">placement: ${problems.length} problem${problems.length > 1 ? "s" : ""}</span>`;
}

// One row per brick branch of the job, and one per entry the placement keeps for a branch that is none, so it can be dropped.
function placementCard() {
  const aliases = Object.keys(offers).sort();
  const stray = Object.keys(draft).filter((a) => !offers[a]).sort();
  if (!aliases.length && !stray.length) return "";
  const problems = doc.placementProblems || [];
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
  return `<section class="card"><div class="card-head"><h2>Placement</h2>
      <span class="actions"><button id="x-place" class="primary" ${placementDirty() && !busy ? "" : "disabled"}>Save placement</button></span></div>
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
  const deployedPlanMatches = deployment && doc.plan && sameFragments(doc.plan.plan, plan);
  const saved = doc.plan;
  const savedCurrent = saved && doc.planCurrent && sameFragments(saved.plan, plan);
  const run = lastRunOf(id);
  const running = run?.state === "running" && deployedPlanMatches;
  // Fragment names are stable per pipeline and node: a run's edges apply when they are this plan's.
  const edgeKey = (e) => `${e.producerFragment}|${e.producerAlias}|${e.consumerFragment}|${e.consumerAlias}`;
  const planEdges = new Set(plan.edges.map(edgeKey));
  const runMatches = !!run?.result && run.result.edgeCounts.length === planEdges.size && run.result.edgeCounts.every((e) => planEdges.has(edgeKey(e)));
  const canDeploy = savedCurrent && !isActive(run);
  const canRun = deployment && deployment.state !== "failed" && deployedPlanMatches;

  const status = [
    saved ? (savedCurrent ? `<span class="chip ok">plan saved</span>` : `<span class="chip warn">saved plan is stale</span>`) : `<span class="chip">plan not saved</span>`,
    deployment ? `<span class="chip ${deployment.state === "ready" ? "ok" : deployment.state === "failed" ? "bad" : "warn"}">${deployedPlanMatches ? "deployed" : "an older plan is deployed"}: ${esc(deployment.state)}</span>` : `<span class="chip">not deployed</span>`,
    placementChip(),
  ].join(" ");

  host.innerHTML = `
    <div class="view-head">
      <h1>${esc(doc.yaml.split("\n")[0].replace(/^#\s*/, ""))} <span class="mono muted">${esc(id)}</span></h1>
      <p class="hint">Placed automatically: every brick on the node that offers it, every other step on the runner. A source wired straight into a sink crosses no runner.</p>
      <div class="row">${status}
        <span class="actions">
          <a class="button" href="#/library">← Library</a>
          <button id="x-save" ${plan.deployable && !savedCurrent && !busy && !placementDirty() ? "" : "disabled"} title="${placementDirty() ? "Save the placement first" : ""}">Save plan</button>
          <button id="x-deploy" ${canDeploy && !busy ? "" : "disabled"} title="${savedCurrent ? "" : "Save the plan first"}">${deployedPlanMatches ? "Redeploy" : "Deploy"}</button>
          <button id="x-run" class="primary" ${canRun && !busy ? "" : "disabled"}>Run</button>
          ${deployment ? `<button id="x-undeploy" class="ghost danger" ${isActive(run) || busy ? "disabled" : ""}>Undeploy</button>` : ""}
        </span>
      </div>
    </div>
    ${placementCard()}
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
    if (b.id === "x-place" || b.dataset.drop) {
      if (b.dataset.drop) delete draft[b.dataset.drop];
      const placement = Object.fromEntries(Object.entries(draft).filter(([, v]) => v));
      const saved = await api(`/api/library/${encodeURIComponent(id)}/placement`, { placement, message: `Place ${id}` }, "PUT");
      await load();
      toast(saved.redistribute ? "Placement saved: its plan must be saved again." : "Placement saved.", "info");
    } else if (b.id === "x-save") {
      const message = prompt("Describe this plan (the commit message):", `Distribute ${id}`);
      if (message !== null) {
        await api(`/api/library/${encodeURIComponent(id)}/distribute`, { save: true, message });
        doc = await api(`/api/library/${encodeURIComponent(id)}`);
        toast("Plan saved in the library.", "info");
      }
    } else if (b.id === "x-deploy") {
      toast(`Deploying ${id}…`, "info");
      await api(`/api/library/${encodeURIComponent(id)}/deploy`, {});
      toast(`${id} deployed: every fragment registered.`, "info");
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
