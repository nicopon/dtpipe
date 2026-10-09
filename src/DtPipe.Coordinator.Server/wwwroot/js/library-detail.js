// A library pipeline's page: the plan the coordinator computes with no help from the user (every
// brick on its node, every other step on the runner), drawn in lanes, its refusals, its fragments'
// jobs, the job's text read only, and its last run. The plan is saved to the library, deployed and run from here.
import { $, api, deploymentOf, duration, esc, isActive, lastRunOf, shortNode, toast } from "./core.js";
import { drawLanes, fragmentJobs, legend, planProblems } from "./placement.js";

let host = null;
let id = null;
let doc = null;
let plan = null;
let brickOfUnit = {};
let busy = false;

export function mount(el, params) {
  host = el;
  id = params[0];
  doc = null; plan = null;
  host.addEventListener("click", onClick);
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
  } catch (e) {
    host.innerHTML = `<p class="mismatch">${esc(e.message)}</p>`;
    return;
  }
  render();
}

const sameFragments = (a, b) => !!a && !!b &&
  JSON.stringify(a.fragments.map((f) => [f.name, f.version]).sort()) === JSON.stringify(b.fragments.map((f) => [f.name, f.version]).sort());

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
  ].join(" ");

  host.innerHTML = `
    <div class="view-head">
      <h1>${esc(doc.yaml.split("\n")[0].replace(/^#\s*/, ""))} <span class="mono muted">${esc(id)}</span></h1>
      <p class="hint">Placed automatically: every brick on the node that offers it, every other step on the runner. A source wired straight into a sink crosses no runner.</p>
      <div class="row">${status}
        <span class="actions">
          <a class="button" href="#/library">← Library</a>
          <button id="x-save" ${plan.deployable && !savedCurrent && !busy ? "" : "disabled"}>Save plan</button>
          <button id="x-deploy" ${canDeploy && !busy ? "" : "disabled"} title="${savedCurrent ? "" : "Save the plan first"}">${deployedPlanMatches ? "Redeploy" : "Deploy"}</button>
          <button id="x-run" class="primary" ${canRun && !busy ? "" : "disabled"}>Run</button>
          ${deployment ? `<button id="x-undeploy" class="ghost danger" ${isActive(run) || busy ? "disabled" : ""}>Undeploy</button>` : ""}
        </span>
      </div>
    </div>
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
    if (b.id === "x-save") {
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
