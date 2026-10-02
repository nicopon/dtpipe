// Distribution: the plan the coordinator computes for a library pipeline, with no help from the
// user: every brick on its node, every other step on the runner. Drawn in lanes (sources, runner,
// sinks); saved to the library, deployed and run from here.
import { $, api, deploymentOf, duration, esc, fmt, isActive, lastRunOf, nodeColor, shortNode, store, toast } from "./core.js";

const BOX_W = 290;
const LANE_GAP = 110;

let host = null;
let id = null;
let doc = null;
let plan = null;
let brickOfUnit = {};
let busy = false;

export function mount(el, params) {
  host = el;
  id = params[0] || null;
  doc = null; plan = null;
  if (!id) { picker(); return; }
  host.addEventListener("click", onClick);
  load();
}

export function update(kind) {
  if (!id) return;
  if (kind === "library") load();
  else if (["deployments", "runs", "nodes", "all"].includes(kind)) render();
}

async function picker() {
  const entries = await api("/api/library");
  host.innerHTML = `
    <div class="view-head"><h1>Distribution</h1><p class="hint">Pick a pipeline to see where each of its steps would run.</p></div>
    <section class="card picker">${entries.map((e) => `<a href="#/distribution/${encodeURIComponent(e.id)}"><b>${esc(e.title)}</b><span class="hint mono">${esc(e.id)} · ${e.distributed ? (e.planCurrent ? "plan saved" : "plan stale") : "not distributed"}</span></a>`).join("")}</section>`;
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

function lane(fragment) {
  const node = store.nodes.find((n) => n.name === fragment.node);
  if (node?.role === "Runner") return 1;
  return fragment.edges.some((e) => e.direction === "Inbound") ? 2 : 0;
}

function unitLine(unit) {
  const b = unit.branch;
  const brick = store.bricks.find((x) => x.key === brickOfUnit[unit.id]);
  let what;
  if (brick) what = `${brick.kind === "Source" ? "reads" : "writes"} <b>${esc(brick.title)}</b>`;
  else if (b.processor === "sql") what = `SQL over ${esc([...b.from, ...b.ref].join(", "))}`;
  else if (b.processor === "merge") what = `merges ${esc(b.from.join(", "))}`;
  else if (b.transformers.length) what = `${esc(b.transformers.map((t) => t.type).join(" → "))} over ${esc(b.from.join(", "))}`;
  else if (b.input) what = `reads <code>${esc(b.input)}</code>`;
  else what = "passes rows through";
  return `<div class="unit-line"><code>${esc(unit.id)}</code> <span>${what}</span></div>`;
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
  ].join(" ");

  host.innerHTML = `
    <div class="view-head">
      <h1>${esc(doc.yaml.split("\n")[0].replace(/^#\s*/, ""))} <span class="mono muted">${esc(id)}</span></h1>
      <p class="hint">Placed automatically: every brick on the node that offers it, every other step on the runner. A source wired straight into a sink crosses no runner.</p>
      <div class="row">${status}
        <span class="actions">
          <a class="button" href="#/designer/${encodeURIComponent(id)}">← Design</a>
          <button id="x-save" ${plan.deployable && !savedCurrent && !busy ? "" : "disabled"}>Save plan</button>
          <button id="x-deploy" ${canDeploy && !busy ? "" : "disabled"} title="${savedCurrent ? "" : "Save the plan first"}">${deployedPlanMatches ? "Redeploy" : "Deploy"}</button>
          <button id="x-run" class="primary" ${canRun && !busy ? "" : "disabled"}>Run</button>
          ${deployment ? `<button id="x-undeploy" class="ghost danger" ${isActive(run) || busy ? "disabled" : ""}>Undeploy</button>` : ""}
        </span>
      </div>
    </div>
    ${plan.errors.length || plan.flowRejection ? `<section class="card"><ul class="msgs errors">${[...plan.errors, plan.flowRejection].filter(Boolean).map((e) => `<li>${esc(e)}</li>`).join("")}</ul></section>` : ""}
    <section class="card">
      <div class="card-head"><h2>Fragments and edges</h2>
        <div class="legend"><span><i class="sw network"></i>arrow edge between nodes</span><span><i class="sw forbidden"></i>refused by the flow matrix</span></div></div>
      <div class="lanes-head"><span>sources</span><span>runner</span><span>sinks</span></div>
      <div class="lanes" id="x-lanes"></div>
    </section>
    ${runPanel(run, runMatches || !run?.result)}
    <section class="card"><h2>Fragment jobs</h2>
      ${plan.fragments.map((f) => `<details class="frag-card" style="--node-color:${nodeColor(f.node)}"><summary><b>${esc(f.name)}</b> <span class="hint">group ${esc(f.group)} · version <code>${esc(f.version.slice(0, 12))}</code> · ${f.edges.map((e) => `${e.direction === "Inbound" ? "in" : "out"} ${esc(e.alias)}`).join(", ")}</span></summary><pre>${esc(f.yaml)}</pre></details>`).join("")}
    </section>`;
  drawLanes(run, running, runMatches);
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

function drawLanes(run, running, current) {
  const el = $("x-lanes");
  const lanes = [[], [], []];
  for (const f of plan.fragments) lanes[lane(f)].push(f);
  el.innerHTML = plan.fragments.map((f) => {
    const units = plan.units.filter((u) => f.units.includes(u.id));
    return `<div class="xbox" data-fragment="${esc(f.name)}" style="--node-color:${nodeColor(f.node)}">
      <div class="xbox-head"><span class="node-name">${esc(f.node)}</span><span class="chip">${esc(f.group)}</span></div>
      ${units.map(unitLine).join("")}
    </div>`;
  }).join("") + `<svg></svg>`;

  let height = 0;
  lanes.forEach((list, i) => {
    let y = 0;
    for (const f of list) {
      const box = el.querySelector(`[data-fragment="${CSS.escape(f.name)}"]`);
      box.style.left = `${i * (BOX_W + LANE_GAP)}px`;
      box.style.top = `${y}px`;
      y += box.offsetHeight + 24;
    }
    height = Math.max(height, y);
  });
  el.style.height = `${height + 10}px`;
  el.style.width = `${3 * BOX_W + 2 * LANE_GAP}px`;

  const svg = el.querySelector("svg");
  svg.setAttribute("width", el.offsetWidth);
  svg.setAttribute("height", el.offsetHeight);
  const counts = current ? run?.result?.edgeCounts || [] : [];
  const rect = (name) => { const b = el.querySelector(`[data-fragment="${CSS.escape(name)}"]`); return { x: b.offsetLeft, y: b.offsetTop, w: b.offsetWidth, h: b.offsetHeight }; };
  const outIndex = {}, inIndex = {};
  svg.innerHTML = plan.edges.map((e) => {
    const a = rect(e.producerFragment), b = rect(e.consumerFragment);
    const oi = (outIndex[e.producerFragment] = (outIndex[e.producerFragment] ?? -1) + 1);
    const ii = (inIndex[e.consumerFragment] = (inIndex[e.consumerFragment] ?? -1) + 1);
    const x1 = a.x + a.w, y1 = a.y + 22 + oi * 16, x2 = b.x, y2 = b.y + 22 + ii * 16;
    const m = (x1 + x2) / 2;
    const c = counts.find((x) => x.producerFragment === e.producerFragment && x.consumerFragment === e.consumerFragment && x.consumerAlias === e.consumerAlias);
    const label = !e.allowed ? "refused" : c ? `${e.consumerAlias} · ${fmt(c.sent)} → ${fmt(c.received)}` : e.consumerAlias;
    const cls = !e.allowed ? "forbidden" : `network ${running ? "flowing" : ""}`;
    return `<path class="edge ${cls}" d="M${x1},${y1} C${m},${y1} ${m},${y2} ${x2},${y2}"/>
      <text class="edge-label ${c ? (c.sent === c.received ? "ok" : "bad") : ""}" x="${x1 + 8}" y="${y1 - 5}">${esc(label)}</text>`;
  }).join("");
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
