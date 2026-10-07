// Placement: a plan drawn in lanes (sources, runner, sinks), one frame per fragment in its node's
// colour, the edges between them, and the fragments' jobs. Shared by the library's detail page and
// the designer's Placement mode, so one drawing serves both.
import { esc, fmt, nodeColor, store } from "./core.js";

const BOX_W = 290;
const LANE_GAP = 110;

function lane(fragment) {
  const node = store.nodes.find((n) => n.name === fragment.node);
  if (node?.role === "Runner") return 1;
  return fragment.edges.some((e) => e.direction === "Inbound") ? 2 : 0;
}

function unitLine(unit, brickOfUnit) {
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

// The plan's problems: what stops it being deployed.
export function planProblems(plan) {
  return [...(plan?.errors || []), plan?.flowRejection].filter(Boolean);
}

export const legend = `<div class="legend"><span><i class="sw network"></i>arrow edge between nodes</span><span><i class="sw forbidden"></i>refused by the flow matrix</span></div>`;

// The jobs each fragment runs, folded.
export function fragmentJobs(plan) {
  return plan.fragments.map((f) => `<details class="frag-card" style="--node-color:${nodeColor(f.node)}"><summary><b>${esc(f.name)}</b> <span class="hint">group ${esc(f.group)} · version <code>${esc(f.version.slice(0, 12))}</code> · ${f.edges.map((e) => `${e.direction === "Inbound" ? "in" : "out"} ${esc(e.alias)}`).join(", ")}</span></summary><pre>${esc(f.yaml)}</pre></details>`).join("");
}

// Draws plan into el. run, running and current say what to show on the edges: a run's row counts
// when it ran this plan's edges, and the flowing animation while it is running.
export function drawLanes(el, plan, brickOfUnit, { run = null, running = false, current = false } = {}) {
  const lanes = [[], [], []];
  for (const f of plan.fragments) lanes[lane(f)].push(f);
  el.innerHTML = plan.fragments.map((f) => {
    const units = plan.units.filter((u) => f.units.includes(u.id));
    return `<div class="xbox" data-fragment="${esc(f.name)}" style="--node-color:${nodeColor(f.node)}">
      <div class="xbox-head"><span class="node-name">${esc(f.node)}</span><span class="chip">${esc(f.group)}</span></div>
      ${units.map((u) => unitLine(u, brickOfUnit)).join("")}
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
