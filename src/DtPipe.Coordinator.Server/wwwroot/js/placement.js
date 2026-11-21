// Placement: a plan drawn in lanes (sources, runner, sinks), one frame per fragment in its node's
// colour, the edges between them, how each is protected, and the fragments' jobs.
import { esc, fmt, nodeColor, protectionOf, store } from "./core.js";

const BOX_W = 290;
const LANE_GAP = 110;

function lane(fragment) {
  const node = store.nodes.find((n) => n.name === fragment.node);
  if (node?.role === "Runner") return 1;
  return fragment.edges.some((e) => e.direction === "Inbound") ? 2 : 0;
}

function unitLine(unit, brickOfUnit, extra) {
  const b = unit.branch;
  const brick = store.bricks.find((x) => x.key === brickOfUnit[unit.id]);
  let what;
  if (brick) what = `${brick.kind === "Source" ? "reads" : "writes"} <b>${esc(brick.title)}</b>`;
  else if (b.processor === "sql") what = `SQL over ${esc([...b.from, ...b.ref].join(", "))}`;
  else if (b.processor === "merge") what = `merges ${esc(b.from.join(", "))}`;
  else if (b.transformers.length) what = `${esc(b.transformers.map((t) => t.type).join(" → "))} over ${esc(b.from.join(", "))}`;
  else if (b.input) what = `reads <code>${esc(b.input)}</code>`;
  else what = "passes rows through";
  return `<div class="unit-line"><code>${esc(unit.id)}</code> <span>${what}</span>${extra ? extra(unit.id) : ""}</div>`;
}

// The plan's problems: what stops it being deployed.
export function planProblems(plan) {
  return [...(plan?.errors || []), plan?.flowRejection].filter(Boolean);
}

// The glyph of an edge's protection, in a box of 12 by 14: a closed lock for an edge held sealed, an open one for an edge whose ends
// cannot join.
const LOCK_SHAPES = {
  sealed: `<rect x="1" y="6" width="10" height="7.5" rx="1.5"/><path d="M3 6V4a3 3 0 0 1 6 0v2"/>`,
  broken: `<rect x="1" y="6" width="10" height="7.5" rx="1.5"/><path d="M3 6V4a3 3 0 0 1 6 0"/>`,
};
const lockIcon = (kind) => `<svg class="edge-lock ${kind}" viewBox="0 0 12 14" width="12" height="14" aria-hidden="true">${LOCK_SHAPES[kind]}</svg>`;

// How one edge is marked, from the server's judgment of its pair of fragments: broken when the server refuses it, with its sentence;
// sealed when it holds it at a level that seals; a level this page has no word for in the server's own word. An edge held in clear,
// one the server leaves unknown (no level and no refusal) and one it judged not at all have no mark and are drawn as they always were.
function markOf(judgment) {
  if (!judgment) return null;
  if (typeof judgment.refusal === "string" && judgment.refusal) return { kind: "broken", word: "cannot join", title: judgment.refusal };
  const level = protectionOf(judgment.protection);
  if (!level || level.sealed === false) return null;
  const title = `The replicas connected now would hold this edge ${level.sealed ? "sealed" : `at ${level.text}`}${level.keyed ? ", with a pre-shared key" : ""}.`;
  return { kind: level.sealed ? "sealed" : "level", word: level.text, title };
}

// The mark of each edge of the plan, in the plan's order. The judgment is the plan route's `edgeProtections`, one per pair of
// fragments, so two edges between the same fragments share it. This page compares no two levels and reads no sentence.
function marksOf(plan, edgeProtections) {
  const pair = (producer, consumer) => JSON.stringify([producer, consumer]);
  const judged = new Map();
  for (const j of Array.isArray(edgeProtections) ? edgeProtections : []) if (j) judged.set(pair(j.producerFragment, j.consumerFragment), j);
  return plan.edges.map((e) => (e.allowed ? markOf(judged.get(pair(e.producerFragment, e.consumerFragment))) : null));
}

// The legend of the plan's edges: it names a lock only when the diagram draws one.
export function legendFor(plan, edgeProtections) {
  const kinds = new Set(marksOf(plan, edgeProtections).map((m) => m?.kind));
  return `<div class="legend"><span><i class="sw network"></i>arrow edge between nodes</span><span><i class="sw forbidden"></i>refused by the flow matrix</span>${kinds.has("sealed") ? `<span>${lockIcon("sealed")}held sealed</span>` : ""}${kinds.has("broken") ? `<span>${lockIcon("broken")}ends that cannot join: hover the lock for why</span>` : ""}</div>`;
}

// The point of an edge's curve at abscissa x, which its parameter only increases.
function pointAt(x1, y1, x2, y2, x) {
  const m = (x1 + x2) / 2;
  const at = (t) => {
    const u = 1 - t;
    return [u * u * u * x1 + 3 * u * u * t * m + 3 * u * t * t * m + t * t * t * x2, u * u * u * y1 + 3 * u * u * t * y1 + 3 * u * t * t * y2 + t * t * t * y2];
  };
  let lo = 0, hi = 1;
  for (let i = 0; i < 24; i++) {
    const mid = (lo + hi) / 2;
    if (at(mid)[0] < x) lo = mid; else hi = mid;
  }
  return at((lo + hi) / 2);
}

// A mark on an edge, by its consumer's end: its word, and for a lock, the glyph on the curve with the server's sentence in its title.
// The word carries the meaning on its own, so that a mark reads without its colour.
function markMarkup(mark, x, y) {
  const title = `<title>${esc(mark.title)}</title>`;
  if (mark.kind === "level") return `<text class="edge-protection" x="${x + 10}" y="${y + 4}" text-anchor="end">${title}${esc(mark.word)}</text>`;
  return `<text class="edge-protection ${mark.kind}" x="${x - 11}" y="${y + 4}" text-anchor="end">${title}${esc(mark.word)}</text>`
    + `<g class="edge-lock ${mark.kind}" data-protection="${mark.kind}" transform="translate(${x - 6},${y - 7})" role="img" aria-label="${esc(mark.word)}">${title}<circle class="edge-lock-bg" cx="6" cy="7" r="8"/>${LOCK_SHAPES[mark.kind]}</g>`;
}

// The jobs each fragment runs, folded.
export function fragmentJobs(plan) {
  return plan.fragments.map((f) => `<details class="frag-card" style="--node-color:${nodeColor(f.node)}"><summary><b>${esc(f.name)}</b> <span class="hint">${f.groups.length > 1 ? "groups" : "group"} ${esc(f.groups.join(" + "))} · version <code>${esc(f.version.slice(0, 12))}</code> · ${f.edges.map((e) => `${e.direction === "Inbound" ? "in" : "out"} ${esc(e.alias)}`).join(", ")}</span></summary><pre>${esc(f.yaml)}</pre></details>`).join("");
}

// Draws plan into el. run, running and current say what to show on the edges: a run's row counts
// when it ran this plan's edges, and the flowing animation while it is running. edgeProtections is the plan route's judgment of each
// edge: a closed lock on an edge held sealed, a broken one on an edge whose ends cannot join, nothing on any other. It is the plan's,
// never the run's: the protections a run reported are not drawn here.
export function drawLanes(el, plan, brickOfUnit, { run = null, running = false, current = false, extra = null, edgeProtections = [] } = {}) {
  const lanes = [[], [], []];
  for (const f of plan.fragments) lanes[lane(f)].push(f);
  el.innerHTML = plan.fragments.map((f) => {
    const units = plan.units.filter((u) => f.units.includes(u.id));
    return `<div class="xbox" data-fragment="${esc(f.name)}" style="--node-color:${nodeColor(f.node)}">
      <div class="xbox-head"><span class="node-name">${esc(f.node)}</span><span class="chip">${esc(f.groups.join(" + "))}</span></div>
      ${units.map((u) => unitLine(u, brickOfUnit, extra)).join("")}
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
  const marks = marksOf(plan, edgeProtections);
  const outIndex = {}, inIndex = {};
  svg.innerHTML = plan.edges.map((e, i) => {
    const a = rect(e.producerFragment), b = rect(e.consumerFragment);
    const oi = (outIndex[e.producerFragment] = (outIndex[e.producerFragment] ?? -1) + 1);
    const ii = (inIndex[e.consumerFragment] = (inIndex[e.consumerFragment] ?? -1) + 1);
    const x1 = a.x + a.w, y1 = a.y + 22 + oi * 16, x2 = b.x, y2 = b.y + 22 + ii * 16;
    const m = (x1 + x2) / 2;
    const c = counts.find((x) => x.producerFragment === e.producerFragment && x.consumerFragment === e.consumerFragment && x.consumerAlias === e.consumerAlias);
    const label = !e.allowed ? "refused" : c ? `${e.consumerAlias} · ${fmt(c.sent)} → ${fmt(c.received)}` : e.consumerAlias;
    const mark = marks[i];
    const tone = mark?.kind === "sealed" || mark?.kind === "broken" ? ` ${mark.kind}` : "";
    const cls = !e.allowed ? "forbidden" : `network${tone} ${running ? "flowing" : ""}`;
    const [mx, my] = mark ? pointAt(x1, y1, x2, y2, x2 - 18) : [0, 0];
    return `<path class="edge ${cls}" d="M${x1},${y1} C${m},${y1} ${m},${y2} ${x2},${y2}"/>
      <text class="edge-label ${c ? (c.sent === c.received ? "ok" : "bad") : ""}" x="${x1 + 8}" y="${y1 - 5}">${esc(label)}</text>${mark ? markMarkup(mark, mx, my) : ""}`;
  }).join("");
}
