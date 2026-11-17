// Events: the stream of /api/events as this page remembers it (the nodes' log lines, the children's stderr, the
// coordinator's own lines and the page's), filtered by node, level and text. The page is the only memory of it.
import { $, LOG_CAPACITY, esc, markLogsSeen, nodeColor, runHref, store } from "./core.js";

// A level filter keeps the levels it names; "all" keeps every level a view shows (child stderr, Debug, aside).
const LEVELS = { all: null, warnings: ["Warning", "Error", "Critical"], errors: ["Error", "Critical"] };

let host = null;
let filters = { node: "", level: "all", text: "", debug: false };
let following = true;       // Pause freezes the box; Follow draws it again and sticks to the newest line
let shownUpTo = 0;          // the last line (`n`) the frozen box shows
let nodeNames = "";         // the node select's options, to redraw it only when they change
let timer = null;

/** The lines a view shows: the node, the level and the text of `f` applied to `lines`, oldest first. */
export function visible(lines, f) {
  const text = f.text.trim().toLowerCase();
  const levels = LEVELS[f.level] ?? null;
  return lines.filter((l) =>
    (!f.node || l.node === f.node)
    && (f.debug || l.level !== "Debug")
    && (!levels || levels.includes(l.level))
    && (!text || [l.message, l.node, l.fragment, l.runId].some((v) => v && String(v).toLowerCase().includes(text))));
}

/** One line of the box. Everything a node or a run wrote is escaped; the run id, the node's word, links to `#/runs/<id encoded>`. */
export function lineHtml(l) {
  const run = l.runId ? ` <a class="run-link" href="${esc(runHref(l.runId))}" title="The page of this run">${esc(l.runId)}</a>` : "";
  return `<div class="${esc(l.level)}"><span class="t">${esc(l.at.toLocaleTimeString())}</span> <span class="n" style="color:${esc(nodeColor(l.node))}">${esc(l.node)}</span>${l.fragment ? ` <span class="t">${esc(l.fragment)}</span>` : ""}${run} ${esc(l.message)}</div>`;
}

export function mount(el, _params, query) {
  host = el;
  filters = { node: query?.get("node") ?? "", level: "all", text: "", debug: false };
  following = true;
  nodeNames = "";
  host.innerHTML = `
    <div class="view-head"><h1>Events</h1>
      <p class="hint">What this page remembers of the coordinator's stream, nothing more: the last ${LOG_CAPACITY} lines it received, the log lines among the server's last 400 events (150 at most) replayed when it connects, and nothing kept once the page is closed or reloaded.</p></div>
    <section class="card events-card">
      <div class="card-head"><h2>Stream <span id="ev-count" class="hint"></span></h2>
        <div class="actions"><select id="ev-node" aria-label="Node"></select>
          <select id="ev-level" aria-label="Level"><option value="all">every level</option><option value="warnings">warnings and errors</option><option value="errors">errors only</option></select>
          <input id="ev-text" type="search" placeholder="Filter the text" aria-label="Text">
          <label class="check"><input type="checkbox" id="ev-debug"> child stderr</label>
          <button id="ev-follow" class="ghost small"></button><button id="ev-clear" class="ghost small">Clear</button></div></div>
      <div id="ev-log" class="log mono"></div></section>`;
  $("ev-node").addEventListener("change", (e) => { filters.node = e.target.value; draw(true); });
  $("ev-level").addEventListener("change", (e) => { filters.level = e.target.value; draw(true); });
  $("ev-text").addEventListener("input", (e) => { filters.text = e.target.value; draw(true); });
  $("ev-debug").addEventListener("change", (e) => { filters.debug = e.target.checked; draw(true); });
  $("ev-follow").addEventListener("click", () => { following = !following; if (following) draw(true); else label(); });
  $("ev-clear").addEventListener("click", () => { store.logs = []; draw(true); });
  markLogsSeen();
  renderNodes();
  draw(true);
}

export function leave() {
  clearTimeout(timer);
  markLogsSeen();
  host = null;
}

export function update(kind) {
  if (!host) return;
  if (kind === "logs") {
    markLogsSeen();
    clearTimeout(timer);
    timer = setTimeout(() => draw(false), 200);
  } else if (["all", "nodes"].includes(kind)) renderNodes();
}

function renderNodes() {
  const select = $("ev-node");
  if (!select) return;
  const names = [...new Set([...store.nodes.map((n) => n.name), "coordinator", ...(filters.node ? [filters.node] : [])])];
  if (names.join("\n") === nodeNames) return;
  nodeNames = names.join("\n");
  select.innerHTML = `<option value="">every node</option>` + names.map((x) => `<option ${x === filters.node ? "selected" : ""}>${esc(x)}</option>`).join("");
}

function label() {
  const button = $("ev-follow");
  if (!button) return;
  const behind = Math.max(0, (store.logs.at(-1)?.n ?? 0) - shownUpTo);
  button.textContent = following ? "Pause" : behind ? `Follow (${behind} new)` : "Follow";
}

// `force` draws a paused box too, and puts it at its newest line: a filter, Clear, Follow.
function draw(force) {
  const el = $("ev-log");
  if (!el) return;
  if (!following && !force) { label(); return; }
  const stick = force || el.scrollTop + el.clientHeight >= el.scrollHeight - 20;
  const lines = visible(store.logs, filters);
  el.innerHTML = lines.map(lineHtml).join("");
  $("ev-count").textContent = `${lines.length} of ${store.logs.length} lines`;
  shownUpTo = store.logs.at(-1)?.n ?? 0;
  if (stick) el.scrollTop = el.scrollHeight;
  label();
}
