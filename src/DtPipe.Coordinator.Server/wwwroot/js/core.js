// What every view shares: the API client, the live store fed by /api/events, and small helpers.
// The coordinator decides everything; the views only draw the store and send requests.

// What this page keeps of the events it received: the lines of the Events view and of a run's page, the oldest dropped first.
export const LOG_CAPACITY = 1500;

export const store = {
  nodes: [],
  bricks: [],
  matrix: {},
  deployments: [],
  last: new Map(),   // the latest run that is over of each pipeline, by pipeline id: /api/runs/last, however far back it ended
  queue: [],         // the runs in flight, then the queued ones
  logs: [],
  bytes: { total: 0 },
  health: { status: "ok", pipelines: [] }, // whether each armed pipeline can be launched: /api/health, read again when its inputs move
  connected: false,
  httpKeys: false, // this coordinator exchanges identity keys over plain HTTP
  me: null,       // { name, email, roles }: who is signed in, and what the server lets them do
};

const listeners = new Set();
export function subscribe(fn) { listeners.add(fn); return () => listeners.delete(fn); }
function emit(kind) { for (const fn of listeners) { try { fn(kind); } catch (e) { console.error(e); } } }

export const $ = (id) => document.getElementById(id);
export const esc = (s) => String(s ?? "").replace(/[&<>"']/g, (c) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[c]));
/** A count with its noun, in the plural unless the count is one. */
export const plural = (n, word) => `${n} ${word}${n === 1 ? "" : "s"}`;
// A version without its build metadata: "1.17.0+<commit>" reads "1.17.0".
export const tagOf = (version) => (version ? String(version).split("+")[0] : "");
export const fmt = (n) => (n ?? 0).toLocaleString("en-US");
export const nodeColor = (node) => (node ? `var(--${node}, var(--muted))` : "var(--muted)");
// A fragment reads "pipeline@node"; a cause that is a sentence (a refused transfer, a row-count
// mismatch) names fragments inside it and is shown whole.
export const shortNode = (fragment) => {
  const text = String(fragment ?? "");
  return text.includes(" ") ? text : text.split("@").pop();
};

/** The rows a run's edges delivered, counted on every edge: a row that crosses two edges counts twice. */
export const rowsCrossed = (run) => run?.result?.edgeCounts?.reduce((s, e) => s + (e.received ?? 0), 0) || 0;
export const ROWS_CROSSED_HINT = "Counted on every edge of the run: a row that crosses two edges counts twice.";

/** What a run says of why it ended as it did, and of what it may have left behind: the same words wherever a run is shown. */
export function endNotes(run) {
  const r = run.result;
  const cause = run.reason ?? (r?.cause ? `${shortNode(r.cause)}${r.causeIsUnresponsive ? " (unresponsive)" : ""}` : null);
  const where = run.reason && r?.cause && run.reason !== r.cause ? ` <span class="hint">(${esc(shortNode(r.cause))})</span>` : "";
  const started = run.plan != null && ["failed", "cancelled"].includes(run.state);
  return [
    run.refusal && run.refusal !== run.reason ? `<p class="mismatch">${esc(run.refusal)}</p>` : "",
    cause ? `<p><b>Cause:</b> ${esc(cause)}${where}</p>` : "",
    run.sinkMayBePartial
      ? `<p class="mismatch">The coordinator restarted while this run was in progress: its outputs may hold part of what it wrote.</p>`
      : started ? `<p class="hint">It stopped before its end: an output it was writing may hold part of the rows, or none.</p>` : "",
  ].join("");
}

export function bytes(b) {
  return b > 1 << 30 ? (b / (1 << 30)).toFixed(2) + " GiB" : b > 1 << 20 ? (b / (1 << 20)).toFixed(1) + " MiB" : b > 1024 ? (b / 1024).toFixed(0) + " KiB" : b + " B";
}

export function ago(date) {
  if (!date) return "";
  const s = (Date.now() - new Date(date)) / 1000;
  if (s < 60) return "just now";
  if (s < 3600) return `${Math.floor(s / 60)} min ago`;
  if (s < 86400) return `${Math.floor(s / 3600)} h ago`;
  return new Date(date).toLocaleDateString();
}

export function duration(run) {
  if (!run?.finishedAt || !run.startedAt) return "";
  return ((new Date(run.finishedAt) - new Date(run.startedAt)) / 1000).toFixed(1) + " s";
}

/** Whether the signed-in person's role allows `role`: the server sends every role the person holds, the highest first. */
export const can = (role) => !store.me || store.me.roles.includes(role);

/** What a refusal for the person's role says, in the words of the pages: the role they have and who can do it instead. */
export function forbidden() {
  const role = store.me?.roles?.[0];
  return `Your role${role ? ` (${role})` : ""} does not allow this: ask ${role === "designer" ? "an administrator" : "a designer or an administrator"}.`;
}

/** Disables, inside `root`, what the person's role does not allow: an element marked `data-needs="<role>"`, with the reason as its title. */
export function gate(root) {
  for (const el of root.querySelectorAll("[data-needs]")) {
    if (can(el.dataset.needs)) continue;
    el.disabled = true;
    el.title = forbidden();
  }
}

async function request(path, body, method) {
  // The CSRF header rides every call: the server refuses a change without it.
  const init = { method: method || (body === undefined ? "GET" : "POST"), headers: { "X-Dtpipe-Csrf": "1" } };
  if (body !== undefined) { init.headers["Content-Type"] = "application/json"; init.body = JSON.stringify(body); }
  const response = await fetch(path, init);
  // The session ended: open the page again, which signs the person in through the provider.
  if (response.status === 401) { location.reload(); throw new Error("signed out"); }
  const text = await response.text();
  let data = {};
  try { data = text ? JSON.parse(text) : {}; } catch { data = { error: text }; }
  if (!response.ok) {
    const error = new Error(data.error || data.detail || (response.status === 403 ? forbidden() : data.title) || `${response.status} ${response.statusText}`);
    error.status = response.status;
    throw error;
  }
  return { data, response };
}

export async function api(path, body, method) { return (await request(path, body, method)).data; }

/** A list the server counts: its items and how many match the filters before the page cut them (`X-Total-Count`; the items' own count when the header is absent). */
export async function apiPage(path) {
  const { data, response } = await request(path);
  const header = response.headers.get("X-Total-Count");
  const total = header === null ? NaN : Number(header);
  return { items: data, total: Number.isInteger(total) && total >= 0 ? total : data.length };
}

/** The address after `#/`: its view, the segments after it (decoded), and the `?…` that may end it as URLSearchParams. */
export function parseAddress(hash) {
  const [path, ...rest] = String(hash ?? "").replace(/^#\/?/, "").split("?");
  const decode = (segment) => { try { return decodeURIComponent(segment); } catch { return segment; } };
  const [name, ...params] = path.split("/").map(decode);
  return { name, params: params.filter(Boolean), query: new URLSearchParams(rest.join("?")) };
}

/** The address of a run's page. A run id on a line is the node's word: it is encoded here and escaped where it enters a document. */
export const runHref = (runId) => `#/runs/${encodeURIComponent(runId)}`;

let toastTimer = null;
export function toast(message, kind = "error") {
  const el = $("toast");
  el.textContent = message;
  el.className = `toast ${kind}`;
  el.hidden = false;
  clearTimeout(toastTimer);
  toastTimer = setTimeout(() => { el.hidden = true; }, kind === "error" ? 8000 : 3000);
  if (kind === "error") addLog({ node: "coordinator", level: "Error", message });
}

// `n` numbers the lines in the order the page received them, which `markLogsSeen` and the Events badge read.
let logSeq = 0;
let seenSeq = 0;

export function addLog(line) {
  if (/:\s*$/.test(line.message)) return; // a label whose value was empty, e.g. a silent child's stderr
  store.logs.push({ at: new Date(), ...line, n: ++logSeq });
  if (store.logs.length > LOG_CAPACITY) store.logs.splice(0, store.logs.length - LOG_CAPACITY);
  emit("logs");
}

/** The errors and the warnings among the lines that arrived since the Events view was last open (never opened: every line held). */
export function unseenLogs() {
  let errors = 0, warnings = 0;
  for (const l of store.logs) {
    if (l.n <= seenSeq) continue;
    if (l.level === "Error" || l.level === "Critical") errors++;
    else if (l.level === "Warning") warnings++;
  }
  return { errors, warnings };
}

/** Every line held so far counts as seen: the Events view is open, or has just been left. */
export function markLogsSeen() { seenSeq = logSeq; }

export function parseCsv(text) {
  const rows = [];
  let row = [], field = "", quoted = false;
  for (let i = 0; i < text.length; i++) {
    const c = text[i];
    if (quoted) {
      if (c === '"' && text[i + 1] === '"') { field += '"'; i++; }
      else if (c === '"') quoted = false;
      else field += c;
    } else if (c === '"') quoted = true;
    else if (c === ",") { row.push(field); field = ""; }
    else if (c === "\n") { row.push(field.replace(/\r$/, "")); rows.push(row); row = []; field = ""; }
    else field += c;
  }
  if (field || row.length) { row.push(field.replace(/\r$/, "")); rows.push(row); }
  return rows;
}

// `types` are the columns' types by name, when the caller knows them: the CSV carries values only.
export function csvTable(csv, limit = 200, types = null) {
  const [header, ...rows] = parseCsv(csv || "");
  if (!header) return `<p class="hint">No rows.</p>`;
  const typeOf = (name) => (types?.find((c) => c.name === name)?.type ?? "");
  return `<div class="table-wrap"><table><tr>${header.map((h) => `<th>${esc(h)}${typeOf(h) ? `<div class="hint">${esc(typeOf(h))}</div>` : ""}</th>`).join("")}</tr>${rows.slice(0, limit).map((r) => `<tr>${r.map((v) => `<td>${esc(v)}</td>`).join("")}</tr>`).join("")}</table></div>`;
}

export function deploymentOf(pipelineId) { return store.deployments.find((d) => d.pipelineId === pipelineId) || null; }
export function lastRunOf(pipelineId) {
  return store.queue.find((r) => r.pipelineId === pipelineId) || store.last.get(pipelineId) || null;
}
export const healthOf = (pipelineId) => store.health.pipelines.find((p) => p.id === pipelineId) || null;

// Whether an armed pipeline can be launched, as the coordinator's health read says it (what a supervisor reads at /health).
export function healthChip(pipelineId) {
  const h = healthOf(pipelineId);
  if (!h) return "";
  const told = h.causes.filter((c) => c.severity !== "info").map((c) => c.message);
  return `<span class="chip ${h.status === "ok" ? "ok" : h.status === "warning" ? "warn" : "bad"}" title="${esc(told.join(" ") || "Nothing to report.")}">health: ${esc(h.status)}</span>`;
}

let healthTimer = null;
export function refreshHealth() {
  clearTimeout(healthTimer);
  healthTimer = setTimeout(async () => {
    try { store.health = await api("/api/health"); emit("health"); } catch { /* the next event reads it again */ }
  }, 300);
}

// What a page calls a run's state. The key is the word the server sends and what the pages' styles and tests read;
// a state with no entry shows as received.
const RUN_STATE_LABEL = { cancelled: "stopped" };
export const runStateLabel = (state) => RUN_STATE_LABEL[state] ?? state;
export const isActive = (run) => run && ["queued", "running"].includes(run.state);

// ---------------------------------------------------------------- live store

async function refreshBricks() {
  try { store.bricks = await api("/api/bricks"); emit("bricks"); } catch { /* the next nodes event retries */ }
}

let bricksTimer = null;
function onRun(run) {
  if (isActive(run)) {
    store.queue = [...store.queue.filter((r) => r.runId !== run.runId), run]
      .sort((a, b) => (a.state === "running") !== (b.state === "running") ? (a.state === "running" ? -1 : 1)
        : a.state === "running" ? new Date(a.startedAt) - new Date(b.startedAt) : new Date(a.queuedAt) - new Date(b.queuedAt));
  } else {
    store.queue = store.queue.filter((r) => r.runId !== run.runId);
    if (run.pipelineId) store.last.set(run.pipelineId, run);
    addLog({ node: "coordinator", level: run.state === "succeeded" ? "Information" : "Warning", message: `${run.runId} ${run.pipelineId ?? ""} ${runStateLabel(run.state)}${run.refusal ? `: ${run.refusal}` : ""}`, runId: run.runId });
  }
  emit("runs");
  // A run that is over is in the journal, which the Runs view reads from the server again.
  if (!isActive(run)) emit("journal");
}

export async function start() {
  const [me, info, nodes, bricks, matrix, deployments, last, queue] = await Promise.all([
    api("/api/me"), api("/api/info"), api("/api/nodes"), api("/api/bricks"), api("/api/flow-matrix"), api("/api/deployments"), api("/api/runs/last"), api("/api/runs/queue"),
  ]);
  Object.assign(store, {
    me, httpKeys: info.httpKeys, version: info.version, nodes, bricks, matrix: matrix.groups, deployments, queue,
    last: new Map(last.map((r) => [r.pipelineId, r])),
  });
  emit("all");
  refreshHealth();
  // ?static renders once without the live stream (a headless browser waits on it forever).
  if (new URLSearchParams(location.search).has("static")) return;

  const source = new EventSource("/api/events");
  source.onopen = () => { store.connected = true; emit("connection"); };
  source.onerror = () => { store.connected = false; emit("connection"); };
  source.onmessage = (message) => {
    const { type, data, at } = JSON.parse(message.data);
    switch (type) {
      case "nodes":
        store.nodes = data;
        clearTimeout(bricksTimer);
        bricksTimer = setTimeout(refreshBricks, 300);
        emit("nodes");
        refreshHealth();
        break;
      case "deployments": store.deployments = data; emit("deployments"); refreshHealth(); break;
      case "run": onRun(data); break;
      case "deploy":
        if (data.state === "failed") addLog({ node: "coordinator", level: "Error", message: `${data.pipelineId}: deployment failed: ${data.message}` });
        refreshHealth();
        break;
      case "log":
        // `runId` is the run of the fragment that logged, as its node said it; a line the coordinator writes has none.
        addLog({ node: data.node, fragment: data.fragment, level: data.level, message: data.message, ...(typeof data.runId === "string" && data.runId ? { runId: data.runId } : {}), ...(at ? { at: new Date(at) } : {}) });
        break;
      case "bytes": store.bytes = data; emit("bytes"); break;
      case "library": emit("library"); refreshHealth(); break;
      case "rights":
        api("/api/flow-matrix").then((m) => { store.matrix = m.groups; emit("rights"); }).catch(() => emit("rights"));
        refreshHealth();
        break;
    }
  };
}

/** What became of the cursors a run reported: the value each kept, or why one did not advance. */
export function cursorsOf(run) {
  if (!run.cursors?.length) return "";
  const rows = run.cursors.map((c) => c.kept
    ? `<li>cursor of <b>${esc(c.alias)}</b> kept at <code>${esc(c.value)}</code></li>`
    : `<li class="mismatch">${esc(c.reason)}</li>`).join("");
  return `<ul class="msgs">${rows}</ul>`;
}

// The plan a run ran, as the coordinator journaled it when the run started: the runner it chose, each fragment at its version,
// and each binding with the brick version it met and what its mode found.
export function ranPlan(p) {
  if (!p) return "";
  const fragments = p.fragments.map((f) => `<li><span class="mono">${esc(f.name)}</span> on ${esc(f.node)} <span class="hint">${esc(f.version.slice(0, 12))}</span></li>`).join("");
  const bindings = p.bindings.map((b) => `<li><span class="mono">${esc(b.alias)}</span> → brick ${esc(b.brick)} on ${esc(b.node)} <span class="hint">${esc(b.brickVersion.slice(0, 12))} · ${esc(b.mode)}: ${esc(b.verdict)}</span></li>`).join("");
  return `<p><b>Plan it ran</b> <span class="hint">${esc(p.planId)}${p.runner ? ` · runner ${esc(p.runner)}` : ""}</span></p>
    <ul class="msgs">${fragments}</ul>${bindings ? `<ul class="msgs">${bindings}</ul>` : ""}`;
}
