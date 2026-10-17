// What every view shares: the API client, the live store fed by /api/events, and small helpers.
// The coordinator decides everything; the views only draw the store and send requests.

export const store = {
  nodes: [],
  bricks: [],
  matrix: {},
  deployments: [],
  runs: [],          // the journal, newest first
  queue: [],         // the runs in flight, then the queued ones
  logs: [],
  bytes: { total: 0, activeTransfers: 0 },
  health: { status: "ok", pipelines: [] }, // whether each armed pipeline can be launched: /api/health, read again when its inputs move
  connected: false,
  labMode: false, // the server's own word: the Lab view and the query panel exist only then
  me: null,       // { name, email, roles }: who is signed in, and what the server lets them do
};

const listeners = new Set();
export function subscribe(fn) { listeners.add(fn); return () => listeners.delete(fn); }
function emit(kind) { for (const fn of listeners) { try { fn(kind); } catch (e) { console.error(e); } } }

export const $ = (id) => document.getElementById(id);
export const esc = (s) => String(s ?? "").replace(/[&<>"']/g, (c) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[c]));
export const fmt = (n) => (n ?? 0).toLocaleString("en-US");
export const nodeColor = (node) => (node ? `var(--${node}, var(--muted))` : "var(--muted)");
// A fragment reads "pipeline@node"; a cause that is a sentence (a refused transfer, a row-count
// mismatch) names fragments inside it and is shown whole.
export const shortNode = (fragment) => {
  const text = String(fragment ?? "");
  return text.includes(" ") ? text : text.split("@").pop();
};

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

export async function api(path, body, method) {
  // The CSRF header rides every call: the server refuses a change without it.
  const init = { method: method || (body === undefined ? "GET" : "POST"), headers: { "X-Dtpipe-Csrf": "1" } };
  if (body !== undefined) { init.headers["Content-Type"] = "application/json"; init.body = JSON.stringify(body); }
  const response = await fetch(path, init);
  // The session ended: open the page again, which signs the person in through the provider.
  if (response.status === 401) { location.reload(); throw new Error("signed out"); }
  const text = await response.text();
  let data = {};
  try { data = text ? JSON.parse(text) : {}; } catch { data = { error: text }; }
  if (!response.ok) throw new Error(data.error || data.title || `${response.status} ${response.statusText}`);
  return data;
}

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

export function addLog(line) {
  if (/:\s*$/.test(line.message)) return; // a label whose value was empty, e.g. a silent child's stderr
  store.logs.push({ ...line, at: new Date() });
  if (store.logs.length > 1500) store.logs.splice(0, store.logs.length - 1500);
  emit("logs");
}

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
  return store.queue.find((r) => r.pipelineId === pipelineId) || store.runs.find((r) => r.pipelineId === pipelineId) || null;
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
    store.runs = [run, ...store.runs.filter((r) => r.runId !== run.runId)].slice(0, 200);
    addLog({ node: "coordinator", level: run.state === "succeeded" ? "Information" : "Warning", message: `${run.runId} ${run.pipelineId ?? ""} ${run.state}${run.refusal ? `: ${run.refusal}` : ""}` });
  }
  emit("runs");
}

export async function start() {
  const [me, info, nodes, bricks, matrix, deployments, runs, queue] = await Promise.all([
    api("/api/me"), api("/api/info"), api("/api/nodes"), api("/api/bricks"), api("/api/flow-matrix"), api("/api/deployments"), api("/api/runs"), api("/api/runs/queue"),
  ]);
  Object.assign(store, { me, labMode: info.labMode, nodes, bricks, matrix: matrix.groups, deployments, runs, queue });
  emit("all");
  refreshHealth();
  // ?static renders once without the live stream (a headless browser waits on it forever).
  if (new URLSearchParams(location.search).has("static")) return;

  const source = new EventSource("/api/events");
  source.onopen = () => { store.connected = true; emit("connection"); };
  source.onerror = () => { store.connected = false; emit("connection"); };
  source.onmessage = (message) => {
    const { type, data } = JSON.parse(message.data);
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
      case "log": addLog({ node: data.node, fragment: data.fragment, level: data.level, message: data.message }); break;
      case "bytes": store.bytes = data; emit("bytes"); break;
      case "library": emit("library"); refreshHealth(); break;
      case "rights":
        api("/api/flow-matrix").then((m) => { store.matrix = m.groups; emit("rights"); }).catch(() => emit("rights"));
        refreshHealth();
        break;
    }
  };
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
