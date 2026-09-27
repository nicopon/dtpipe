// What every view shares: the API client, the live store fed by /api/events, and small helpers.
// The coordinator decides everything; the views only draw the store and send requests.

export const store = {
  nodes: [],
  bricks: [],
  matrix: {},
  deployments: [],
  runs: [],          // the journal, newest first
  queue: [],         // the run in flight, then the queued ones
  logs: [],
  bytes: { total: 0, activeTransfers: 0 },
  connected: false,
  draft: null,       // { id, model }: a pipeline imported but not saved yet, handed to the designer
};

const listeners = new Set();
export function subscribe(fn) { listeners.add(fn); return () => listeners.delete(fn); }
function emit(kind) { for (const fn of listeners) { try { fn(kind); } catch (e) { console.error(e); } } }

export const $ = (id) => document.getElementById(id);
export const esc = (s) => String(s ?? "").replace(/[&<>"']/g, (c) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[c]));
export const fmt = (n) => (n ?? 0).toLocaleString("en-US");
export const nodeColor = (node) => (node ? `var(--${node}, var(--muted))` : "var(--muted)");
export const shortNode = (fragment) => String(fragment ?? "").split("@").pop();

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
  const init = { method: method || (body === undefined ? "GET" : "POST") };
  if (body !== undefined) { init.headers = { "Content-Type": "application/json" }; init.body = JSON.stringify(body); }
  const response = await fetch(path, init);
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

export function csvTable(csv, limit = 200) {
  const [header, ...rows] = parseCsv(csv || "");
  if (!header) return `<p class="hint">No rows.</p>`;
  return `<div class="table-wrap"><table><tr>${header.map((h) => `<th>${esc(h)}</th>`).join("")}</tr>${rows.slice(0, limit).map((r) => `<tr>${r.map((v) => `<td>${esc(v)}</td>`).join("")}</tr>`).join("")}</table></div>`;
}

export function deploymentOf(pipelineId) { return store.deployments.find((d) => d.pipelineId === pipelineId) || null; }
export function lastRunOf(pipelineId) {
  return store.queue.find((r) => r.pipelineId === pipelineId) || store.runs.find((r) => r.pipelineId === pipelineId) || null;
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
      .sort((a, b) => (a.state === "running" ? -1 : b.state === "running" ? 1 : new Date(a.queuedAt) - new Date(b.queuedAt)));
  } else {
    store.queue = store.queue.filter((r) => r.runId !== run.runId);
    store.runs = [run, ...store.runs.filter((r) => r.runId !== run.runId)].slice(0, 200);
    addLog({ node: "coordinator", level: run.state === "succeeded" ? "Information" : "Warning", message: `${run.runId} ${run.pipelineId ?? ""} ${run.state}${run.refusal ? `: ${run.refusal}` : ""}` });
  }
  emit("runs");
}

export async function start() {
  const [nodes, bricks, matrix, deployments, runs, queue] = await Promise.all([
    api("/api/nodes"), api("/api/bricks"), api("/api/flow-matrix"), api("/api/deployments"), api("/api/runs"), api("/api/runs/queue"),
  ]);
  Object.assign(store, { nodes, bricks, matrix: matrix.groups, deployments, runs, queue });
  emit("all");
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
        break;
      case "deployments": store.deployments = data; emit("deployments"); break;
      case "run": onRun(data); break;
      case "deploy":
        if (data.state === "failed") addLog({ node: "coordinator", level: "Error", message: `${data.pipelineId}: deployment failed: ${data.message}` });
        break;
      case "fragment":
        addLog({ node: data.node, fragment: data.fragment, level: data.state === "Failed" ? "Warning" : "Information", message: `${data.state}${data.exitCode != null ? ` (exit ${data.exitCode})` : ""}${data.message ? ` · ${data.message}` : ""}` });
        break;
      case "log": addLog({ node: data.node, fragment: data.fragment, level: data.level, message: data.message }); break;
      case "bytes": store.bytes = data; emit("bytes"); break;
      case "library": emit("library"); break;
    }
  };
}
