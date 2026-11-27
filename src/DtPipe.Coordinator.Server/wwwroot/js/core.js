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
let tipSeq = 0;
let activeTip = null;
let pinnedTip = null;
let hideTimer = null;
let pointerOpening = false;
let ignoreFocusScrollUntil = 0;

/** Puts a tip beside its anchor, kept in the window: the tip is fixed so no scrolling card clips it. */
function placeTip(anchor, tip) {
  const box = anchor.getBoundingClientRect();
  const left = Math.max(8, Math.min(box.left - 12, window.innerWidth - tip.offsetWidth - 8));
  const below = box.bottom + 6;
  const top = below + tip.offsetHeight > window.innerHeight - 8 ? Math.max(8, box.top - tip.offsetHeight - 6) : below;
  tip.style.left = `${left}px`;
  tip.style.top = `${top}px`;
}

function openTip(trigger, pin = false) {
  const tip = trigger.querySelector(".info-text");
  if (!tip) return;
  clearTimeout(hideTimer);
  hideTimer = null;
  if (activeTip && activeTip !== trigger) closeTip(activeTip);
  activeTip = trigger;
  if (pin) pinnedTip = trigger;
  trigger.setAttribute("aria-expanded", "true");
  placeTip(trigger, tip);
}

function closeTip(trigger = activeTip) {
  clearTimeout(hideTimer);
  hideTimer = null;
  ignoreFocusScrollUntil = 0;
  if (!trigger) return;
  trigger.setAttribute("aria-expanded", "false");
  if (activeTip === trigger) activeTip = null;
  if (pinnedTip === trigger) pinnedTip = null;
}

function scheduleClose(trigger) {
  clearTimeout(hideTimer);
  hideTimer = setTimeout(() => {
    if (activeTip === trigger && pinnedTip !== trigger && !trigger.matches(":focus-within")) closeTip(trigger);
  }, 150);
}

const tipTriggerOf = (target) => (target instanceof Element ? target.closest(".info, .info-chip") : null);

document.addEventListener("pointerover", (e) => {
  if (e.pointerType === "touch") return;
  const trigger = tipTriggerOf(e.target);
  if (trigger) openTip(trigger, false);
});

document.addEventListener("pointerout", (e) => {
  if (e.pointerType === "touch" || !activeTip) return;
  const related = e.relatedTarget instanceof Node ? e.relatedTarget : null;
  if (related && activeTip.contains(related)) return;
  scheduleClose(activeTip);
});

document.addEventListener("pointerdown", (e) => {
  const target = e.target instanceof Element ? e.target : null;
  if (tipTriggerOf(target) && !target?.closest(".info-text")) {
    pointerOpening = true;
    setTimeout(() => { pointerOpening = false; }, 0);
  }
});

document.addEventListener("focusin", (e) => {
  if (pointerOpening) return;
  const trigger = tipTriggerOf(e.target);
  if (trigger) {
    ignoreFocusScrollUntil = performance.now() + 80;
    openTip(trigger, false);
  }
});

document.addEventListener("focusout", (e) => {
  if (!activeTip || pinnedTip === activeTip) return;
  const related = e.relatedTarget instanceof Node ? e.relatedTarget : null;
  if (related && activeTip.contains(related)) return;
  closeTip(activeTip);
});

document.addEventListener("click", (e) => {
  pointerOpening = false;
  const target = e.target instanceof Element ? e.target : null;
  if (target?.closest(".info-text")) return;
  const trigger = tipTriggerOf(target);
  if (trigger) {
    if (pinnedTip === trigger || (e.detail === 0 && activeTip === trigger)) closeTip(trigger);
    else openTip(trigger, true);
  } else if (activeTip) {
    closeTip(activeTip);
  }
});

document.addEventListener("keydown", (e) => {
  if (e.key === "Escape" && activeTip) {
    e.preventDefault();
    e.stopPropagation();
    closeTip(activeTip);
  }
}, true);

document.addEventListener("scroll", (e) => {
  if (!activeTip) return;
  const target = e.target instanceof Node ? e.target : null;
  if (target instanceof Element && target.closest(".info-text")) return;
  if (performance.now() < ignoreFocusScrollUntil && target?.contains(activeTip)) {
    const tip = activeTip.querySelector(".info-text");
    if (tip) placeTip(activeTip, tip);
    return;
  }
  closeTip(activeTip);
}, { capture: true, passive: true });

window.addEventListener("resize", () => { if (activeTip) closeTip(activeTip); });

/** A ⓘ right after the text it explains: opens on hover, focus, or click and dismisses on Escape or scroll. */
export function info(text, { label = "More information", heading = "", preline = false } = {}) {
  const tipId = `info-tip-${++tipSeq}`;
  const body = preline ? `<span class="lib-header">${esc(text)}</span>` : esc(text);
  return `<button type="button" class="info" aria-label="${esc(label)}" aria-expanded="false" aria-describedby="${tipId}"><span aria-hidden="true">ⓘ</span><span id="${tipId}" class="info-text" role="tooltip">${heading ? `<b>${esc(heading)}</b>` : ""}${body}</span></button>`;
}

/** A status chip button with a rich tooltip explaining why it has that state. */
export function infoChip(label, cls, text, heading = "Why") {
  const tipId = `info-tip-${++tipSeq}`;
  return `<button type="button" class="info-chip ${cls}" aria-expanded="false" aria-describedby="${tipId}">${esc(label)}<span id="${tipId}" class="info-text" role="tooltip">${heading ? `<b>${esc(heading)}</b>` : ""}${esc(text)}</span></button>`;
}
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

// How the pages say a level of the data plane, keyed by the word the server sends. `sealed` says whether the level seals the
// transfers and `keyed` whether a pre-shared key is behind it. A level this table has no entry for reads as the server wrote it,
// with `sealed` null; none given at all is unknown, and the pages say neither "in clear" nor "sealed" of it.
const PROTECTION_WORDS = new Map([
  ["none", { text: "in clear", sealed: false, keyed: false }],
  ["encrypted", { text: "sealed", sealed: true, keyed: false }],
  ["encrypted-psk", { text: "sealed, key", sealed: true, keyed: true }],
]);

/** What a level of the data plane reads as, or null when the server gave none: unknown. */
export function protectionOf(level) {
  if (level == null || level === "") return null;
  return PROTECTION_WORDS.get(level) ?? { text: String(level), sealed: null, keyed: false };
}

/** The words of a level, "unknown" when none is given. A level with a key and a fingerprint announced for it ends with it, cut short. */
export function protectionWord(level, keyId = null) {
  const known = protectionOf(level);
  if (!known) return "unknown";
  return known.keyed && keyId ? `${known.text} ${String(keyId).slice(0, 8)}…` : known.text;
}

// What a run's cause names, by the `causeKind` word the server gives it. An edge and a receiver are said as such; any other kind,
// a kind this table has no entry for and a result with none read as a fragment or a sentence.
const CAUSE_KINDS = new Map([
  ["edge", { word: "edge", hint: "An edge between two fragments failed end-to-end encryption." }],
  ["mute-receiver", { word: "mute receiver", hint: "The cause is a receiver that stopped acknowledging what its sender sent." }],
]);

/** The word that says a run's cause is an edge or a receiver, as a chip and a space; nothing for any other cause. */
export function causeKindChip(run) {
  const kind = CAUSE_KINDS.get(run?.result?.causeKind);
  return kind ? `<span class="chip" title="${esc(kind.hint)}">${esc(kind.word)}</span> ` : "";
}

/** The cause of a run in one line: the server's reason, else the cause itself, whole when it is an edge and by its node when it is a fragment. */
export const causeLine = (run) => run.reason ?? (run.result?.cause ? (run.result.causeKind === "edge" ? run.result.cause : shortNode(run.result.cause)) : "");

/** The rows a run's edges delivered, counted on every edge: a row that crosses two edges counts twice. */
export const rowsCrossed = (run) => run?.result?.edgeCounts?.reduce((s, e) => s + (e.received ?? 0), 0) || 0;
export const ROWS_CROSSED_HINT = "Counted per edge: a row crossing two edges counts twice.";

/** What a run says of why it ended as it did, and of what it may have left behind: the same words wherever a run is shown. */
export function endNotes(run) {
  const r = run.result;
  const cause = run.reason ?? (r?.cause ? `${causeLine(run)}${r.causeIsUnresponsive ? " (unresponsive)" : ""}` : null);
  // The reason of an edge or of a receiver names it already.
  const where = run.reason && r?.cause && run.reason !== r.cause && !CAUSE_KINDS.has(r.causeKind) ? ` <span class="hint">(${esc(shortNode(r.cause))})</span>` : "";
  const started = run.plan != null && ["failed", "cancelled"].includes(run.state);
  return [
    run.refusal && run.refusal !== run.reason ? `<p class="mismatch">${esc(run.refusal)}</p>` : "",
    cause ? `<p><b>Cause:</b> ${causeKindChip(run)}${esc(cause)}${where}</p>` : "",
    run.sinkMayBePartial
      ? `<p class="mismatch">Interrupted by a coordinator restart: outputs may be partial.</p>`
      : started ? `<p class="hint">Stopped early: outputs may be partial, or empty.</p>` : "",
  ].join("");
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
  return `Requires ${role === "designer" ? "admin" : "designer or admin"}${role ? ` (you are ${role})` : ""}.`;
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

/** The lines this page holds that name run `runId`. The id is the node's word, never checked: a line is attached by equality and nothing else. */
export const linesOfRun = (runId) => store.logs.filter((l) => l.runId === runId);

/**
 * The errors and the warnings among the lines that arrived since the Events view was last open (never opened: every line held),
 * but the backfill: a line the server replayed when the page first connected is not news, and one it replays after a cut that
 * the page did not hold is.
 */
export function unseenLogs() {
  let errors = 0, warnings = 0;
  for (const l of store.logs) {
    if (l.n <= seenSeq || l.backfill) continue;
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

// What the stream numbers: each line it sends carries a `seq` under the `boot` of the process that numbered it, and each connection
// replays the last lines of its backlog. A line is new to the page when its `boot` is another, or its `seq` is past the greatest the
// page took under this one; a line of a server that numbers nothing is always new. This lives here, not in `store.logs`, which
// Clear empties and the capacity cuts: a line dropped from there must not come back at the next connection.
let streamBoot = null;
let streamSeq = 0;
function isNewLine({ seq, boot }) {
  if (!Number.isSafeInteger(seq) || typeof boot !== "string" || !boot) return true;
  if (boot !== streamBoot) { streamBoot = boot; streamSeq = seq; return true; }
  if (seq <= streamSeq) return false;
  streamSeq = seq;
  return true;
}

// How many times the stream opened. Every opening reads the state again; `opened` only tells the first, the page's first connection,
// whose replayed log lines are its backfill, from the ones after a cut.
let opened = 0;

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

// What the page reads of the server to hold its state: at start, and again each time the stream opens. Each entry is a
// path and where its answer lands in the store. `me` and `info` are read at start alone: no event of the stream moves them.
const STATE_READS = [
  ["/api/nodes", (nodes) => ({ nodes })],
  ["/api/bricks", (bricks) => ({ bricks })],
  ["/api/flow-matrix", (matrix) => ({ matrix: matrix.groups })],
  ["/api/deployments", (deployments) => ({ deployments })],
  ["/api/runs/last", (last) => ({ last: new Map(last.map((r) => [r.pipelineId, r])) })],
  ["/api/runs/queue", (queue) => ({ queue })],
];

/** The store's fields as the server holds them now. */
async function readState() {
  return Object.assign({}, ...(await Promise.all(STATE_READS.map(async ([path, put]) => put(await api(path))))));
}

// The state the page holds can be older than the server's: `start()` reads it before the stream exists, and a cut leaves it as it was when
// the stream dropped, so a run that ended meanwhile still shows in flight. Every opening of the stream reads the state again, the first
// included: the server subscribes to its bus before it writes the stream's first byte, so an event published after the opening's
// subscription arrives on the stream and one published before it is in the answers. The page holds the events that arrive meanwhile, for
// an answer may be older than an event it overlaps and would undo it. The held events are applied over the answers in the order they
// arrived; a log line is no state and never waits. One resync owns the held events at a time: a stream that opens again while one is
// reading hands them to the newer, whose answers alone are applied.
// A read that does not answer in time or fails leaves the state as the events made it, and says so.
const RESYNC_TIMEOUT = 15000;
// What the views are told once the state is read again, at each opening of the stream. A view that loads its own data (the library, the rights, the journal) does it on its kind.
const RESYNC_KINDS = ["nodes", "bricks", "deployments", "runs", "journal", "library", "rights"];
let held = null;
let resyncs = 0;

async function resync() {
  const mine = ++resyncs;
  held ??= [];
  let patch = null, failure = null, timer;
  try {
    patch = await Promise.race([readState(), new Promise((_, no) => { timer = setTimeout(no, RESYNC_TIMEOUT, new Error("no answer in time")); })]);
  } catch (e) { failure = e; } finally { clearTimeout(timer); }
  if (mine !== resyncs) return;
  const events = held;
  held = null;
  if (patch) Object.assign(store, patch);
  for (const event of events) onEvent(event);
  if (failure) toast(`State may be stale: ${failure.message}`);
  for (const kind of RESYNC_KINDS) emit(kind);
  refreshHealth();
}

export async function start() {
  const [me, info, state] = await Promise.all([api("/api/me"), api("/api/info"), readState()]);
  Object.assign(store, { me, httpKeys: info.httpKeys, version: info.version, ...state });
  emit("all");
  refreshHealth();
  // ?static renders once without the live stream (a headless browser waits on it forever).
  if (new URLSearchParams(location.search).has("static")) return;

  const source = new EventSource("/api/events");
  source.onopen = () => {
    store.connected = true;
    emit("connection");
    opened++;
    resync();
  };
  source.onerror = () => { store.connected = false; emit("connection"); };
  source.onmessage = (message) => {
    const event = JSON.parse(message.data);
    if (held && event.type !== "log") held.push(event); else onEvent(event);
  };
}

function onEvent({ type, data, at, seq, boot, replay }) {
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
      if (!isNewLine({ seq, boot })) break;
      // `runId` is the run of the fragment that logged, as its node said it; a line the coordinator writes has none.
      // The lines replayed to the page's first connection are its backfill: held, never counted as news.
      addLog({ node: data.node, fragment: data.fragment, level: data.level, message: data.message, ...(typeof data.runId === "string" && data.runId ? { runId: data.runId } : {}), ...(at ? { at: new Date(at) } : {}), ...(replay === true && opened === 1 ? { backfill: true } : {}) });
      break;
    case "library": emit("library"); refreshHealth(); break;
    case "rights":
      api("/api/flow-matrix").then((m) => { store.matrix = m.groups; emit("rights"); }).catch(() => emit("rights"));
      refreshHealth();
      break;
  }
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
