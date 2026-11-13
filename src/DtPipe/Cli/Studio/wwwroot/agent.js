// @ts-check
// The agent's panel: one conversation with the model the command line named, read by polling GET /api/agent. The server
// says everything the panel shows (steps, verdict, question, the card waiting for the person, the plan); the page sends the
// prompt, the open job's cards and the person's answers, and writes no YAML. A write the agent asks for waits for an answer
// here: nothing is saved or run until the person clicks.

import { $, api, el, fill, status } from "./dom.js";
import { applySizes, bindSplit } from "./designer.js";
import { invalidate, onZone } from "./render.js";
import { state } from "./state.js";

const POLL_MS = 800;
const CHOICE_LABEL = {
  save: "Save", run: "Run", refuse: "Refuse",
  send: "Send anyway", cancel: "Cancel the turn", withhold: "Keep it here",
};

const STATUS_LABEL = { completed: "completed", awaitinginput: "awaiting your answer", incomplete: "incomplete" };

let hooks = {};
let pollTimer = null;
let polling = false;
let shown = 0;
// The last plan of the agent the page took (or left): the next revision the server answers is a newer one.
let appliedRevision = 0;
// The composed text of the cards when the agent last read or wrote them: the person's edit since is not overwritten.
let synced = null;
// The plan text the page put on the cards: the button of a plan already there is not offered.
let appliedYaml = null;
// Plans are applied one after the other, each over the cards the last one left.
let applying = Promise.resolve();
// Whether the conversation follows its last entry: true until the person scrolls up to read.
let following = true;

onZone("agent", renderAgent);

/**
 * Builds the panel in `pane` when this studio has an agent. The host gives what only it knows: `applyPlan(yaml)` puts a plan
 * on the open job's cards, `openPlan(yaml)` opens one as a new job, `reopen(path)` reads a saved job again, `jobsChanged()`
 * reads the folder's jobs again.
 * @param {HTMLElement} pane @param {{ applyPlan: (yaml: string) => Promise<void>, openPlan: (yaml: string) => Promise<void>, reopen: (path: string) => Promise<void>, jobsChanged: () => Promise<void>, followPlan: (yaml: string) => Promise<boolean> }} host
 */
export async function mountAgent(pane, host) {
  hooks = host;
  const first = await api("/api/agent?after=0");
  if (!first.enabled) return;
  state.agent.enabled = true;
  pane.hidden = false;
  pane.parentElement?.classList.add("with-agent");
  fill(pane,
    el("div", { class: "split split-col split-agent", "data-split": "agent", role: "separator", "aria-orientation": "vertical", "aria-label": "Resize the agent", tabindex: "0" }),
    el("div", { class: "row agent-head" },
      el("h2", {}, "Agent"),
      el("select", { id: "agent-model", title: "The model the provider lists; another one starts a new conversation" }),
      el("button", { class: "small ghost", title: "Forget this conversation", onclick: reset }, "new")),
    el("p", { id: "agent-endpoint", class: "hint mono", title: "Set on the command line (--llm-provider, --llm-url): the page cannot change it" }),
    el("p", { id: "agent-meter", class: "hint mono", hidden: true, title: "The turn, its step, and the prompt the model read for its last call (what a turn adds to the conversation grows it)" }),
    el("p", { id: "agent-warning", class: "hint warn", hidden: true }),
    el("p", { id: "agent-behind", class: "hint warn", hidden: true }, "You changed the job while the agent worked: its plan is kept below, and replaces your cards only if you apply it."),
    el("div", { id: "agent-log", class: "agent-log" }),
    el("div", { id: "agent-now" }),
    el("div", { class: "agent-input" },
      el("textarea", { id: "agent-prompt", rows: "3", placeholder: "Ask the agent… (Ctrl+Enter sends)", onkeydown: onPromptKey }),
      el("div", { class: "row" },
        el("span", { class: "hint", title: "The open job goes with every message, and what the agent gives lands on its cards, unsaved" }, "works on the open job"),
        el("button", { id: "agent-send", class: "primary", onclick: () => send() }, "Send"),
        el("button", { id: "agent-stop", class: "danger", hidden: true, onclick: stop }, "Stop"))),
    el("div", { id: "agent-note-row", class: "row", hidden: true },
      el("input", { id: "agent-note", placeholder: "A note for the trace" }),
      el("button", { class: "small", onclick: note }, "note")));
  // Its width is the pane's own: the pointer's distance to the pane's right edge.
  bindSplit(/** @type {HTMLElement} */ (pane.querySelector(".split-agent")), "agent", (e) => pane.getBoundingClientRect().right - e.clientX);
  applySizes();
  appliedRevision = first.job?.revision ?? 0;
  accept(first, true);
  await loadModels();
  if (first.busy || first.pending) poll();
}

async function loadModels() {
  try {
    const { models, model } = await api("/api/agent/models");
    const select = /** @type {HTMLSelectElement} */ ($("agent-model"));
    const names = model && !models.includes(model) ? [model, ...models] : models;
    fill(select, names.map((name) => el("option", { value: name, selected: name === (state.agent.status?.model ?? model) }, name)));
  } catch (e) {
    status(e.message, "bad");
  }
}

/**
 * What the server answered: new entries are appended to the log, the rest is drawn by the zone. A save the person allowed is
 * read back, and a plan newer than the page's goes onto the cards; the entries a reload replays change nothing.
 * @param {any} answer @param {boolean} [replay]
 */
function accept(answer, replay = false) {
  if (answer.count < shown) { fill($("agent-log")); shown = 0; }
  const log = $("agent-log");
  following = log.scrollHeight - log.scrollTop - log.clientHeight < 40;
  for (const entry of answer.entries || []) log.append(entryView(entry));
  shown = answer.count;
  if (!replay) for (const entry of answer.entries || []) if (entry.kind === "step" && entry.saved) savedJob(entry.saved);
  if (!replay && answer.job?.yaml && answer.job.revision > appliedRevision) {
    const yaml = answer.job.yaml;
    applying = applying.then(() => follow(yaml)).catch((e) => status(e.message, "bad"));
  }
  appliedRevision = Math.max(appliedRevision, answer.job?.revision ?? 0);
  state.agent.status = answer;
  invalidate("agent");
}

/**
 * The folder lists a job the agent saved; it is read again when it is the one open, or when none is: the person saw this very
 * text on the card, so the page keeps no other hash than the file's and its next save finds the job it opened.
 * @param {string} saved
 */
function savedJob(saved) {
  hooks.jobsChanged();
  // A draft the agent opened and the person left alone gives way to the file the agent saved under another name.
  const draft = state.job && !state.job.hash && (state.cards.result?.yaml ?? null) === synced;
  if (!state.job || saved === state.job.path || draft) applying = applying.then(() => hooks.reopen(saved)).then(markSynced).catch((e) => status(e.message, "bad"));
}

const markSynced = () => { synced = state.cards.result?.yaml ?? null; };

/** A plan on the open job's cards, unless the person changed them since the agent last saw them: the button keeps it then. @param {string} yaml */
async function follow(yaml) {
  if (state.job && (state.cards.result?.yaml ?? null) !== synced) {
    state.agent.behind = true;
    invalidate("agent");
    return;
  }
  state.agent.behind = false;
  if (!(await hooks.followPlan(yaml))) return;
  appliedYaml = yaml;
  markSynced();
  status("the agent's plan is on the cards: save it to keep it", "good");
}

function entryView(entry) {
  if (entry.kind === "user") return el("div", { class: "msg user" }, entry.text);
  if (entry.kind === "notice") return el("div", { class: "verdict warn" }, entry.text);
  if (entry.kind === "verdict") {
    return el("div", { class: `verdict ${entry.status === "completed" ? "good" : "warn"}` },
      [STATUS_LABEL[entry.status] || entry.status, entry.reason, entry.tools, entry.duration].filter(Boolean).join(" · "));
  }
  if (!entry.tool) return el("div", { class: `msg agent${entry.error ? " bad" : ""}` }, entry.text);
  return el("details", { class: `step${entry.error ? " bad" : ""}` },
    el("summary", {}, `${entry.error ? "✗" : "·"} ${entry.tool}`),
    entry.text ? el("p", { class: "hint" }, entry.text) : null,
    entry.args ? el("pre", {}, entry.args) : null,
    entry.result ? el("pre", {}, entry.result) : null);
}

function renderAgent() {
  const s = state.agent.status;
  if (!state.agent.enabled || !s) return;
  $("agent-endpoint").textContent = `${s.provider} · ${s.endpoint}`;
  const warning = $("agent-warning");
  warning.hidden = s.local;
  warning.textContent = s.local ? "" : `The model is at ${s.endpoint}: what its tools read (schemas, rows) and the open job go there.`;
  renderMeter(s);
  $("agent-behind").hidden = !state.agent.behind;
  $("agent-send").toggleAttribute("disabled", s.busy);
  $("agent-stop").hidden = !s.busy;
  $("agent-note-row").hidden = !s.trace;
  fill($("agent-now"),
    s.busy && s.live ? el("div", { class: "live hint" }, el("b", {}, s.live.tool || "the model"), el("pre", {}, s.live.text)) : null,
    s.busy && !s.live && !s.pending ? el("div", { class: "live hint" }, "working…") : null,
    s.pending ? cardView(s.pending) : null,
    !s.busy && s.question ? questionView(s.question) : null,
    !s.busy && s.plan?.yaml ? planView(s.plan) : null);
  // A card drawn below the conversation takes its height: the last entry stays in sight.
  if (following) { const log = $("agent-log"); log.scrollTop = log.scrollHeight; }
}

const tokens = (n) => (n >= 1000 ? `${(n / 1000).toFixed(1)}k` : String(n));

/** What the turn has cost the conversation so far: its number, its step, the context the model last read. @param {any} s */
function renderMeter(s) {
  const m = s.meter;
  const meter = $("agent-meter");
  meter.hidden = !m || m.turn === 0;
  if (meter.hidden) return;
  meter.textContent = [
    `turn ${m.turn}`,
    s.busy ? `step ${m.steps + 1}` : `${m.steps} step${m.steps === 1 ? "" : "s"}`,
    m.contextTokens ? `context ${tokens(m.contextTokens)}${m.window ? ` / ${tokens(m.window)}` : ""} tokens` : null,
  ].filter(Boolean).join(" · ");
  meter.classList.toggle("warn", !!m.window && m.contextTokens > 0.85 * m.window);
}

/** The card a write waits on: what it would do, and the person's choices. */
function cardView(card) {
  const b = card.body || {};
  const text = card.kind === "save"
    ? [el("p", {}, `The agent asks to save ${b.path} (${b.replaces ? "replaces the file" : "a new file"}).`),
      b.rewrites ? el("p", { class: "hint" }, "The studio composes the job: comments and key order of the agent's text are not kept.") : null,
      el("pre", {}, b.yaml)]
    : [el("p", {}, `${b.path || "The open job"} holds credentials as values at: ${(b.at || []).join(", ")}.`),
        el("p", { class: "hint" }, `They would reach the model at ${state.agent.status?.endpoint}.`)];
  return el("div", { class: "agent-card" }, text,
    el("div", { class: "row" }, card.choices.map((choice) =>
      el("button", { class: choice === "refuse" || choice === "cancel" ? "" : choice === "send" ? "danger" : "primary", onclick: () => answer(card, choice) },
        CHOICE_LABEL[choice] || choice))));
}

function questionView(question) {
  return el("div", { class: "agent-card" }, el("p", {}, question.text),
    question.options?.length ? el("div", { class: "row wrap" }, question.options.map((o) => el("button", { class: "small", onclick: () => send(o) }, o))) : null);
}

function planView(plan) {
  return el("details", { class: "agent-plan" },
    el("summary", {}, `The agent's plan (${plan.state.toLowerCase()})`, plan.message ? el("span", { class: "hint" }, ` · ${plan.message}`) : null),
    el("pre", {}, plan.yaml),
    el("div", { class: "row" },
      state.job && plan.yaml !== appliedYaml ? el("button", { class: "small", title: "Replace the open job's cards with the plan; nothing is saved", onclick: () => hooks.applyPlan(plan.yaml) }, "Apply to the open job") : null,
      el("button", { class: "small", title: "Open the plan as a new job, not saved yet", onclick: () => hooks.openPlan(plan.yaml) }, "Open as a new job")));
}

async function answer(card, choice) {
  try {
    await api("/api/agent/answer", { id: card.id, choice, digest: card.digest });
  } catch (e) {
    status(e.message, "bad");
  }
  poll();
}

function onPromptKey(e) {
  if (e.key === "Enter" && (e.ctrlKey || e.metaKey)) { e.preventDefault(); send(); }
}

/** Sends a prompt, with the open job's cards when the person joins it: the server composes their text. @param {string} [text] */
async function send(text) {
  const prompt = /** @type {HTMLTextAreaElement} */ ($("agent-prompt"));
  const message = (text ?? prompt.value).trim();
  if (!message) return;
  const model = /** @type {HTMLSelectElement} */ ($("agent-model")).value || undefined;
  const job = state.cards.model
    ? { path: state.job?.path ?? null, unsaved: state.cards.dirty || !state.job?.hash, model: state.cards.model }
    : null;
  try {
    markSynced();
    state.agent.behind = false;
    await api("/api/agent/turn", { prompt: message, model, job });
    if (text === undefined) prompt.value = "";
  } catch (e) {
    status(e.message, "bad");
  }
  poll();
}

async function stop() {
  try { await api("/api/agent/stop", {}); } catch (e) { status(e.message, "bad"); }
  poll();
}

async function reset() {
  if (shown > 0 && !confirm("Forget this conversation?")) return;
  try { await api("/api/agent", undefined, "DELETE"); } catch (e) { status(e.message, "bad"); return; }
  poll();
}

async function note() {
  const input = /** @type {HTMLInputElement} */ ($("agent-note"));
  if (!input.value.trim()) return;
  try { await api("/api/agent/note", { text: input.value }); input.value = ""; } catch (e) { status(e.message, "bad"); }
}

/** Reads what changed since the last entry shown, again while a turn runs or a card waits. */
async function poll() {
  clearTimeout(pollTimer);
  if (polling) { pollTimer = setTimeout(poll, POLL_MS); return; }
  polling = true;
  try {
    const answer = await api(`/api/agent?after=${shown}&rev=${appliedRevision}`);
    accept(answer);
    if (answer.busy || answer.pending) pollTimer = setTimeout(poll, POLL_MS);
  } catch (e) {
    status(e.message, "bad");
  } finally {
    polling = false;
  }
}
