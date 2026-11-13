// @ts-check
// The agent's panel: one conversation with the model the command line named, read by polling GET /api/agent. The server
// says everything the panel shows (steps, verdict, question, the card waiting for the person, the plan); the page sends the
// prompt, the open job's cards and the person's answers, and writes no YAML. A write the agent asks for waits for an answer
// here: nothing is saved or run until the person clicks.

import { $, api, el, fill, status } from "./dom.js";
import { invalidate, onZone } from "./render.js";
import { state } from "./state.js";

const POLL_MS = 800;
const CHOICE_LABEL = {
  save: "Save", run: "Run", refuse: "Refuse",
  send: "Send anyway", omit: "Send without the job", cancel: "Cancel the turn", withhold: "Keep it here",
};

let hooks = {};
let pollTimer = null;
let polling = false;
let shown = 0;

onZone("agent", renderAgent);

/**
 * Builds the panel in `pane` when this studio has an agent. The host gives what only it knows: `applyPlan(yaml)` puts a plan
 * on the open job's cards, `openPlan(yaml)` opens one as a new job, `reopen(path)` reads a saved job again.
 * @param {HTMLElement} pane @param {{ applyPlan: (yaml: string) => Promise<void>, openPlan: (yaml: string) => Promise<void>, reopen: (path: string) => Promise<void> }} host
 */
export async function mountAgent(pane, host) {
  hooks = host;
  const first = await api("/api/agent?after=0");
  if (!first.enabled) return;
  state.agent.enabled = true;
  pane.hidden = false;
  pane.parentElement?.classList.add("with-agent");
  fill(pane,
    el("div", { class: "row agent-head" },
      el("h2", {}, "Agent"),
      el("select", { id: "agent-model", title: "The model the provider lists; another one starts a new conversation" }),
      el("button", { class: "small ghost", title: "Forget this conversation", onclick: reset }, "new")),
    el("p", { id: "agent-warning", class: "hint warn", hidden: true }),
    el("div", { id: "agent-log", class: "agent-log" }),
    el("div", { id: "agent-now" }),
    el("div", { class: "agent-input" },
      el("textarea", { id: "agent-prompt", rows: "3", placeholder: "Ask the agent… (Ctrl+Enter sends)", onkeydown: onPromptKey }),
      el("div", { class: "row" },
        el("label", { class: "hint", title: "The open job's text reaches the model with your message" },
          el("input", { id: "agent-attach", type: "checkbox", checked: true }), " join the open job"),
        el("button", { id: "agent-send", class: "primary", onclick: () => send() }, "Send"),
        el("button", { id: "agent-stop", class: "danger", hidden: true, onclick: stop }, "Stop"))),
    el("div", { id: "agent-note-row", class: "row", hidden: true },
      el("input", { id: "agent-note", placeholder: "A note for the trace" }),
      el("button", { class: "small", onclick: note }, "note")));
  accept(first);
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

/** What the server answered: new entries are appended to the log, the rest is drawn by the zone. @param {any} answer */
function accept(answer) {
  if (answer.count < shown) { fill($("agent-log")); shown = 0; }
  const log = $("agent-log");
  for (const entry of answer.entries || []) log.append(entryView(entry));
  shown = answer.count;
  if (answer.entries?.length) log.scrollTop = log.scrollHeight;
  for (const entry of answer.entries || []) if (entry.kind === "step" && entry.tool === "save-job") savedJob(entry.result);
  state.agent.status = answer;
  invalidate("agent");
}

/** A job the agent saved is read again when it is the one open and holds no change of the person's. */
function savedJob(result) {
  try {
    const saved = JSON.parse(result || "{}").saved;
    if (saved && saved === state.job?.path && !state.cards.dirty) hooks.reopen(saved);
  } catch { /* a result cut at its bound is no JSON: nothing to read again */ }
}

function entryView(entry) {
  if (entry.kind === "user") return el("div", { class: "msg user" }, entry.text);
  if (entry.kind === "verdict") {
    return el("div", { class: `verdict ${entry.status === "completed" ? "good" : "warn"}` },
      [entry.status, entry.reason, entry.tools, entry.duration].filter(Boolean).join(" · "));
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
  const warning = $("agent-warning");
  warning.hidden = s.local;
  warning.textContent = s.local ? "" : `The model is at ${s.endpoint}: what its tools read (schemas, rows) and the open job go there.`;
  $("agent-send").toggleAttribute("disabled", s.busy);
  $("agent-stop").hidden = !s.busy;
  $("agent-note-row").hidden = !s.trace;
  fill($("agent-now"),
    s.busy && s.live ? el("div", { class: "live hint" }, el("b", {}, `step ${s.live.step}${s.live.tool ? ` · ${s.live.tool}` : ""}`), el("pre", {}, s.live.text)) : null,
    s.busy && !s.live && !s.pending ? el("div", { class: "live hint" }, "working…") : null,
    s.pending ? cardView(s.pending) : null,
    !s.busy && s.question ? questionView(s.question) : null,
    !s.busy && s.plan?.yaml ? planView(s.plan) : null);
}

/** The card a write waits on: what it would do, and the person's choices. */
function cardView(card) {
  const b = card.body || {};
  const text = card.kind === "save"
    ? [el("p", {}, `The agent asks to save ${b.path} (${b.replaces ? "replaces the file" : "a new file"}).`),
      b.rewrites ? el("p", { class: "hint" }, "The studio composes the job: comments and key order of the agent's text are not kept.") : null,
      el("pre", {}, b.yaml)]
    : card.kind === "run"
      ? [el("p", {}, `The agent asks to run ${b.path} for real${b.sample ? " (a sample: nothing real is written)" : ""}. It writes:`),
        el("ul", {}, (b.writes || []).map((w) => el("li", {}, `${w.alias} → ${w.throwaway ? "a throwaway file" : w.output}`, w.block ? el("pre", {}, w.block) : null)))]
      : [el("p", {}, `${b.path || "The open job"} holds credentials as values at: ${(b.at || []).join(", ")}.`),
        el("p", { class: "hint" }, `They would reach the model at ${state.agent.status?.endpoint}.`)];
  return el("div", { class: "agent-card" }, text,
    el("div", { class: "row" }, card.choices.map((choice) =>
      el("button", { class: choice === "refuse" || choice === "cancel" ? "" : choice === "run" || choice === "send" ? "danger" : "primary", onclick: () => answer(card, choice) },
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
      state.job ? el("button", { class: "small", title: "Replace the open job's cards with the plan; nothing is saved", onclick: () => hooks.applyPlan(plan.yaml) }, "Apply to the open job") : null,
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
  const attach = /** @type {HTMLInputElement} */ ($("agent-attach")).checked;
  const model = /** @type {HTMLSelectElement} */ ($("agent-model")).value || undefined;
  const job = attach && state.cards.model
    ? { path: state.job?.path ?? null, unsaved: state.cards.dirty || !state.job?.hash, model: state.cards.model }
    : null;
  try {
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
    const answer = await api(`/api/agent?after=${shown}`);
    accept(answer);
    if (answer.busy || answer.pending) pollTimer = setTimeout(poll, POLL_MS);
  } catch (e) {
    status(e.message, "bad");
  } finally {
    polling = false;
  }
}
