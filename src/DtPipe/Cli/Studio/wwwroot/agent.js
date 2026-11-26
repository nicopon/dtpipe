// @ts-check
// The agent's panel: one conversation with the model connected to the studio, read by polling GET /api/agent. The server
// says everything the panel shows (steps, verdict, question, the card waiting for the person, the plan); the page sends the
// prompt, the open job's cards and the person's answers, and writes no YAML. A write the agent asks for waits for an answer
// here: nothing is saved or run until the person clicks.

import { $, api, el, fill, status, svg } from "./dom.js";
import { applySizes, bindSplit, cardChanges, editingText, showTab } from "./designer.js";
import { openAgentConnectDialog } from "./dialogs.js";
import { invalidate, onZone } from "./render.js";
import { state } from "./state.js";

const POLL_MS = 800;
const CHOICE_LABEL = {
  save: "Save", run: "Run", refuse: "Refuse",
  send: "Send anyway", cancel: "Stop the turn", withhold: "Keep it here",
};

const STATUS_LABEL = { completed: "completed", awaitinginput: "awaiting your answer", incomplete: "incomplete", cancelled: "stopped", interrupted: "stopped" };

// How far the agent's plan has got, said as a sentence: the server's state is an identifier, never a label.
const PLAN_LABEL = {
  drafted: "not checked yet", validated: "checked, and the job is valid", invalid: "checked, and the job is refused",
  dryrunok: "tried on a few rows, nothing written", applied: "written", failed: "failed", saved: "saved in the folder",
};

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
// The text of the person's last message: a turn that ends without a result gives it back to the field.
let asked = null;
// Whether the conversation follows its last entry: true until the person scrolls up to read.
let following = true;
// The live zone: what the turn at work shows now (a step, a card, a question, a plan). The page holds the node,
// never looks it up in the log: the log empties and refills around it, and the agent's drawing puts it last.
const live = el("div", { id: "agent-now" });

onZone("agent", renderAgent);
onZone("agent", renderAgentLink);
onZone("agent", renderEmpty);

/**
 * Builds the panel in `pane`: the conversation when a model is connected, else an empty panel that offers to connect one.
 * The host gives what only it knows: `applyPlan(yaml)` puts a plan on the open job's cards, `openPlan(yaml)` opens one as a
 * new job, `reopen(path)` reads a saved job again, `jobsChanged()` reads the folder's jobs again.
 * @param {HTMLElement} pane @param {{ applyPlan: (yaml: string) => Promise<boolean>, openPlan: (yaml: string) => Promise<boolean>, reopen: (path: string, ask?: boolean) => Promise<boolean>, jobsChanged: () => Promise<void>, followPlan: (yaml: string) => Promise<boolean> }} host
 */
export async function mountAgent(pane, host) {
  hooks = host;
  pane.hidden = false;
  pane.parentElement?.classList.add("with-agent");
  fill(pane,
    el("div", { class: "split split-col split-agent", "data-split": "agent", role: "separator", "aria-orientation": "vertical", "aria-label": "Resize the agent", tabindex: "0" }),
    el("div", { class: "row agent-head" },
      el("h2", {}, "Agent"),
      el("select", { id: "agent-model", "aria-label": "Model", title: "The model the provider lists; another one starts a new conversation", onchange: changeModel }),
      el("button", { id: "agent-new", class: "small ghost", title: "Forget this conversation", onclick: reset }, "new")),
    el("div", { id: "agent-empty", class: "agent-empty", hidden: true }),
    el("div", { id: "agent-body", class: "agent-body", hidden: true },
      el("p", { id: "agent-endpoint", class: "hint mono", title: "Provider and address (change them with the header's model button)" }),
      el("p", { id: "agent-meter", class: "hint mono", hidden: true, title: "Turn · step · prompt tokens of the last call" }),
      el("p", { id: "agent-warning", class: "hint warn", hidden: true }),
      el("p", { id: "agent-behind", class: "hint warn", hidden: true }),
      el("div", { id: "agent-log", class: "agent-log" }, live),
      el("div", { class: "agent-input" },
        el("textarea", { id: "agent-prompt", "aria-label": "Message to the agent", rows: "3", placeholder: "Ask the agent… (Ctrl+Enter sends)", onkeydown: onPromptKey }),
        el("div", { class: "row" },
          el("span", { class: "hint", title: "The open job goes with every message; results land unsaved" }, "works on the open job"),
          el("button", { id: "agent-send", class: "primary", onclick: () => send() }, "Send"),
          el("button", { id: "agent-stop", class: "danger", hidden: true, onclick: stop }, "Stop")))));
  // Its width is the pane's own: the pointer's distance to the pane's right edge.
  bindSplit(/** @type {HTMLElement} */ (pane.querySelector(".split-agent")), "agent", (e) => pane.getBoundingClientRect().right - e.clientX);
  applySizes();
  $("agent-link").addEventListener("click", openConnect);
  await refreshAgent();
}

/**
 * Reads the connection and, when a model is connected, its conversation from the start: after the page connected, replaced or
 * forgot a model nothing of the old conversation stays, and the plans of the new one are read from none.
 */
async function refreshAgent() {
  clearTimeout(pollTimer);
  state.agent.connection = await api("/api/agent/connection");
  const first = await api("/api/agent?after=0");
  dropLog();
  synced = null;
  appliedRevision = first.job?.revision ?? 0;
  state.agent.enabled = !!first.enabled;
  state.agent.status = null;
  state.agent.problem = null;
  $("agent-body").hidden = !state.agent.enabled;
  $("agent-model").hidden = !state.agent.enabled;
  $("agent-new").hidden = !state.agent.enabled;
  $("agent-empty").hidden = state.agent.enabled;
  if (state.agent.enabled) {
    accept(first, true);
    await loadModels();
    if (first.busy || first.pending) poll();
  }
  invalidate("agent");
}

/** The connection dialog: what the server says of the connection now, and the page's way to connect, disconnect or delete a past one. */
async function openConnect() {
  let connection;
  try { connection = await api("/api/agent/connection"); } catch (e) { status(e.message, "bad"); return; }
  openAgentConnectDialog({
    connection,
    onConnect: async (body) => {
      if (shown > 0 && !confirm("Connecting a model starts a new conversation: forget this one?")) return false;
      const answer = await api("/api/agent/connection", body, "PUT");
      await refreshAgent();
      return answer;
    },
    onDisconnect: async () => {
      await api("/api/agent/connection/disconnect", {});
      await refreshAgent();
    },
    onForgetEntry: (entry) => api("/api/agent/connection/history", { provider: entry.provider, url: entry.url }, "DELETE"),
  });
}

async function loadModels() {
  try {
    const { models, model } = await api("/api/agent/models");
    const select = /** @type {HTMLSelectElement} */ ($("agent-model"));
    const names = model && !models.includes(model) ? [model, ...models] : models;
    fill(select, names.map((name) => el("option", { value: name, selected: name === (state.agent.status?.model ?? model) }, name)));
    state.agent.problem = model && !models.includes(model)
      ? { short: "model not listed", text: `${model} is not among the ${models.length} listed models: pick another.` }
      : null;
  } catch (e) {
    // A studio that does not answer is the header's studio line; the model's server is this one's.
    if (!e.transport) state.agent.problem = { short: "model's server not answering", text: e.message };
  }
  invalidate("agent");
}

/**
 * What the server answered: new entries are appended to the log, the rest is drawn by zone. A save the person allowed is
 * read back, and a plan newer than the page's goes onto the cards; the entries a reload replays change nothing. The panel
 * is drawn from the answer whatever fails on the way: a stale panel would show what is no longer the server's.
 * @param {any} answer @param {boolean} [replay]
 */
function accept(answer, replay = false) {
  const log = $("agent-log");
  following = log.scrollHeight - log.scrollTop - log.clientHeight < 40;
  try {
    for (const entry of answer.entries || []) {
      turnOf(entry).append(entryView(entry));
      shown++;
      if (entry.kind === "user") asked ??= entry.text;
      else if (!replay && entry.kind === "verdict") giveBack(entry.status);
    }
    if (!replay) for (const entry of answer.entries || []) if (entry.kind === "step" && entry.saved) savedJob(entry.saved);
    if (!replay && answer.job?.yaml && answer.job.revision > appliedRevision) {
      const yaml = answer.job.yaml;
      applying = applying.then(() => follow(yaml)).catch((e) => status(e.message, "bad"));
    }
    appliedRevision = Math.max(appliedRevision, answer.job?.revision ?? 0);
  } finally {
    state.agent.status = answer;
    invalidate("agent");
  }
}

/** The conversation the server reset: nothing of the old one stays on the page, which then reads the new one whole. */
function dropLog() {
  for (const turn of [...$("agent-log").getElementsByClassName("turn")]) turn.remove();
  shown = 0;
  following = true;
  appliedYaml = null;
  asked = null;
  state.agent.behind = null;
  state.agent.marks = null;
}

/** A turn that ended without a result gives the person's text back to the field, unless they typed another since: Send retries it. @param {string} status */
function giveBack(status) {
  if (status === "completed" || status === "awaitinginput" || asked === null) return;
  const prompt = /** @type {HTMLTextAreaElement} */ ($("agent-prompt"));
  if (!prompt.value.trim()) prompt.value = asked;
}

/**
 * The folder lists a job the agent saved; it is read again when it is the one open, or when none is: the person saw this very
 * text on the card, so the page keeps no other hash than the file's and its next save finds the job it opened. When the person
 * changed the open job's cards since the agent last read them, the cards stay and the file waits behind the notice's Apply.
 * @param {string} saved
 */
function savedJob(saved) {
  hooks.jobsChanged();
  applying = applying.then(async () => {
    // A file saved under another name is read only in place of a draft the agent opened and the person left alone.
    if (state.job && saved !== state.job.path && (state.job.hash || moved())) return;
    if (moved()) { hold({ saved }); return; }
    if (await hooks.reopen(saved)) markSynced();
  }).catch((e) => status(e.message, "bad"));
}

const markSynced = () => { synced = state.cards.result?.yaml ?? null; };

/**
 * Whether the person changed the open job's cards since the agent last read or wrote them, or holds a YAML text not applied:
 * what the agent gives then waits.
 */
const moved = () => !!state.job && ((state.cards.result?.yaml ?? null) !== synced || editingText());

/** What the agent gave, kept behind the notice's Apply: a plan, or the file it saved. @param {{ yaml?: string, saved?: string }} given */
function hold(given) {
  state.agent.behind = given;
  invalidate("agent");
}

/** The cards are what the agent last gave, or another job is open: no notice is left to apply. */
function settled() {
  markSynced();
  state.agent.behind = null;
  invalidate("agent");
}

/** The person's save replaced the file the agent saved: the notice no longer has it to apply. @param {string} path */
export function fileReplaced(path) {
  if (state.agent.behind?.saved !== path) return;
  state.agent.behind = null;
  invalidate("agent");
}

/** A plan on the open job's cards, unless the person changed them since the agent last saw them: the notice keeps it then. @param {string} yaml */
async function follow(yaml) {
  await mark(yaml);
  // The plan's state is the server's: one its check refused, or whose try failed, waits for a click and lands on the steps by none.
  if (refused(yaml) || failed(yaml)) return;
  if (moved()) { hold({ yaml }); return; }
  if (!(await hooks.followPlan(yaml))) return;
  appliedYaml = yaml;
  settled();
  status("the agent's plan is on the steps: save it to keep it", "good");
}

/**
 * The cards a plan adds, changes and removes against the open job's cards as they are now, both taken apart by the server, so
 * the page compares cards of one making and never reads YAML. With no job open there is nothing to mark.
 * @param {string} yaml
 */
async function mark(yaml) {
  const current = state.job && state.cards.model ? state.cards.result : null;
  if (!current) { state.agent.marks = null; return; }
  const none = { yaml, unknown: true, added: [], changed: [], removed: [] };
  if (current.errors?.length || !current.yaml) { state.agent.marks = none; return; }
  try {
    const [before, after] = await Promise.all([current.yaml, yaml].map((text) => api("/api/design/decompose", { yaml: text, layout: null })));
    state.agent.marks = before.errors?.length || after.errors?.length ? none : { yaml, ...cardChanges(before.model, after.model) };
  } catch {
    state.agent.marks = none;
  }
  invalidate("agent");
}

/** Applies what the person asked for, over their cards once they agree: a plan on the cards, or the saved file read again. @param {{ yaml?: string, saved?: string }} given */
async function apply(given) {
  if (!(await (given.saved ? hooks.reopen(given.saved, true) : hooks.applyPlan(given.yaml ?? "")))) return;
  if (given.yaml) appliedYaml = given.yaml;
  settled();
}

const lastTurn = () => /** @type {HTMLElement | undefined} */ ([...$("agent-log").getElementsByClassName("turn")].pop());

/** The block a turn's entries go in: a person's message opens one, and stays at the top of the log while that turn scrolls. @param {any} entry */
function turnOf(entry) {
  const last = lastTurn();
  if (last && entry.kind !== "user") return last;
  const turn = el("section", { class: "turn" });
  $("agent-log").append(turn);
  return turn;
}

const GLYPHS = {
  up: { label: "Good answer", path: "M14 9V5a3 3 0 0 0-3-3l-4 9v11h11.28a2 2 0 0 0 2-1.7l1.38-9a2 2 0 0 0-2-2.3zM7 22H4a2 2 0 0 1-2-2v-7a2 2 0 0 1 2-2h3" },
  down: { label: "Bad answer", path: "M10 15v4a3 3 0 0 0 3 3l4-9V2H5.72a2 2 0 0 0-2 1.7l-1.38 9a2 2 0 0 0 2 2.3zm7-13h2.67A2.31 2.31 0 0 1 22 4v7a2.31 2.31 0 0 1-2.33 2H17" },
  comment: { label: "Comment on this answer", path: "M21 15a2 2 0 0 1-2 2H7l-4 4V5a2 2 0 0 1 2-2h14a2 2 0 0 1 2 2z" },
};

/** The person's verdict on a turn, written to the trace: a thumb either way, or a comment. Active only while the trace is. @param {number} turn */
function feedbackView(turn) {
  /** @type {Record<string, HTMLElement>} */
  const buttons = {};
  const input = el("input", { "aria-label": "Your comment on this answer", placeholder: "Your comment on this answer", onkeydown: (e) => { if (e.key === "Enter") submit(); } });
  const form = el("div", { class: "feedback-comment", hidden: true }, input, el("button", { class: "small", onclick: () => submit() }, "Send"));
  const rate = async (rating) => {
    if (!(await sendFeedback(turn, rating))) return;
    buttons.up.classList.toggle("on", rating === "up");
    buttons.down.classList.toggle("on", rating === "down");
  };
  const submit = async () => {
    if (!input.value.trim() || !(await sendFeedback(turn, "comment", input.value))) return;
    input.value = "";
    form.hidden = true;
    buttons.comment.classList.add("on");
  };
  for (const [rating, glyph] of Object.entries(GLYPHS)) {
    const icon = svg("svg", { viewBox: "0 0 24 24", width: "14", height: "14", fill: "none", stroke: "currentColor", "stroke-width": "2", "stroke-linecap": "round", "stroke-linejoin": "round", "aria-hidden": "true" });
    icon.append(svg("path", { d: glyph.path }));
    buttons[rating] = el("button", {
      type: "button", class: "icon", "data-label": glyph.label, title: glyph.label, "aria-label": glyph.label,
      onclick: rating === "comment" ? () => { form.hidden = !form.hidden; if (!form.hidden) input.focus(); } : () => rate(rating),
    }, icon);
  }
  return [el("span", { class: "feedback-icons" }, Object.values(buttons)), form];
}

async function sendFeedback(turn, rating, text) {
  try { await api("/api/agent/feedback", { turn, rating, text }); return true; } catch (e) { status(e.message, "bad"); return false; }
}

function entryView(entry) {
  if (entry.kind === "user") return el("div", { class: "msg user" }, entry.text);
  if (entry.kind === "notice") return el("div", { class: "verdict warn" }, entry.text);
  if (entry.kind === "verdict") {
    const line = el("div", { class: `verdict ${entry.status === "completed" ? "good" : "warn"}` },
    [STATUS_LABEL[entry.status] || entry.status, entry.reason, entry.tools, entry.duration].filter(Boolean).join(" · "));
  return entry.turn ? el("div", { class: "verdict-row" }, line, feedbackView(entry.turn)) : line;
  }
  if (!entry.tool) return el("div", { class: `msg agent${entry.error ? " bad" : ""}` }, entry.text);
  return el("details", { class: `step${entry.error ? " bad" : ""}` },
    el("summary", {}, `${entry.error ? "✗" : "·"} ${entry.tool}`),
    entry.text ? el("p", { class: "hint" }, entry.text) : null,
    entry.args ? el("pre", {}, entry.args) : null,
    entry.result ? el("pre", {}, entry.result) : null);
}

// The header's button for the agent: the model and its provider, a warning when its server does not answer or the model is
// not among those listed, and what is missing when none is connected.
function renderAgentLink() {
  const host = $("agent-link");
  const connection = state.agent.connection;
  if (!state.agent.enabled) {
    const needsKey = connection?.needsKey;
    fill(host, el("span", { class: needsKey ? "dot warn" : "dot" }), needsKey ? " model: key needed" : " no agent");
    host.title = needsKey ? "Key not kept: connect again" : "Connect a model to ask an agent";
    return;
  }
  const problem = state.agent.problem;
  const model = state.agent.status?.model || connection?.model;
  fill(host, el("span", { class: problem ? "dot warn" : "dot good" }), ` ${problem ? problem.short : [model, state.agent.status?.provider ?? connection?.provider].filter(Boolean).join(" · ") || "ready"}`);
  host.title = problem?.text ?? "The model of the agent: click to connect another";
}

// The empty panel: nothing is connected, or a remembered model waits for its key.
function renderEmpty() {
  const host = $("agent-empty");
  if (state.agent.enabled) return;
  const needsKey = state.agent.connection?.needsKey;
  fill(host,
    el("p", { class: "hint" }, needsKey
      ? "The model you remembered needs its key again."
      : "No model connected. Connect Ollama or an OpenAI-compatible server."),
    el("button", { id: "agent-connect", class: "primary", onclick: openConnect }, needsKey ? "Reconnect…" : "Connect a model…"));
}

function renderAgent() {
  const s = state.agent.status;
  if (!state.agent.enabled || !s) return;
  // The live zone closes the last turn, so a turn's message and its card scroll together; with no turn it closes the log.
  const host = lastTurn() ?? $("agent-log");
  if (host.lastElementChild !== live) host.append(live);
  $("agent-endpoint").textContent = `${s.provider} · ${s.endpoint}`;
  const warning = $("agent-warning");
  const notes = [
    s.local ? null : `Sent to ${s.endpoint}: tool results (schemas, rows) and the open job.`,
    state.agent.problem?.text,
  ].filter(Boolean);
  warning.hidden = notes.length === 0;
  warning.textContent = notes.join(" ");
  renderMeter(s);
  renderBehind(state.agent.behind);
  $("agent-send").toggleAttribute("disabled", s.busy);
  $("agent-stop").hidden = !s.busy;
  for (const button of document.querySelectorAll("#agent-log .feedback-icons button")) {
    button.toggleAttribute("disabled", !s.trace);
    button.title = s.trace ? button.dataset.label : `${button.dataset.label}: start the studio with --agent-trace`;
  }
  fill(live,
    s.busy && s.live ? el("div", { class: "live hint" }, el("b", {}, s.live.tool || "the model"), el("pre", {}, s.live.text)) : null,
    s.busy && !s.live && !s.pending ? el("div", { class: "live hint" }, "working…") : null,
    s.pending ? cardView(s.pending) : null,
    !s.busy && s.question ? questionView(s.question) : null,
    !s.busy && s.plan?.yaml ? planView(s.plan) : null);
  // A card drawn below the conversation takes its height: the last entry stays in sight.
  if (following) { const log = $("agent-log"); log.scrollTop = log.scrollHeight; }
}

/** The notice of what waits for the person's Apply, with its button in sight. @param {{ yaml?: string, saved?: string } | null} given */
function renderBehind(given) {
  const notice = $("agent-behind");
  notice.hidden = !given;
  fill(notice, !given ? null : [
    given.saved
      ? `Job edited while the agent saved ${given.saved}: your steps are kept; Apply replaces them. `
      : "Job edited while the agent worked: your steps are kept; Apply replaces them. ",
    given.yaml ? marksView(given.yaml, false) : null,
    el("button", { class: "small", title: "Replace the open job's steps with what the agent gave", disabled: refused(given.yaml), onclick: () => apply(given) }, "Apply"),
    refused(given.yaml) ? el("span", { class: "hint" }, " The check refused this plan: it does not apply.") : null]);
}

/**
 * The cards a plan touches, each marked added, changed or removed: what Apply would do to the cards, or what the plan did to
 * those it came to. Nothing when the plan was not compared.
 * @param {string} yaml @param {boolean} applied
 */
function marksView(yaml, applied) {
  const marks = state.agent.marks;
  if (!marks || marks.yaml !== yaml) return null;
  if (marks.unknown) return el("span", { class: "plan-marks hint" }, "Changed steps cannot be named. ");
  const items = [
    ...marks.added.map((a) => el("span", { class: "mark mark-added" }, `${a} added`)),
    ...marks.changed.map((a) => el("span", { class: "mark mark-changed" }, `${a} changed`)),
    ...marks.removed.map((a) => el("span", { class: "mark mark-removed" }, `${a} removed`)),
  ];
  return el("span", { class: "plan-marks" },
    items.length ? [applied ? "Steps it changed: " : "Steps it would change: ", items, " "] : "It changes no step. ");
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
    ? [el("p", {}, `The agent asks to save ${b.path} (${b.replaces ? "replaces file" : "new file"}).`),
      b.rewrites ? el("p", { class: "hint" }, "Comments and key order are not kept.") : null,
      el("pre", {}, b.yaml)]
    : [el("p", {}, `${b.path || "The open job"} has credentials as values at: ${(b.at || []).join(", ")}.`),
        el("p", { class: "hint" }, `They would reach the model at ${state.agent.status?.endpoint}.`)];
  return el("div", { class: "agent-card" }, text,
    el("div", { class: "row agent-actions" }, card.choices.map((choice) =>
      el("button", { class: choice === "refuse" || choice === "cancel" ? "" : choice === "send" ? "danger" : "primary", onclick: () => answer(card, choice) },
        CHOICE_LABEL[choice] || choice))));
}

function questionView(question) {
  return el("div", { class: "agent-card" }, el("p", {}, question.text),
    question.options?.length ? el("div", { class: "row wrap" }, question.options.map((o) => el("button", { class: "small", onclick: () => send(o) }, o))) : null);
}

/**
 * Whether the check refused this text of the plan: then nothing puts it on the steps or opens it, since what it would give is no
 * job. The verdict is the server's (the plan's state), never judged again here. @param {string} [yaml]
 */
const refused = (yaml) => planIs(yaml, "invalid");

/** Whether the agent's try of this text of the plan (a sample, a save) failed: it does not arrive on the steps by itself, a click puts it there. @param {string} [yaml] */
const failed = (yaml) => planIs(yaml, "failed");

/** @param {string | undefined} yaml @param {string} wanted */
const planIs = (yaml, wanted) => !!yaml && state.agent.status?.plan?.yaml === yaml && state.agent.status.plan.state?.toLowerCase() === wanted;

/** The plan, its text folded and its buttons in sight; Apply is the notice's while the person's cards hold it back. */
function planView(plan) {
  const marks = state.agent.behind?.yaml === plan.yaml ? null : marksView(plan.yaml, plan.yaml === appliedYaml && !moved());
  const blocked = refused(plan.yaml);
  return el("div", {},
    el("details", { class: "agent-plan" },
      el("summary", {}, `The agent's plan: ${PLAN_LABEL[plan.state.toLowerCase()] || plan.state.toLowerCase()}`, plan.message ? el("span", { class: "hint" }, ` · ${plan.message.split("\n")[0]}`) : null),
      el("pre", {}, plan.yaml)),
    marks ? el("p", {}, marks) : null,
    el("div", { class: "row agent-actions" },
      state.job && !state.agent.behind && (plan.yaml !== appliedYaml || moved()) ? el("button", { class: "small", title: "Replace the open job's steps with the plan; nothing is saved", disabled: blocked, "aria-describedby": blocked ? "agent-plan-refused" : null, onclick: () => apply({ yaml: plan.yaml }) }, "Apply to the open job") : null,
      el("button", { class: "small", title: "Open the plan as a new job, not saved yet", disabled: blocked, "aria-describedby": blocked ? "agent-plan-refused" : null, onclick: async () => { if (await hooks.openPlan(plan.yaml)) settled(); } }, "Open as a new job")),
    // The check's words keep their lines: a parser's error carries the line it read and a caret under the column.
    blocked ? el("div", { id: "agent-plan-refused", class: "hint" }, "The check refused this plan: nothing applied or opened",
      plan.message ? [":", el("pre", {}, plan.message)] : ".", " Ask the agent to correct it.")
      : failed(plan.yaml) && plan.yaml !== appliedYaml ? el("p", { class: "hint" }, "This plan's try failed: it waits until you apply or open it.") : null);
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
  // A message carries the cards: with a YAML text not applied, the agent would work on a job the person no longer sees.
  if (editingText()) { showTab("yaml"); status("The YAML is not applied: apply or revert it, then send.", "bad"); return; }
  // The log shows the message masked; a turn that fails gives back what was typed.
  asked = message;
  const model = /** @type {HTMLSelectElement} */ ($("agent-model")).value || undefined;
  const job = state.cards.model
    ? { path: state.job?.path ?? null, unsaved: state.cards.dirty || !state.job?.hash, model: state.cards.model }
    : null;
  try {
    settled();
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

async function reset() { await forget("Forget this conversation?"); }

/** Another model starts a new conversation, so the page asks as `new` does, and the choice goes back when refused. @param {Event} e */
async function changeModel(e) {
  const select = /** @type {HTMLSelectElement} */ (e.target);
  const current = state.agent.status?.model;
  if (shown > 0 && current && select.value !== current && !(await forget("Another model starts a new conversation: forget this one?"))) select.value = current;
}

/** Forgets the conversation once the person agrees (nothing to ask when there is none); false when it stays. @param {string} question */
async function forget(question) {
  if (shown > 0 && !confirm(question)) return false;
  try { await api("/api/agent", undefined, "DELETE"); } catch (e) { status(e.message, "bad"); return false; }
  poll();
  return true;
}


/** Reads what changed since the last entry shown, again while a turn runs or a card waits. */
async function poll() {
  clearTimeout(pollTimer);
  if (polling) { pollTimer = setTimeout(poll, POLL_MS); return; }
  polling = true;
  let answer = null;
  try {
    answer = await api(`/api/agent?after=${shown}&rev=${appliedRevision}`);
    // Fewer entries than the page drew: the server reset the conversation, and what it holds now is read from its start.
    if (answer.count < shown) {
      dropLog();
      answer = await api(`/api/agent?after=0&rev=${appliedRevision}`);
    }
    accept(answer);
  } catch (e) {
    status(e.message, "bad");
  } finally {
    polling = false;
  }
  if (answer?.busy || answer?.pending) pollTimer = setTimeout(poll, POLL_MS);
}
