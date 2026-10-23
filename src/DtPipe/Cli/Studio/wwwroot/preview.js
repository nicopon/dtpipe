// @ts-check
// Preview shows what every card delivers: one sample of the whole job, through the real engine, with the writers
// neutralised. A card keeps the rows of the stage that is its own (its read for a source, its branch's end for a step),
// whether it is pinned or not. A pinned card also keeps a checkpoint of them, so a later run reads it back instead of
// rereading its source. The server holds the pins and the checkpoints beside the job and settles them against the job's
// text; the page only shows what it answers, and never decides what is still true.

import { api, el, status } from "./dom.js";
import { brickBranches, cardBranch, cardWatch, compose, focusCard, showTab, stageTable } from "./designer.js";
import { invalidate } from "./render.js";
import { freshPreview, sequence, state } from "./state.js";

const dbg = state.preview;
const MAX_DEBUG_ROWS = 100000;

// One counter per kind of request. A state change and a plan are answered about the same job but are not the same
// request: a stale answer of one must not drop the other, and a preview is ordered by the queue and the job instead.
const syncing = sequence();
const planning = sequence();
const holding = sequence();
let debugTimer = null;

const placeId = (branch, at) => `${branch}#${at}`;
// What a pin's rows are filed under: its card, which keeps its name when the branch is renamed.
const watcherOf = (alias) => dbg.watch.find((w) => w.card === alias) || null;
// Where a card is pinned now: a branch changes name when cards are joined or split, a card does not.
const placeOf = (alias) => { const place = cardWatch(alias); return place && { ...place, card: alias }; };
// What the server is told of a pin; null once its card is gone.
const spec = (w) => { const place = w.card ? placeOf(w.card) : { branch: w.branch, at: w.at }; return place && { ...place }; };
const sameWatcher = (a, b) => a.branch === b.branch && a.at === b.at;
const newDraft = () => Array.from(crypto.getRandomValues(new Uint8Array(16)), (b) => b.toString(16).padStart(2, "0")).join("");
const wholeBranch = (place) => { const b = place && dbg.branches.find((x) => x.branch === place.branch); return !!b && !b.cuttable && b.transformers > 0; };
const composedYaml = () => state.cards.result?.yaml;
// Where a suspended run waits for a sink card: its entry in the queue the server lists, or null (a card that is no sink, or no run
// suspended). The run was of the file as saved, whose branch is the sink's own name when the studio did not write the file, and the
// branch the cards compose for it when it did.
const waitsOn = (alias, branch) => branch === alias || branch === cardBranch(alias);
const queueOf = (alias) => (dbg.real && !cardWatch(alias) ? dbg.queue.find((q) => waitsOn(alias, q.branch)) ?? null : null);
const sinkOf = (branch) => (state.cards.model?.steps || []).find((s) => s.kind === "sink" && waitsOn(s.alias, branch))?.alias ?? null;

/** What a preview showed belongs to the job it was for: another job starts clean. The rows asked stay as the person left them. */
export function forgetPreview() {
  clearTimeout(debugTimer);
  syncing.cancel();
  planning.cancel();
  holding.cancel();
  Object.assign(dbg, freshPreview(), { rows: dbg.rows });
}

// A job on disk is named by its path; one not saved yet by a draft, which its first save hands to the job.
function debugScope() {
  const job = state.job;
  if (job.hash) return { path: job.path };
  dbg.draft ??= newDraft();
  return { draft: dbg.draft };
}

/** A job saved for the first time takes over what its draft pinned. */
export async function adoptDraft() {
  const draft = dbg.draft;
  if (!draft) return;
  await debugSync({ adopt: draft }, true);
  dbg.draft = null;
}

/** The rows a job was left asking for. @param {number} rows */
export function restoreRows(rows) {
  dbg.rows = Math.max(1, Math.min(MAX_DEBUG_ROWS, Math.trunc(rows) || dbg.rows));
}

const redrawPreview = () => invalidate("bar", "canvas", "drawer");

// A preview is true of the text that produced it: once an edit changes what a card's rows depend on, they are gone and
// the card says so. The digests come from the server; the page compares strings.
function takePlaces(places) {
  dbg.places = new Map(places.map((p) => [placeId(p.branch, p.at), p.digest]));
  for (const [alias, shown] of [...dbg.shown]) {
    const place = cardWatch(alias);
    if (!place || dbg.places.get(placeId(place.branch, place.at)) !== shown.digest) {
      dbg.shown.delete(alias);
      dbg.edited.add(alias);
    }
  }
}

/** Takes what the server holds for the job: its pins, its places, and a real run left suspended. */
export function takeView(view) {
  dbg.loaded = true;
  dbg.real = !!view.real;
  dbg.watch = view.watch;
  dbg.queue = view.queue ?? [];
  if (!dbg.real) dbg.held.clear();
  dbg.branches = view.branches;
  takePlaces(view.places);
  redrawPreview();
  if (dbg.real) loadHeld();
}

// Tells the server what the page pins or forgets and what the job's text now is; it answers what it still holds.
// The server takes one state change at a time (409 otherwise), so the page sends them one after the other.
/** @type {Promise<any>} */
let debugQueue = Promise.resolve();
// `extra` may be a function, read when its turn comes: what a click asks is built from what the server last said.
function queued(task) {
  const turn = debugQueue.then(task);
  debugQueue = turn.catch(() => {});
  return turn;
}

// `quiet`: the page asked on its own, after an edit: a busy engine is no news to a person who clicked nothing.
export const debugSync = (extra = {}, quiet = false) => queued(() => debugSyncNow(extra, quiet));

async function debugSyncNow(extra, quiet) {
  if (!state.job || dbg.running) return false;
  const yaml = composedYaml();
  if (!yaml || state.cards.result.errors?.length) return false;
  const forJob = state.job;
  const ticket = syncing.begin();
  const asked = typeof extra === "function" ? extra() : extra;
  // A suspended real run keeps its rows for its own text: an edit is judged when the run is continued, not here.
  if (dbg.real && !asked.resetAll && dbg.loaded) return false;
  // Once the server's pins are known, every sync tells where their cards sit now.
  const watch = asked.watch ?? (dbg.loaded ? dbg.watch.map(spec) : undefined);
  try {
    const answer = await api("/api/debug/state", {
      ...debugScope(), yaml, rows: dbg.rows, ...asked, watch: watch?.filter(Boolean),
    });
    if (syncing.stale(ticket) || forJob !== state.job) return false;
    takeView(answer.view);
    return true;
  } catch (e) {
    if (!(quiet && e.status === 409)) status(e.message, "bad");
    return false;
  }
}

// Asks, without holding anything, what each place of the text now depends on: pure, so it never meets a busy engine.
async function planNow() {
  const yaml = composedYaml();
  if (!state.job || !yaml || state.cards.result.errors?.length) return;
  const forJob = state.job;
  const ticket = planning.begin();
  try {
    const answer = await api("/api/debug/plan", { yaml, rows: dbg.rows, path: forJob.hash ? forJob.path : undefined });
    if (planning.stale(ticket) || forJob !== state.job || dbg.running) return;
    dbg.branches = answer.view.branches;
    takePlaces(answer.view.places);
    redrawPreview();
  } catch { /* the text does not plan yet: Problems says why */ }
}

/**
 * After every composition the digests are read again, which never meets a busy engine, and the server settles what the
 * pins hold against the text. The first one after a job opens also learns the pins and the real run the job holds.
 */
export function debugComposed() {
  clearTimeout(debugTimer);
  debugTimer = setTimeout(() => {
    planNow();
    if (!dbg.loaded || (dbg.watch.length > 0 && !dbg.real)) debugSync({}, true);
  }, 300);
}

/** @param {Event} e */
export async function setDebugRows(e) {
  const input = /** @type {HTMLInputElement} */ (e.target);
  dbg.rows = Math.max(1, Math.min(MAX_DEBUG_ROWS, parseInt(input.value, 10) || 1000));
  input.value = String(dbg.rows);
  invalidate("bar");
  await (dbg.watch.length > 0 && !dbg.real ? debugSync() : planNow());
}

// A preview is one more turn of the queue: it never meets a state change still holding the engine slot.
export const runPreview = (place) => (!state.job || dbg.running ? Promise.resolve() : queued(() => runPreviewNow(place)));

// The sample of a branch that failed names it: the cards it runs show the failure, the others keep what they showed.
function failedCards(report) {
  const errors = [report.error, ...(report.errors || [])].filter(Boolean).join(" ");
  const branches = new Set([...errors.matchAll(/Branch '([^']+)'/g)].map((m) => m[1].split("__level")[0]));
  return new Set((state.cards.model?.steps || []).map((s) => s.alias).filter((alias) => branches.has(cardBranch(alias))));
}

// Each card takes the stage that is its own, from the piece of the run the server names for it. A place the run did not
// produce (read back from a pin, or upstream of a stop) keeps what it showed, as long as it is still true.
function takeReport(answer) {
  const byAlias = new Map((answer.report.branches || []).map((b) => [b.branch, b]));
  const places = new Map(answer.view.places.map((p) => [placeId(p.branch, p.at), p]));
  for (const step of state.cards.model?.steps || []) {
    const place = cardWatch(step.alias);
    const found = place && places.get(placeId(place.branch, place.at));
    const stages = (found?.alias && byAlias.get(found.alias)?.stages) || [];
    const stage = place?.at === "read" ? stages.find((s) => s.index === 0) : stages.reduce((last, s) => (!last || s.index > last.index ? s : last), null);
    if (!stage) continue;
    dbg.shown.set(step.alias, { stage, digest: found.digest });
    dbg.edited.delete(step.alias);
  }
}

async function runPreviewNow(place) {
  if (!state.job || dbg.running) return;
  const result = await compose();
  if (result.errors?.length) { showTab("problems"); status("The cards do not compose into a job yet: see Problems.", "bad"); return; }
  const forJob = state.job;
  clearTimeout(debugTimer);
  dbg.running = true;
  dbg.failed = new Set();
  redrawPreview();
  status("preview…", "warn");
  try {
    const answer = await api("/api/debug", { ...debugScope(), yaml: result.yaml, rows: dbg.rows, bricks: brickBranches(), upTo: place });
    // Another job may have been opened while it ran: its answer is not that job's.
    if (forJob !== state.job) { status("preview ended", "warn"); return; }
    dbg.running = false;
    dbg.ranYaml = result.yaml;
    takeView(answer.view);
    takeReport(answer);
    if (!answer.report.success) dbg.failed = failedCards(answer.report);
    dbg.notice = answer.notice;
    dbg.report = answer.report;
    if (place?.card) focusCard(place.card); else showTab("preview");
    status(answer.report.success ? (place?.card ? `ran up to ${place.card}` : "preview done") : "preview failed", answer.report.success ? "good" : "bad");
  } catch (e) {
    if (e.status === 422 && state.vars.variables.some((v) => !v.ready)) showTab("variables");
    status(e.message, "bad");
  } finally {
    dbg.running = false;
    redrawPreview();
    // An edit made while the run went on was not judged: the rows just taken are those of the text that ran.
    if (forJob === state.job && composedYaml() !== result.yaml) debugComposed();
  }
}

// The state of a card, for its badge: what it shows, and whether that is still true of the text.
function cardState(alias) {
  const sameText = composedYaml() === dbg.ranYaml;
  if (dbg.running) return { name: "running", glyph: "⟳", text: "running…" };
  if (sameText && dbg.failed.has(alias)) return { name: "error", glyph: "✕", text: "failed" };
  const place = cardWatch(alias);
  if (!place) {
    const waiting = queueOf(alias);
    if (waiting) return { name: "held", glyph: "⏸", text: waiting.rows != null ? `${waiting.rows} rows queued` : waiting.key ? "rows queued" : "no rows held" };
    // A writer holds no rows: it says only whether the last preview, run on this very text, reached it.
    return sameText && dbg.report?.success ? { name: "fresh", glyph: "●", text: "not written" } : { name: "never", glyph: "○", text: "no preview" };
  }
  const shown = dbg.shown.get(alias);
  if (shown) {
    const stage = shown.stage;
    return { name: "fresh", glyph: "●", text: `${stage.rowsSeen}${stage.more ? "+" : ""} rows · ${(stage.columns || []).length} col` };
  }
  return dbg.edited.has(alias) ? { name: "edited", glyph: "○", text: "edited since preview" } : { name: "never", glyph: "○", text: "no preview" };
}

/** The badge of a card. @param {string} alias */
export function cardBadge(alias) {
  const cardState_ = cardState(alias);
  const watcher = watcherOf(alias);
  const place = cardWatch(alias);
  const title = cardState_.name === "held" ? "A real run is suspended before its targets are written: Write writes every target it waits on, Abandon forgets its rows."
    : wholeBranch(place) && place.at === "read" ? "This card runs whole with its branch: its rows are those of its read, and it cannot be pinned inside." : cardState_.text;
  return el("div", { class: `dcard-badge ${cardState_.name}`, "data-card-state": cardState_.name, title },
    `${cardState_.glyph} ${cardState_.text}${watcher ? " · 📌" : ""}`);
}

/** The actions of a card. @param {string} alias */
export function cardTools(alias) {
  const place = cardWatch(alias);
  if (!place) {
    // A sink a suspended run waits on writes or forgets the rows of the whole run: it is the only place to do either.
    if (!queueOf(alias)) return null;
    const word = (name, label, title, extra = "") => el("button", { class: `ctool wide ${extra}`, "data-card-tool": name, title, "aria-label": title }, label);
    return el("div", { class: "dcard-tools queue" },
      word("write", "Write", "Write the rows this run holds to every target it waits on"),
      word("abandon", "Abandon", "Forget the rows this run holds, for every target: nothing was written", "danger"));
  }
  const watcher = watcherOf(alias);
  const whole = place.at === "read" && wholeBranch(place);
  const tool = (name, glyph, title, extra = {}) => el("button",
    { class: "ctool", "data-card-tool": name, title, "aria-label": title, ...extra }, glyph);
  return el("div", { class: "dcard-tools" },
    tool("play", "▶", "Run the job up to this card"),
    tool("pin", "📌", whole ? "This card runs whole with its branch: it cannot be pinned" : watcher
      ? "Pinned: it keeps its rows and a checkpoint of them. Click to unpin and forget them"
      : "Pin: keep this card's rows and a checkpoint of them, so a later run reads them back instead of rereading the source",
    { class: watcher ? "ctool pin on" : "ctool pin", "aria-pressed": String(!!watcher), "aria-disabled": whole ? "true" : null }));
}

/** @param {string} alias @param {string} name */
export async function cardTool(alias, name) {
  if (name === "abandon") { if (!dbg.running) await abandonRun(); return; }
  const place = placeOf(alias);
  if (!place || dbg.running) return;
  if (dbg.real) {
    // The rows of a real run belong to its text; a card can still be looked at, nothing else changes.
    focusCard(alias);
    status("A real run is suspended: continue it, or abandon it to change what is pinned.", "warn");
    return;
  }
  const watcher = watcherOf(alias);
  if (name === "play") {
    await runPreview(place);
  } else if (name === "pin") {
    if (place.at === "read" && wholeBranch(place)) {
      status(`${alias} runs whole with its branch: pin the step after it.`, "warn");
      return;
    }
    // Unpinning forgets the checkpoint; the rows already shown stay, they are still true.
    await debugSync(() => ({ watch: watcher ? dbg.watch.filter((w) => !sameWatcher(w, watcher)).map(spec) : [...dbg.watch.map(spec), place] }));
    focusCard(alias);
  }
}

export async function forgetPins() {
  if (await debugSync({ watch: [], resetAll: true })) status("pins forgotten", "good");
}

/** The Preview tab: the rows of the selected card. @param {string | null} alias */
export function previewView(alias) {
  const place = alias ? cardWatch(alias) : null;
  const parts = [];
  if (dbg.running) parts.push(el("p", { class: "warn" }, "Running…"));
  const waiting = alias && !place ? queueOf(alias) : null;
  if (waiting) {
    const held = dbg.held.get(waiting.branch);
    if (held) {
      const stage = held.stage;
      const total = waiting.rows ?? `${stage.rowsSeen}${stage.more ? "+" : ""}`;
      const cut = (stage.sample || []).length < (waiting.rows ?? stage.rowsSeen) ? " (the first 1000 shown)" : "";
      parts.push(el("p", { class: "muted" }, `${alias}: ${total} rows queued for its target${cut}. Nothing is written until you write.`),
        el("div", { class: "scroll" }, stageTable(stage)));
    } else {
      parts.push(el("p", { class: "hint" }, waiting.key
        ? `${alias} waits on rows a real run holds: loading them…`
        : `The suspended run holds none of the rows ${alias} waits on: abandon it, then review again.`));
    }
    parts.push(el("p", { class: "hint" }, "Write writes every target the run waits on; Abandon forgets the rows of all of them."));
  } else if (!place) {
    parts.push(el("p", { class: "hint" }, alias
      ? `${alias} writes a target and holds no rows of its own: select the card before it.`
      : "Select a card to see the rows it delivers. Preview runs the whole job; ▶ on a card runs it up to that card. The writers are neutralised: no target is written."));
  } else {
    if (place.at === "read" && wholeBranch(place)) {
      parts.push(el("p", { class: "hint" }, `${alias} belongs to the branch ${place.branch}, which runs whole (a stream processor or a job the cut refuses): `
        + "it cannot be pinned inside, only at its end."));
    }
    const watcher = watcherOf(alias);
    const shown = dbg.shown.get(alias);
    if (shown) {
      const stage = shown.stage;
      const total = `${stage.rowsSeen}${stage.more ? "+" : ""}`;
      const cut = (stage.sample || []).length < stage.rowsSeen ? " (the first 1000 shown)" : "";
      parts.push(el("p", { class: "muted" }, `${alias}: ${total} rows${dbg.real ? " held" : " delivered"}${cut}. The target was not written.`),
        el("div", { class: "scroll" }, stageTable(stage)));
    } else if (dbg.edited.has(alias)) {
      parts.push(el("p", { class: "hint" }, `${alias} was modified since its preview: run Preview to see its rows again.`));
    } else if (dbg.real && watcher?.key) {
      parts.push(el("p", { class: "hint" }, `${alias} holds the rows of a real run: loading them…`));
    } else {
      parts.push(el("p", { class: "hint" }, `No preview of ${alias} yet: Preview in the bar runs the whole job, ▶ on the card runs it up to there.`));
    }
    if (watcher) {
      parts.push(el("p", { class: "hint" }, watcher.key
        ? "📌 Pinned: its checkpoint is held, and a later run reads it back instead of rereading what precedes it."
        : "📌 Pinned: its checkpoint is kept at the next run."));
    }
  }
  const errors = dbg.report?.errors || (dbg.report?.error ? [dbg.report.error] : []);
  if (dbg.report && !dbg.report.success && composedYaml() === dbg.ranYaml) {
    parts.push(el("p", { class: "bad" }, `The last preview failed${dbg.report.stage ? ` (${dbg.report.stage})` : ""}.`),
      errors.length ? el("ul", { class: "msgs bad" }, errors.map((m) => el("li", {}, m))) : null);
  }
  if (dbg.notice) parts.push(el("p", { class: "hint warn" }, dbg.notice));
  return parts;
}

// ---------------------------------------------------------------- a real run left holding rows

// The rows a suspended real run holds come from the server, which keeps them: loaded for every held card the page shows
// nothing of. A newer view, or another job, makes a load still going stale: its rows are dropped.
let heldLoading = Promise.resolve(null);
function loadHeld() {
  const ticket = holding.begin();
  const forJob = state.job;
  const stale = () => holding.stale(ticket) || forJob !== state.job;
  heldLoading = (async () => {
    let first = null;
    let loaded = false;
    let sink = null;
    for (const w of dbg.watch) {
      if (!w.key || !w.card) continue;
      first ??= w.card;
      if (dbg.shown.get(w.card)?.key === w.key) continue;
      try {
        const { stage } = await api("/api/debug/rows", { ...debugScope(), key: w.key });
        if (stale()) return null;
        dbg.shown.set(w.card, { stage, digest: dbg.places.get(placeId(w.branch, w.at)), key: w.key });
        dbg.edited.delete(w.card);
        loaded = true;
      } catch (e) { if (!stale()) status(e.message, "bad"); }
    }
    for (const q of dbg.queue) {
      if (!q.key) continue;
      sink ??= sinkOf(q.branch);
      if (dbg.held.get(q.branch)?.key === q.key) continue;
      try {
        const { stage } = await api("/api/debug/rows", { ...debugScope(), key: q.key });
        if (stale()) return null;
        dbg.held.set(q.branch, { stage, key: q.key });
        loaded = true;
      } catch (e) { if (!stale()) status(e.message, "bad"); }
    }
    if (stale()) return null;
    // Rows the page did not have (a reload, another tab) are shown where they are held: a sink the run waits on first, as it is what is read before writing.
    first = sink ?? first;
    if (loaded && first) focusCard(first); else invalidate("drawer");
    return first;
  })();
  return heldLoading;
}

/** A staged run ends holding rows or none: the cards redraw from what the server holds, and the card it stopped at shows them. */
export async function showHeld(view) {
  takeView(view);
  const first = dbg.real ? await heldLoading : null;
  if (first) focusCard(first);
  return !!first;
}

export async function abandonRun() {
  if (await debugSync({ resetAll: true })) { dbg.shown.clear(); dbg.held.clear(); redrawPreview(); status("real run abandoned: nothing was written", "good"); }
}
