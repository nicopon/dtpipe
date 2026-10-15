// dtpipe studio: the jobs of a folder, opened as cards, sampled through the real engine and saved.

import { $, api, el, fill, forgetToken, status, token } from "./dom.js";
import { coordinator, onCoordinator, refreshCoordinator } from "./coordinator.js";
import {
  brickBranches, cardBranch, cards, cardWatch, compose, composed, focusCard, isDirty, markSaved, mountDesigner, openBlank, openJob, refreshCanvas,
  refreshDrawer, setCatalog, showTab, stageTable,
} from "./designer.js";

// The job open: its path, the hash a save must find on disk (null for a job not written yet), and
// whether saving rewrites a text written by hand.
let current = null;

// The variables of the composed job, by name, and the keyring's aliases: never a value the person
// did not type.
let vars = { variables: [], aliases: [] };
let varsSeq = 0;

async function loadJobs() {
  const { folder, jobs } = await api("/api/jobs");
  $("folder").textContent = folder;
  fill($("jobs"), jobs.length
    ? jobs.map((path) => el("li", {}, el("button", { class: path === current?.path ? "job open" : "job", onclick: () => open(path) }, path)))
    : el("li", { class: "muted" }, "No .yaml or .yml file in this folder."));
}

// The coordinator's library, when a person is signed in: each pipeline opens as a new file of the folder.
async function loadLibrary() {
  const pane = $("library-pane");
  if (coordinator()?.state !== "signed-in") { pane.hidden = true; return; }
  pane.hidden = false;
  try {
    const { pipelines } = await api("/api/library");
    fill($("library"), pipelines.length
      ? pipelines.map((p) => el("li", {}, el("button", { class: "job", title: p.description || p.title || p.id, onclick: () => openFromLibrary(p.id) }, p.id)))
      : el("li", { class: "muted" }, "The library holds no pipeline."));
  } catch (e) {
    fill($("library"), el("li", { class: "warn" }, e.message));
  }
}

async function openFromLibrary(id) {
  if (!leaveCurrent()) return;
  try {
    const { path } = await api("/api/library/open", { id });
    await loadJobs();
    await open(path, true);
  } catch (e) {
    status(e.message, "bad");
  }
}

// What a preview, a run or a publication showed belongs to the job it was for: another job starts clean.
// A run still going is kept, since it goes on whatever is open. The rows asked stay as the person left them.
function forgetResults() {
  clearTimeout(debugTimer);
  debugSeq++;
  planSeq++;
  dbg = { ...freshPreview(), rows: dbg.rows };
  publication = null;
  if (run && run.phase !== "running") run = null;
}

const leaveCurrent = () => !isDirty() || confirm(`${current?.path || "This job"} has changes that are not saved. Leave it anyway?`);

async function open(path, left = false) {
  if (!left && !leaveCurrent()) return;
  try {
    const job = await api(`/api/job?path=${encodeURIComponent(path)}`);
    current = { path, hash: job.hash, rewrites: job.rewrites };
    forgetResults();
    $("job-name").textContent = path;
    await openJob(job.yaml, job.layout);
    status(job.rewrites ? "saving rewrites this file" : "", job.rewrites ? "warn" : "");
    await loadJobs();
  } catch (e) {
    status(e.message, "bad");
  }
}

async function newJob() {
  if (!leaveCurrent()) return;
  const path = prompt("The new job's path in the folder (its folder must exist):", "new-job.yaml");
  if (!path) return;
  if (!/\.ya?ml$/i.test(path)) { status("A job is a .yaml or .yml file.", "bad"); return; }
  current = { path, hash: null, rewrites: false };
  forgetResults();
  $("job-name").textContent = `${path} (new)`;
  await openBlank(path.replace(/^.*\//, "").replace(/\.ya?ml$/i, ""));
  await loadJobs();
}

// A command line cut into the arguments of one process, as a shell would: whitespace separates, quotes
// group, a backslash escapes outside single quotes. The studio takes the array, never a string to cut.
function splitCommand(text) {
  const args = [];
  let word = null;
  let quote = null;
  for (let i = 0; i < text.length; i++) {
    const c = text[i];
    if (quote) {
      if (c === quote) quote = null;
      else if (c === "\\" && quote === "\"" && i + 1 < text.length) word += text[++i];
      else word += c;
    } else if (c === "'" || c === "\"") {
      quote = c;
      word ??= "";
    } else if (c === "\\" && i + 1 < text.length) {
      word = (word ?? "") + text[++i];
    } else if (/\s/.test(c)) {
      if (word !== null) { args.push(word); word = null; }
    } else {
      word = (word ?? "") + c;
    }
  }
  if (quote) throw new Error("A quote is not closed.");
  if (word !== null) args.push(word);
  return args;
}

// The job a dtpipe command line stands for, as --export-job writes it, opened as a new job not saved yet.
async function importCommand() {
  if (!leaveCurrent()) return;
  const text = prompt("A dtpipe command line (no ${{…}} is resolved; --bind-input and --bind-output are refused):", "dtpipe --input csv:in.csv --output csv:out.csv");
  if (!text) return;
  try {
    const args = splitCommand(text);
    if (args[0] === "dtpipe") args.shift();
    const { yaml } = await api("/api/import", { args });
    const path = prompt("The imported job's path in the folder (its folder must exist):", "imported.yaml");
    if (!path) return;
    if (!/\.ya?ml$/i.test(path)) { status("A job is a .yaml or .yml file.", "bad"); return; }
    current = { path, hash: null, rewrites: false };
    forgetResults();
    $("job-name").textContent = `${path} (new)`;
    await openJob(yaml, null, true);
    status("imported: save it to keep it", "good");
    await loadJobs();
  } catch (e) {
    status(e.message, "bad");
  }
}

// The server writes the job it composes from the cards, and the cards' layout under .dtpipe/.
async function save() {
  if (!current) return;
  const result = await compose();
  if (current.rewrites && !confirm("Saving writes this job as the studio composes it: comments inside it and the order of its keys are not kept. Save anyway?")) return;
  const acceptErrors = result.errors?.length > 0;
  if (acceptErrors && !confirm(`${result.errors.length} problem(s) remain: the job written may not run. Save anyway?`)) return;
  try {
    const draft = current.hash ? null : current.draft;
    const answer = await api("/api/job", { path: current.path, hash: current.hash, model: cards(), acceptErrors }, "PUT");
    current = { path: current.path, hash: answer.hash, rewrites: false };
    // What a preview pinned before the first save belongs to the job now.
    if (draft) await debugSync({ adopt: draft }, true);
    $("job-name").textContent = current.path;
    markSaved();
    status("saved", "good");
    await loadJobs();
  } catch (e) {
    status(e.message, "bad");
  }
}

// ---------------------------------------------------------------- variables

async function loadVariables(result) {
  const seq = ++varsSeq;
  try {
    const answer = await api("/api/variables", { path: current?.path || null, yaml: result?.yaml || "", bricks: brickBranches() });
    if (seq !== varsSeq) return;
    vars = answer;
  } catch (e) {
    if (seq === varsSeq) vars = { variables: [], aliases: [], error: e.message };
  }
  refreshDrawer();
}

const launchOf = () => Object.fromEntries(vars.variables.filter((v) => v.kind === "env" && v.override).map((v) => [v.name, v.override]));

async function launch(name, chosen) {
  const overrides = launchOf();
  if (chosen) overrides[name] = chosen; else delete overrides[name];
  try {
    await api("/api/launch", { path: current.path, overrides }, "PUT");
    await loadVariables(await compose());
  } catch (e) { status(e.message, "bad"); }
}

function describe(v) {
  if (v.kind === "keyring") return v.defined ? "in the keyring" : `not in the keyring: dtpipe secret set ${v.name}`;
  if (v.kind === "cursor") return "a cursor's state file: the run resolves it";
  if (v.override?.keyring) return `launched with the keyring alias ${v.override.keyring}${v.ready ? "" : " (missing)"}`;
  if (v.override) return "launched with a text typed here";
  return v.defined ? "set in the environment" : "not set: choose a value";
}

function variableRow(v) {
  const state = v.ready ? (v.kind === "cursor" ? "info" : "good") : "bad";
  const control = [];
  if (v.kind === "env") {
    const choice = v.override?.keyring ? `keyring:${v.override.keyring}` : v.override ? "text" : "";
    const select = el("select", { "aria-label": `Launch ${v.name} with` },
      el("option", { value: "", selected: choice === "" }, v.defined ? "the environment" : "nothing yet"),
      el("option", { value: "text", selected: choice === "text" }, "a text…"),
      vars.aliases.map((a) => el("option", { value: `keyring:${a}`, selected: choice === `keyring:${a}` }, `keyring: ${a}`)));
    const text = el("input", { type: "password", class: "mono", value: v.override?.value ?? "", placeholder: "value", hidden: choice !== "text", "aria-label": `Value of ${v.name}` });
    const reveal = el("button", { class: "small ghost", hidden: choice !== "text", title: "Show or hide the text" }, "show");
    select.addEventListener("change", () => {
      if (select.value === "text") { text.hidden = false; reveal.hidden = false; text.focus(); return; }
      launch(v.name, select.value ? { keyring: select.value.slice("keyring:".length) } : null);
    });
    text.addEventListener("change", () => launch(v.name, { value: text.value }));
    reveal.addEventListener("click", () => { text.type = text.type === "password" ? "text" : "password"; reveal.textContent = text.type === "password" ? "show" : "hide"; });
    control.push(select, text, reveal);
  }
  return el("tr", {},
    el("td", {}, el("span", { class: `dot ${state}` }), " ", el("code", {}, `\${{${v.expression}}}`)),
    el("td", { class: "wrap" }, describe(v)),
    el("td", {}, control));
}

function variablesView() {
  if (vars.error) return el("p", { class: "bad" }, vars.error);
  if (!vars.variables.length) return el("p", { class: "hint" }, "This job reads no ${{…}} variable.");
  return [
    el("p", { class: "hint" }, "The job keeps each ${{NAME}}; what the studio launches it with stays on this machine, under .dtpipe/, outside git. A keyring value is read when a sample runs and never shown."),
    current ? null : el("p", { class: "warn" }, "Open or create a job to keep launch values."),
    el("table", { class: "vars" }, el("tbody", {}, vars.variables.map(variableRow))),
  ];
}

// The coordinator's bricks for the palette; the catalog read last when it cannot be read again.
async function loadBricks() {
  try {
    const answer = await api("/api/bricks");
    setCatalog(answer);
    if (answer.error) status(answer.error, "warn");
  } catch (e) {
    status(e.message, "bad");
  }
}

// ---------------------------------------------------------------- preview

// Preview shows what every card delivers: one sample of the whole job, through the real engine, with the writers
// neutralised. A card keeps the rows of the stage that is its own (its read for a source, its branch's end for a step),
// whether it is pinned or not. A pinned card also keeps a checkpoint of them, so a later run reads it back instead of
// rereading its source. The server holds the pins and the checkpoints beside the job and settles them against the job's
// text; the page only shows what it answers, and never decides what is still true.
//   watch: { branch, at, card, level, key, alias } per pin, `card` being the card it sits on, which keeps its name when
//   the branch that runs it is renamed; loaded: the server's answer for this job was read; branches: { branch, transformers,
//   cuttable }; places: the digest of what each (branch, read|end) depends on, by `branch#at`; shown: what each card showed
//   last, by card; edited: the cards whose rows an edit made untrue; running: a run is under way; real: a real run is
//   suspended before its target is written, holding every row it read; ranYaml: the text the last run was for.
const freshPreview = () => ({
  rows: 1000, loaded: false, real: false, watch: [], branches: [], places: new Map(), shown: new Map(), edited: new Set(),
  failed: new Set(), running: false, notice: null, report: null, ranYaml: null,
});
let dbg = freshPreview();
let debugSeq = 0;
let planSeq = 0;
let debugTimer = null;

const MAX_DEBUG_ROWS = 100000;
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

// A job on disk is named by its path; one not saved yet by a draft, which its first save hands to the job.
function debugScope() {
  if (current.hash) return { path: current.path };
  current.draft ??= newDraft();
  return { draft: current.draft };
}

function redrawPreview() {
  if ($("d-preview")) $("d-preview").disabled = dbg.running;
  for (const id of ["d-forget-pins", "d-watch-rows"]) if ($(id)) $(id).disabled = dbg.running || dbg.real;
  if ($("d-forget-pins")) $("d-forget-pins").hidden = dbg.watch.length === 0;
  if ($("d-abandon")) $("d-abandon").hidden = !dbg.real;
  if ($("d-run")) $("d-run").textContent = dbg.real ? "Continue the run" : "Run";
  refreshCanvas();
  refreshDrawer();
}

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

function applyView(view) {
  dbg.loaded = true;
  dbg.real = !!view.real;
  dbg.watch = view.watch;
  dbg.branches = view.branches;
  takePlaces(view.places);
  redrawPreview();
  if (dbg.real) loadHeld();
}

// Tells the server what the page pins or forgets and what the job's text now is; it answers what it still holds.
// The server takes one state change at a time (409 otherwise), so the page sends them one after the other.
let debugQueue = Promise.resolve();
// `extra` may be a function, read when its turn comes: what a click asks is built from what the server last said.
function queued(task) {
  const turn = debugQueue.then(task);
  debugQueue = turn.catch(() => {});
  return turn;
}

// `quiet`: the page asked on its own, after an edit: a busy engine is no news to a person who clicked nothing.
const debugSync = (extra = {}, quiet = false) => queued(() => debugSyncNow(extra, quiet));

async function debugSyncNow(extra, quiet) {
  if (!current || dbg.running) return false;
  const result = composed();
  if (!result?.yaml || result.errors?.length) return false;
  const forJob = current;
  const seq = ++debugSeq;
  const asked = typeof extra === "function" ? extra() : extra;
  // A suspended real run keeps its rows for its own text: an edit is judged when the run is continued, not here.
  if (dbg.real && !asked.resetAll && dbg.loaded) return false;
  // Once the server's pins are known, every sync tells where their cards sit now.
  const watch = asked.watch ?? (dbg.loaded ? dbg.watch.map(spec) : undefined);
  try {
    const answer = await api("/api/debug/state", {
      ...debugScope(), yaml: result.yaml, rows: dbg.rows, ...asked, watch: watch?.filter(Boolean),
    });
    if (seq !== debugSeq || forJob !== current) return false;
    applyView(answer.view);
    return true;
  } catch (e) {
    if (!(quiet && e.status === 409)) status(e.message, "bad");
    return false;
  }
}

// Asks, without holding anything, what each place of the text now depends on: pure, so it never meets a busy engine.
async function planNow() {
  const result = composed();
  if (!current || !result?.yaml || result.errors?.length) return;
  const forJob = current;
  const seq = ++planSeq;
  try {
    const answer = await api("/api/debug/plan", { yaml: result.yaml, rows: dbg.rows });
    if (seq !== planSeq || forJob !== current || dbg.running) return;
    dbg.branches = answer.view.branches;
    takePlaces(answer.view.places);
    redrawPreview();
  } catch { /* the text does not plan yet: Problems says why */ }
}

// After every composition the digests are read again, which never meets a busy engine, and the server settles what the
// pins hold against the text. The first one after a job opens also learns the pins and the real run the job holds.
function debugComposed() {
  clearTimeout(debugTimer);
  debugTimer = setTimeout(() => {
    planNow();
    if (!dbg.loaded || (dbg.watch.length > 0 && !dbg.real)) debugSync({}, true);
  }, 300);
}

async function setDebugRows(e) {
  dbg.rows = Math.max(1, Math.min(MAX_DEBUG_ROWS, parseInt(e.target.value, 10) || 1000));
  e.target.value = String(dbg.rows);
  await (dbg.watch.length > 0 && !dbg.real ? debugSync() : planNow());
}

// A preview is one more turn of the queue: it never meets a state change still holding the engine slot.
const runPreview = (place) => (!current || dbg.running ? Promise.resolve() : queued(() => runPreviewNow(place)));

// The sample of a branch that failed names it: the cards it runs show the failure, the others keep what they showed.
function failedCards(report) {
  const errors = [report.error, ...(report.errors || [])].filter(Boolean).join(" ");
  const branches = new Set([...errors.matchAll(/Branch '([^']+)'/g)].map((m) => m[1].split("__level")[0]));
  return new Set((cards()?.steps || []).map((s) => s.alias).filter((alias) => branches.has(cardBranch(alias))));
}

// Each card takes the stage that is its own, from the piece of the run the server names for it. A place the run did not
// produce (read back from a pin, or upstream of a stop) keeps what it showed, as long as it is still true.
function takeReport(answer) {
  const byAlias = new Map((answer.report.branches || []).map((b) => [b.branch, b]));
  const places = new Map(answer.view.places.map((p) => [placeId(p.branch, p.at), p]));
  for (const step of cards()?.steps || []) {
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
  if (!current || dbg.running) return;
  const result = await compose();
  if (result.errors?.length) { showTab("problems"); status("The cards do not compose into a job yet: see Problems.", "bad"); return; }
  const forJob = current;
  clearTimeout(debugTimer);
  dbg.running = true;
  dbg.failed = new Set();
  redrawPreview();
  status("preview…", "warn");
  try {
    const answer = await api("/api/debug", { ...debugScope(), yaml: result.yaml, rows: dbg.rows, bricks: brickBranches(), upTo: place });
    // Another job may have been opened while it ran: its answer is not that job's.
    if (forJob !== current) { status("preview ended", "warn"); return; }
    dbg.running = false;
    dbg.ranYaml = result.yaml;
    applyView(answer.view);
    takeReport(answer);
    if (!answer.report.success) dbg.failed = failedCards(answer.report);
    dbg.notice = answer.notice;
    dbg.report = answer.report;
    if (place?.card) focusCard(place.card); else showTab("preview");
    status(answer.report.success ? (place?.card ? `ran up to ${place.card}` : "preview done") : "preview failed", answer.report.success ? "good" : "bad");
  } catch (e) {
    if (e.status === 422 && vars.variables.some((v) => !v.ready)) showTab("variables");
    status(e.message, "bad");
  } finally {
    dbg.running = false;
    redrawPreview();
    // An edit made while the run went on was not judged: the rows just taken are those of the text that ran.
    if (forJob === current && composed()?.yaml !== result.yaml) debugComposed();
  }
}

// The state of a card, for its badge: what it shows, and whether that is still true of the text.
function cardState(alias) {
  const sameText = composed()?.yaml === dbg.ranYaml;
  if (dbg.running) return { name: "running", glyph: "⟳", text: "running…" };
  if (sameText && dbg.failed.has(alias)) return { name: "error", glyph: "✕", text: "failed" };
  const place = cardWatch(alias);
  if (!place) {
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

function cardBadge(alias) {
  const state = cardState(alias);
  const watcher = watcherOf(alias);
  const place = cardWatch(alias);
  const title = wholeBranch(place) && place.at === "read" ? "This card runs whole with its branch: its rows are those of its read, and it cannot be pinned inside." : state.text;
  return el("div", { class: `dcard-badge ${state.name}`, "data-card-state": state.name, title },
    `${state.glyph} ${state.text}${watcher ? " · 📌" : ""}`);
}

function cardTools(alias) {
  const place = cardWatch(alias);
  if (!place) return null;
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

async function cardTool(alias, name) {
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

async function forgetPins() {
  if (await debugSync({ watch: [], resetAll: true })) status("pins forgotten", "good");
}

function previewView(alias) {
  const place = alias ? cardWatch(alias) : null;
  const parts = [];
  if (dbg.running) parts.push(el("p", { class: "warn" }, "Running…"));
  if (!place) {
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
  if (dbg.report && !dbg.report.success && composed()?.yaml === dbg.ranYaml) {
    parts.push(el("p", { class: "bad" }, `The last preview failed${dbg.report.stage ? ` (${dbg.report.stage})` : ""}.`),
      errors.length ? el("ul", { class: "msgs bad" }, errors.map((m) => el("li", {}, m))) : null);
  }
  if (dbg.notice) parts.push(el("p", { class: "hint warn" }, dbg.notice));
  return parts;
}

// ---------------------------------------------------------------- run

// The real run of the saved job. phase: "confirm" (what it would write, to confirm), "running", "done".
let run = null;
let runTimer = null;

const runLabel = (status) => status.code === 0 ? "succeeded" : status.code === 130 ? "stopped" : `failed (code ${status.code})`;

async function prepareRun() {
  if (!current) return;
  if (isDirty() || !current.hash) {
    status("Save the job first: a run executes the job saved in the folder.", "bad");
    return;
  }
  try {
    const forPath = current.path;
    // A run is the job's own, written whole. Only a real run left suspended is continued from the rows it holds.
    const staged = dbg.real ? { debug: true } : {};
    const prepared = await api("/api/run/prepare", { path: current.path, hash: current.hash, bricks: brickBranches(), ...staged });
    if (current?.path !== forPath) return;
    run = { phase: "confirm", prepared, bricks: brickBranches(), hash: current.hash, path: current.path, staged };
    showTab("run");
  } catch (e) {
    run = null;
    status(e.message, "bad");
  }
}

async function startRun() {
  try {
    await api("/api/run", { path: run.path, hash: run.hash, bricks: run.bricks, digest: run.prepared.digest, ...run.staged });
    run = { ...run, phase: "running", status: null };
    pollRun();
    showTab("run");
  } catch (e) {
    status(e.message, "bad");
  }
}

function pollRun() {
  clearInterval(runTimer);
  runTimer = setInterval(async () => {
    try {
      const now = await api("/api/run");
      run = { ...run, status: now, phase: now.running ? "running" : "done" };
      if (!now.running) {
        clearInterval(runTimer);
        status(`run ${runLabel(now)}`, now.code === 0 ? "good" : "warn");
        // A run that stopped holding rows shows them; the verdict is in the status line.
        if (now.view && await showHeld(now.view)) return;
      }
      showTab("run");
    } catch (e) {
      clearInterval(runTimer);
      status(e.message, "bad");
    }
  }, 1000);
}

// A staged run ends holding rows or none: the cards redraw from what the server holds, and the card it stopped at shows them.
async function showHeld(view) {
  applyView(view);
  const first = dbg.real ? await heldLoading : null;
  if (first) focusCard(first);
  return !!first;
}

// The rows a suspended real run holds come from the server, which keeps them: loaded for every held card the page shows nothing of.
let heldLoading = Promise.resolve(null);
function loadHeld() {
  heldLoading = (async () => {
    let first = null;
    let loaded = false;
    for (const w of dbg.watch) {
      if (!w.key || !w.card) continue;
      first ??= w.card;
      if (dbg.shown.get(w.card)?.key === w.key) continue;
      try {
        const { stage } = await api("/api/debug/rows", { ...debugScope(), key: w.key });
        dbg.shown.set(w.card, { stage, digest: dbg.places.get(placeId(w.branch, w.at)), key: w.key });
        dbg.edited.delete(w.card);
        loaded = true;
      } catch (e) { status(e.message, "bad"); }
    }
    // Rows the page did not have (a reload, another tab) are shown where they are held.
    if (loaded && first) focusCard(first); else refreshDrawer();
    return first;
  })();
  return heldLoading;
}

async function abandonRun() {
  if (await debugSync({ resetAll: true })) { dbg.shown.clear(); redrawPreview(); status("real run abandoned: nothing was written", "good"); }
}

async function stopRun() {
  try { await api("/api/run/stop", {}); status("stop asked", "warn"); } catch (e) { status(e.message, "bad"); }
}

// What a staged run will do, stage by stage, and what it holds on disk meanwhile.
function stagedView(staged) {
  const size = (bytes) => bytes >= 1048576 ? `${(bytes / 1048576).toFixed(1)} MB` : `${Math.max(1, Math.round(bytes / 1024))} kB`;
  const stopsAt = staged.stopsAt ? (staged.stopsAt.card || staged.stopsAt.branch) : null;
  return [
    staged.writesNow
      ? el("p", { class: "warn" }, "This run is real and reaches the end of the job: its last stage writes the target. A sink brick is written to a throwaway file here, never to its node.")
      : el("p", { class: "warn" }, `This run is real but stops at ${stopsAt}, before anything is written. It holds every row it read, encrypted in this studio's session, `
        + "until you continue the run (the same button writes the target) or abandon it. The rows of a sample are not used."),
    el("ol", { class: "run-stages" }, staged.stages.map((st) => el("li", {},
      el("b", {}, st.branch), " reads ", el("code", {}, st.reads || "its source"),
      st.keeps ? ", keeps every row it delivers" : "",
      st.writes ? [", WRITES ", el("code", {}, st.writes)] : ", writes nothing"))),
    staged.held.length ? el("p", { class: "hint" }, "Held from before: ", staged.held.map((h) =>
      `${h.card || h.branch} (${size(h.bytes)}, read ${h.readAt ? new Date(h.readAt).toLocaleString() : "earlier"})`).join("; "),
      ". Continuing writes these rows, not a new read of the source.") : null,
    staged.discards ? el("p", { class: "warn" }, `${staged.discards} held checkpoint(s) no longer match the job or the values it reads with, or come from a sample: they are forgotten.`) : null,
  ];
}

function runView() {
  if (!run) return el("p", { class: "hint" }, "Run executes the job saved in this folder, as dtpipe --job would, on this machine. You confirm what it writes first.");
  if (run.phase === "confirm") {
    const writes = run.prepared.writes;
    const staged = run.prepared.staged;
    return [
      staged ? stagedView(staged) : el("p", { class: "warn" }, "This run is real: its writers write their targets. A sink brick is written to a throwaway file here, never to its node."),
      writes.length ? writes.map((w) => el("div", { class: "run-write" },
        el("p", {}, el("b", {}, w.alias), w.throwaway ? " → a throwaway file (the brick's node is not reached)" : [" → ", el("code", {}, w.output)]),
        w.block ? el("pre", {}, w.block) : null)) : el("p", { class: "hint" }, "No branch of this job writes anywhere."),
      el("div", { class: "row" },
        el("button", { id: "d-run-confirm", class: "primary", onclick: startRun }, "Confirm and run"),
        el("button", { class: "ghost", onclick: () => { run = null; showTab("run"); } }, "Cancel")),
    ];
  }
  const now = run.status;
  if (run.phase === "running" || !now) {
    return [el("p", {}, "Running ", el("code", {}, run.path), "…"),
      el("button", { id: "d-run-stop", class: "ghost danger", onclick: stopRun }, "Stop")];
  }
  const failures = Object.entries(now.failures || {});
  return [
    el("p", { class: now.code === 0 ? "good" : "warn" }, `${run.path}: ${runLabel(now)}.`),
    now.stopRequested && now.code === 130 ? el("p", { class: "hint" }, "You stopped it: what it had written before is not undone.") : null,
    now.error ? el("p", { class: "warn" }, now.error) : null,
    failures.map(([alias, why]) => el("p", { class: "warn" }, el("b", {}, alias), ": ", why)),
  ];
}

// ---------------------------------------------------------------- publication

// The last publication's outcome, for its tab: { id, saved }, { id, conflict } or, while the person
// chooses the node of the bricks several nodes offer, { id, choosing }.
let publication = null;
let publishing = null;
// The node chosen for each brick branch, by alias: sent with the next publication, left out where empty.
let placementChoice = {};

const short = (commit) => String(commit || "").slice(0, 8) || "nothing";
const defaultId = (path) => path.replace(/^.*\//, "").replace(/\.ya?ml$/i, "").toLowerCase().replace(/[^a-z0-9-]+/g, "-").replace(/^-+|-+$/g, "") || "pipeline";

// The coordinator receives the job saved in the folder, never the cards: its layout and its launch
// values stay on this machine. `replace` names the commit the person chose to replace after a conflict.
// A brick that several nodes offer asks the person for its node first; one that a single node offers needs no choice.
async function publish(replace) {
  if (!current) return;
  if (isDirty() || !current.hash) {
    status("Save the job first: the coordinator receives the job saved in the folder.", "bad");
    return;
  }
  if (replace === undefined) {
    let last = null;
    try { last = (await api(`/api/publish?path=${encodeURIComponent(current.path)}`)).publication; } catch { /* none */ }
    const id = prompt("The pipeline's id in the coordinator's library (lowercase letters, digits and '-'):", last?.id || defaultId(current.path));
    if (!id) return;
    const message = prompt("Describe this version (the commit message):", `Publish ${current.path} from dtpipe studio`);
    if (message === null) return;
    publishing = { id, message };
    placementChoice = {};
    let branches = [];
    try { branches = (await api(`/api/placement?path=${encodeURIComponent(current.path)}`)).branches; } catch { /* the coordinator names what it can */ }
    if (branches.some((b) => b.nodes.length > 1)) {
      publication = { id, choosing: branches };
      status("choose the node of each brick, then publish", "warn");
      showTab("publish");
      return;
    }
  }
  return send(replace);
}

async function send(replace) {
  try {
    const placement = Object.fromEntries(Object.entries(placementChoice).filter(([, node]) => node));
    const saved = await api("/api/publish", { path: current.path, hash: current.hash, id: publishing.id, message: publishing.message, replace, placement });
    publication = { id: publishing.id, saved };
    status(`published at ${short(saved.commit)}`, "good");
  } catch (e) {
    if (e.status === 422) {
      publication = { id: publishing.id, refused: { error: e.data?.error || e.message, report: e.data?.report ?? null } };
      status("not published: the coordinator's check refused the job", "bad");
      showTab("publish");
      return;
    }
    if (e.status !== 412) { status(e.message, "bad"); return; }
    const commit = e.data?.commit ?? null;
    publication = { id: publishing.id, conflict: { error: e.data?.error || e.message, commit } };
    showTab("publish");
    const question = commit
      ? `The library holds ${publishing.id} at ${short(commit)}, not the version this studio published last. Replace it with this job?`
      : `${publishing.id} is no longer in the library. Publish this job as a new pipeline?`;
    if (confirm(question)) return publish(commit ?? "");
    status("not published", "warn");
    return;
  }
  showTab("publish");
}

// One selector per brick branch: the node that runs it, among the nodes that offer the brick. Left empty, the branch is
// published unnamed, and the coordinator keeps the node it already holds for it, or lists it as a problem to settle there.
function choosingView({ id, choosing }) {
  return [
    el("p", {}, `Which node runs each brick of ${id}? The runner is never named.`),
    el("table", {}, choosing.map(({ alias, nodes }) => el("tr", {},
      el("td", {}, el("code", {}, alias)),
      el("td", {}, el("select", {
        "aria-label": `Node of ${alias}`,
        onchange: (e) => { placementChoice[alias] = e.target.value; },
      },
      el("option", { value: "", selected: !placementChoice[alias] && nodes.length > 1 }, "leave it to the coordinator"),
      nodes.map((node) => el("option", { value: node, selected: placementChoice[alias] ? placementChoice[alias] === node : nodes.length === 1 }, node))))))),
    el("p", {}, el("button", { id: "d-publish-now", onclick: () => send() }, "Publish")),
  ];
}

function publicationView() {
  if (!coordinator()) return el("p", { class: "hint" }, "Open the studio with --coordinator to publish a job into a coordinator's library.");
  if (!publication) {
    return el("p", { class: "hint" }, "Publish sends the job as saved in this folder to the coordinator's library. Its layout and its launch values stay on this machine.");
  }
  if (publication.choosing) return choosingView(publication);
  if (publication.refused) {
    const { error, report } = publication.refused;
    return [el("p", { class: "warn" }, `${publication.id} was not published: ${error}`),
      report ? el("pre", {}, report) : el("p", { class: "hint" }, "The check could not run: it needs a runner and the nodes of the bricks online.")];
  }
  if (publication.conflict) {
    const { error, commit } = publication.conflict;
    return [el("p", { class: "warn" }, `${publication.id} was not published: ${error}`),
      commit ? el("p", {}, "The library holds the commit ", el("code", {}, short(commit)), ".") : null];
  }
  const saved = publication.saved;
  return [
    el("p", { class: "good" }, `${saved.id} is published at the commit `, el("code", {}, short(saved.commit)), "."),
    saved.redistribute ? el("p", { class: "warn" }, "This pipeline had a plan: it no longer matches. Redistribute it, then redeploy it from the coordinator's console.") : null,
    saved.deployed ? el("p", { class: "warn" }, "The version in production stays the previous one until it is redeployed.") : null,
  ];
}

async function quit() {
  if (isDirty() && !confirm("Changes are not saved. Quit anyway?")) return;
  try {
    await api("/api/quit", {});
  } catch (e) {
    // A refusal (a run in progress) leaves the studio open; only a server already gone is a quit.
    if (e.status) { status(e.message, "bad"); return; }
  }
  forgetToken();
  fill(document.body, el("p", { class: "closed" }, "dtpipe studio has stopped. You can close this tab."));
}

async function start() {
  const components = await api("/api/components");
  mountDesigner($("designer"), components, {
    toolbar: () => [
      el("button", { id: "d-save", title: "Write the job and its layout (Ctrl+S)", onclick: () => save() }, "Save"),
      el("button", { id: "d-preview", class: "primary", title: "Run the whole job over the first source rows, with the writers neutralised: every card shows the rows it delivers",
        onclick: () => runPreview(null) }, "▶ Preview"),
      el("label", { id: "d-watch-rows-label", class: "rows", title: "Source rows each source reads (each card shows at most 1000 of them)" },
        "rows ", el("input", { id: "d-watch-rows", type: "number", min: "1", max: String(MAX_DEBUG_ROWS), value: String(dbg.rows), onchange: setDebugRows })),
      el("button", { id: "d-forget-pins", class: "ghost small", hidden: dbg.watch.length === 0, title: "Unpin every card and forget the checkpoints they kept", onclick: forgetPins }, "Forget pins"),
      el("button", { id: "d-abandon", class: "ghost danger", hidden: !dbg.real, title: "Forget the rows this real run holds; nothing was written",
        onclick: abandonRun }, "Abandon"),
      el("button", { id: "d-run", title: "Run the saved job here, after you confirm what it writes", onclick: () => prepareRun() }, dbg.real ? "Continue the run" : "Run"),
      coordinator() ? el("button", { id: "d-publish", title: "Send the saved job to the coordinator's library", onclick: () => publish() }, "Publish") : null,
    ],
    onComposed: (result) => { loadVariables(result); debugComposed(); },
    cardTools,
    cardBadge,
    cardTool,
    previewView,
    extraTabs: [{
      name: "variables",
      label: "Variables ",
      badge: () => { const n = vars.variables.filter((v) => !v.ready).length; return n ? el("span", { class: "count" }, n) : null; },
      render: variablesView,
    }, {
      name: "run",
      label: "Run",
      render: runView,
    }, {
      name: "publish",
      label: "Publish",
      render: publicationView,
    }],
  });
  await loadJobs();
  onCoordinator(loadBricks);
  onCoordinator(loadLibrary);
  await refreshCoordinator();
  await loadBricks();
  await loadLibrary();
}

$("quit").addEventListener("click", quit);
$("new-job").addEventListener("click", newJob);
$("import-command").addEventListener("click", importCommand);
$("library-refresh").addEventListener("click", loadLibrary);
document.addEventListener("keydown", (e) => {
  if (e.key === "s" && (e.ctrlKey || e.metaKey) && current) { e.preventDefault(); save(); }
});
window.addEventListener("beforeunload", (e) => { if (isDirty()) e.preventDefault(); });

if (!token) status("Open the address dtpipe studio printed: it carries this session's token.", "bad");
else start().catch((e) => status(e.message, "bad"));
