// @ts-check
// dtpipe studio: the jobs of a folder, opened as cards, sampled through the real engine and saved. This module
// wires the screens together and owns the job open and the list of jobs; each screen keeps its own part of the
// state (state.js) and its own module.

import { $, api, el, fill, forgetToken, status, token } from "./dom.js";
import { onCoordinator, refreshCoordinator } from "./coordinator.js";
import { compose, markSaved, mountDesigner, openBlank, openJob, restoreSizes, setCatalog } from "./designer.js";
import { readView, rememberView, sizesRead, viewRestored } from "./prefs.js";
import { afterFlush, invalidate, onZone } from "./render.js";
import { sequence, state } from "./state.js";
import { adoptDraft, cardBadge, cardTool, cardTools, debugComposed, forgetPins, forgetPreview, previewView, restoreRows, runPreview, setDebugRows } from "./preview.js";
import { forgetRun, prepareRun, restoreReview, runView } from "./run.js";
import { forgetPublication, publicationView, publish } from "./publish.js";
import { loadVariables, onLaunchChanged, unreadyCount, variablesView } from "./variables.js";

const MAX_DEBUG_ROWS = 100000;
const opening = sequence();

// ---------------------------------------------------------------- the jobs

async function loadJobs() {
  const { folder, jobs } = await api("/api/jobs");
  state.jobs.folder = folder;
  state.jobs.paths = jobs;
  invalidate("jobs");
}

// The coordinator's library, when a person is signed in: each pipeline opens as a new file of the folder.
async function loadLibrary() {
  const library = state.jobs.library;
  if (state.coordinator?.state !== "signed-in") { library.shown = false; invalidate("jobs"); return; }
  library.shown = true;
  try {
    library.pipelines = (await api("/api/library")).pipelines;
    library.error = null;
  } catch (e) {
    library.pipelines = null;
    library.error = e.message;
  }
  invalidate("jobs");
}

function renderJobs() {
  const { folder, paths, library } = state.jobs;
  $("folder").textContent = folder;
  $("job-name").textContent = state.job ? `${state.job.path}${state.job.hash ? "" : " (new)"}` : "";
  fill($("jobs"), paths.length
    ? paths.map((path) => el("li", {}, el("button", { class: path === state.job?.path ? "job open" : "job", onclick: () => open(path) }, path)))
    : el("li", { class: "muted" }, "No .yaml or .yml file in this folder."));
  $("library-pane").hidden = !library.shown;
  if (!library.shown) return;
  fill($("library"), library.error ? el("li", { class: "warn" }, library.error)
    : library.pipelines?.length
      ? library.pipelines.map((p) => el("li", {}, el("button", { class: "job", title: p.description || p.title || p.id, onclick: () => openFromLibrary(p.id) }, p.id)))
      : el("li", { class: "muted" }, "The library holds no pipeline."));
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
function forgetResults() {
  restoreReview(false);
  forgetPreview();
  forgetPublication();
  forgetRun();
}

const leaveCurrent = () => !state.cards.dirty || confirm(`${state.job?.path || "This job"} has changes that are not saved. Leave it anyway?`);

async function open(path, left = false) {
  if (!left && !leaveCurrent()) return;
  // A job asked for while another is still being read replaces it: only the last one asked for is opened.
  const ticket = opening.begin();
  try {
    const [job, view] = await Promise.all([api(`/api/job?path=${encodeURIComponent(path)}`), readView(path)]);
    if (opening.stale(ticket)) return;
    state.job = { path, hash: job.hash, rewrites: job.rewrites };
    forgetResults();
    invalidate("jobs");
    // The rows asked are the person's last, unless the job kept its own.
    if (view.job?.rows) restoreRows(view.job.rows);
    restoreReview(view.job?.review === true);
    if (!(await openJob(job.yaml, job.layout, false, view.job))) return;
    viewRestored(path, view.job);
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
  opening.cancel();
  state.job = { path, hash: null, rewrites: false };
  forgetResults();
  viewRestored(path);
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
    opening.cancel();
    state.job = { path, hash: null, rewrites: false };
    forgetResults();
    viewRestored(path);
    await openJob(yaml, null, true);
    status("imported: save it to keep it", "good");
    await loadJobs();
  } catch (e) {
    status(e.message, "bad");
  }
}

// The server writes the job it composes from the cards, and the cards' layout under .dtpipe/.
async function save() {
  const job = state.job;
  if (!job) return;
  const result = await compose();
  if (job.rewrites && !confirm("Saving writes this job as the studio composes it: comments inside it and the order of its keys are not kept. Save anyway?")) return;
  const acceptErrors = result.errors?.length > 0;
  if (acceptErrors && !confirm(`${result.errors.length} problem(s) remain: the job written may not run. Save anyway?`)) return;
  try {
    const answer = await api("/api/job", { path: job.path, hash: job.hash, model: state.cards.model, acceptErrors }, "PUT");
    state.job = { path: job.path, hash: answer.hash, rewrites: false };
    // What a preview pinned before the first save belongs to the job now.
    if (!job.hash) await adoptDraft();
    invalidate("jobs");
    markSaved();
    status("saved", "good");
    await loadJobs();
  } catch (e) {
    status(e.message, "bad");
  }
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

// ---------------------------------------------------------------- the bar

// The bar is built once, with the designer's shell; this draws what it shows from the state, in place, so the field
// of rows keeps its focus while a preview comes and goes.
function renderBar() {
  const dbg = state.preview;
  const set = (id, props) => { const node = $(id); if (node) Object.assign(node, props); };
  const suspended = dbg.real;
  set("d-preview", { disabled: dbg.running || suspended, title: suspended ? SUSPENDED : PREVIEW_TITLE });
  set("d-suspended", { hidden: !suspended });
  // The rows are typed here: a drawing never rewrites what the person is typing.
  set("d-watch-rows", { disabled: dbg.running || dbg.real, ...(document.activeElement === $("d-watch-rows") ? {} : { value: String(dbg.rows) }) });
  set("d-forget-pins", { disabled: dbg.running || dbg.real, hidden: dbg.watch.length === 0 });
  set("d-run", { disabled: dbg.running || suspended, title: suspended ? SUSPENDED : RUN_TITLE });
}

const SUSPENDED = "A real run is suspended before its targets are written: write it or abandon it from the sink cards.";
const PREVIEW_TITLE = "Run the whole job over the first source rows, with the writers neutralised: every card shows the rows it delivers";
const RUN_TITLE = "Run the saved job here and write its targets, after you confirm what it writes";

function barButtons() {
  return [
    el("button", { id: "d-save", title: "Write the job and its layout (Ctrl+S)", onclick: () => save() }, "Save"),
    el("button", { id: "d-preview", class: "primary", title: PREVIEW_TITLE, onclick: () => runPreview(null) }, "▶ Preview"),
    el("label", { id: "d-watch-rows-label", class: "rows", title: "Source rows each source reads (each card shows at most 1000 of them)" },
      "rows ", el("input", { id: "d-watch-rows", type: "number", min: "1", max: String(MAX_DEBUG_ROWS), value: String(state.preview.rows), onchange: setDebugRows })),
    el("button", { id: "d-forget-pins", class: "ghost small", hidden: true, title: "Unpin every card and forget the checkpoints they kept", onclick: forgetPins }, "Forget pins"),
    el("span", { id: "d-suspended", class: "suspended", hidden: true, title: SUSPENDED }, "⏸ run suspended: write or abandon it on the sink cards"),
    el("button", { id: "d-run", class: "danger", title: RUN_TITLE, onclick: () => prepareRun() }, "Write targets…"),
    state.coordinator ? el("button", { id: "d-publish", title: "Send the saved job to the coordinator's library", onclick: () => publish() }, "Publish") : null,
  ];
}

async function quit() {
  if (state.cards.dirty && !confirm("Changes are not saved. Quit anyway?")) return;
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
  state.components = await api("/api/components");
  onZone("bar", renderBar);
  onZone("jobs", renderJobs);
  onLaunchChanged(debugComposed);
  afterFlush(rememberView);
  mountDesigner($("designer"), {
    toolbar: barButtons,
    onComposed: (result) => { loadVariables(result); debugComposed(); },
    cardTools,
    cardBadge,
    // A sink writes the run it waits on; the rest of the card's tools belong to the preview.
    cardTool: (alias, name) => (name === "write" ? prepareRun(true) : cardTool(alias, name)),
    previewView,
    extraTabs: [{
      name: "variables",
      label: "Variables ",
      badge: () => { const n = unreadyCount(); return n ? el("span", { class: "count" }, n) : null; },
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
  // The panes come back as they were left, before anything is kept of them.
  const kept = await readView();
  if (kept.sizes) restoreSizes(kept.sizes);
  sizesRead();
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
  if (e.key === "s" && (e.ctrlKey || e.metaKey) && state.job) { e.preventDefault(); save(); }
});
window.addEventListener("beforeunload", (e) => { if (state.cards.dirty) e.preventDefault(); });

if (!token) status("Open the address dtpipe studio printed: it carries this session's token.", "bad");
else start().catch((e) => status(e.message, "bad"));
