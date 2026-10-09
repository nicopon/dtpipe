// dtpipe studio: the jobs of a folder, opened as cards, sampled through the real engine and saved.

import { $, api, el, fill, forgetToken, status, token } from "./dom.js";
import { coordinator, onCoordinator, refreshCoordinator } from "./coordinator.js";
import { brickBranches, cards, compose, isDirty, markSaved, mountDesigner, openBlank, openJob, refreshDrawer, setCatalog, showTab } from "./designer.js";

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

const leaveCurrent = () => !isDirty() || confirm(`${current?.path || "This job"} has changes that are not saved. Leave it anyway?`);

async function open(path, left = false) {
  if (!left && !leaveCurrent()) return;
  try {
    const job = await api(`/api/job?path=${encodeURIComponent(path)}`);
    current = { path, hash: job.hash, rewrites: job.rewrites };
    publication = null;
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
  publication = null;
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
    publication = null;
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
    const answer = await api("/api/job", { path: current.path, hash: current.hash, model: cards(), acceptErrors }, "PUT");
    current = { path: current.path, hash: answer.hash, rewrites: false };
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

async function sample(yaml, rows) {
  try {
    return await api("/api/sample", { yaml, rows, path: current?.path || null, bricks: brickBranches() });
  } catch (e) {
    if (e.status === 422) showTab("variables");
    throw e;
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
    const prepared = await api("/api/run/prepare", { path: current.path, hash: current.hash, bricks: brickBranches() });
    run = { phase: "confirm", prepared, bricks: brickBranches(), hash: current.hash, path: current.path };
    showTab("run");
  } catch (e) {
    run = null;
    status(e.message, "bad");
  }
}

async function startRun() {
  try {
    await api("/api/run", { path: run.path, hash: run.hash, bricks: run.bricks, digest: run.prepared.digest });
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
      if (!now.running) { clearInterval(runTimer); status(`run ${runLabel(now)}`, now.code === 0 ? "good" : "warn"); }
      showTab("run");
    } catch (e) {
      clearInterval(runTimer);
      status(e.message, "bad");
    }
  }, 1000);
}

async function stopRun() {
  try { await api("/api/run/stop", {}); status("stop asked", "warn"); } catch (e) { status(e.message, "bad"); }
}

function runView() {
  if (!run) return el("p", { class: "hint" }, "Run executes the job saved in this folder, as dtpipe --job would, on this machine. You confirm what it writes first.");
  if (run.phase === "confirm") {
    const writes = run.prepared.writes;
    return [
      el("p", { class: "warn" }, "This run is real: its writers write their targets. A sink brick is written to a throwaway file here, never to its node."),
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

// The last publication's outcome, for its tab: { id, saved } or { id, conflict }.
let publication = null;
let publishing = null;

const short = (commit) => String(commit || "").slice(0, 8) || "nothing";
const defaultId = (path) => path.replace(/^.*\//, "").replace(/\.ya?ml$/i, "").toLowerCase().replace(/[^a-z0-9-]+/g, "-").replace(/^-+|-+$/g, "") || "pipeline";

// The coordinator receives the job saved in the folder, never the cards: its layout and its launch
// values stay on this machine. `replace` names the commit the person chose to replace after a conflict.
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
  }
  try {
    const saved = await api("/api/publish", { path: current.path, hash: current.hash, id: publishing.id, message: publishing.message, replace });
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

function publicationView() {
  if (!coordinator()) return el("p", { class: "hint" }, "Open the studio with --coordinator to publish a job into a coordinator's library.");
  if (!publication) {
    return el("p", { class: "hint" }, "Publish sends the job as saved in this folder to the coordinator's library. Its layout and its launch values stay on this machine.");
  }
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
      el("button", { id: "d-run", title: "Run the saved job here, after you confirm what it writes", onclick: () => prepareRun() }, "Run"),
      coordinator() ? el("button", { id: "d-publish", title: "Send the saved job to the coordinator's library", onclick: () => publish() }, "Publish") : null,
    ],
    onComposed: loadVariables,
    sample,
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
