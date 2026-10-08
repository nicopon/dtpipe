// dtpipe studio: the jobs of a folder, opened as cards, sampled through the real engine and saved.

import { $, api, el, fill, forgetToken, status, token } from "./dom.js";
import { refreshCoordinator } from "./coordinator.js";
import { cards, compose, isDirty, markSaved, mountDesigner, openBlank, openJob, refreshDrawer, showTab } from "./designer.js";

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

const leaveCurrent = () => !isDirty() || confirm(`${current?.path || "This job"} has changes that are not saved. Leave it anyway?`);

async function open(path) {
  if (!leaveCurrent()) return;
  try {
    const job = await api(`/api/job?path=${encodeURIComponent(path)}`);
    current = { path, hash: job.hash, rewrites: job.rewrites };
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
  $("job-name").textContent = `${path} (new)`;
  await openBlank(path.replace(/^.*\//, "").replace(/\.ya?ml$/i, ""));
  await loadJobs();
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
    const answer = await api("/api/variables", { path: current?.path || null, yaml: result?.yaml || "" });
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
    return await api("/api/sample", { yaml, rows, path: current?.path || null });
  } catch (e) {
    if (e.status === 422) showTab("variables");
    throw e;
  }
}

async function quit() {
  if (isDirty() && !confirm("Changes are not saved. Quit anyway?")) return;
  try { await api("/api/quit", {}); } catch { /* the server may already be gone */ }
  forgetToken();
  fill(document.body, el("p", { class: "closed" }, "dtpipe studio has stopped. You can close this tab."));
}

async function start() {
  const components = await api("/api/components");
  mountDesigner($("designer"), components, {
    toolbar: () => el("button", { id: "d-save", title: "Write the job and its layout (Ctrl+S)", onclick: () => save() }, "Save"),
    onComposed: loadVariables,
    sample,
    extraTabs: [{
      name: "variables",
      label: "Variables ",
      badge: () => { const n = vars.variables.filter((v) => !v.ready).length; return n ? el("span", { class: "count" }, n) : null; },
      render: variablesView,
    }],
  });
  await loadJobs();
  await refreshCoordinator();
}

$("quit").addEventListener("click", quit);
$("new-job").addEventListener("click", newJob);
document.addEventListener("keydown", (e) => {
  if (e.key === "s" && (e.ctrlKey || e.metaKey) && current) { e.preventDefault(); save(); }
});
window.addEventListener("beforeunload", (e) => { if (isDirty()) e.preventDefault(); });

if (!token) status("Open the address dtpipe studio printed: it carries this session's token.", "bad");
else start().catch((e) => status(e.message, "bad"));
