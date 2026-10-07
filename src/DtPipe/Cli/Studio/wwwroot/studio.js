// dtpipe studio: the jobs of a folder, opened and sampled through the real engine.
// Every value the page shows comes from a job or a source, so it is set as text, never as HTML: a
// script slipped into a sampled value would run dtpipe with the person's own access.

const TOKEN_KEY = "dtpipe-studio-token";
const $ = (id) => document.getElementById(id);

// The token arrives in the fragment, which no request carries, and leaves the address bar at once.
const token = (() => {
  const fromUrl = location.hash.slice(1);
  if (fromUrl) {
    sessionStorage.setItem(TOKEN_KEY, fromUrl);
    history.replaceState(null, "", location.pathname);
    return fromUrl;
  }
  return sessionStorage.getItem(TOKEN_KEY) || "";
})();

let current = null;

function el(tag, props = {}, ...children) {
  const node = document.createElement(tag);
  for (const [k, v] of Object.entries(props)) {
    if (k === "class") node.className = v;
    else if (k.startsWith("on")) node.addEventListener(k.slice(2), v);
    else node.setAttribute(k, v);
  }
  for (const c of children.flat()) if (c != null) node.append(c instanceof Node ? c : String(c));
  return node;
}

async function api(path, body) {
  const response = await fetch(path, {
    method: body === undefined ? "GET" : "POST",
    headers: { "X-Dtpipe-Studio": token, "Content-Type": "application/json" },
    body: body === undefined ? undefined : JSON.stringify(body),
  });
  const text = await response.text();
  const data = text ? JSON.parse(text) : {};
  if (!response.ok) throw new Error(data.error || `${response.status} ${response.statusText}`);
  return data;
}

function status(text, kind = "") {
  $("status").textContent = text;
  $("status").className = `pill ${kind}`;
}

async function loadJobs() {
  const { folder, jobs } = await api("/api/jobs");
  $("folder").textContent = folder;
  const list = $("jobs");
  list.replaceChildren(...(jobs.length
    ? jobs.map((path) => el("li", {}, el("button", { class: path === current ? "job open" : "job", onclick: () => openJob(path) }, path)))
    : [el("li", { class: "muted" }, "No .yaml or .yml file in this folder.")]));
}

async function openJob(path) {
  try {
    const { yaml } = await api(`/api/job?path=${encodeURIComponent(path)}`);
    current = path;
    $("yaml").value = yaml;
    $("job-name").textContent = path;
    $("job-name").className = "";
    await loadJobs();
  } catch (e) {
    status(e.message, "bad");
  }
}

async function sample() {
  const yaml = $("yaml").value;
  if (!yaml.trim()) return status("Nothing to sample.", "bad");
  $("sample").disabled = true;
  status("sampling…", "busy");
  try {
    const result = await api("/api/sample", { yaml, rows: Number($("rows").value) || 20 });
    render(result);
    status(result.success ? "sampled" : "failed", result.success ? "good" : "bad");
  } catch (e) {
    status(e.message, "bad");
  } finally {
    $("sample").disabled = false;
  }
}

const list = (title, items, kind) => (items && items.length
  ? el("div", { class: `notes ${kind}` }, el("b", {}, title), el("ul", {}, items.map((i) => el("li", {}, i))))
  : null);

function stageTable(stage) {
  const columns = stage.columns || [];
  return el("table", {},
    el("thead", {}, el("tr", {}, columns.map((c) => el("th", {}, c.Name ?? c.name, el("small", {}, c.type))))),
    el("tbody", {}, (stage.sample || []).map((row) => el("tr", {},
      row.map((v) => (v === null ? el("td", { class: "null" }, "null") : el("td", {}, v)))))));
}

function branchCard(branch) {
  return el("article", { class: "branch" },
    el("h3", {}, branch.branch),
    el("p", { class: "muted" },
      `${branch.rowsDelivered} rows delivered to the neutralised writer · source protection: ${branch.sourceProtection}`),
    list("Schema errors", branch.schemaCompatibility?.errors, "bad"),
    list("Schema warnings", branch.schemaCompatibility?.warnings, "warn"),
    branch.schemaInspectionError ? list("Schema inspection", [branch.schemaInspectionError], "warn") : null,
    list("Hints", branch.performanceHints, "info"),
    (branch.stages || []).map((stage) => el("section", { class: "stage" },
      el("h4", {}, `${stage.index}. ${stage.name}`,
        el("span", { class: "muted" }, ` ${stage.rowsSeen} rows${stage.columnar ? " · columnar" : ""}`)),
      el("div", { class: "scroll" }, stageTable(stage)))));
}

function render(result) {
  const errors = result.errors || (result.error ? [result.error] : []);
  $("results").replaceChildren(...[
    el("p", { class: result.success ? "good" : "bad" },
      result.success ? `Sampled ${result.rowsRequested} source rows. The target was not written.` : `The sample failed${result.stage ? ` (${result.stage})` : ""}. The target was not written.`),
    list("Errors", errors, "bad"),
    ...(result.branches || []).map(branchCard),
  ].filter(Boolean));
}

async function quit() {
  try { await api("/api/quit", {}); } catch { /* the server may already be gone */ }
  sessionStorage.removeItem(TOKEN_KEY);
  document.body.replaceChildren(el("p", { class: "closed" }, "dtpipe studio has stopped. You can close this tab."));
}

$("sample").addEventListener("click", sample);
$("quit").addEventListener("click", quit);
$("yaml").addEventListener("keydown", (e) => {
  if (e.key === "Enter" && (e.ctrlKey || e.metaKey)) { e.preventDefault(); sample(); }
});

if (!token) status("Open the address dtpipe studio printed: it carries this session's token.", "bad");
else loadJobs().catch((e) => status(e.message, "bad"));
