// Library: the pipelines kept in the coordinator's git repository, their plan and deployment
// state, their history, and the ways in: a new pipeline, pasted YAML, or a dtpipe command line.
import { ago, api, deploymentOf, esc, isActive, lastRunOf, store, toast } from "./core.js";

let host = null;
let entries = [];
let historyOf = null;      // pipeline id whose history is unfolded
let history = [];
let shown = null;          // { commit, yaml, diff }
let importing = false;

export function mount(el) {
  host = el;
  host.addEventListener("click", onClick);
  load();
}

export function update(kind) {
  if (kind === "library") load();
  else if (["all", "deployments", "runs"].includes(kind)) render();
}

async function load() {
  try { entries = await api("/api/library"); } catch (e) { toast(e.message); }
  render();
}

function planChip(e) {
  if (!e.distributed) return `<span class="chip">not distributed</span>`;
  return e.planCurrent ? `<span class="chip ok">plan saved</span>` : `<span class="chip warn" title="The job changed after its plan was saved">plan stale</span>`;
}

function deploymentChip(id) {
  const d = deploymentOf(id);
  return d ? `<span class="chip ${d.state === "failed" ? "bad" : d.state === "ready" ? "ok" : "warn"}">${esc(d.state)}</span>` : `<span class="chip">not deployed</span>`;
}

function runCell(id) {
  const run = lastRunOf(id);
  if (!run) return `<span class="hint">never run</span>`;
  const rows = run.result?.edgeCounts?.reduce((s, e) => s + (e.received ?? 0), 0);
  return `<span class="outcome-chip ${esc(run.state)}">${esc(run.state)}</span> <span class="hint">${isActive(run) ? "" : ago(run.finishedAt)}${rows ? ` · ${rows.toLocaleString("en-US")} rows` : ""}</span>`;
}

function render() {
  if (!host) return;
  const rows = entries.map((e) => `
    <tr>
      <td><a href="#/designer/${encodeURIComponent(e.id)}" class="strong">${esc(e.title)}</a><div class="hint mono">${esc(e.id)}</div></td>
      <td class="wrap hint">${esc(e.description || "")}</td>
      <td>${planChip(e)} ${deploymentChip(e.id)}</td>
      <td>${runCell(e.id)}</td>
      <td class="hint">${esc(e.message || "")}<div>${esc(ago(e.updatedAt))}</div></td>
      <td class="actions-cell">
        <a class="button small" href="#/designer/${encodeURIComponent(e.id)}">Design</a>
        <a class="button small" href="#/distribution/${encodeURIComponent(e.id)}">Distribute</a>
        <button class="small ghost" data-history="${esc(e.id)}">${historyOf === e.id ? "Hide history" : "History"}</button>
        <button class="small ghost danger" data-delete="${esc(e.id)}">Delete</button>
      </td>
    </tr>
    ${historyOf === e.id ? `<tr class="history-row"><td colspan="6">${historyPanel(e.id)}</td></tr>` : ""}`).join("");

  host.innerHTML = `
    <div class="view-head">
      <h1>Library</h1>
      <p class="hint">Pipelines kept in the coordinator's own git repository. Each save is a commit; each one is a plain dtpipe job that also runs whole on one machine.</p>
      <div class="actions">
        <button class="primary" data-new>New pipeline</button>
        <button data-import>${importing ? "Close import" : "Import YAML or a command line"}</button>
      </div>
    </div>
    ${importing ? importPanel() : ""}
    <section class="card">
      <table class="library">
        <tr><th>pipeline</th><th>description</th><th>state</th><th>last run</th><th>last change</th><th></th></tr>
        ${rows || `<tr><td colspan="6" class="hint">The library is empty.</td></tr>`}
      </table>
    </section>`;
}

function historyPanel(id) {
  const list = history.map((c) => `
    <div class="commit ${shown?.commit === c.hash ? "active" : ""}">
      <code>${esc(c.hash.slice(0, 8))}</code> <span>${esc(c.message)}</span> <span class="hint">${esc(new Date(c.date).toLocaleString())}</span>
      <span class="actions"><button class="small ghost" data-show="${esc(c.hash)}">show</button>
      <button class="small ghost" data-restore="${esc(c.hash)}" data-id="${esc(id)}">restore</button></span>
    </div>`).join("");
  return `<div class="history-panel"><div>${list || `<p class="hint">No commit.</p>`}</div>
    ${shown ? `<div class="grid2"><div><h3>Change</h3><pre class="diff">${diffHtml(shown.diff)}</pre></div><div><h3>Job at ${esc(shown.commit.slice(0, 8))}</h3><pre>${esc(shown.yaml)}</pre></div></div>` : ""}</div>`;
}

function diffHtml(diff) {
  return esc(diff || "(no change to this pipeline)").split("\n").map((l) =>
    l.startsWith("+") && !l.startsWith("+++") ? `<span class="add">${l}</span>` : l.startsWith("-") && !l.startsWith("---") ? `<span class="del">${l}</span>` : l).join("\n");
}

function importPanel() {
  return `
    <section class="card import">
      <div class="row"><label>Id <input id="import-id" class="mono" placeholder="my-pipeline" pattern="[a-z0-9][a-z0-9-]*"></label>
      <span class="hint">lowercase letters, digits and '-'</span></div>
      <div class="grid2">
        <div><h3>A dtpipe job (YAML)</h3><textarea id="import-yaml" rows="10" spellcheck="false" placeholder="orders:\n  input: duck:\${{LAB_SALES_DB}}\n  ..."></textarea>
          <button data-import-yaml>Open in the designer</button></div>
        <div><h3>A dtpipe command line</h3><textarea id="import-command" rows="4" spellcheck="false" class="mono" placeholder="-i sqlite:\${{LAB_CRM_DB}} -q &quot;SELECT * FROM customers&quot; -o duck:\${{LAB_WAREHOUSE_DB}} --table t"></textarea>
          <p class="hint">Converted by dtpipe's own <code>--export-job</code>, then taken apart into one card per step; a branch that reads or writes a brick becomes that brick.</p>
          <button data-import-command>Convert and open</button></div>
      </div>
    </section>`;
}

function splitArgs(line) {
  const args = [];
  let current = "", quote = null, has = false;
  for (let i = 0; i < line.length; i++) {
    const c = line[i];
    if (quote) {
      if (c === quote) quote = null;
      else if (c === "\\" && quote === '"' && i + 1 < line.length) current += line[++i];
      else current += c;
    } else if (c === "'" || c === '"') { quote = c; has = true; }
    else if (/\s/.test(c)) { if (has || current) { args.push(current); current = ""; has = false; } }
    else if (c === "\\" && i + 1 < line.length) { current += line[++i]; has = true; }
    else { current += c; has = true; }
  }
  if (has || current) args.push(current);
  if (args[0] === "dtpipe") args.shift();
  return args;
}

function askId(suggestion = "") {
  const id = (prompt("Pipeline id (lowercase letters, digits and '-'):", suggestion) || "").trim();
  if (!id) return null;
  if (!/^[a-z0-9][a-z0-9-]{0,62}$/.test(id)) { toast(`'${id}' is not a pipeline id.`); return null; }
  if (entries.some((e) => e.id === id)) { toast(`'${id}' already exists.`); return null; }
  return id;
}

async function openDraft(id, yaml) {
  const result = await api("/api/design/decompose", { yaml });
  store.draft = { id, model: result.model };
  location.hash = `#/designer/${encodeURIComponent(id)}`;
}

async function onClick(e) {
  const t = e.target.closest("button");
  if (!t) return;
  try {
    if (t.dataset.new !== undefined) {
      const id = askId();
      if (!id) return;
      store.draft = { id, model: { title: "Untitled pipeline", description: "", check: null, steps: [], layout: {} } };
      location.hash = `#/designer/${encodeURIComponent(id)}`;
    } else if (t.dataset.import !== undefined) {
      importing = !importing;
      render();
    } else if (t.dataset.importYaml !== undefined || t.dataset.importCommand !== undefined) {
      const id = document.getElementById("import-id").value.trim();
      if (!/^[a-z0-9][a-z0-9-]{0,62}$/.test(id)) { toast("Give the pipeline an id first: lowercase letters, digits and '-'."); return; }
      if (entries.some((x) => x.id === id)) { toast(`'${id}' already exists.`); return; }
      let yaml = document.getElementById("import-yaml").value;
      if (t.dataset.importCommand !== undefined) {
        yaml = (await api("/api/pipelines/from-command", { args: splitArgs(document.getElementById("import-command").value) })).yaml;
      }
      await openDraft(id, yaml);
    } else if (t.dataset.history) {
      const id = t.dataset.history;
      shown = null;
      if (historyOf === id) { historyOf = null; render(); return; }
      historyOf = id;
      history = await api(`/api/library/${encodeURIComponent(id)}/history`);
      render();
    } else if (t.dataset.show) {
      const at = await api(`/api/library/${encodeURIComponent(historyOf)}/at/${t.dataset.show}`);
      shown = { commit: t.dataset.show, ...at };
      render();
    } else if (t.dataset.restore) {
      const id = t.dataset.id;
      const at = await api(`/api/library/${encodeURIComponent(id)}/at/${t.dataset.restore}`);
      await api(`/api/library/${encodeURIComponent(id)}`, { yaml: at.yaml, layout: at.layout, message: `Restore ${t.dataset.restore.slice(0, 8)}` }, "PUT");
      history = await api(`/api/library/${encodeURIComponent(id)}/history`);
      shown = null;
      toast(`${id} restored to ${t.dataset.restore.slice(0, 8)}.`, "info");
      await load();
    } else if (t.dataset.delete) {
      const id = t.dataset.delete;
      if (!confirm(`Delete ${id} from the library? Its history stays in the repository.`)) return;
      await api(`/api/library/${encodeURIComponent(id)}`, undefined, "DELETE");
      if (historyOf === id) historyOf = null;
      await load();
    }
  } catch (err) {
    toast(err.message);
  }
}
