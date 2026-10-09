// Library: the pipelines kept in the coordinator's git repository, their plan and deployment
// state and their history. A pipeline is written with dtpipe studio and published into it.
import { ago, api, deploymentOf, esc, isActive, lastRunOf, toast } from "./core.js";
import * as detail from "./library-detail.js";

let host = null;
let entries = [];
let historyOf = null;      // pipeline id whose history is unfolded
let history = [];
let shown = null;          // { commit, yaml, diff }
let page = null;           // the detail page, when #/library/{id} is open

export function mount(el, params) {
  host = el;
  page = params[0] ? detail : null;
  if (page) { page.mount(el, params); return; }
  host.addEventListener("click", onClick);
  load();
}

export function update(kind) {
  if (page) { page.update(kind); return; }
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

// Deploy, Run and Undeploy from the list. The list knows only whether the saved plan is current and
// what is deployed; the page of the pipeline compares the deployed plan with today's, and the server
// decides in every case.
function deployRowActions(e) {
  const deployment = deploymentOf(e.id);
  const active = isActive(lastRunOf(e.id));
  const deploy = e.distributed && e.planCurrent && !active;
  const run = deployment && deployment.state !== "failed";
  return `<button class="small" data-deploy="${esc(e.id)}" ${deploy ? "" : "disabled"} title="${e.distributed && e.planCurrent ? "" : "Save its plan first, on its page"}">${deployment ? "Redeploy" : "Deploy"}</button>
        <button class="small primary" data-run="${esc(e.id)}" ${run ? "" : "disabled"}>Run</button>
        ${deployment ? `<button class="small ghost danger" data-undeploy="${esc(e.id)}" ${active ? "disabled" : ""}>Undeploy</button>` : ""}`;
}

function render() {
  if (!host) return;
  const rows = entries.map((e) => `
    <tr>
      <td><a href="#/library/${encodeURIComponent(e.id)}" class="strong">${esc(e.title)}</a><div class="hint mono">${esc(e.id)}</div></td>
      <td class="wrap hint">${esc(e.description || "")}</td>
      <td>${planChip(e)} ${deploymentChip(e.id)}</td>
      <td>${runCell(e.id)}</td>
      <td class="hint">${esc(e.message || "")}<div>${esc(ago(e.updatedAt))}</div></td>
      <td class="actions-cell">
        <a class="button small" href="#/library/${encodeURIComponent(e.id)}" title="Its plan, its fragments and its runs">Open</a>
        ${deployRowActions(e)}
        <button class="small ghost" data-history="${esc(e.id)}">${historyOf === e.id ? "Hide history" : "History"}</button>
        <button class="small ghost danger" data-delete="${esc(e.id)}">Delete</button>
      </td>
    </tr>
    ${historyOf === e.id ? `<tr class="history-row"><td colspan="6">${historyPanel(e.id)}</td></tr>` : ""}`).join("");

  host.innerHTML = `
    <div class="view-head">
      <h1>Library</h1>
      <p class="hint">Pipelines kept in the coordinator's own git repository. Each save is a commit; each one is a plain dtpipe job that also runs whole on one machine. Write one with <code>dtpipe studio --coordinator</code> and publish it here.</p>
    </div>
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

async function onClick(e) {
  const t = e.target.closest("button");
  if (!t) return;
  try {
    if (t.dataset.history) {
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
      await api(`/api/library/${encodeURIComponent(id)}`, { yaml: at.yaml, message: `Restore ${t.dataset.restore.slice(0, 8)}` }, "PUT");
      history = await api(`/api/library/${encodeURIComponent(id)}/history`);
      shown = null;
      toast(`${id} restored to ${t.dataset.restore.slice(0, 8)}.`, "info");
      await load();
    } else if (t.dataset.deploy) {
      const id = t.dataset.deploy;
      toast(`Deploying ${id}…`, "info");
      await api(`/api/library/${encodeURIComponent(id)}/deploy`, {});
      toast(`${id} deployed: every fragment registered.`, "info");
    } else if (t.dataset.run) {
      const run = await api("/api/runs", { pipelineId: t.dataset.run });
      toast(`${run.runId} ${run.state}.`, "info");
    } else if (t.dataset.undeploy) {
      await api(`/api/deployments/${encodeURIComponent(t.dataset.undeploy)}`, undefined, "DELETE");
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
