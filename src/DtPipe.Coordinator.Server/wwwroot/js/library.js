// Library: the pipelines kept in the coordinator's git repository, their plan and deployment
// state and their history. A pipeline is written with dtpipe studio and published into it.
import { ROWS_CROSSED_HINT, ago, api, deploymentOf, esc, healthChip, healthOf, isActive, lastRunOf, rowsCrossed, runStateLabel, toast } from "./core.js";
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
  host.addEventListener("keydown", (ev) => { if (ev.key === "Escape") closeMenus(null); });
  load();
}

export function update(kind) {
  if (page) { page.update(kind); return; }
  if (kind === "library") load();
  else if (["all", "deployments", "runs", "health"].includes(kind)) render();
}

async function load() {
  try { entries = await api("/api/library"); } catch (e) { toast(e.message); }
  render();
}

// Why the bindings of a pipeline's bricks do not hold: a node it names is absent or does not offer the brick, a brick has none,
// or its text is no longer the brick it is bound to.
function bindingsChip(e) {
  const problems = e.bindingProblems || [];
  if (!problems.length) return "";
  const blocking = problems.some((p) => p.blocking);
  return `<span class="chip ${blocking ? "bad" : "warn"}" title="${esc(problems.map((p) => p.message).join(" "))}">bindings: ${problems.length} problem${problems.length > 1 ? "s" : ""}</span>`;
}

// A pipeline is armed at a commit: the list says so, and says when the library has moved on from it.
function deploymentChip(e) {
  const d = deploymentOf(e.id);
  if (!d) return `<span class="chip">not deployed</span>`;
  const older = d.commit && e.commit && d.commit !== e.commit;
  return `<span class="chip ${d.state === "failed" ? "bad" : d.state === "ready" ? "ok" : "warn"}">${esc(d.state)}${d.commit ? ` @ ${esc(d.commit.slice(0, 8))}` : ""}</span>`
    + (older ? ` <span class="chip warn" title="The library holds a later version of the job: deploy it again to run that one">older version</span>` : "");
}

function runCell(id) {
  const run = lastRunOf(id);
  if (!run) return `<span class="hint">never run</span>`;
  const rows = rowsCrossed(run);
  const label = esc(runStateLabel(run.state));
  const chip = run.reason
    ? `<span class="tip"><span class="outcome-chip ${esc(run.state)} tip-t" tabindex="0">${label}</span><span class="tip-b"><b>Why</b>${esc(run.reason)}</span></span>`
    : `<span class="outcome-chip ${esc(run.state)}">${label}</span>`;
  return `${chip}<span title="${esc(ROWS_CROSSED_HINT)}">${isActive(run) ? "" : esc(ago(run.finishedAt))}${rows ? ` · ${rows.toLocaleString("en-US")} rows crossed` : ""}</span>`;
}

// Deploy and Run stay in the bar; Undeploy and Delete, rare and destructive, sit in the menu. Deploying arms the pipeline at
// the commit the library holds, and the server decides in every case: it refuses bindings that do not hold and a plan no node could run.
function deployRowActions(e) {
  const deployment = deploymentOf(e.id);
  const active = isActive(lastRunOf(e.id));
  const unbound = (e.bindingProblems || []).some((p) => p.blocking);
  const deploy = !active && !unbound;
  const run = deployment && deployment.state !== "failed";
  return `<button class="small primary" data-needs="designer" data-run="${esc(e.id)}" ${run ? "" : "disabled"}>Run</button>
        <button class="small" data-needs="designer" data-deploy="${esc(e.id)}" ${deploy ? "" : "disabled"} title="${unbound ? "Its bindings do not hold" : ""}">Deploy</button>`;
}

function moreMenu(e) {
  const deployment = deploymentOf(e.id);
  const active = isActive(lastRunOf(e.id));
  return `<details class="menu"><summary aria-label="More actions for ${esc(e.title)}" title="More actions">⋯</summary>
    <div class="menu-pop">
      ${deployment ? `<button class="small" data-needs="designer" data-undeploy="${esc(e.id)}" ${active ? "disabled" : ""}>Undeploy</button><div class="sep"></div>` : ""}
      <button class="small danger" data-needs="designer" data-delete="${esc(e.id)}">Delete…</button>
    </div></details>`;
}

// The stripe says what needs a look: a run that did not go through, a problem reported, or nothing.
function severity(e) {
  const run = lastRunOf(e.id);
  if (run && ["failed", "error", "refused"].includes(run.state)) return "bad";
  const d = deploymentOf(e.id);
  if (d && d.state === "failed") return "bad";
  const h = healthOf(e.id);
  if ((e.bindingProblems || []).length || (h && h.status !== "ok")) return "warn";
  return "";
}

// The description and the reason of the last run: a popover where a pointer hovers, a printed line where none does.
function descriptionTip(e) {
  if (!e.description) return "";
  return `<span class="tip"><span class="info-dot tip-t" tabindex="0" role="note" aria-label="Description">i</span><span class="tip-b"><b>Description</b>${esc(e.description)}</span></span>`;
}

function render() {
  if (!host) return;
  const items = entries.map((e) => {
    const run = lastRunOf(e.id);
    const printed = [e.description ? `<div class="lib-printed">${esc(e.description)}</div>` : "",
      run && run.reason ? `<div class="lib-printed bad">${esc(run.reason)}</div>` : ""].join("");
    return `
    <article class="lib-item ${severity(e)}">
      <div class="lib-facts">
        <div class="lib-title"><a href="#/library/${encodeURIComponent(e.id)}" title="${esc(e.title)}">${esc(e.title)}</a>${descriptionTip(e)}<span class="mono">${esc(e.id)}</span></div>
        <div class="chips-wrap">${deploymentChip(e)} ${healthChip(e.id)} ${bindingsChip(e)}</div>
        <div class="lib-run">${runCell(e.id)}</div>
      </div>
      ${printed}
      <div class="lib-actions">
        <a class="button small" href="#/library/${encodeURIComponent(e.id)}" title="The plan a launch would make, and its runs">Open</a>
        ${deployRowActions(e)}
        <button class="small ghost" data-history="${esc(e.id)}">${historyOf === e.id ? "Hide versions" : "Versions"}</button>
        ${moreMenu(e)}
        <span class="lib-when" title="${esc(e.message || "")}">${esc(e.message || "")}${e.message ? " · " : ""}${esc(ago(e.updatedAt))}</span>
      </div>
      ${historyOf === e.id ? `<div class="lib-history">${historyPanel(e.id)}</div>` : ""}
    </article>`;
  }).join("");

  host.innerHTML = `
    <div class="view-head">
      <h1>Library</h1>
      <p class="hint">Pipelines kept in the coordinator's own git repository. Each save is a commit; each one is a plain dtpipe job that also runs whole on one machine. Write one with <code>dtpipe studio --coordinator</code> and publish it here.</p>
    </div>
    <section class="lib-list">${items || `<p class="hint" style="padding:12px 14px">The library is empty.</p>`}</section>`;
}

function historyPanel(id) {
  const list = history.map((c) => `
    <div class="commit ${shown?.commit === c.hash ? "active" : ""}">
      <code>${esc(c.hash.slice(0, 8))}</code> <span>${esc(c.message)}</span> <span class="hint">${esc(new Date(c.date).toLocaleString())}</span>
      <span class="actions"><button class="small ghost" data-show="${esc(c.hash)}">show</button>
      <button class="small ghost" data-needs="designer" data-restore="${esc(c.hash)}" data-id="${esc(id)}">restore</button></span>
    </div>`).join("");
  return `<div class="history-panel"><div>${list || `<p class="hint">No commit.</p>`}</div>
    ${shown ? `<div class="grid2"><div><h3>Change</h3><pre class="diff">${diffHtml(shown.diff)}</pre></div><div><h3>Job at ${esc(shown.commit.slice(0, 8))}</h3><pre>${esc(shown.yaml)}</pre></div></div>` : ""}</div>`;
}

function diffHtml(diff) {
  return esc(diff || "(no change to this pipeline)").split("\n").map((l) =>
    l.startsWith("+") && !l.startsWith("+++") ? `<span class="add">${l}</span>` : l.startsWith("-") && !l.startsWith("---") ? `<span class="del">${l}</span>` : l).join("\n");
}

// One menu open at a time; a click elsewhere, a choice in it or Escape closes it.
function closeMenus(except) {
  for (const m of host.querySelectorAll("details.menu[open]")) if (m !== except) m.open = false;
}

async function onClick(e) {
  closeMenus(e.target.closest("details.menu"));
  const t = e.target.closest("button");
  if (!t) return;
  t.closest("details.menu")?.removeAttribute("open");
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
      toast(`${run.runId} ${runStateLabel(run.state)}.`, "info");
    } else if (t.dataset.undeploy) {
      await api(`/api/deployments/${encodeURIComponent(t.dataset.undeploy)}`, undefined, "DELETE");
    } else if (t.dataset.delete) {
      const id = t.dataset.delete;
      if (!confirm(`Delete ${id} from the library? Its versions stay in the repository.`)) return;
      await api(`/api/library/${encodeURIComponent(id)}`, undefined, "DELETE");
      if (historyOf === id) historyOf = null;
      await load();
    }
  } catch (err) {
    toast(err.message);
  }
}
