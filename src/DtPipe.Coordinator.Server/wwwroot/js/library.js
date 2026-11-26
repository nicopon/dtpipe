// Library: the pipelines kept in the coordinator's git repository, their deployment state and their last run. A
// pipeline is written with dtpipe studio and published into it. The list is searched and filtered here; a pipeline's
// plan, runs and versions are on its own page, `library-detail.js`.
import { $, ROWS_CROSSED_HINT, ago, api, deploymentOf, esc, healthOf, info, isActive, lastRunOf, plural, rowsCrossed, runHref, runStateLabel, toast } from "./core.js";
import { deploymentChip, statusChip } from "./pipeline-status.js";
import * as detail from "./library-detail.js";

// What the show filter keeps. "Needs a look" is what the stripe marks (`severity`): one definition of it.
const SHOWS = {
  all: ["All", () => true],
  look: ["Needs a look", (e) => severity(e) !== ""],
  deployed: ["Deployed", (e) => !!deploymentOf(e.id)],
  undeployed: ["Not deployed", (e) => !deploymentOf(e.id)],
};

let host = null;
let entries = [];
let loaded = false;        // the library has been read once since the view opened
let failure = "";          // what a read that failed said, shown while nothing has been read
let asking = 0;            // the newest read of the library: an older answer is dropped
let filter = { q: "", show: "all" };
let addressTimer = null;   // the pending write of the search into the address
let page = null;           // the detail page, when #/library/{id} is open

/** The pipelines `f` keeps: the search text in the id or the comments, without case, and the show filter. */
export function visible(list, f) {
  const text = f.q.trim().toLowerCase();
  const keep = SHOWS[f.show][1];
  return list.filter((e) => (!text || [e.id, e.header].some((v) => v && String(v).toLowerCase().includes(text))) && keep(e));
}

/** What the count says: the pipelines the list shows of those the library holds. */
export const countLabel = (shown, total) => `${shown} of ${plural(total, "pipeline")}`;

export function mount(el, params, query) {
  host = el;
  page = params[0] ? detail : null;
  if (page) { page.mount(el, params); return; }
  const show = query?.get("show");
  filter = { q: query?.get("q") ?? "", show: Object.hasOwn(SHOWS, show) ? show : "all" };
  entries = [];
  loaded = false;
  failure = "";
  // The bar is drawn once: an event redraws the list, never the field the person is typing in.
  host.innerHTML = `
    <div class="view-head">
      <h1>Library</h1>
      <p class="hint">Pipelines in the coordinator's git repository. ${info("Each save is a commit. Each pipeline is a plain dtpipe job. Write one with dtpipe studio --coordinator and publish it here.")}</p>
    </div>
    <div class="lib-bar">
      <input id="lib-q" type="search" placeholder="Search by id or comments" aria-label="Search pipelines" value="${esc(filter.q)}">
      <select id="lib-show" aria-label="Show">${Object.entries(SHOWS).map(([key, [label]]) => `<option value="${key}" ${key === filter.show ? "selected" : ""}>${label}</option>`).join("")}</select>
      <span id="lib-count" class="hint"></span>
    </div>
    <section id="lib-items" class="lib-list"></section>`;
  $("lib-q").addEventListener("input", (ev) => {
    filter.q = ev.target.value;
    renderList();
    clearTimeout(addressTimer);
    addressTimer = setTimeout(writeAddress, 250);
  });
  $("lib-show").addEventListener("change", (ev) => {
    filter.show = ev.target.value;
    renderList();
    writeAddress();
  });
  host.addEventListener("click", onClick);
  host.addEventListener("keydown", (ev) => { if (ev.key === "Escape") closeMenus(null); });
  renderList();
  load();
}

export function leave() {
  clearTimeout(addressTimer);
  asking++;
  if (page) page.leave();
  page = null;
  host = null;
}

export function update(kind) {
  if (page) { page.update(kind); return; }
  if (!host) return;
  if (kind === "library") load();
  else if (["all", "deployments", "runs", "health"].includes(kind)) renderList();
}

// The address says what the list shows, and the bare address shows everything.
function writeAddress() {
  clearTimeout(addressTimer);
  const query = new URLSearchParams();
  if (filter.q.trim()) query.set("q", filter.q.trim());
  if (filter.show !== "all") query.set("show", filter.show);
  history.replaceState(null, "", `#/library${query.size ? `?${query}` : ""}`);
}

async function load() {
  const mine = ++asking;
  try {
    const answer = await api("/api/library");
    if (mine !== asking || !host) return;
    entries = answer;
    loaded = true;
    failure = "";
  } catch (e) {
    if (mine !== asking || !host) return;
    failure = e.message;
    toast(e.message);
  }
  renderList();
}

// The last run of a pipeline, named as such and linked to its page.
function runCell(id) {
  const label = `<span class="lib-label">Last run</span>`;
  const run = lastRunOf(id);
  if (!run) return `${label}<span class="hint">none</span>`;
  const rows = rowsCrossed(run);
  const state = esc(runStateLabel(run.state));
  const chip = run.reason
    ? `<span class="tip"><span class="outcome-chip ${esc(run.state)} tip-t" tabindex="0">${state}</span><span class="tip-b"><b>Why</b>${esc(run.reason)}</span></span>`
    : `<span class="outcome-chip ${esc(run.state)}">${state}</span>`;
  const link = `<a class="mono" href="${esc(runHref(run.runId))}" title="This run's page">${esc(run.runId)}</a>`;
  const when = [isActive(run) ? "" : ago(run.finishedAt), rows ? `${rows.toLocaleString("en-US")} rows moved` : ""].filter(Boolean).join(" · ");
  return `${label}${link}${chip}<span title="${esc(ROWS_CROSSED_HINT)}">${esc(when)}</span>`;
}

// Why a launch is refused before it is asked for, or "" when none is: nothing armed, an arming that is not over or failed, or a
// health read that says the armed version cannot be launched. The server decides in every case.
function runBlock(e, d) {
  if (!d) return "Not deployed";
  if (d.state !== "ready") return d.state === "failed" ? "The deployment failed" : "The deployment is not over";
  const h = healthOf(e.id);
  const causes = h?.status === "critical" ? h.causes.filter((c) => c.severity === "critical") : [];
  return causes.length ? `Cannot be launched: ${causes.map((c) => c.message).join(" ")}` : "";
}

// Open is the row's one primary action and Run follows it; Undeploy and Delete, rare and destructive, sit in the menu. Deploying
// is the pipeline page's alone, where each brick's mode is chosen. A button that is off says why.
function runRowActions(e) {
  const runWhy = runBlock(e, deploymentOf(e.id));
  return `<button class="small" data-needs="designer" data-run="${esc(e.id)}" ${runWhy ? "disabled" : ""} title="${esc(runWhy)}">Run</button>`;
}

// When the library last changed this pipeline, and with what message. A message the library writes by itself says nothing and is left out.
function savedCell(e) {
  const own = e.message === `Save ${e.id}` || e.message === `Bind ${e.id}`;
  const message = e.message && !own ? e.message : "";
  const when = e.updatedAt ? `Saved ${ago(e.updatedAt)}` : "";
  const text = [when, message ? `“${message}”` : ""].filter(Boolean).join(" · ");
  return `<span class="lib-when" title="${esc(message)}">${esc(text)}</span>`;
}

function moreMenu(e) {
  const deployment = deploymentOf(e.id);
  const active = isActive(lastRunOf(e.id));
  return `<details class="menu"><summary aria-label="More actions for ${esc(e.id)}" title="More actions">⋯</summary>
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

// The header comments: a popover where a pointer hovers, a printed block where none does.
function headerTip(e) {
  if (!e.header) return "";
  return `<span class="tip"><span class="info-dot tip-t" tabindex="0" role="note" aria-label="Comments">i</span><span class="tip-b"><b>Comments</b><span class="lib-header">${esc(e.header)}</span></span></span>`;
}

// One pipeline. The reasons of a failed deployment and of the last run are also printed, for where no pointer hovers.
function item(e) {
  const run = lastRunOf(e.id);
  const d = deploymentOf(e.id);
  const printed = [e.header ? `<div class="lib-printed lib-header">${esc(e.header)}</div>` : "",
    d && d.state === "failed" && d.message ? `<div class="lib-printed bad">${esc(d.message)}</div>` : "",
    run && run.reason ? `<div class="lib-printed bad">${esc(run.reason)}</div>` : ""].join("");
  return `
    <article class="lib-item ${severity(e)}">
      <div class="lib-facts">
        <div class="lib-title"><a class="mono" href="#/library/${encodeURIComponent(e.id)}" title="${esc(e.id)}">${esc(e.id)}</a>${headerTip(e)}</div>
        <div class="chips-wrap">${deploymentChip(e)} ${statusChip(e)}</div>
        <div class="lib-run">${runCell(e.id)}</div>
      </div>
      ${printed}
      <div class="lib-actions">
        <a class="button small primary" href="#/library/${encodeURIComponent(e.id)}" title="Plan, brick modes, deploy, runs, versions">Open</a>
        ${runRowActions(e)}
        ${moreMenu(e)}
        ${savedCell(e)}
      </div>
    </article>`;
}

// Draws the list and its count, never the bar: the field, its caret and its focus stay as the person left them.
function renderList() {
  const el = host && $("lib-items");
  if (!el) return;
  const shown = visible(entries, filter);
  $("lib-count").textContent = loaded ? countLabel(shown.length, entries.length) : "";
  el.innerHTML = shown.length ? shown.map(item).join("")
    : `<p class="${failure && !loaded ? "mismatch" : "hint"}" style="padding:12px 14px">${
      !loaded ? (failure ? esc(failure) : "Reading the library…") : entries.length ? "No pipeline matches." : "The library is empty."}</p>`;
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
    if (t.dataset.run) {
      const run = await api("/api/runs", { pipelineId: t.dataset.run });
      toast(`${run.runId} ${runStateLabel(run.state)}.`, "info");
    } else if (t.dataset.undeploy) {
      await api(`/api/deployments/${encodeURIComponent(t.dataset.undeploy)}`, undefined, "DELETE");
    } else if (t.dataset.delete) {
      const id = t.dataset.delete;
      if (!confirm(`Delete ${id} from the library? Its versions stay in the repository.`)) return;
      await api(`/api/library/${encodeURIComponent(id)}`, undefined, "DELETE");
      await load();
    }
  } catch (err) {
    toast(err.message);
  }
}
