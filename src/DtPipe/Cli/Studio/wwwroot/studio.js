// @ts-check
// dtpipe studio: the jobs of a folder, opened as steps, sampled through the real engine and saved. This module
// wires the screens together and owns the workspace files, the coordinator library, and the open job.

import { $, api, el, fill, forgetToken, plural, status, svg, token } from "./dom.js";
import { onCoordinator, refreshCoordinator, signIn } from "./coordinator.js";
import { bindSplit, closeDesigner, compose, editingText, holdsEdits, markSaved, mountDesigner, openBlank, openJob, restoreSizes, setCatalog, showTab } from "./designer.js";
import { nextFreePath, openDeleteDialog, openFromCliDialog, openImportYamlDialog, openInspectLibraryDialog, openNewJobDialog, openPullDialog, openRenameDialog, openSaveAsDialog, openWorkspaceDialog, shortCommit } from "./dialogs.js";
import { holdView, readView, rememberView, sizesRead, viewRestored } from "./prefs.js";
import { afterFlush, invalidate, onZone } from "./render.js";
import { sequence, state } from "./state.js";
import { adoptDraft, cancelPreview, cardBadge, cardTool, cardTools, debugComposed, forgetPins, forgetPreview, heldBranch, heldTargets, previewView, restoreRows, runPreview, setDebugRows } from "./preview.js";
import { forgetRun, prepareRun, restoreReview, runView } from "./run.js";
import { computeSyncState, forgetPublication, publicationProblems, publicationView, publish } from "./publish.js";
import { loadVariables, onLaunchChanged, unreadyCount, variablesView } from "./variables.js";
import { fileReplaced, mountAgent } from "./agent.js";

const MAX_DEBUG_ROWS = 100000;
const opening = sequence();
const openFolders = new Set();
let lastShownDirty = null;
// Set once the page leaves for another folder: the reload asks nothing, the person already chose what becomes of the changes.
let leaving = false;

// ---------------------------------------------------------------- workspace & library

async function loadJobs() {
  const { folder, jobs, folders, publications } = await api("/api/jobs");
  state.jobs.folder = folder;
  state.jobs.paths = jobs;
  state.jobs.folders = folders?.length ? folders : [""];
  state.jobs.publications = publications || {};
  invalidate("jobs");
}

async function loadLibrary() {
  const library = state.jobs.library;
  if (!state.coordinator || state.coordinator.state !== "signed-in") {
    library.shown = false;
    library.pipelines = null;
    library.error = null;
    invalidate("jobs");
    return;
  }
  library.shown = true;
  try {
    library.pipelines = (await api("/api/library")).pipelines;
    library.error = null;
  } catch (e) {
    library.pipelines = null;
    library.error = e.message;
  }
  invalidate("jobs");
  await linkRead();
}

async function linkRead() {
  if (state.coordinator) await refreshCoordinator().catch(() => {});
}

function syncMeta(pub, syncState) {
  switch (syncState) {
    case "synced":
      return { cls: "good", short: `↗ ${pub.id}`, badge: `↗ ${pub.id} · in sync`, title: `In sync with library pipeline '${pub.id}' (${shortCommit(pub.commit)})` };
    case "modified":
      return { cls: "accent", short: `↑ ${pub.id}`, badge: `↑ ${pub.id} · modified`, title: `Local edits ahead of library pipeline '${pub.id}' (${shortCommit(pub.commit)}): publish to update` };
    case "outdated":
      return { cls: "warn", short: `↓ ${pub.id}`, badge: `↓ ${pub.id} · update`, title: `Update available on coordinator for '${pub.id}' (local ${shortCommit(pub.commit)})` };
    case "diverged":
      return { cls: "bad", short: `⇅ ${pub.id}`, badge: `⇅ ${pub.id} · diverged`, title: `Diverged: both local file and library pipeline '${pub.id}' changed` };
    case "removed":
      return { cls: "warn", short: `! ${pub.id}`, badge: `! ${pub.id} · removed`, title: `'${pub.id}' is no longer in the coordinator library` };
    default:
      return { cls: "muted", short: `↗ ${pub.id}`, badge: `↗ ${pub.id} · offline`, title: `Linked to library pipeline '${pub.id}' (sign in to check for updates)` };
  }
}

function linkedPathsForPipeline(id) {
  return Object.entries(state.jobs.publications)
    .filter(([, pub]) => pub.id === id)
    .map(([path]) => path);
}

function primaryLinkedPath(id) {
  const matches = linkedPathsForPipeline(id);
  if (!matches.length) return null;
  if (state.job?.path && matches.includes(state.job.path)) return state.job.path;
  if (matches.includes(`${id}.yaml`)) return `${id}.yaml`;
  return matches[0];
}

function renderJobSync() {
  const host = $("job-sync");
  if (!host) return;
  const pub = state.job?.hash ? state.jobs.publications[state.job.path] : null;
  if (!pub || !state.coordinator) {
    host.hidden = true;
    fill(host);
    return;
  }
  const syncState = computeSyncState(pub, state.jobs.library.pipelines);
  const meta = syncMeta(pub, syncState);
  host.hidden = false;
  fill(host, el("button", {
    type: "button",
    class: `job-sync-btn sync-${meta.cls}`,
    title: meta.title,
    "aria-label": meta.title,
    onclick: () => {
      if (state.coordinator?.state !== "signed-in") signIn();
      else if (syncState === "outdated" || syncState === "diverged") pullPipelineById(pub.id, state.job.path);
      else if (syncState === "modified") publishAndRead();
      else { state.view.side = "library"; invalidate("jobs"); }
    },
  }, meta.badge));
}

function jobRow(path, isDraft = false) {
  const isOpen = path === state.job?.path;
  const label = path.includes("/") ? path.slice(path.lastIndexOf("/") + 1) : path;
  const dirty = isOpen && holdsEdits();
  const pub = !isDraft ? state.jobs.publications[path] : null;
  const syncState = computeSyncState(pub, state.jobs.library.pipelines);
  const meta = pub ? syncMeta(pub, syncState) : null;
  const signedIn = state.coordinator?.state === "signed-in";

  const openBtn = el("button", {
    type: "button",
    class: isOpen ? "job open" : "job",
    title: path,
    onclick: () => { if (!isDraft) open(path); },
  }, label);

  const menu = isDraft ? null : el("details", { class: "menu row-menu" },
    el("summary", { class: "small ghost", "aria-label": `Actions for ${path}`, title: `Actions for ${path}` }, "⋯"),
    el("div", { class: "menu-pop" },
      signedIn && (syncState === "outdated" || syncState === "diverged")
        ? el("button", { type: "button", class: "small", onclick: () => pullPipelineById(pub.id, path) }, "Pull update…") : null,
      isOpen
        ? el("button", { type: "button", class: "small", onclick: () => saveAs() }, "Save as…") : null,
      el("button", { type: "button", class: "small", onclick: () => renameJob(path) }, "Rename…"),
      el("button", { type: "button", class: "small danger", onclick: () => deleteJob(path) }, "Delete…")));

  return el("li", { class: `job-row${isOpen ? " is-open" : ""}` },
    openBtn,
    isDraft ? el("span", { class: "chip muted" }, "new") : null,
    !isDraft && dirty ? el("span", { class: "dirty-dot", title: "Unsaved changes", "aria-label": "Unsaved changes" }, "●") : null,
    menu,
    meta ? el("span", { class: `sync-tag sync-${meta.cls}`, title: meta.title }, meta.badge) : null);
}

function renderWorkspace() {
  const { paths } = state.jobs;
  const entries = paths.map((p) => ({ path: p, draft: false }));
  if (state.job && !state.job.hash && !paths.includes(state.job.path)) {
    entries.unshift({ path: state.job.path, draft: true });
  }
  const q = state.jobs.filter.trim().toLowerCase();
  const visible = q ? entries.filter((e) => e.path.toLowerCase().includes(q)) : entries;
  if (!visible.length) {
    fill($("jobs"), el("li", { class: "muted" }, entries.length ? "No matching job file." : "No .yaml or .yml file in this folder."));
    return;
  }
  if (!visible.some((e) => e.path.includes("/"))) {
    fill($("jobs"), visible.map((e) => jobRow(e.path, e.draft)));
    return;
  }

  const rootItems = visible.filter((e) => !e.path.includes("/"));
  /** @type {Map<string, typeof visible>} */
  const groups = new Map();
  for (const e of visible) {
    if (!e.path.includes("/")) continue;
    const dir = e.path.slice(0, e.path.lastIndexOf("/"));
    if (!groups.has(dir)) groups.set(dir, []);
    groups.get(dir).push(e);
  }

  const groupNodes = [...groups.entries()].map(([dir, items]) => {
    if (!openFolders.has(`seen:${dir}`)) {
      openFolders.add(`seen:${dir}`);
      openFolders.add(dir);
    }
    const details = el("details", { class: "folder-group", open: openFolders.has(dir) || !!q },
      el("summary", { class: "folder-name" }, el("span", { class: "mono" }, `${dir}/`)),
      el("ul", {}, items.map((e) => jobRow(e.path, e.draft))));
    details.addEventListener("toggle", () => {
      if (details.open) openFolders.add(dir);
      else openFolders.delete(dir);
    });
    return el("li", { class: "folder-item" }, details);
  });

  fill($("jobs"), [...rootItems.map((e) => jobRow(e.path, e.draft)), ...groupNodes]);
}

function renderLibrary() {
  const gate = $("library-gate");
  const filterInput = $("library-filter");
  const list = $("library");
  const link = state.coordinator;
  if (!link) {
    gate.hidden = true;
    filterInput.hidden = true;
    fill(list);
    return;
  }

  if (link.state !== "signed-in") {
    filterInput.hidden = true;
    fill(list);
    gate.hidden = false;
    const host = new URL(link.url).host;
    if (link.unreachable) {
      fill(gate,
        el("p", { class: "warn" }, `Coordinator (${host}) unreachable: ${link.unreachable}`),
        el("button", { type: "button", class: "small", onclick: () => refreshCoordinator() }, "Retry"));
    } else if (link.state === "signing-in") {
      fill(gate,
        el("p", { class: "hint" }, `Finish signing in to ${host} in the browser tab.`),
        el("button", { type: "button", class: "small", onclick: () => signIn() }, "Open sign-in tab"));
    } else {
      fill(gate,
        el("p", { class: "hint" }, link.reason || `Sign in to ${host} to browse and pull pipelines from the coordinator library.`),
        el("button", { type: "button", class: "small primary", onclick: () => signIn() }, link.byKey ? "Sign in with the key" : "Sign in"));
    }
    return;
  }

  gate.hidden = true;
  filterInput.hidden = false;
  const library = state.jobs.library;
  if (library.error) {
    fill(list, el("li", { class: "warn" }, library.error));
    return;
  }
  if (!library.pipelines) {
    fill(list, el("li", { class: "muted" }, "Loading library…"));
    return;
  }
  const q = library.filter.trim().toLowerCase();
  const items = q
    ? library.pipelines.filter((p) => [p.id, p.title, p.description].some((v) => v && String(v).toLowerCase().includes(q)))
    : library.pipelines;
  if (!items.length) {
    fill(list, el("li", { class: "muted" }, library.pipelines.length ? "No matching pipeline." : "The library holds no pipeline."));
    return;
  }

  fill(list, items.map((p) => {
    const linkedPath = primaryLinkedPath(p.id);
    const linkedPub = linkedPath ? state.jobs.publications[linkedPath] : null;
    const syncState = linkedPub ? computeSyncState(linkedPub, library.pipelines) : null;
    const outdated = syncState === "outdated" || syncState === "diverged";

    const primaryLabel = !linkedPath ? "Pull…"
      : outdated ? "Update…"
      : "Open local";
    const onPrimary = () => {
      if (linkedPath && !outdated) open(linkedPath);
      else pullPipeline(p, linkedPath, syncState);
    };

    const syncStatusText = syncState === "diverged" ? "diverged"
      : syncState === "outdated" ? "update available"
      : syncState === "modified" ? "local edits"
      : syncState === "synced" ? "in sync"
      : "";
    const syncStatusCls = syncState === "diverged" ? "bad"
      : syncState === "outdated" ? "warn"
      : syncState === "modified" ? "accent"
      : "good";

    const extraMenu = linkedPath ? el("details", { class: "menu row-menu" },
      el("summary", { class: "small ghost", "aria-label": `More actions for ${p.id}`, title: `More actions for ${p.id}` }, "⋯"),
      el("div", { class: "menu-pop" },
        outdated ? el("button", { type: "button", class: "small", onclick: () => open(linkedPath) }, "Open local file") : null,
        el("button", { type: "button", class: "small", onclick: () => pullPipeline(p, linkedPath, syncState) }, "Pull as new copy…"))) : null;

    return el("li", { class: "lib-entry" },
      el("div", { class: "lib-head" },
        el("span", { class: "lib-title", title: p.title || p.id }, p.title || p.id),
        p.commit ? el("code", { class: "lib-commit" }, shortCommit(p.commit)) : null),
      el("div", { class: "lib-id mono muted" }, p.id),
      p.description ? el("p", { class: "lib-desc hint" }, p.description) : null,
      linkedPath ? el("div", { class: "lib-meta" },
        el("span", { class: "hint" }, "Local: ", el("code", {}, linkedPath)),
        syncStatusText ? el("span", { class: `sync-tag sync-${syncStatusCls}` }, syncStatusText) : null) : null,
      el("div", { class: "row lib-actions" },
        el("button", { type: "button", class: "small primary", onclick: onPrimary }, primaryLabel),
        el("button", {
          type: "button",
          class: "small ghost",
          onclick: () => openInspectLibraryDialog({
            pipeline: p,
            linkedPath,
            syncState,
            onPull: () => pullPipeline(p, linkedPath, syncState),
            onOpenLocal: (localPath) => open(localPath),
          }),
        }, "Preview"),
        extraMenu));
  }));
}

function renderJobs() {
  const { folder } = state.jobs;
  $("folder").textContent = folder;
  $("folder").title = `${folder}: open another folder`;
  $("folder").setAttribute("aria-label", `Folder ${folder}: open another folder`);
  $("job-name").textContent = state.job ? `${state.job.path}${state.job.hash ? "" : " (new)"}` : "";
  $("job-name").title = state.job ? state.job.path : "";
  lastShownDirty = holdsEdits();
  renderJobSync();
  const fromLib = $("new-from-library");
  if (fromLib) fromLib.hidden = !state.coordinator;

  const updates = Object.values(state.jobs.publications).filter((p) => {
    const s = computeSyncState(p, state.jobs.library.pipelines);
    return s === "outdated" || s === "diverged";
  }).length;
  const libBtn = $("side-library");
  if (libBtn) {
    libBtn.hidden = !state.coordinator;
    libBtn.classList.toggle("has-updates", updates > 0);
    const label = updates ? `Library (${plural(updates, "update")} available)` : "Library";
    libBtn.setAttribute("aria-label", label);
    libBtn.title = updates
      ? `Library: ${plural(updates, "pipeline")} updated on the coordinator`
      : "Library: pipelines on the coordinator";
  }

  renderWorkspace();
  renderLibrary();
  renderSide();
}

// ---------------------------------------------------------------- the side panel

const SIDES = /** @type {const} */ (["jobs", "library", "steps"]);
const SIDE_ICONS = {
  jobs: ["M3 7a2 2 0 0 1 2-2h4l2 2h8a2 2 0 0 1 2 2v8a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2z"],
  library: ["M4 7c0-1.7 3.6-3 8-3s8 1.3 8 3-3.6 3-8 3-8-1.3-8-3z", "M4 7v5c0 1.7 3.6 3 8 3s8-1.3 8-3V7", "M4 12v5c0 1.7 3.6 3 8 3s8-1.3 8-3v-5"],
  steps: ["M3 3h7v7H3z", "M14 3h7v7h-7z", "M14 14h7v7h-7z", "M3 14h7v7H3z"],
};
let sideDrawn = null;

const sideShown = () => {
  if (state.view.side === "library" && !state.coordinator) return state.job ? "steps" : "jobs";
  return state.view.side ?? (state.job ? "steps" : "jobs");
};

function renderSide() {
  const side = sideShown();
  document.querySelector("main")?.classList.toggle("side-closed", side === "none");
  $("side-pane").hidden = side === "none";
  for (const name of SIDES) {
    const view = $(`side-${name}-view`);
    const btn = $(`side-${name}`);
    if (view) view.hidden = side !== name;
    if (btn) btn.setAttribute("aria-pressed", String(side === name));
  }
  if (side === "steps" && sideDrawn !== "steps") invalidate("palette");
  sideDrawn = side;
}

/** @param {"jobs" | "library" | "steps"} name */
function chooseSide(name) {
  state.view.side = sideShown() === name ? "none" : name;
  invalidate("jobs");
}

function mountSide() {
  for (const name of SIDES) {
    const icon = svg("svg", { viewBox: "0 0 24 24", width: "20", height: "20", fill: "none", stroke: "currentColor", "stroke-width": "1.8", "stroke-linejoin": "round", "aria-hidden": "true" });
    for (const d of SIDE_ICONS[name]) icon.append(svg("path", { d }));
    $(`side-${name}`).append(icon);
    $(`side-${name}`).addEventListener("click", () => chooseSide(name));
  }
  const pane = $("side-pane");
  bindSplit(/** @type {HTMLElement} */ (document.querySelector(".split-pal")), "pal", (e) => e.clientX - pane.getBoundingClientRect().left);
  renderSide();
}

// ---------------------------------------------------------------- library pull & workspace operations

function pullPipeline(pipeline, linkedPath, syncState) {
  openPullDialog({
    pipeline,
    folders: state.jobs.folders,
    existingPaths: state.jobs.paths,
    linkedPath,
    syncState,
    openJobPath: state.job?.path ?? null,
    openJobHasEdits: holdsEdits(),
    onPulled: async (pulledPath) => {
      await loadJobs();
      await open(pulledPath, pulledPath === state.job?.path);
      status(`pulled ${pulledPath} from library`, "good");
    },
    onOpenLocal: async (localPath) => {
      await open(localPath);
    },
  });
}

function pullPipelineById(id, linkedPathOverride = null) {
  const pipeline = state.jobs.library.pipelines?.find((p) => p.id === id) || { id, title: id, description: null, commit: null };
  const linkedPath = linkedPathOverride || primaryLinkedPath(id);
  const linkedPub = linkedPath ? state.jobs.publications[linkedPath] : null;
  const syncState = linkedPub ? computeSyncState(linkedPub, state.jobs.library.pipelines) : null;
  pullPipeline(pipeline, linkedPath, syncState);
}

function forgetResults() {
  restoreReview(false);
  forgetPreview();
  forgetPublication();
  forgetRun();
}

const leaveCurrent = () => !holdsEdits() || confirm(`${state.job?.path || "This job"} has changes that are not saved. Leave it anyway?`);

async function open(path, left = false, replacing = null) {
  if (!left && !leaveCurrent()) return false;
  const replaces = state.job?.path === path ? replacing : null;
  const ticket = opening.begin();
  try {
    const [job, view] = await Promise.all([api(`/api/job?path=${encodeURIComponent(path)}`), readView(path)]);
    if (opening.stale(ticket)) return false;
    state.job = { path, hash: job.hash, rewrites: job.rewrites };
    forgetResults();
    invalidate("jobs");
    if (view.job?.rows) restoreRows(view.job.rows);
    restoreReview(view.job?.review === true);
    if (!(await openJob(job.yaml, job.layout, false, view.job, replaces))) return false;
    viewRestored(path, view.job);
    status(job.rewrites ? "saving rewrites this file" : "", job.rewrites ? "warn" : "");
    await loadJobs();
    return true;
  } catch (e) {
    status(e.message, "bad");
    return false;
  }
}

async function openDraftYaml(path, yaml, message) {
  opening.cancel();
  state.job = { path, hash: null, rewrites: false };
  forgetResults();
  viewRestored(path);
  const opened = await openJob(yaml, null, true);
  if (message) status(message, "good");
  await loadJobs();
  return opened;
}

function startNewBlank() {
  if (!leaveCurrent()) return;
  openNewJobDialog({
    folders: state.jobs.folders,
    existingPaths: state.jobs.paths,
    onCreate: async ({ path, title }) => {
      opening.cancel();
      state.job = { path, hash: null, rewrites: false };
      forgetResults();
      viewRestored(path);
      await openBlank(title || path.replace(/^.*\//, "").replace(/\.ya?ml$/i, ""));
      await loadJobs();
    },
  });
}

function startFromCli() {
  if (!leaveCurrent()) return;
  openFromCliDialog({
    folders: state.jobs.folders,
    existingPaths: state.jobs.paths,
    onImport: async ({ path, yaml }) => {
      await openDraftYaml(path, yaml, "imported from CLI: save it to keep it");
    },
  });
}

function startFromYaml() {
  if (!leaveCurrent()) return;
  openImportYamlDialog({
    folders: state.jobs.folders,
    existingPaths: state.jobs.paths,
    onImport: async ({ path, yaml }) => {
      await openDraftYaml(path, yaml, "imported YAML: save it to keep it");
    },
  });
}

async function hashOf(path) {
  if (state.job?.path === path && state.job.hash) return state.job.hash;
  const doc = await api(`/api/job?path=${encodeURIComponent(path)}`);
  return doc.hash;
}

async function renameJob(path) {
  let hash;
  try { hash = await hashOf(path); }
  catch (e) { status(e.message, "bad"); return; }
  openRenameDialog({
    folders: state.jobs.folders,
    existingPaths: state.jobs.paths,
    path,
    hasEdits: state.job?.path === path && holdsEdits(),
    onRename: async (to) => {
      const res = await api("/api/job/rename", { from: path, to, hash });
      if (state.job?.path === path) {
        state.job = { path: res.path, hash: res.hash, rewrites: state.job.rewrites };
        viewRestored(res.path, { tab: state.view.drawer });
      }
      await loadJobs();
      status(`renamed to ${res.path}`, "good");
    },
  });
}

async function deleteJob(path) {
  let hash;
  try { hash = await hashOf(path); }
  catch (e) { status(e.message, "bad"); return; }
  const isOpen = state.job?.path === path;
  const pub = state.jobs.publications[path];
  openDeleteDialog({
    path,
    isOpen,
    hasEdits: isOpen && holdsEdits(),
    publicationId: pub?.id || null,
    onDelete: async () => {
      const query = `path=${encodeURIComponent(path)}${hash ? `&hash=${encodeURIComponent(hash)}` : ""}`;
      await api(`/api/job?${query}`, undefined, "DELETE");
      if (state.job?.path === path) {
        opening.cancel();
        closeDesigner();
        state.job = null;
        forgetResults();
        fill($("designer"), el("p", { class: "muted intro" },
          "Open a job from Workspace, on the left. It opens as steps, from its sources to its outputs. Preview runs it through the real engine over the first source rows, with the outputs neutralised, and every step shows the rows it delivers: nothing is written. Save writes the job the studio composes, and keeps where its steps are under .dtpipe/, outside git."));
        fill($("side-steps-view"), el("p", { class: "hint" }, "Open a job: its sources, steps and outputs are placed from here."));
        loadVariables({ yaml: "", steps: [], errors: [], warnings: [] });
      }
      await loadJobs();
      status(`deleted ${path}`, "good");
    },
  });
}

async function save() {
  const job = state.job;
  if (!job) return;
  if (editingText()) { showTab("yaml"); status("The YAML is not applied: apply or revert it, then save.", "bad"); return; }
  const result = await compose();
  if (result.unanswered) return;
  if (job.rewrites && !confirm("Saving writes this job as the studio composes it: comments inside it and the order of its keys are not kept. Save anyway?")) return;
  const acceptErrors = result.errors?.length > 0;
  if (acceptErrors && !confirm(`${plural(result.errors.length, "problem")} ${result.errors.length === 1 ? "remains" : "remain"}: the job written may not run. Save anyway?`)) return;
  const write = (hash) => api("/api/job", { path: job.path, hash, model: state.cards.model, acceptErrors }, "PUT");
  try {
    let answer;
    try {
      answer = await write(job.hash);
    } catch (e) {
      if (e.status !== 409 || !job.hash || state.agent.behind?.saved !== job.path) throw e;
      const disk = await api(`/api/job?path=${encodeURIComponent(job.path)}`);
      if (!confirm(`The agent saved ${job.path} after you opened it. Saving replaces the agent's text with your steps: what the agent wrote is lost (its notice's Apply shows it on the steps instead). Replace it?`)) throw e;
      answer = await write(disk.hash);
      fileReplaced(job.path);
    }
    state.job = { path: job.path, hash: answer.hash, rewrites: false };
    if (!job.hash) await adoptDraft();
    invalidate("jobs");
    markSaved();
    status("saved", "good");
    await loadJobs();
  } catch (e) {
    status(e.message, "bad");
  }
}

async function saveAs() {
  const job = state.job;
  if (!job) return;
  if (editingText()) { showTab("yaml"); status("The YAML is not applied: apply or revert it, then save.", "bad"); return; }
  const result = await compose();
  if (result.unanswered) return;
  const acceptErrors = result.errors?.length > 0;
  if (acceptErrors && !confirm(`${plural(result.errors.length, "problem")} ${result.errors.length === 1 ? "remains" : "remain"}: the job written may not run. Save anyway?`)) return;

  const suggested = job.hash
    ? nextFreePath(job.path, state.jobs.paths)
    : (state.jobs.paths.includes(job.path) ? nextFreePath(job.path, state.jobs.paths) : job.path);

  openSaveAsDialog({
    folders: state.jobs.folders,
    existingPaths: state.jobs.paths,
    currentPath: job.path,
    suggestedPath: suggested,
    onSaveAs: async (targetPath) => {
      const answer = await api("/api/job", { path: targetPath, hash: null, model: state.cards.model, acceptErrors }, "PUT");
      const wasDraft = !job.hash;
      state.job = { path: targetPath, hash: answer.hash, rewrites: false };
      if (wasDraft) await adoptDraft();
      viewRestored(targetPath, { tab: state.view.drawer });
      markSaved();
      await loadJobs();
      status(`saved as ${targetPath}`, "good");
    },
  });
}

async function loadBricks() {
  try {
    const answer = await api("/api/bricks");
    setCatalog(answer);
    if (answer.error) status(answer.error, "warn");
  } catch (e) {
    status(e.message, "bad");
  }
  await linkRead();
}

// ---------------------------------------------------------------- the bar

function renderBar() {
  const dbg = state.preview;
  const set = (id, props) => { const node = $(id); if (node) Object.assign(node, props); };
  const suspended = dbg.real;
  set("d-preview", { disabled: dbg.running || suspended, title: suspended ? SUSPENDED : PREVIEW_TITLE });
  set("d-preview-cancel", { hidden: !dbg.running });
  set("d-suspended", { hidden: !suspended });
  set("d-watch-rows", { disabled: dbg.running || dbg.real, ...(document.activeElement === $("d-watch-rows") ? {} : { value: String(dbg.rows) }) });
  set("d-forget-pins", { disabled: dbg.running || dbg.real, hidden: dbg.watch.length === 0 });
  set("d-run", { disabled: dbg.running || suspended, title: suspended ? SUSPENDED : RUN_TITLE });
  set("d-publish", { hidden: !state.coordinator });
  if (state.job && lastShownDirty !== holdsEdits()) invalidate("jobs");
}

function writeHeld(alias) {
  const branch = heldBranch(alias);
  return branch ? prepareRun(true, heldTargets(), [branch]) : undefined;
}
const SUSPENDED = "A real run is suspended before its outputs are written: write it or abandon it from the output steps.";
const PREVIEW_TITLE = "Run the whole job over the first source rows, with the writers neutralised: every step shows the rows it delivers";
const RUN_TITLE = "Run the saved job here and write its outputs, after you confirm what it writes";

async function publishAndRead() {
  await publish({
    onPublished: async () => {
      await loadJobs();
      await loadLibrary();
    },
    onPullRemote: (id) => pullPipelineById(id, state.job?.path),
  });
}

function barButtons() {
  return [
    el("button", { id: "d-save", title: "Write the job and its layout (Ctrl+S)", onclick: () => save() }, "Save"),
    el("button", { id: "d-save-as", class: "ghost", title: "Save a copy of this job under another path", onclick: () => saveAs() }, "Save as…"),
    el("button", { id: "d-preview", class: "primary", title: PREVIEW_TITLE, onclick: () => runPreview(null) }, "▶ Preview"),
    el("button", { id: "d-preview-cancel", class: "ghost danger", hidden: true, title: "Stop the preview: the engine stops at its next batch of rows", onclick: cancelPreview }, "Stop"),
    el("label", { id: "d-watch-rows-label", class: "rows", title: "Source rows each source reads, up to the preview's cap: a step says when a source stopped there" },
      "rows ", el("input", { id: "d-watch-rows", type: "number", min: "1", max: String(MAX_DEBUG_ROWS), value: String(state.preview.rows), onchange: setDebugRows })),
    el("button", { id: "d-forget-pins", class: "ghost small", hidden: true, title: "Unpin every step and forget the checkpoints they kept", onclick: forgetPins }, "Forget pins"),
    el("span", { id: "d-suspended", class: "suspended", hidden: true, title: SUSPENDED }, "⏸ run suspended: write or abandon it on the output steps"),
    el("button", { id: "d-run", class: "danger", title: RUN_TITLE, onclick: () => prepareRun() }, "Run for real…"),
    el("button", { id: "d-publish", hidden: !state.coordinator, title: "Send the saved job to the coordinator's library", onclick: () => publishAndRead() }, "Publish"),
  ];
}

// ---------------------------------------------------------------- the folder

/** Opens another folder; the page then reads everything again from it, as a reload does. */
async function openWorkspace() {
  let workspace;
  try {
    workspace = await api("/api/workspace");
  } catch (e) {
    status(e.message, "bad");
    return;
  }
  openWorkspaceDialog({
    workspace,
    onTest: (path) => api("/api/workspace/test", { path }),
    unsaved: holdsEdits,
    onOpen: async (request, discard) => {
      const unsaved = !discard && holdsEdits();
      holdView(true);
      try {
        await api("/api/workspace", { ...request, unsaved }, "PUT");
      } catch (e) {
        holdView(false);
        throw e;
      }
      leaving = true;
      location.reload();
    },
    onSave: async () => { await save(); return !holdsEdits(); },
  });
}

async function quit() {
  if (holdsEdits() && !confirm("Changes are not saved. Quit anyway?")) return;
  try {
    await api("/api/quit", {});
  } catch (e) {
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
    cardTool: (alias, name) => (name === "write" ? writeHeld(alias) : cardTool(alias, name)),
    previewView,
    publicationProblems,
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
  const kept = await readView();
  if (kept.sizes) restoreSizes(kept.sizes);
  if (["jobs", "library", "steps", "none"].includes(kept.side)) state.view.side = /** @type {any} */ (kept.side);
  mountSide();
  sizesRead();
  await loadJobs();
  if (typeof kept.open === "string" && state.jobs.paths.includes(kept.open)) await open(kept.open, true);
  onCoordinator(loadBricks);
  onCoordinator(loadLibrary);
  // Another coordinator has other publications; what a publication showed was said by the one that is gone.
  let connectedTo = null;
  onCoordinator((link) => {
    if ((link?.url ?? null) !== connectedTo) { connectedTo = link?.url ?? null; forgetPublication(); }
    loadJobs().catch(() => {});
  });
  await refreshCoordinator();
  await loadBricks();
  await loadLibrary();
  await mountAgent($("agent-pane"), { applyPlan, openPlan, reopen: (path, ask = false) => open(path, !ask, "the file the agent saved"), jobsChanged: loadJobs, followPlan });
}

// ---------------------------------------------------------------- the agent's plans

async function applyPlan(yaml) {
  if (!state.job || !leaveCurrent()) return false;
  if (!(await openJob(yaml, state.cards.model?.layout, true, null, "the agent's plan"))) return false;
  status("the agent's plan is on the steps: save it to keep it", "good");
  return true;
}

async function followPlan(yaml) {
  const probe = await api("/api/design/decompose", { yaml, layout: null });
  if (probe.errors?.length) return false;
  if (state.job) return openJob(yaml, state.cards.model?.layout, true, { card: state.cards.selected?.alias, tab: state.view.drawer }, "the agent's plan");
  const taken = new Set(state.jobs.paths);
  let path = "agent-job.yaml";
  for (let n = 2; taken.has(path); n++) path = `agent-job-${n}.yaml`;
  opening.cancel();
  state.job = { path, hash: null, rewrites: false };
  forgetResults();
  viewRestored(path);
  return openJob(yaml, null, true);
}

async function openPlan(yaml) {
  if (!leaveCurrent()) return false;
  const suggested = state.jobs.paths.includes("agent-plan.yaml")
    ? nextFreePath("agent-plan.yaml", state.jobs.paths) : "agent-plan.yaml";
  return new Promise((resolve) => {
    let settled = false;
    const finish = (v) => { if (!settled) { settled = true; resolve(v); } };
    openNewJobDialog({
      folders: state.jobs.folders,
      existingPaths: state.jobs.paths,
      initialPath: suggested,
      title: "Open agent plan as new job",
      hideTitle: true,
      onCreate: async ({ path }) => {
        const opened = await openDraftYaml(path, yaml, "the agent's plan: save it to keep it");
        finish(opened);
      },
      onCancel: () => finish(false),
    });
  });
}

// ---------------------------------------------------------------- wire controls

$("quit").addEventListener("click", quit);
$("folder").addEventListener("click", openWorkspace);
$("new-blank").addEventListener("click", startNewBlank);
$("new-from-cli").addEventListener("click", startFromCli);
$("new-from-yaml").addEventListener("click", startFromYaml);
$("new-from-library").addEventListener("click", () => {
  state.view.side = "library";
  invalidate("jobs");
});
$("jobs-refresh").addEventListener("click", async () => {
  try { await loadJobs(); status("workspace refreshed"); } catch (e) { status(e.message, "bad"); }
});
$("library-refresh").addEventListener("click", async () => {
  try { await loadLibrary(); status("library refreshed"); } catch (e) { status(e.message, "bad"); }
});
$("jobs-filter").addEventListener("input", (e) => {
  state.jobs.filter = /** @type {HTMLInputElement} */ (e.target).value;
  invalidate("jobs");
});
$("library-filter").addEventListener("input", (e) => {
  state.jobs.library.filter = /** @type {HTMLInputElement} */ (e.target).value;
  invalidate("jobs");
});

document.addEventListener("click", (e) => {
  const target = /** @type {HTMLElement | null} */ (e.target);
  const menu = target?.closest?.("details.menu");
  for (const openMenu of document.querySelectorAll("details.menu[open]")) {
    if (openMenu !== menu) openMenu.removeAttribute("open");
  }
  if (menu && target?.closest?.(".menu-pop button")) menu.removeAttribute("open");
});

document.addEventListener("keydown", (e) => {
  if (e.key === "Escape") {
    for (const openMenu of document.querySelectorAll("details.menu[open]")) openMenu.removeAttribute("open");
  }
  if (e.key === "s" && (e.ctrlKey || e.metaKey) && state.job) { e.preventDefault(); save(); }
});
window.addEventListener("beforeunload", (e) => { if (holdsEdits() && !leaving) e.preventDefault(); });

if (!token) status("Open the address dtpipe studio printed: it carries this session's token.", "bad");
else start().catch((e) => status(e.message, "bad"));
