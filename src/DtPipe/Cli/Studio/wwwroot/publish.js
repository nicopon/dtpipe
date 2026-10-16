// @ts-check
// Publishing the saved job to the coordinator's library: the id and message, the node of each brick several nodes
// offer, and the outcome (published, refused by the coordinator's check, or in conflict with the library).

import { api, el, status } from "./dom.js";
import { showTab } from "./designer.js";
import { state } from "./state.js";

const publication = state.publication;

const short = (commit) => String(commit || "").slice(0, 8) || "nothing";
const defaultId = (path) => path.replace(/^.*\//, "").replace(/\.ya?ml$/i, "").toLowerCase().replace(/[^a-z0-9-]+/g, "-").replace(/^-+|-+$/g, "") || "pipeline";

/** What a publication showed belongs to the job it was for: another job starts clean. */
export function forgetPublication() {
  publication.outcome = null;
}

/**
 * The coordinator receives the job saved in the folder, never the cards: its layout and its launch values stay on this
 * machine. `replace` names the commit the person chose to replace after a conflict. A brick that several nodes offer asks
 * the person for its node first; one that a single node offers needs no choice.
 * @param {string} [replace]
 */
export async function publish(replace) {
  const job = state.job;
  if (!job) return;
  if (state.cards.dirty || !job.hash) {
    status("Save the job first: the coordinator receives the job saved in the folder.", "bad");
    return;
  }
  if (replace === undefined) {
    let last = null;
    try { last = (await api(`/api/publish?path=${encodeURIComponent(job.path)}`)).publication; } catch { /* none */ }
    const id = prompt("The pipeline's id in the coordinator's library (lowercase letters, digits and '-'):", last?.id || defaultId(job.path));
    if (!id) return;
    const message = prompt("Describe this version (the commit message):", `Publish ${job.path} from dtpipe studio`);
    if (message === null) return;
    publication.request = { id, message };
    publication.placement = {};
    let branches = [];
    try { branches = (await api(`/api/placement?path=${encodeURIComponent(job.path)}`)).branches; } catch { /* the coordinator names what it can */ }
    if (branches.some((b) => b.nodes.length > 1)) {
      publication.outcome = { id, choosing: branches };
      status("choose the node of each brick, then publish", "warn");
      showTab("publish");
      return;
    }
  }
  return send(replace);
}

/** @param {string} [replace] */
async function send(replace) {
  const job = state.job;
  const request = publication.request;
  try {
    const placement = Object.fromEntries(Object.entries(publication.placement).filter(([, node]) => node));
    const saved = await api("/api/publish", { path: job.path, hash: job.hash, id: request.id, message: request.message, replace, placement });
    publication.outcome = { id: request.id, saved };
    status(`published at ${short(saved.commit)}`, "good");
  } catch (e) {
    if (e.status === 422) {
      publication.outcome = { id: request.id, refused: { error: e.data?.error || e.message, report: e.data?.report ?? null } };
      status("not published: the coordinator's check refused the job", "bad");
      showTab("publish");
      return;
    }
    if (e.status !== 412) { status(e.message, "bad"); return; }
    const commit = e.data?.commit ?? null;
    publication.outcome = { id: request.id, conflict: { error: e.data?.error || e.message, commit } };
    showTab("publish");
    const question = commit
      ? `The library holds ${request.id} at ${short(commit)}, not the version this studio published last. Replace it with this job?`
      : `${request.id} is no longer in the library. Publish this job as a new pipeline?`;
    if (confirm(question)) return publish(commit ?? "");
    status("not published", "warn");
    return;
  }
  showTab("publish");
}

// One selector per brick branch: the node that runs it, among the nodes that offer the brick. Left empty, the branch is
// published unnamed, and the coordinator keeps the node it already holds for it, or lists it as a problem to settle there.
function choosingView({ id, choosing }) {
  const chosen = publication.placement;
  return [
    el("p", {}, `Which node runs each brick of ${id}? The runner is never named.`),
    el("table", {}, choosing.map(({ alias, nodes }) => el("tr", {},
      el("td", {}, el("code", {}, alias)),
      el("td", {}, el("select", {
        "aria-label": `Node of ${alias}`,
        onchange: (e) => { chosen[alias] = e.target.value; },
      },
      el("option", { value: "", selected: !chosen[alias] && nodes.length > 1 }, "leave it to the coordinator"),
      nodes.map((node) => el("option", { value: node, selected: chosen[alias] ? chosen[alias] === node : nodes.length === 1 }, node))))))),
    el("p", {}, el("button", { id: "d-publish-now", onclick: () => send() }, "Publish")),
  ];
}

/** The Publish tab. */
export function publicationView() {
  const outcome = publication.outcome;
  if (!state.coordinator) return el("p", { class: "hint" }, "Open the studio with --coordinator to publish a job into a coordinator's library.");
  if (!outcome) {
    return el("p", { class: "hint" }, "Publish sends the job as saved in this folder to the coordinator's library. Its layout and its launch values stay on this machine.");
  }
  if (outcome.choosing) return choosingView(outcome);
  if (outcome.refused) {
    const { error, report } = outcome.refused;
    return [el("p", { class: "warn" }, `${outcome.id} was not published: ${error}`),
      report ? el("pre", {}, report) : el("p", { class: "hint" }, "The check could not run: it needs a runner and the nodes of the bricks online.")];
  }
  if (outcome.conflict) {
    const { error, commit } = outcome.conflict;
    return [el("p", { class: "warn" }, `${outcome.id} was not published: ${error}`),
      commit ? el("p", {}, "The library holds the commit ", el("code", {}, short(commit)), ".") : null];
  }
  const saved = outcome.saved;
  return [
    el("p", { class: "good" }, `${saved.id} is published at the commit `, el("code", {}, short(saved.commit)), "."),
    saved.redistribute ? el("p", { class: "warn" }, "This pipeline had a plan: it no longer matches. Redistribute it, then redeploy it from the coordinator's console.") : null,
    saved.deployed ? el("p", { class: "warn" }, "The version in production stays the previous one until it is redeployed.") : null,
  ];
}
