// @ts-check
// Publishing the saved job to the coordinator's library: the id and message, and the outcome (published, refused by the
// coordinator's check, or in conflict with the library), and what a publication would refuse: the runner's dry run, which
// the server replays on demand.

import { api, el, status } from "./dom.js";
import { brickBranches, showTab } from "./designer.js";
import { invalidate } from "./render.js";
import { sequence, state } from "./state.js";

const publication = state.publication;
const checking = sequence();

const short = (commit) => String(commit || "").slice(0, 8) || "nothing";
const defaultId = (path) => path.replace(/^.*\//, "").replace(/\.ya?ml$/i, "").toLowerCase().replace(/[^a-z0-9-]+/g, "-").replace(/^-+|-+$/g, "") || "pipeline";
const signedIn = () => state.coordinator?.state === "signed-in";

/** What a publication showed belongs to the job it was for: another job starts clean. */
export function forgetPublication() {
  publication.outcome = null;
  checking.cancel();
  publication.check = null;
}

/**
 * Asks the server for the runner's dry run of the job: the one saved in the folder when `saved` (what a publication sends),
 * else the cards' composition. It starts a process, so only a person's gesture asks for it, never an edit. The verdict, or
 * null when there is none to have (no job, signed out) or the answer came for another job or a later check.
 * @param {boolean} saved
 * @returns {Promise<import("./state.js").PublicationCheck | null>}
 */
export async function checkPublication(saved) {
  const job = state.job;
  const yaml = state.cards.result?.yaml;
  if (!job || yaml === undefined || !signedIn()) return null;
  const ticket = checking.begin();
  publication.check = { yaml, pending: true };
  invalidate("drawer");
  /** @type {import("./state.js").PublicationCheck} */
  let check;
  try {
    const verdict = await api("/api/publish/check", saved ? { path: job.path, hash: job.hash } : { yaml });
    check = { yaml, pending: false, ok: verdict.ok, report: verdict.report ?? null };
  } catch (e) {
    check = { yaml, pending: false, error: e.message };
  }
  if (checking.stale(ticket) || state.job !== job) return null;
  publication.check = check;
  invalidate("drawer");
  return check;
}

// The verdict for the cards as they are now; one for an earlier composition says nothing of them.
const currentCheck = () => (publication.check && publication.check.yaml === state.cards.result?.yaml ? publication.check : null);

/** The runner's refusal as it said it, and what the person can read of it. @param {import("./state.js").PublicationCheck} check */
const refusalView = (check) => [
  el("p", { class: "warn" }, "The runner's dry run refuses this job, as a publication would:"),
  el("pre", {}, check.report || ""),
  brickBranches().length ? null
    : el("p", { class: "hint" }, "None of these cards is a coordinator's brick, and a published job reads and writes bricks."),
];

/**
 * The second verdict of Problems, never mixed with the composition's: what a publication would refuse, as the coordinator's
 * runner says it. Signed out, or with no coordinator, there is none, and the page says so rather than guess.
 */
export function publicationProblems() {
  const title = el("h3", {}, "Would be refused at publication");
  if (!state.coordinator) {
    return [title, el("p", { class: "hint" }, "No verdict: this studio was opened without --coordinator, whose runner judges a publication.")];
  }
  if (!signedIn()) {
    return [title, el("p", { class: "hint" }, "No verdict while signed out: the check reads each brick's rows through the coordinator. Sign in to ask for it.")];
  }
  const check = publication.check;
  const current = currentCheck();
  const ask = el("div", { class: "row" },
    el("button", { class: "small", disabled: !!check?.pending, onclick: () => checkPublication(false) },
      check?.pending ? "Checking…" : current ? "Check again" : "Check publication"),
    el("span", { class: "hint" }, "The runner's dry run over one row of each brick: it starts a process, so it runs when asked and before Publish, never as you edit."));
  if (check?.pending) return [title, ask, el("p", { class: "hint" }, "The runner's dry run is running…")];
  if (!current) return [title, ask, check ? el("p", { class: "hint" }, "The cards changed since the last check.") : null];
  if (current.error) return [title, ask, el("p", { class: "warn" }, `The check did not run: ${current.error}`)];
  if (current.ok) {
    return [title, ask, el("p", { class: "good" }, "Nothing: the runner's dry run takes this job."),
      el("p", { class: "hint" }, "A publication also has each sink brick's node judge its output, which this check does not ask.")];
  }
  return [title, ask, refusalView(current)];
}

/**
 * The coordinator receives the job saved in the folder, never the cards: its layout and its launch values stay on this
 * machine. `replace` names the commit the person chose to replace after a conflict. A brick is a reference that names its
 * node, so a publication carries no binding.
 * @param {string} [replace]
 */
export async function publish(replace) {
  const job = state.job;
  if (!job) return;
  if (state.cards.dirty || !job.hash) {
    status("Save the job first: the coordinator receives the job saved in the folder.", "bad");
    return;
  }
  // What the studio can already tell is said before a question: the coordinator's own check of the job runs at publication.
  if (state.cards.result?.errors?.length) {
    showTab("problems");
    status("The cards do not compose into a job: see Problems before publishing.", "bad");
    return;
  }
  if (replace === undefined) {
    // What the runner would refuse is said before a question; a check that could not run leaves the publication's own.
    if (signedIn()) {
      status("checking what a publication would refuse…");
      const check = await checkPublication(true);
      if (state.job !== job) return;
      if (check?.ok === false) {
        showTab("problems");
        status("A publication would refuse this job: see Problems.", "bad");
        return;
      }
      status(check?.error ? `The publication's check did not run (${check.error}): the coordinator checks the job again.` : "");
    }
    let last = null;
    try { last = (await api(`/api/publish?path=${encodeURIComponent(job.path)}`)).publication; } catch { /* none */ }
    const id = prompt("The pipeline's id in the coordinator's library (lowercase letters, digits and '-'):", last?.id || defaultId(job.path));
    if (!id) return;
    let idError = null;
    try { idError = (await api(`/api/publish?path=${encodeURIComponent(job.path)}&id=${encodeURIComponent(id)}`)).idError; } catch { /* publication says */ }
    if (idError) { status(idError, "bad"); return; }
    const message = prompt("Describe this version (the commit message):", `Publish ${job.path} from dtpipe studio`);
    if (message === null) return;
    publication.request = { id, message, previous: last?.id === id ? last.commit ?? null : null };
  }
  return send(replace);
}

/** @param {string} [replace] */
async function send(replace) {
  const job = state.job;
  const request = publication.request;
  try {
    const saved = await api("/api/publish", { path: job.path, hash: job.hash, id: request.id, message: request.message, replace });
    const unchanged = !!request.previous && saved.commit === request.previous;
    publication.outcome = { id: request.id, saved, unchanged };
    status(unchanged ? `already published at ${short(saved.commit)}, nothing changed` : `published at ${short(saved.commit)}`, "good");
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
    // The coordinator says why: the id was taken meanwhile, or the version this studio published last is no longer in place.
    const question = commit
      ? `${e.data?.error || `The library holds ${request.id} at ${short(commit)}.`} Replace it with this job?`
      : `${request.id} is no longer in the library. Publish this job as a new pipeline?`;
    if (confirm(question)) return publish(commit ?? "");
    status("not published", "warn");
    return;
  }
  showTab("publish");
}

/** The Publish tab. */
export function publicationView() {
  if (!state.coordinator) return el("p", { class: "hint" }, "Open the studio with --coordinator to publish a job into a coordinator's library.");
  // A refusal the runner's dry run gave the cards as they are is said here too, before anything else.
  const check = currentCheck();
  return [check && check.ok === false ? refusalView(check) : null, outcomeView(publication.outcome)];
}

/** What the last publication did, or what Publish does when there is none. @param {any} outcome */
function outcomeView(outcome) {
  if (!outcome) {
    return el("p", { class: "hint" }, "Publish sends the job as saved in this folder to the coordinator's library. Its layout and its launch values stay on this machine: "
      + "the coordinator has no launch value to give a ${{NAME}} variable, so its check refuses a job that reads one.");
  }
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
    el("p", { class: "good" }, `${saved.id} is ${outcome.unchanged ? "already published, nothing changed, at" : "published at"} the commit `, el("code", {}, short(saved.commit)), "."),
    saved.deployed ? el("p", { class: "warn" }, "The version in production stays the previous one until you press Deploy in the coordinator's console.") : null,
  ];
}
