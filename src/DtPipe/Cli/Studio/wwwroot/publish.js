// @ts-check
// Publishing the saved job to the coordinator's library: pre-checks (runner dry run and edge protections),
// unified publish modal, 412 conflict diff & resolution, and the Publish tab view.

import { api, el, fill, status, tabOf } from "./dom.js";
import { brickBranches, holdsEdits, showTab } from "./designer.js";
import { openDialog, renderDiff, shortCommit } from "./dialogs.js";
import { openCoordinatorConnect } from "./coordinator.js";
import { invalidate } from "./render.js";
import { sequence, state } from "./state.js";

const publication = state.publication;
const checking = sequence();
const judging = sequence();

const short = shortCommit;
const defaultId = (path) => path.replace(/^.*\//, "").replace(/\.ya?ml$/i, "").toLowerCase().replace(/[^a-z0-9-]+/g, "-").replace(/^-+|-+$/g, "") || "pipeline";
const signedIn = () => state.coordinator?.state === "signed-in";

/**
 * Compares a local job's saved coordinator link (`pub`) against the coordinator's library list.
 * @param {{ id: string, commit: string | null, localModified: boolean | null } | null | undefined} pub
 * @param {any[] | null | undefined} pipelines
 * @returns {"synced" | "modified" | "outdated" | "diverged" | "removed" | "unknown" | null}
 */
export function computeSyncState(pub, pipelines) {
  if (!pub) return null;
  if (!pipelines) return "unknown";
  const remote = pipelines.find((p) => p.id === pub.id);
  if (!remote) return "removed";
  const remoteMoved = remote.commit !== pub.commit;
  const localMoved = pub.localModified === true;
  if (remoteMoved && localMoved) return "diverged";
  if (remoteMoved) return "outdated";
  if (localMoved) return "modified";
  return "synced";
}

/** What a publication showed belongs to the job it was for: another job starts clean. */
export function forgetPublication() {
  publication.outcome = null;
  checking.cancel();
  publication.check = null;
  judging.cancel();
  publication.edges = null;
}

/**
 * Asks the server for the runner's dry run of the job: the one saved in the folder when `saved` (what a publication sends),
 * else the steps' composition. It starts a process, so only a person's gesture asks for it, never an edit.
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

const currentCheck = () => (publication.check && publication.check.yaml === state.cards.result?.yaml ? publication.check : null);

/**
 * Asks the server what the coordinator's nodes would do with the edges of the job, now: the one saved in the folder when `saved`,
 * else the steps' composition.
 * @param {boolean} saved
 * @returns {Promise<import("./state.js").EdgeVerdict | null>}
 */
export async function judgeEdges(saved) {
  const job = state.job;
  const yaml = state.cards.result?.yaml;
  if (!job || yaml === undefined || !signedIn()) return null;
  const ticket = judging.begin();
  publication.edges = { yaml, pending: true };
  invalidate("drawer");
  /** @type {import("./state.js").EdgeVerdict} */
  let verdict;
  try {
    const answer = await api("/api/publish/edges", saved ? { path: job.path, hash: job.hash } : { yaml });
    verdict = { yaml, pending: false, deployable: !!answer.deployable, refusal: answer.refusal ?? null, edges: answer.edgeProtections ?? [] };
  } catch (e) {
    verdict = { yaml, pending: false, error: e.message };
  }
  if (judging.stale(ticket) || state.job !== job) return null;
  publication.edges = verdict;
  invalidate("drawer");
  return verdict;
}

const currentEdges = () => (publication.edges && publication.edges.yaml === state.cards.result?.yaml ? publication.edges : null);
const brokenEdges = (verdict) => (verdict?.edges ?? []).filter((edge) => edge.refusal);
const edgeName = (edge) => `${edge.producerFragment} → ${edge.consumerFragment}`;

const brokenNote = () => {
  const broken = brokenEdges(currentEdges());
  return broken.length ? `${broken.length === 1 ? "an edge of this job is" : `${broken.length} edges of this job are`} broken now: ${broken.map(edgeName).join(", ")}` : "";
};

const askPublication = (saved) => Promise.all([checkPublication(saved), judgeEdges(saved)]);

const refusalView = (check) => [
  el("p", { class: "warn" }, "The runner's dry run refuses this job, as a publication would:"),
  el("pre", {}, check.report || ""),
  brickBranches().length ? null
    : el("p", { class: "hint" }, "None of these steps is a coordinator's brick, and a published job reads and writes bricks."),
];

export function publicationProblems() {
  const refusal = refusalProblems();
  return tabOf([refusal.body, edgeProblems()], refusal.actions);
}

/** The first verdict of Problems: its words, and the button that asks for it, which sits in the drawer's action bar. */
function refusalProblems() {
  const title = el("h3", {}, "Would be refused at publication");
  if (!state.coordinator) {
    return { body: [title, el("p", { class: "hint" }, "No verdict: no coordinator is connected, whose runner judges a publication.")], actions: [] };
  }
  if (!signedIn()) {
    return { body: [title, el("p", { class: "hint" }, "No verdict while signed out: the check reads each brick's rows through the coordinator. Sign in to ask for it.")], actions: [] };
  }
  const check = publication.check;
  const current = currentCheck();
  const ask = el("button", { class: "small", disabled: !!check?.pending, onclick: () => askPublication(false) },
    check?.pending ? "Checking…" : current ? "Check again" : "Check publication");
  const why = el("p", { class: "hint" }, "The runner's dry run over one row of each brick: it starts a process, so it runs when asked and before Publish, never as you edit.");
  const said = (...parts) => ({ body: [title, why, ...parts], actions: [ask] });
  if (check?.pending) return said(el("p", { class: "hint" }, "The runner's dry run is running…"));
  if (!current) return said(check ? el("p", { class: "hint" }, "The steps changed since the last check.") : null);
  if (current.error) return said(el("p", { class: "warn" }, `The check did not run: ${current.error}`));
  if (current.ok) {
    return said(el("p", { class: "good" }, "Nothing: the runner's dry run takes this job."),
      el("p", { class: "hint" }, "A publication also has each sink brick's node judge its output, which this check does not ask."));
  }
  return said(refusalView(current));
}

function edgeProblems() {
  const title = el("h3", {}, "What the coordinator's nodes would do with the edges");
  if (!state.coordinator) return [title, el("p", { class: "hint" }, "No verdict: no coordinator is connected, whose nodes judge the edges.")];
  if (!signedIn()) return [title, el("p", { class: "hint" }, "No verdict while signed out: the coordinator's nodes judge the edges. Sign in to ask.")];
  const verdict = publication.edges;
  const current = currentEdges();
  const asked = el("span", { class: "hint" }, "Asked with Check publication and before Publish, never as you edit: the nodes' state is live.");
  if (verdict?.pending) return [title, el("p", { class: "hint" }, "The coordinator is judging the edges…")];
  if (!current) return [title, asked, verdict ? el("p", { class: "hint" }, "The steps changed since the edges were last judged.") : null];
  if (current.error) return [title, el("p", { class: "hint" }, `No verdict: the coordinator's judgment of the edges was not read (${current.error}).`)];
  const broken = brokenEdges(current);
  if (broken.length) {
    return [title,
      el("p", { class: "warn" }, "The coordinator would refuse to arm a job with these edges as its nodes stand now:"),
      el("ul", { class: "msgs warn" }, broken.map((edge) => el("li", {}, `${edgeName(edge)}: ${edge.refusal}`))),
      el("p", { class: "hint" }, "Publishing goes on: the nodes may change before the job is armed.")];
  }
  const edges = current.edges ?? [];
  if (!current.deployable || (edges.length > 0 && edges.every((edge) => edge.protection == null))) {
    return [title, el("p", { class: "hint" }, `No verdict: ${current.refusal || "the coordinator cannot tell now how its nodes would hold these edges."}`), asked];
  }
  return [title, el("p", { class: "good" }, "No edge of this job is broken."), asked];
}

/**
 * Opens the unified Publish modal for the currently saved job, running pre-checks and handling conflicts inline.
 * @param {{ onPublished?: () => Promise<void> | void, onPullRemote?: (id: string) => void }} [options]
 */
export async function publish(options = {}) {
  const { onPublished, onPullRemote } = options;
  const job = state.job;
  if (!job) return;
  if (holdsEdits() || !job.hash) {
    status("Save the job first: the coordinator receives the job saved in the folder.", "bad");
    return;
  }
  if (state.cards.result?.errors?.length) {
    showTab("problems");
    status("The steps do not compose into a job: see Problems before publishing.", "bad");
    return;
  }
  if (!signedIn()) {
    status("Sign in to the coordinator before publishing.", "bad");
    return;
  }

  let closed = false;
  let last = null;
  let editingId = false;
  let idTouched = false;
  let idValue = defaultId(job.path);
  let messageValue = `Publish ${job.path} from dtpipe studio`;
  let checkingNow = true;
  let submitting = false;
  /** @type {{ error: string, report: string | null } | null} */
  let refusal = null;
  /** @type {{ error: string, commit: string | null, id: string, message: string, loading: boolean, remoteYaml: string | null, localYaml: string | null, diffError: string | null } | null} */
  let conflict = null;

  openDialog("d-publish-dialog", "Publish to coordinator", ({ body, close }) => {
    const checksBox = el("div", { class: "dialog-checks" });
    const formArea = el("div");

    function renderChecks() {
      if (checkingNow) {
        fill(checksBox, el("p", { class: "hint" }, "Running pre-publication checks (runner dry run and edge protections)…"));
        return;
      }
      const check = currentCheck();
      const edges = currentEdges();
      const broken = brokenEdges(edges);
      fill(checksBox,
        check?.ok === false ? [el("div", { class: "dialog-check-box warn" }, refusalView(check))]
          : check?.ok === true ? el("p", { class: "good" }, "Runner dry run passed.")
          : check?.error ? el("p", { class: "warn" }, `Pre-check did not run (${check.error}): the coordinator checks the job again.`) : null,
        broken.length ? el("div", { class: "dialog-check-box warn" },
          el("p", { class: "warn" }, "Broken edges (publishing continues, but arming would refuse):"),
          el("ul", { class: "msgs warn" }, broken.map((edge) => el("li", {}, `${edgeName(edge)}: ${edge.refusal}`)))) : null);
    }

    async function sendPublish(replace) {
      const id = idValue.trim();
      const message = messageValue;
      const previous = last?.id === id ? (last.commit ?? null) : null;
      publication.request = { id, message, previous };
      submitting = true;
      refusal = null;
      renderBody();
      try {
        const saved = await api("/api/publish", { path: job.path, hash: job.hash, id, message, replace });
        if (closed) return;
        const unchanged = !!previous && saved.commit === previous;
        publication.outcome = { id, saved, unchanged };
        const broken = brokenNote();
        close();
        status(`${unchanged ? `already published at ${short(saved.commit)}, nothing changed` : `published at ${short(saved.commit)}`}${broken ? `; ${broken}` : ""}`,
          broken ? "warn" : "good");
        showTab("publish");
        await onPublished?.();
      } catch (e) {
        if (closed) return;
        submitting = false;
        if (e.status === 422) {
          publication.outcome = { id, refused: { error: e.data?.error || e.message, report: e.data?.report ?? null } };
          refusal = { error: e.data?.error || e.message, report: e.data?.report ?? null };
          conflict = null;
          showTab("publish");
          renderBody();
          return;
        }
        if (e.status === 412) {
          const commit = e.data?.commit ?? null;
          publication.outcome = { id, conflict: { error: e.data?.error || e.message, commit } };
          showTab("publish");
          conflict = { error: e.data?.error || e.message, commit, id, message, loading: !!commit, remoteYaml: null, localYaml: null, diffError: null };
          renderBody();
          if (commit) {
            Promise.all([
              api(`/api/library/job?id=${encodeURIComponent(id)}`),
              api(`/api/job?path=${encodeURIComponent(job.path)}`),
            ]).then(([remoteDoc, localDoc]) => {
              if (closed || !conflict) return;
              conflict.loading = false;
              conflict.remoteYaml = remoteDoc.yaml;
              conflict.localYaml = localDoc.yaml;
              renderBody();
            }).catch((diffErr) => {
              if (closed || !conflict) return;
              conflict.loading = false;
              conflict.diffError = diffErr.message;
              renderBody();
            });
          }
          return;
        }
        refusal = { error: e.message, report: null };
        renderBody();
      }
    }

    function renderBody() {
      const focused = document.activeElement instanceof HTMLInputElement && formArea.contains(document.activeElement)
        ? { id: document.activeElement.id, start: document.activeElement.selectionStart, end: document.activeElement.selectionEnd }
        : null;
      renderChecks();

      if (conflict) {
        const c = conflict;
        const overwriteLabel = c.commit ? `Overwrite (${short(c.commit)})` : "Publish as new pipeline";
        const overwriteBtn = el("button", {
          id: "d-publish-overwrite",
          type: "button",
          class: "danger",
          disabled: submitting,
          onclick: () => sendPublish(c.commit ?? ""),
        }, submitting ? "Publishing…" : overwriteLabel);

        fill(formArea,
          el("div", { class: "dialog-conflict" },
            el("p", { class: "warn" }, c.error || `The library holds ${c.id} at ${short(c.commit)}.`),
            c.commit ? el("p", { class: "hint" }, "Library commit in place: ", el("code", {}, short(c.commit))) : null,
            c.loading ? el("p", { class: "hint" }, "Loading diff between library and workspace…") : null,
            c.diffError ? el("p", { class: "hint warn" }, `Could not load diff: ${c.diffError}`) : null,
            c.remoteYaml !== null && c.localYaml !== null
              ? renderDiff(c.remoteYaml, c.localYaml, `library (${short(c.commit)})`, job.path) : null,
            refusal ? [el("p", { class: "bad" }, refusal.error), refusal.report ? el("pre", { class: "dialog-pre" }, refusal.report) : null] : null,
            el("div", { class: "row dialog-actions" },
              el("button", { type: "button", class: "ghost", onclick: close }, "Cancel"),
              c.commit && onPullRemote ? el("button", {
                type: "button",
                class: "ghost",
                disabled: submitting,
                onclick: () => { close(); onPullRemote(c.id); },
              }, "Pull remote first") : null,
              overwriteBtn)));
        return;
      }

      const checkBlocked = !checkingNow && currentCheck()?.ok === false;
      const validId = /^[a-z0-9][a-z0-9-]{0,62}$/.test(idValue.trim());
      const existingRemote = !last?.id || last.id !== idValue.trim()
        ? state.jobs.library.pipelines?.find((p) => p.id === idValue.trim()) : null;

      const submitBtn = /** @type {HTMLButtonElement} */ (el("button", {
        id: "d-publish-submit",
        type: "submit",
        class: "primary",
        disabled: checkingNow || submitting || checkBlocked || !validId,
      }, submitting ? "Publishing…" : "Publish"));

      const idInput = /** @type {HTMLInputElement} */ (el("input", {
        id: "d-publish-id",
        type: "text",
        class: "mono",
        value: idValue,
        "aria-label": "Pipeline ID",
        placeholder: "my-pipeline",
      }));
      const idHint = el("div", { class: "hint" });
      const idWarn = el("div", { class: "bad dialog-inline-error", hidden: true });
      const idExistWarn = el("div", { class: "warn dialog-inline-error", hidden: true });

      function syncIdHints() {
        const trimmed = idValue.trim();
        const ok = /^[a-z0-9][a-z0-9-]{0,62}$/.test(trimmed);
        const dup = (!last?.id || last.id !== trimmed) && !!state.jobs.library.pipelines?.some((p) => p.id === trimmed);
        idWarn.hidden = ok;
        idWarn.textContent = ok ? "" : "Use lowercase letters, digits, and '-' (starting with a letter or digit).";
        idExistWarn.hidden = !ok || !dup;
        idExistWarn.textContent = ok && dup ? `Pipeline '${trimmed}' already exists in the library.` : "";
        submitBtn.disabled = checkingNow || submitting || (!checkingNow && currentCheck()?.ok === false) || !ok;
      }

      idInput.addEventListener("input", () => { idTouched = true; idValue = idInput.value; syncIdHints(); });

      const messageInput = /** @type {HTMLInputElement} */ (el("input", {
        id: "d-publish-message",
        type: "text",
        value: messageValue,
        "aria-label": "Commit message",
        placeholder: "Describe this change",
      }));
      messageInput.addEventListener("input", () => { messageValue = messageInput.value; });

      const idField = !editingId && last?.id
        ? el("div", { class: "dialog-field" },
          el("div", { class: "row" },
            el("span", {}, "Updating library pipeline ", el("code", {}, last.id),
              last.commit ? [" (from ", el("code", {}, short(last.commit)), ")"] : null),
            el("button", {
              type: "button",
              class: "small ghost",
              onclick: () => { idTouched = true; editingId = true; renderBody(); },
            }, "Change ID…")))
        : el("div", { class: "dialog-field" },
          el("label", { for: "d-publish-id" }, "Pipeline ID"),
          idInput,
          idHint,
          idWarn,
          idExistWarn);
      if (existingRemote && !editingId) idExistWarn.hidden = false;

      const form = el("form", {
        class: "dialog-form",
        onsubmit: (e) => {
          e.preventDefault();
          if (checkingNow || submitting || currentCheck()?.ok === false) return;
          if (!/^[a-z0-9][a-z0-9-]{0,62}$/.test(idValue.trim())) return;
          sendPublish(null);
        },
      },
        idField,
        el("div", { class: "dialog-field" },
          el("label", { for: "d-publish-message" }, "Commit message"),
          messageInput),
        refusal ? [el("p", { class: "bad" }, refusal.error), refusal.report ? el("pre", { class: "dialog-pre" }, refusal.report) : null] : null,
        el("div", { class: "row dialog-actions" },
          el("button", { type: "button", class: "ghost", onclick: close }, "Cancel"),
          submitBtn));

      fill(formArea, form);
      syncIdHints();
      if (focused) {
        const again = /** @type {HTMLInputElement | null} */ (formArea.querySelector(`#${CSS.escape(focused.id)}`));
        if (again) {
          again.focus();
          if (focused.start !== null && focused.end !== null) again.setSelectionRange(focused.start, focused.end);
        }
      }
    }

    fill(body,
      el("p", { class: "hint" }, "Publishes ", el("code", {}, job.path), " as saved on disk. Its layout and launch values stay local."),
      checksBox,
      formArea);
    renderBody();

    Promise.all([
      api(`/api/publish?path=${encodeURIComponent(job.path)}`).catch(() => ({ publication: null })),
      askPublication(true),
    ]).then(([pubInfo]) => {
      if (closed) return;
      last = pubInfo?.publication ?? null;
      if (!idTouched) {
        if (last?.id) {
          idValue = last.id;
          editingId = false;
        } else {
          editingId = true;
        }
      }
      checkingNow = false;
      renderBody();
    });
  }, { onClose: () => { closed = true; } });
}

/** The Publish tab. */
export function publicationView() {
  if (!state.coordinator) {
    return tabOf(el("p", { class: "hint" }, "No coordinator is connected: connect one to publish a job into its library."),
      [el("button", { id: "publish-connect", class: "small primary", onclick: openCoordinatorConnect }, "Connect a coordinator…")]);
  }
  const check = currentCheck();
  return [check && check.ok === false ? refusalView(check) : null, outcomeView(publication.outcome)];
}

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
