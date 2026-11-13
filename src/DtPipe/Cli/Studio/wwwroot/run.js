// @ts-check
// The real run of the saved job: what it would write, confirmed first; then the run, polled until it ends. A run that
// reviews first stops before every writer and is left suspended; the sink cards write it or abandon it.

import { api, el, fill, status, targetOf } from "./dom.js";
import { brickBranches, showTab } from "./designer.js";
import { invalidate } from "./render.js";
import { showHeld } from "./preview.js";
import { sequence, state } from "./state.js";

let runTimer = null;

const runLabel = (code) => code === 0 ? "succeeded" : code === 130 ? "stopped" : `failed (code ${code})`;

/** A run still going is kept when another job opens, since it goes on whatever is open. */
export function forgetRun() {
  if (state.run && state.run.phase !== "running") state.run = null;
  preparing.cancel();
  closeDialog();
}

/** The choice the job was left with. @param {boolean} review */
export function restoreReview(review) {
  state.writing.review = review === true;
}

// ---------------------------------------------------------------- the confirmation: a dialog over the page

/** The open confirmation's dialog; there is at most one. @type {HTMLDialogElement | null} */
let dialog = null;
// One counter for the confirmations asked: the answer to an earlier choice never replaces the one to the latest.
const preparing = sequence();
const plural = (n, word) => `${n} ${word}${n === 1 ? "" : "s"}`;

/** The targets the confirmation lists: each one that writes, or, for a continuation, each one that holds rows. @param {any} run */
function listedOf(run) {
  const writes = run.prepared?.writes ?? [];
  return run.asked.continuation ? writes.filter((w) => run.asked.held.includes(w.alias)) : writes;
}

function closeDialog() {
  const old = dialog;
  dialog = null;
  if (!old) return;
  if (old.open) old.close();
  old.remove();
}

/** Escape and Cancel: the confirmation is dropped, nothing runs. */
function dropConfirmation() {
  preparing.cancel();
  if (state.run?.phase === "confirm") state.run = null;
  closeDialog();
  invalidate("drawer");
}

/**
 * Opens the confirmation of a run. `continuation` writes the rows a suspended run holds, among the targets `held` that hold
 * some; otherwise the run reads the sources again, and stops before its writers when the case "Review before writing" is
 * ticked. `targets` are the ones it writes: every one when null.
 * @param {boolean} [continuation] @param {string[] | null} [held] @param {string[] | null} [targets]
 */
export async function prepareRun(continuation = false, held = null, targets = null) {
  const job = state.job;
  if (!job) return;
  if (state.cards.dirty || !job.hash) {
    status("Save the job first: a run executes the job saved in the folder.", "bad");
    return;
  }
  await ask({ continuation, review: continuation ? false : state.writing.review, targets, held });
}

/**
 * Asks the server what the run would do for what the controls say now, and shows it. What the digest was prepared for is
 * `confirmed`, and it is what Confirm sends: the controls (`asked`) may be ahead of it while an answer is awaited or refused.
 * @param {{ continuation: boolean, review: boolean, targets: string[] | null, held: string[] | null }} wanted
 */
async function ask(wanted) {
  const job = state.job;
  if (!job) return;
  const ticket = preparing.begin();
  const open = state.run?.phase === "confirm" && state.run.path === job.path ? state.run : null;
  if (open) {
    state.run = { ...open, asked: wanted, busy: true };
    drawDialog();
  }
  const bricks = brickBranches();
  // The mode is always said: a run is the job's own, written whole, unless it reviews first or continues a suspended run.
  const mode = wanted.continuation ? { debug: true, review: false } : { review: wanted.review };
  try {
    const prepared = await api("/api/run/prepare", { path: job.path, hash: job.hash, bricks, ...mode, ...(wanted.targets ? { targets: wanted.targets } : {}) });
    if (preparing.stale(ticket) || state.job?.path !== job.path) return;
    state.run = { phase: "confirm", path: job.path, hash: job.hash, bricks, prepared, asked: wanted, confirmed: wanted, refused: null, busy: false };
  } catch (e) {
    if (preparing.stale(ticket) || state.job?.path !== job.path) return;
    status(e.message, "bad");
    // A refusal inside an open confirmation stays in it, where the controls change the choice; a refused review opens one
    // too, since its case is where the person unticks it. Any other refusal opens nothing.
    if (open) state.run = { ...state.run, refused: e.message, busy: false };
    else state.run = !wanted.continuation && wanted.review
      ? { phase: "confirm", path: job.path, hash: job.hash, bricks, prepared: null, asked: wanted, confirmed: null, refused: e.message, busy: false } : null;
  }
  drawDialog();
}

/** Draws the confirmation in its dialog, or closes the dialog when none is open. */
function drawDialog() {
  const run = state.run;
  if (!run || run.phase !== "confirm") { closeDialog(); return; }
  if (!dialog) {
    dialog = /** @type {HTMLDialogElement} */ (el("dialog", { id: "d-run-dialog", class: "run-dialog", "aria-labelledby": "d-run-title" }));
    dialog.addEventListener("cancel", dropConfirmation);
    document.body.append(dialog);
    dialog.showModal();
  }
  const active = document.activeElement?.id;
  fill(dialog, confirmView(run));
  // A choice redraws the dialog: the control the person is on keeps the focus.
  const again = active ? [...dialog.querySelectorAll("[id]")].find((n) => n.id === active) : null;
  if (again instanceof HTMLElement) again.focus();
}

/** @param {boolean} review */
function setReview(review) {
  state.writing.review = review;
  invalidate("drawer");
  return ask({ ...state.run.asked, review });
}

/** Ticks or unticks a target: a choice of none is never made. @param {string} alias @param {boolean} on */
function choose(alias, on) {
  const run = state.run;
  if (!run || run.phase !== "confirm") return;
  const listed = listedOf(run).map((w) => w.alias);
  const now = new Set(run.asked.targets ?? listed);
  if (on) now.add(alias); else now.delete(alias);
  if (now.size === 0) { drawDialog(); return; }
  // A continuation always says which held targets it writes, since the others may no longer hold rows.
  return ask({ ...run.asked, targets: run.asked.continuation || now.size < listed.length ? listed.filter((a) => now.has(a)) : null });
}

/** What the dialog says: what would be written, to which targets, and the choice of both. @param {any} run */
function confirmView(run) {
  const { asked, prepared } = run;
  const sample = !!prepared?.sample;
  const listed = listedOf(run);
  const chosen = new Set(asked.targets ?? listed.map((w) => w.alias));
  const selectable = !sample && listed.length > 1;
  const count = listed.length ? chosen.size : 0;
  const review = asked.continuation ? null : el("label", { class: "review" },
    el("input", { id: "d-review", type: "checkbox", checked: asked.review, onchange: (e) => setReview(targetOf(e).checked) }),
    " Review before writing ", el("span", { class: "hint" }, "(stop before every writer and read the rows; the sink cards then write or abandon)"));
  const label = run.busy ? "Checking…" : asked.continuation ? `Write ${plural(count, "target")}`
    : sample ? "Run the sample" : !listed.length ? "Run" : asked.review ? `Hold ${plural(count, "target")} for review` : `Write ${plural(count, "target")}`;
  const staged = prepared?.staged;
  return [
    el("h3", { id: "d-run-title" }, asked.continuation ? "Write the held rows" : sample ? "Run the sample" : "Write targets"),
    el("p", { class: "hint" }, el("code", {}, run.path), asked.continuation
      ? " holds the rows read earlier: choose the targets that receive them now; the others stay held."
      : " runs here, as dtpipe --job would."),
    review,
    run.refused ? el("p", { class: "warn" }, run.refused) : null,
    prepared && !run.refused ? [
      sample ? el("p", { class: "warn" }, `This run is a sample: its source bricks are read at ${prepared.sampleRows} rows through the coordinator, and nothing real is written. A sink brick is written to a throwaway file here, never to its node.`) : null,
      staged ? stagedView(staged, sample) : sample ? null : el("p", { class: "warn" }, "This run is real: its writers write their targets. A sink brick is written to a throwaway file here, never to its node."),
    ] : null,
    listed.length ? el("div", { class: "run-targets" }, listed.map((w) => el("div", { class: "run-write" },
      el("label", {},
        selectable ? el("input", { id: `d-target-${w.alias}`, type: "checkbox", checked: chosen.has(w.alias), onchange: (e) => choose(w.alias, targetOf(e).checked) }) : null,
        selectable ? " " : null, el("b", {}, w.alias),
        w.throwaway ? " → a throwaway file (the brick's node is not reached)" : [" → ", el("code", {}, w.output)]),
      w.block ? el("details", {}, el("summary", {}, "writer block"), el("pre", {}, w.block)) : null)))
      : prepared ? el("p", { class: "hint" }, "No branch of this job writes anywhere.") : null,
    el("div", { class: "row run-actions" },
      el("button", { id: "d-run-cancel", class: "ghost", onclick: dropConfirmation }, "Cancel"),
      el("button", { id: "d-run-confirm", class: "primary", disabled: run.busy || !!run.refused || !prepared, onclick: startRun }, label)),
  ];
}

async function startRun() {
  const run = state.run;
  if (!run || run.phase !== "confirm" || run.busy || run.refused || !run.confirmed) return;
  // What was confirmed, never what the controls say now: the digest is of that.
  const { continuation, review, targets } = run.confirmed;
  const mode = continuation ? { debug: true, review: false } : { review };
  try {
    await api("/api/run", { path: run.path, hash: run.hash, bricks: run.bricks, digest: run.prepared.digest, ...mode, ...(targets ? { targets } : {}) });
    state.run = { ...run, phase: "running", status: null };
    closeDialog();
    pollRun();
    showTab("run");
  } catch (e) {
    status(e.message, "bad");
    // The job or its values may have changed since: what the dialog says is asked again.
    await ask(run.asked);
  }
}

function pollRun() {
  clearInterval(runTimer);
  runTimer = setInterval(async () => {
    try {
      const now = await api("/api/run");
      const run = state.run = { ...state.run, status: now, phase: now.running ? "running" : "done" };
      if (!now.running) {
        clearInterval(runTimer);
        status(`run ${runLabel(now.code)}`, now.code === 0 ? "good" : "warn");
        // A run that stopped holding rows shows them, if its job is the one open; the verdict is in the status line.
        if (now.view && run.path === state.job?.path && await showHeld(now.view)) return;
      }
      showTab("run");
    } catch (e) {
      clearInterval(runTimer);
      status(e.message, "bad");
    }
  }, 1000);
}

async function stopRun() {
  try { await api("/api/run/stop", {}); status("stop asked", "warn"); } catch (e) { status(e.message, "bad"); }
}

// What a staged run will do, stage by stage, and what it holds on disk meanwhile.
function stagedView(staged, sample) {
  const size = (bytes) => bytes >= 1048576 ? `${(bytes / 1048576).toFixed(1)} MB` : `${Math.max(1, Math.round(bytes / 1024))} kB`;
  return [
    sample ? null : staged.writesNow
      ? el("p", { class: "warn" }, "This run is real and reaches the end of the job: its last stage writes the targets. A sink brick is written to a throwaway file here, never to its node.")
      : el("p", { class: "warn" }, "This run is real but stops before every writer, so nothing is written. It holds every row it read, encrypted in this studio's session, "
        + "until you write them or abandon them from the sink cards. The rows of a sample are not used."),
    el("ol", { class: "run-stages" }, staged.stages.map((st) => el("li", {},
      el("b", {}, st.branch), " reads ", el("code", {}, st.reads || "its source"),
      st.keeps ? ", keeps every row it delivers" : "",
      st.writes ? [", WRITES ", el("code", {}, st.writes)] : ", writes nothing"))),
    staged.held.length ? el("p", { class: "hint" }, "Held from before: ", staged.held.map((h) =>
      `${h.card || h.branch} (${size(h.bytes)}, read ${h.readAt ? new Date(h.readAt).toLocaleString() : "earlier"})`).join("; "),
      ". Continuing writes these rows, not a new read of the source.") : null,
    staged.discards ? el("p", { class: "warn" }, `${staged.discards} held checkpoint(s) no longer match the job or the values it reads with, or come from a sample: they are forgotten.`) : null,
  ];
}

/** The Run tab: the run under way and its verdict. What it would write is confirmed first, in a dialog (`drawDialog`). */
export function runView() {
  const run = state.run;
  if (!run || run.phase === "confirm") return el("p", { class: "hint" }, "Write targets… runs the job saved in this folder, as dtpipe --job would, on this machine. You confirm what it writes first.");
  const now = run.status;
  if (run.phase === "running" || !now) {
    return [el("p", {}, "Running ", el("code", {}, run.path), "…"),
      el("button", { id: "d-run-stop", class: "ghost danger", onclick: stopRun }, "Stop")];
  }
  const failures = Object.entries(now.failures || {});
  return [
    el("p", { class: now.code === 0 ? "good" : "warn" }, `${run.path}: ${runLabel(now.code)}.`),
    now.stopRequested && now.code === 130 ? el("p", { class: "hint" }, "You stopped it: what it had written before is not undone.") : null,
    now.error ? el("p", { class: "warn" }, now.error) : null,
    state.preview.real && run.path === state.job?.path ? el("p", { class: "warn" }, "⏸ This run is suspended before its targets are written: write it or abandon it from the sink cards.") : null,
    failures.map(([alias, why]) => el("p", { class: "warn" }, el("b", {}, alias), ": ", why)),
  ];
}
