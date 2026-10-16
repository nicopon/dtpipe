// @ts-check
// The real run of the saved job: what it would write, confirmed first; then the run, polled until it ends. A run that
// reviews first stops before every writer and is left suspended; the sink cards write it or abandon it.

import { api, el, status } from "./dom.js";
import { brickBranches, showTab } from "./designer.js";
import { invalidate } from "./render.js";
import { showHeld } from "./preview.js";
import { state } from "./state.js";

let runTimer = null;

const runLabel = (code) => code === 0 ? "succeeded" : code === 130 ? "stopped" : `failed (code ${code})`;

/** A run still going is kept when another job opens, since it goes on whatever is open. */
export function forgetRun() {
  if (state.run && state.run.phase !== "running") state.run = null;
}

/** The choice the job was left with. @param {boolean} review */
export function restoreReview(review) {
  state.writing.review = review === true;
}

/** What the confirmation asks for: the case "Review before writing" being ticked or not. @param {boolean} review */
async function setReview(review) {
  state.writing.review = review;
  invalidate("drawer");
  await prepareRun();
}

/**
 * Opens the confirmation of a run. `continuation` writes the rows a suspended run holds; otherwise the run reads the sources
 * again, and stops before its writers when the case "Review before writing" is ticked.
 * @param {boolean} [continuation]
 */
export async function prepareRun(continuation = false) {
  const job = state.job;
  if (!job) return;

  if (state.cards.dirty || !job.hash) {
    status("Save the job first: a run executes the job saved in the folder.", "bad");
    return;
  }
  try {
    // The mode is always said: a run is the job's own, written whole, unless it reviews first or continues a suspended run.
    const staged = continuation ? { debug: true, review: false } : { review: state.writing.review };
    const prepared = await api("/api/run/prepare", { path: job.path, hash: job.hash, bricks: brickBranches(), ...staged });
    if (state.job?.path !== job.path) return;
    state.run = { phase: "confirm", prepared, bricks: brickBranches(), hash: job.hash, path: job.path, staged: { ...staged, continuation } };
    showTab("run");
  } catch (e) {
    // A refused review keeps its confirmation open: the case is where the person unticks it.
    state.run = !continuation && state.writing.review ? { phase: "confirm", refused: e.message, staged: { review: true, continuation: false } } : null;
    status(e.message, "bad");
    if (state.run) showTab("run");
  }
}

async function startRun() {
  const run = state.run;
  try {
    const { continuation, ...mode } = run.staged;
    await api("/api/run", { path: run.path, hash: run.hash, bricks: run.bricks, digest: run.prepared.digest, ...mode });
    state.run = { ...run, phase: "running", status: null };
    pollRun();
    showTab("run");
  } catch (e) {
    status(e.message, "bad");
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
function stagedView(staged) {
  const size = (bytes) => bytes >= 1048576 ? `${(bytes / 1048576).toFixed(1)} MB` : `${Math.max(1, Math.round(bytes / 1024))} kB`;
  return [
    staged.writesNow
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

/** The Run tab. */
export function runView() {
  const run = state.run;
  if (!run) return el("p", { class: "hint" }, "Write targets… runs the job saved in this folder, as dtpipe --job would, on this machine. You confirm what it writes first.");
  if (run.phase === "confirm") {
    const review = run.staged.continuation ? null : el("label", { class: "review" },
      el("input", { id: "d-review", type: "checkbox", checked: state.writing.review, onchange: (e) => setReview(/** @type {HTMLInputElement} */ (e.target).checked) }),
      " Review before writing ", el("span", { class: "hint" }, "(stop before every writer and read the rows; the sink cards then write or abandon)"));
    if (run.refused) {
      return [review, el("p", { class: "warn" }, run.refused),
        el("div", { class: "row" }, el("button", { class: "ghost", onclick: () => { state.run = null; showTab("run"); } }, "Cancel"))];
    }
    const writes = run.prepared.writes;
    const staged = run.prepared.staged;
    return [
      review,
      staged ? stagedView(staged) : el("p", { class: "warn" }, "This run is real: its writers write their targets. A sink brick is written to a throwaway file here, never to its node."),
      writes.length ? writes.map((w) => el("div", { class: "run-write" },
        el("p", {}, el("b", {}, w.alias), w.throwaway ? " → a throwaway file (the brick's node is not reached)" : [" → ", el("code", {}, w.output)]),
        w.block ? el("pre", {}, w.block) : null)) : el("p", { class: "hint" }, "No branch of this job writes anywhere."),
      el("div", { class: "row" },
        el("button", { id: "d-run-confirm", class: "primary", onclick: startRun }, run.staged.continuation ? "Confirm and write" : state.writing.review ? "Confirm and review" : "Confirm and write"),
        el("button", { class: "ghost", onclick: () => { state.run = null; showTab("run"); } }, "Cancel")),
    ];
  }
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
