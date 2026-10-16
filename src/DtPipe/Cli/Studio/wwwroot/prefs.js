// @ts-check
// How the page was left: the sizes of its panes and, for each saved job, the drawer's tab, the rows asked and the
// selected card. The server keeps them beside the folder's other local files (/api/view), so they survive any
// port and any browser. Nothing here is true of a job and nothing is read back as such: the page draws the same
// without them, and a read or a write that fails is dropped. Rows, checkpoints and tokens never come here.

import { api } from "./dom.js";
import { state } from "./state.js";

/** The tabs worth coming back to: the others (a run, a publication, problems) belong to a moment. */
export const KEPT_TABS = ["yaml", "preview", "variables"];
const SAVE_AFTER_MS = 400;

let restoredFor = null;
let keptTab = "yaml";
let last = "";
let timer = null;

/** What was kept for a job, with the panes' sizes. @param {string} path @returns {Promise<{ sizes: Record<string, number> | null, job: { tab?: string, rows?: number, card?: string } | null }>} */
export async function readView(path) {
  try { return await api(`/api/view?path=${encodeURIComponent(path)}`); } catch { return { sizes: null, job: null }; }
}

/** The job's preferences are being kept from now on: what the page shows is what was restored. @param {string} path @param {{ tab?: string } | null} [kept] */
export function viewRestored(path, kept = null) {
  restoredFor = path;
  keptTab = kept?.tab && KEPT_TABS.includes(kept.tab) ? kept.tab : "yaml";
  last = JSON.stringify(snapshot());
}

function snapshot() {
  const job = state.job;
  if (state.view.drawer && KEPT_TABS.includes(state.view.drawer)) keptTab = state.view.drawer;
  return {
    sizes: { ...state.view.sizes },
    // A job not saved yet, or one whose kept preferences are not read yet, keeps none: they would overwrite what was kept.
    ...(job?.hash && restoredFor === job.path
      ? { path: job.path, job: { tab: keptTab, rows: state.preview.rows, card: state.cards.selected?.alias ?? null } }
      : {}),
  };
}

/** Called after every drawing: sends the view if it changed, once the person has stopped for a moment. */
export function rememberView() {
  const body = snapshot();
  const text = JSON.stringify(body);
  if (text === last) return;
  last = text;
  clearTimeout(timer);
  timer = setTimeout(() => { api("/api/view", body, "PUT").catch(() => { /* the page was left as it is: nothing to undo */ }); }, SAVE_AFTER_MS);
}
