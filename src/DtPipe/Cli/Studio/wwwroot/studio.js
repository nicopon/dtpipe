// dtpipe studio: the jobs of a folder, opened as cards and sampled through the real engine.

import { $, api, el, forgetToken, status, token } from "./dom.js";
import { isDirty, mountDesigner, openJob } from "./designer.js";

let current = null;

async function loadJobs() {
  const { folder, jobs } = await api("/api/jobs");
  $("folder").textContent = folder;
  $("jobs").replaceChildren(...(jobs.length
    ? jobs.map((path) => el("li", {}, el("button", { class: path === current ? "job open" : "job", onclick: () => open(path) }, path)))
    : [el("li", { class: "muted" }, "No .yaml or .yml file in this folder.")]));
}

async function open(path) {
  if (path !== current && isDirty() && !confirm(`${current} has changes that are not saved. Open ${path} anyway?`)) return;
  try {
    const job = await api(`/api/job?path=${encodeURIComponent(path)}`);
    current = path;
    $("job-name").textContent = path;
    await openJob(job.yaml, job.layout);
    status("");
    await loadJobs();
  } catch (e) {
    status(e.message, "bad");
  }
}

async function quit() {
  if (isDirty() && !confirm("Changes are not saved. Quit anyway?")) return;
  try { await api("/api/quit", {}); } catch { /* the server may already be gone */ }
  forgetToken();
  document.body.replaceChildren(el("p", { class: "closed" }, "dtpipe studio has stopped. You can close this tab."));
}

async function start() {
  const components = await api("/api/components");
  mountDesigner($("designer"), components);
  await loadJobs();
}

$("quit").addEventListener("click", quit);
window.addEventListener("beforeunload", (e) => { if (isDirty()) e.preventDefault(); });

if (!token) status("Open the address dtpipe studio printed: it carries this session's token.", "bad");
else start().catch((e) => status(e.message, "bad"));
