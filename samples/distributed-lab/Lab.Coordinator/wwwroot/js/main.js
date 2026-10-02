// The coordinator's page: a hash router over five views, and the status pills of the top bar.
import { $, addLog, bytes, start, store, subscribe, toast } from "./core.js";
import * as nodes from "./nodes.js";
import * as library from "./library.js";
import * as designer from "./designer.js";
import * as distribution from "./distribution.js";
import * as runs from "./runs.js";
import * as access from "./access.js";

const views = { nodes, library, designer, distribution, runs, access };

// For scripted checks (tools/ui_check.py): the live store, to hand the designer a draft.
window.lab = { store };
let current = null;

function route() {
  const [name, ...params] = location.hash.replace(/^#\/?/, "").split("/").map(decodeURIComponent);
  const view = views[name] ? name : "library";
  if (current?.leave && current.leave() === false) return;
  // A fresh element per visit: a view's listeners never outlive it.
  const host = document.createElement("div");
  host.className = `view-body view-${view}`;
  $("view").replaceChildren(host);
  current = views[view];
  for (const a of document.querySelectorAll("#nav a[data-view]")) a.classList.toggle("active", a.dataset.view === view);
  current.mount(host, params.filter(Boolean));
  window.scrollTo(0, 0);
}

function renderPills() {
  $("conn").textContent = store.connected ? "live" : "reconnecting…";
  $("conn").className = `pill ${store.connected ? "on" : "off"}`;
  const online = store.nodes.filter((n) => n.online).length;
  $("nodes-pill").textContent = `${online}/${store.nodes.length} nodes`;
  $("nodes-pill").className = `pill ${online === store.nodes.length && online > 0 ? "on" : "off"}`;
  const running = store.queue.find((r) => r.state === "running");
  const queued = store.queue.filter((r) => r.state === "queued").length;
  $("runs-pill").textContent = running ? `${running.pipelineId} running${queued ? ` · ${queued} queued` : ""}` : queued ? `${queued} queued` : "no run";
  $("runs-pill").className = `pill ${running || queued ? "busy" : ""}`;
  $("bytes").textContent = `hub ${bytes(store.bytes.total)}${store.bytes.activeTransfers > 0 ? ` · ${store.bytes.activeTransfers} transfer(s)` : ""}`;
}

subscribe((kind) => {
  renderPills();
  current?.update?.(kind);
});

window.addEventListener("hashchange", route);
window.addEventListener("beforeunload", (e) => { if (current?.dirty?.()) { e.preventDefault(); e.returnValue = ""; } });
window.addEventListener("error", (e) => addLog({ node: "coordinator", level: "Error", message: `page: ${e.message}` }));
window.addEventListener("unhandledrejection", (e) => addLog({ node: "coordinator", level: "Error", message: `page: ${e.reason?.stack || e.reason}` }));

start().then(() => { renderPills(); route(); }).catch((e) => toast(`The page failed to start: ${e.message}`));
