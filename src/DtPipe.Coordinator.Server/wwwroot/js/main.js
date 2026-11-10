// The coordinator's page: a hash router over its views, and the status pills of the top bar.
import { $, addLog, bytes, start, store, subscribe, toast } from "./core.js";
import * as nodes from "./nodes.js";
import * as library from "./library.js";
import * as runs from "./runs.js";
import * as access from "./access.js";

const views = { nodes, library, runs, access };

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
  // Runs on disjoint data nodes are in flight together: one names its pipeline, several are counted.
  const running = store.queue.filter((r) => r.state === "running");
  const queued = store.queue.filter((r) => r.state === "queued").length;
  const inFlight = running.length === 1 ? `${running[0].pipelineId} running` : running.length ? `${running.length} running` : "";
  $("runs-pill").textContent = [inFlight, queued ? `${queued} queued` : ""].filter(Boolean).join(" · ") || "no run";
  $("runs-pill").title = running.map((r) => r.pipelineId).join(", ");
  $("runs-pill").className = `pill ${running.length || queued ? "busy" : ""}`;
  $("bytes").textContent = `hub ${bytes(store.bytes.total)}`;
}

subscribe((kind) => {
  renderPills();
  current?.update?.(kind);
});

window.addEventListener("hashchange", route);
window.addEventListener("beforeunload", (e) => { if (current?.dirty?.()) { e.preventDefault(); e.returnValue = ""; } });
window.addEventListener("error", (e) => addLog({ node: "coordinator", level: "Error", message: `page: ${e.message}` }));
window.addEventListener("unhandledrejection", (e) => addLog({ node: "coordinator", level: "Error", message: `page: ${e.reason?.stack || e.reason}` }));

start().then(() => {
  $("http-keys-banner").hidden = !store.httpKeys;
  const ver = store.version?.split("+")[0] || store.version;
  if ($("version")) {
    $("version").textContent = ver ? `v${ver}` : "";
    if (store.version && store.version !== ver) $("version").title = `Coordinator v${store.version}`;
  }
  $("me").textContent = `${store.me.email || store.me.name} · ${store.me.roles[0] || "no role"}`;
  renderPills();
  route();
}).catch((e) => toast(`The page failed to start: ${e.message}`));
