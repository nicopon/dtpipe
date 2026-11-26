// The coordinator's page: a hash router over its views, and the status pills of the top bar.
import { $, addLog, esc, gate, parseAddress, start, store, subscribe, tagOf, toast, unseenLogs } from "./core.js";
import * as nodes from "./nodes.js";
import * as library from "./library.js";
import * as runs from "./runs.js";
import * as events from "./events.js";
import * as access from "./access.js";

const views = { nodes, library, runs, events, access };

let current = null;

function route() {
  // The segments after the view are its parameters; a `?…` ending the address is its query.
  const { name, params, query } = parseAddress(location.hash);
  // An address that names no view says so: the library is the page of an empty address only.
  const known = Object.hasOwn(views, name);
  const unknown = name && !known;
  const view = known ? name : "library";
  if (current?.leave && current.leave() === false) return;
  // A fresh element per visit: a view's listeners never outlive it.
  const host = document.createElement("div");
  host.className = `view-body view-${view}`;
  $("view").replaceChildren(host);
  for (const a of document.querySelectorAll("#nav a[data-view]")) a.classList.toggle("active", !unknown && a.dataset.view === view);
  if (unknown) {
    current = null;
    host.innerHTML = `<h2>Page not found</h2><p><code>#/${esc(name)}</code> · <a href="#/library">← Library</a></p>`;
  } else {
    current = views[view];
    current.mount(host, params, query);
  }
  window.scrollTo(0, 0);
  renderBadge();
}

// The Events tab counts the errors and the warnings that arrived since the view was last open; the view marks them seen while it is.
// What the server replayed to the page's first connection is not counted: `unseenLogs` holds the rule.
function renderBadge() {
  const { errors, warnings } = unseenLogs();
  const el = $("events-badge");
  const total = errors + warnings;
  el.hidden = total === 0;
  el.textContent = total > 99 ? "99+" : String(total);
  el.className = `count ${errors ? "bad" : "warn"}`;
  el.title = `${errors} ✕ · ${warnings} ⚠ since Events was last open`;
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
}

subscribe((kind) => {
  renderPills();
  current?.update?.(kind);
  renderBadge();
});

// What the person's role does not allow is disabled as a view draws it, and says so.
new MutationObserver(() => gate($("view"))).observe($("view"), { childList: true, subtree: true });

window.addEventListener("hashchange", route);
window.addEventListener("beforeunload", (e) => { if (current?.dirty?.()) { e.preventDefault(); e.returnValue = ""; } });
window.addEventListener("error", (e) => addLog({ node: "coordinator", level: "Error", message: `page: ${e.message}` }));
window.addEventListener("unhandledrejection", (e) => addLog({ node: "coordinator", level: "Error", message: `page: ${e.reason?.stack || e.reason}` }));

start().then(() => {
  $("http-keys-banner").hidden = !store.httpKeys;
  const ver = tagOf(store.version);
  if ($("version")) {
    $("version").textContent = ver ? `v${ver}` : "";
    if (store.version && store.version !== ver) $("version").title = `Coordinator v${store.version}`;
  }
  $("me").textContent = `${store.me.email || store.me.name} · ${store.me.roles[0] || "no role"}`;
  renderPills();
  route();
}).catch((e) => toast(`The page failed to start: ${e.message}`));
