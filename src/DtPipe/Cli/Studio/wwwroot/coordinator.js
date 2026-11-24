// @ts-check
// The coordinator connected to this studio, if any: who is signed in, signing in through the browser, and the connection
// dialog that sets, changes or forgets it. The page never sees a token or a key: it asks the studio, which keeps them, and
// reads a status.

import { $, api, el, fill, onLinkDoubt, status } from "./dom.js";
import { openCoordinatorConnectDialog } from "./dialogs.js";
import { invalidate, onZone } from "./render.js";
import { state } from "./state.js";

const POLL_MS = 2000;
const POLL_FOR_MS = 10 * 60 * 1000;

/** @type {ReturnType<typeof setInterval> | null} */
let polling = null;
const listeners = [];

// Called with the status (null when none is connected) each time the person signs in or out, or connects another coordinator.
export function onCoordinator(listener) { listeners.push(listener); }

export async function refreshCoordinator() {
  const before = state.coordinator?.state;
  const beforeUrl = state.coordinator?.url;
  const [next, connection] = await Promise.all([api("/api/coordinator"), api("/api/coordinator/connection")]);
  state.coordinator = next.url ? next : null;
  state.coordinatorConnection = connection;
  invalidate("coordinator");
  const changed = state.coordinator?.state !== before || state.coordinator?.url !== beforeUrl;
  if (changed) { invalidate("jobs"); invalidate("bar"); }
  // The drawer's Problems and Publish say what a publication would refuse only while signed in; a poll that changes
  // nothing leaves the drawer, and what is typed in it, alone.
  if (changed) invalidate("drawer");
  if (changed) for (const listener of listeners) listener(state.coordinator);
  // A key signs in by itself, in the background: the status is read again until it is settled.
  if (state.coordinator?.state === "signing-in" && state.coordinator.byKey && polling === null) poll();
  return state.coordinator;
}

/** The connection dialog: what the server says of the connection now, and the page's way to connect, change or forget it. */
export async function openCoordinatorConnect() {
  let connection;
  try { connection = await api("/api/coordinator/connection"); } catch (e) { status(e.message, "bad"); return; }
  openCoordinatorConnectDialog({
    connection,
    onConnect: async (body) => {
      const answer = await api("/api/coordinator/connection", body, "PUT");
      await refreshCoordinator();
      return answer;
    },
    onForget: async () => {
      await api("/api/coordinator/connection", undefined, "DELETE");
      await refreshCoordinator();
    },
  });
}

onZone("coordinator", render);
// A request the coordinator's side failed: what the link last met is read again, so the header says it.
onLinkDoubt(() => { refreshCoordinator().catch(() => {}); });

function render() {
  const link = state.coordinator;
  const connection = state.coordinatorConnection;
  const host = $("coordinator");
  host.hidden = false;
  const change = el("button", { id: "coordinator-change", class: "small ghost", onclick: openCoordinatorConnect,
    title: connection?.source === "flag" ? "Set by the command line: a change holds for this session" : "Connect another coordinator, or forget this one" }, "Change…");
  if (!link) {
    // Nothing is connected: the pill says so, or says that the remembered one waits for its key.
    const name = connection?.origin ? new URL(connection.origin).host : null;
    if (connection?.needsKey) {
      fill(host, el("span", { class: "dot warn" }), " ", name, " · key needed ",
        el("button", { id: "coordinator-connect", class: "small", onclick: openCoordinatorConnect, title: "This coordinator was remembered, but its key is not kept: connect it again" }, "Reconnect…"));
    } else {
      fill(host, el("span", { class: "dot" }), " no coordinator ",
        el("button", { id: "coordinator-connect", class: "small ghost", onclick: openCoordinatorConnect, title: "Connect a coordinator to use its bricks and its library" }, "Connect…"));
    }
    return;
  }
  const name = new URL(link.url).host;
  const origin = connection?.source === "flag" ? el("span", { class: "hint", title: "Set by the command line" }, " (command line)") : null;
  // The last exchange got no answer: the header says so in place of the state it can no longer vouch for, and keeps the
  // button the state offers.
  const silent = link.unreachable ? el("span", { title: link.unreachable }, "unreachable: ", link.unreachable) : null;
  if (link.state === "signed-in" && silent) {
    fill(host, el("span", { class: "dot warn" }), " ", name, origin, " · ", silent,
      " ", el("button", { class: "small ghost", onclick: signOut, title: "Forget this studio's tokens" }, "Sign out"), " ", change);
  } else if (link.state === "signed-in") {
    fill(host, el("span", { class: "dot good" }), " ", name, origin, " · ", link.me?.email || link.me?.name || "signed in",
      " ", el("button", { class: "small ghost", onclick: signOut, title: "Forget this studio's tokens" }, "Sign out"), " ", change);
  } else if (link.state === "signing-in" && link.byKey) {
    fill(host, el("span", { class: "dot warn" }), " ", name, origin, " · signing in with the key… ", change);
  } else if (link.state === "signing-in") {
    fill(host, el("span", { class: "dot warn" }), " ", name, origin, " · finish signing in in the other tab ",
      " ", el("button", { class: "small ghost", onclick: signIn }, "Open again"), " ", change);
  } else {
    const label = link.byKey ? "Sign in with the key" : link.reason ? "Sign in again" : "Sign in";
    fill(host, el("span", { class: "dot bad" }), " ", name, origin, " · ", silent ?? (link.reason || "not signed in"), " ",
      el("button", { class: "small", onclick: signIn }, label), " ", change);
  }
}

// The provider's page opens in a new tab, so the cards of this one stay as they are. With a key the
// studio signs in by itself and answers no address: the status is read again.
export async function signIn() {
  try {
    const { url } = await api("/api/coordinator/login", {});
    if (!url) { await refreshCoordinator(); return; }
    // "noopener" would make window.open return null whether or not the tab opened: the opener link is
    // cut by hand instead, before the tab leaves this origin.
    const tab = window.open("about:blank", "_blank");
    if (tab) { tab.opener = null; tab.location.href = url; }
    else status("The browser blocked the sign-in tab: allow pop-ups for this page.", "bad");
    await refreshCoordinator();
    poll();
  } catch (e) {
    status(e.message, "bad");
    // A refused key signs the person out with its reason, which the status carries.
    refreshCoordinator().catch(() => {});
  }
}

async function signOut() {
  try {
    await api("/api/coordinator/logout", {});
    await refreshCoordinator();
  } catch (e) {
    status(e.message, "bad");
  }
}

function stopPoll() {
  if (polling !== null) clearInterval(polling);
  polling = null;
}

function poll() {
  stopPoll();
  const until = Date.now() + POLL_FOR_MS;
  polling = setInterval(async () => {
    try {
      const now = await refreshCoordinator();
      if (!now || now.state !== "signing-in" || Date.now() > until) stopPoll();
    } catch {
      stopPoll();
    }
  }, POLL_MS);
}
