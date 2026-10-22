// @ts-check
// The coordinator the command line named, if any: who is signed in, and signing in through the
// browser. The page never sees a token: it asks the studio, which keeps them, and reads a status.

import { $, api, el, fill, status } from "./dom.js";
import { invalidate, onZone } from "./render.js";
import { state } from "./state.js";

const POLL_MS = 2000;
const POLL_FOR_MS = 10 * 60 * 1000;

let polling = null;
const listeners = [];

// Called with the status each time the person signs in or out.
export function onCoordinator(listener) { listeners.push(listener); }

export async function refreshCoordinator() {
  const before = state.coordinator?.state;
  const next = await api("/api/coordinator");
  state.coordinator = next.url ? next : null;
  invalidate("coordinator");
  if (state.coordinator && state.coordinator.state !== before) for (const listener of listeners) listener(state.coordinator);
  return state.coordinator;
}

onZone("coordinator", render);

function render() {
  const link = state.coordinator;
  const host = $("coordinator");
  if (!link) { host.hidden = true; return; }
  host.hidden = false;
  const name = new URL(link.url).host;
  const trial = link.trialIdp ? el("strong", { class: "trial-idp", title: "This coordinator runs its own trial identity provider" }, " trial identity provider, not for production ") : "";
  if (link.state === "signed-in") {
    fill(host, el("span", { class: "dot good" }), " ", name, " · ", link.me?.email || link.me?.name || "signed in",
      " ", trial, el("button", { class: "small ghost", onclick: signOut, title: "Forget this studio's tokens" }, "Sign out"));
  } else if (link.state === "signing-in") {
    fill(host, el("span", { class: "dot warn" }), " ", name, " · finish signing in in the other tab ",
      " ", trial, el("button", { class: "small ghost", onclick: signIn }, "Open again"));
  } else {
    fill(host, el("span", { class: "dot bad" }), " ", name, " · ", link.reason || "not signed in", " ",
      el("button", { class: "small", onclick: signIn }, link.reason ? "Sign in again" : "Sign in"));
  }
}

// The provider's page opens in a new tab, so the cards of this one stay as they are.
async function signIn() {
  try {
    const { url } = await api("/api/coordinator/login", {});
    // "noopener" would make window.open return null whether or not the tab opened: the opener link is
    // cut by hand instead, before the tab leaves this origin.
    const tab = window.open("about:blank", "_blank");
    if (tab) { tab.opener = null; tab.location.href = url; }
    else status("The browser blocked the sign-in tab: allow pop-ups for this page.", "bad");
    await refreshCoordinator();
    poll();
  } catch (e) {
    status(e.message, "bad");
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

function poll() {
  clearInterval(polling);
  const until = Date.now() + POLL_FOR_MS;
  polling = setInterval(async () => {
    try {
      const now = await refreshCoordinator();
      if (!now || now.state !== "signing-in" || Date.now() > until) clearInterval(polling);
    } catch {
      clearInterval(polling);
    }
  }, POLL_MS);
}
