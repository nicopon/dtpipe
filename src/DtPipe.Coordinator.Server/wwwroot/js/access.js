// Access: who may send what to whom. Identities of the coordinator's own IDP (who: node name and
// groups), the hosts waiting for approval, the flow matrix between groups (to whom, enforced by the
// hub on every transfer), and the audit of every change.
import { api, esc, nodeColor, store, toast } from "./core.js";

let host = null;
let rights = null;
let matrixDraft = null;      // group -> Set(targets), while edited
let adding = false;

export function mount(el) {
  host = el;
  host.addEventListener("click", onClick);
  host.addEventListener("change", onChange);
  load();
}

export function update(kind) {
  if (kind === "rights") load();
  else if (["nodes", "bricks"].includes(kind)) render();
}

export function leave() {
  return !matrixDraft || confirm("Unsaved rights changes will be lost. Leave anyway?");
}

async function load() {
  try { rights = await api("/api/rights"); } catch (e) { toast(e.message); return; }
  render();
}

const setOf = (list) => new Set(list || []);
const groupsOf = (text) => text.split(",").map((g) => g.trim()).filter(Boolean);

function matrixCell(from, to) {
  const allowed = matrixDraft ? matrixDraft[from]?.has(to) : (rights.matrix[from] || []).includes(to);
  return `<td class="${allowed ? "yes" : "no"}"><input type="checkbox" data-matrix="${esc(from)}" data-to="${esc(to)}" ${allowed ? "checked" : ""} aria-label="${esc(from)} may send to ${esc(to)}"></td>`;
}

function render() {
  if (!host || !rights) return;
  const groups = rights.groups;

  const identities = rights.identities.map((i) => `
    <tr data-identity="${esc(i.clientId)}">
      <td><code>${esc(i.clientId)}</code></td>
      <td><input class="mono narrow" data-field="name" value="${esc(i.name)}" aria-label="Node name of ${esc(i.clientId)}"></td>
      <td><input class="mono" data-field="groups" value="${esc(i.groups.join(", "))}" list="groups-list" aria-label="Groups of ${esc(i.clientId)}, separated by commas"></td>
      <td><input data-field="description" value="${esc(i.description)}" aria-label="Description of ${esc(i.clientId)}"></td>
      <td><label class="check"><input type="checkbox" data-field="enabled" ${i.enabled ? "checked" : ""}> enabled</label></td>
      <td>${i.connected ? `<span class="chip ok">connected</span>` : `<span class="chip">offline</span>`}</td>
      <td class="actions-cell">
        <button class="small" data-save-identity>Save</button>
        <button class="small ghost" data-reconnect ${i.connected ? "" : "disabled"} title="Drop its control connection: its host connects again, under its current groups">Reconnect</button>
        <button class="small ghost danger" data-remove-identity>Remove</button>
      </td>
    </tr>`).join("");

  // What a host proposes for itself is untrusted text: shown escaped, and the administrator decides every field.
  const nameFrom = (proposed) => proposed.toLowerCase().replace(/[^a-z0-9-]+/g, "-").replace(/^-+|-+$/g, "").slice(0, 63);
  const pending = (rights.pending || []).map((p) => `
    <tr data-pending="${esc(p.subject)}">
      <td><code>${esc(p.subject)}</code>${Object.entries(p.labels || {}).map(([k, v]) => `<div class="hint">${esc(k)}: ${esc(v)}</div>`).join("")}</td>
      <td>${esc(p.proposedName)}<div class="hint">${esc(p.description)}</div></td>
      <td class="hint">${p.bricks.length ? p.bricks.map(esc).join(", ") : "no brick"}</td>
      <td><input class="mono narrow" data-field="name" value="${esc(nameFrom(p.proposedName || p.subject))}" aria-label="Node name for ${esc(p.subject)}"></td>
      <td><input class="mono" data-field="groups" placeholder="groups, separated by commas" list="groups-list" aria-label="Groups for ${esc(p.subject)}, separated by commas"></td>
      <td class="actions-cell">
        <button class="small primary" data-accept>Accept</button>
        <button class="small ghost" data-ignore title="Set it aside for 24 hours: it is not shown again until then">Ignore for 24 h</button>
      </td>
    </tr>`).join("");

  host.innerHTML = `
    <div class="view-head"><h1>Access</h1>
      <p class="hint">Who may send what to whom. Every node authenticates with an identity provider the coordinator trusts, which says who it is; the coordinator decides its name and its groups, never the token or the node.</p></div>

    <section class="card">
      <h2>Identity provider</h2>
      <p class="hint">An external OpenID Connect provider, trusted for the <code>${esc(rights.idp.grant)}</code> tokens it signs for this coordinator's audience. It holds the clients and their secrets; a token names its client (<code>sub</code>) and nothing more.</p>
      <table class="kv-table">
        <tr><th>issuer</th><td><code>${esc(rights.idp.issuer)}</code></td></tr>
        <tr><th>audience</th><td><code>${esc(rights.idp.audience)}</code></td></tr>
      </table>
      <p class="hint">A node host presents its token on the control channel, and gives its pipeline node a token provider (<code>NodeClientOptions.AccessTokenProvider</code>) for the data hub.</p>
    </section>

    ${pending ? `<section class="card">
      <h2>Waiting for approval <span class="hint">${rights.pending.length}</span></h2>
      <p class="hint">A host presented a valid token that no identity knows. Its name, description and bricks are what it <i>proposes</i>: you decide its node name and its groups. Nothing runs on it, and it receives no group, until you accept it.</p>
      <table><tr><th>client</th><th>proposes</th><th>bricks</th><th>node name</th><th>groups</th><th></th></tr>${pending}</table>
    </section>` : ""}

    <section class="card">
      <div class="card-head"><h2>Identities <span class="hint">who</span></h2><button data-add>${adding ? "Cancel" : "Add an identity"}</button></div>
      ${adding ? `<div class="row add-identity">
        <input id="a-id" class="mono" placeholder="client id"><input id="a-name" class="mono" placeholder="node name (the client id by default)">
        <input id="a-groups" class="mono" placeholder="groups, separated by commas" list="groups-list"><input id="a-description" placeholder="description">
        <button class="primary" data-create>Create</button></div>` : ""}
      <datalist id="groups-list">${groups.map((g) => `<option>${esc(g)}</option>`).join("")}</datalist>
      <table><tr><th>client</th><th>node name</th><th>groups</th><th>description</th><th></th><th>state</th><th></th></tr>${identities}</table>
      <p class="hint">An identity is a client the IdP already knows, by its client id: its secret lives in the IdP. A node sends to another only if every one of its groups may send to every group of the receiver, in the matrix below. New groups or a disabled identity take effect when the node connects again or renews its token: reconnect it, then redeploy what runs there. A disabled identity may not announce and holds no group.</p>
    </section>

    <section class="card">
      <div class="card-head"><h2>Flow matrix <span class="hint">to whom</span></h2>
        <span class="actions">${matrixDraft ? `<button class="ghost" data-matrix-reset>Discard</button><button class="primary" data-matrix-save>Apply</button>` : ""}</span></div>
      <p class="hint">Rows send, columns receive. A node sends to another only if every group of the sender may send to every group of the receiver. The hub checks it on every transfer it opens, so a change applies to the next run with no redeployment; every plan is checked against it too.</p>
      <table class="matrix edit"><tr><th>from \\ to</th>${groups.map((g) => `<th>${esc(g)}</th>`).join("")}</tr>
        ${groups.map((g) => `<tr><th>${esc(g)}</th>${groups.map((t) => matrixCell(g, t)).join("")}</tr>`).join("")}</table>
    </section>

    <section class="card"><h2>Audit</h2>
      ${(rights.audit || []).slice(0, 40).map((a) => `<div class="audit"><span class="hint">${esc(new Date(a.at).toLocaleString())}${a.by ? ` · ${esc(a.by)}` : ""}</span> ${esc(a.change)}</div>`).join("") || `<p class="hint">No change yet.</p>`}
    </section>`;
}

function onChange(e) {
  const t = e.target;
  if (t.dataset.matrix !== undefined) {
    if (!matrixDraft) matrixDraft = Object.fromEntries(rights.groups.map((g) => [g, setOf(rights.matrix[g])]));
    const row = (matrixDraft[t.dataset.matrix] ||= new Set());
    t.checked ? row.add(t.dataset.to) : row.delete(t.dataset.to);
    render();
  }
}

async function onClick(e) {
  const b = e.target.closest("button");
  if (!b || b.disabled) return;
  const row = b.closest("[data-identity]");
  const waiting = b.closest("[data-pending]");
  try {
    if (b.dataset.accept !== undefined) {
      const subject = waiting.dataset.pending;
      await api(`/api/rights/pending/${encodeURIComponent(subject)}/accept`, {
        name: waiting.querySelector('[data-field="name"]').value.trim(),
        groups: groupsOf(waiting.querySelector('[data-field="groups"]').value),
      });
      toast(`${subject} accepted: its host joins on its next attempt.`, "info");
    } else if (b.dataset.ignore !== undefined) {
      await api(`/api/rights/pending/${encodeURIComponent(waiting.dataset.pending)}/ignore`, {});
      toast(`${waiting.dataset.pending} set aside for 24 hours.`, "info");
    } else if (b.dataset.add !== undefined) { adding = !adding; render(); }
    else if (b.dataset.create !== undefined) {
      const v = (id) => document.getElementById(id).value.trim();
      await api("/api/rights/identities", { clientId: v("a-id"), name: v("a-name") || null, description: v("a-description"), groups: groupsOf(v("a-groups")) });
      adding = false;
      toast(`${v("a-id")} created: it can obtain tokens now.`, "info");
    } else if (b.dataset.saveIdentity !== undefined) {
      const id = row.dataset.identity;
      await api(`/api/rights/identities/${encodeURIComponent(id)}`, {
        clientId: id,
        name: row.querySelector('[data-field="name"]').value.trim(),
        description: row.querySelector('[data-field="description"]').value,
        groups: groupsOf(row.querySelector('[data-field="groups"]').value),
        enabled: row.querySelector('[data-field="enabled"]').checked,
      }, "PUT");
      toast(`${id} saved.`, "info");
    } else if (b.dataset.reconnect !== undefined) {
      const { disconnected } = await api(`/api/rights/identities/${encodeURIComponent(row.dataset.identity)}/reconnect`, {});
      toast(`${disconnected} control connection(s) dropped: the host reconnects with a new token.`, "info");
    } else if (b.dataset.removeIdentity !== undefined) {
      const id = row.dataset.identity;
      if (!confirm(`Remove ${id}? It obtains no more tokens, and its node is disconnected.`)) return;
      await api(`/api/rights/identities/${encodeURIComponent(id)}`, undefined, "DELETE");
    } else if (b.dataset.matrixSave !== undefined) {
      await api("/api/rights/matrix", { matrix: Object.fromEntries(Object.entries(matrixDraft).map(([g, s]) => [g, [...s]])) }, "PUT");
      matrixDraft = null;
      toast("Flow matrix applied: the hub uses it from the next transfer.", "info");
    } else if (b.dataset.matrixReset !== undefined) { matrixDraft = null; render(); return; }
    await load();
  } catch (err) {
    toast(err.message);
  }
}
