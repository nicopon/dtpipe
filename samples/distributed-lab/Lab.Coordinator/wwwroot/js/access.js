// Access: who may send what to whom. Identities of the coordinator's own IDP (who, and in which
// group), the flow matrix between groups (to whom, enforced by the hub on every transfer), the
// policies of source bricks (what, enforced on every plan), and the audit of every change.
import { api, esc, nodeColor, store, toast } from "./core.js";

let host = null;
let rights = null;
let matrixDraft = null;      // group -> Set(targets), while edited
let policyDraft = {};        // brick key -> Set(groups) | null, while edited
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
  const pending = matrixDraft || Object.keys(policyDraft).length;
  return !pending || confirm("Unsaved rights changes will be lost. Leave anyway?");
}

async function load() {
  try { rights = await api("/api/rights"); } catch (e) { toast(e.message); return; }
  render();
}

const setOf = (list) => new Set(list || []);

function matrixCell(from, to) {
  const allowed = matrixDraft ? matrixDraft[from]?.has(to) : (rights.matrix[from] || []).includes(to);
  return `<td class="${allowed ? "yes" : "no"}"><input type="checkbox" data-matrix="${esc(from)}" data-to="${esc(to)}" ${allowed ? "checked" : ""} aria-label="${esc(from)} may send to ${esc(to)}"></td>`;
}

function render() {
  if (!host || !rights) return;
  const groups = rights.groups;
  const nodes = store.nodes.map((n) => n.name);

  const identities = rights.identities.map((i) => `
    <tr data-identity="${esc(i.clientId)}">
      <td><code>${esc(i.clientId)}</code><div class="hint">${esc(i.displayName)}</div></td>
      <td><input class="mono narrow" data-field="group" value="${esc(i.group)}" list="groups-list"></td>
      <td><select data-field="node"><option value="">— none —</option>${nodes.map((n) => `<option ${n === i.node ? "selected" : ""}>${esc(n)}</option>`).join("")}</select></td>
      <td><label class="check"><input type="checkbox" data-field="enabled" ${i.enabled ? "checked" : ""}> enabled</label></td>
      <td>${i.connected ? `<span class="chip ok">connected</span>` : `<span class="chip">offline</span>`}</td>
      <td class="actions-cell">
        <button class="small" data-save-identity>Save</button>
        <button class="small ghost" data-reconnect ${i.connected ? "" : "disabled"} title="Drop its control connection: its host reconnects with a new token, carrying its current group">Reconnect</button>
        <button class="small ghost danger" data-remove-identity>Remove</button>
      </td>
    </tr>`).join("");

  const sources = store.bricks.filter((b) => b.kind === "Source");
  const policies = sources.map((b) => {
    const saved = rights.brickPolicies[b.key];
    const draft = b.key in policyDraft ? policyDraft[b.key] : saved ? setOf(saved) : null;
    const nodeGroup = store.nodes.find((n) => n.name === b.node)?.group;
    return `<tr data-brick="${esc(b.key)}">
      <td><span style="color:${nodeColor(b.node)}">${esc(b.node)}</span> · <b>${esc(b.title)}</b><div class="hint mono">${esc(b.key)}</div></td>
      <td><label class="check"><input type="checkbox" data-any ${draft ? "" : "checked"}> wherever the matrix allows</label></td>
      <td>${draft ? groups.filter((g) => g !== nodeGroup).map((g) => `<label class="check"><input type="checkbox" data-policy="${esc(g)}" ${draft.has(g) ? "checked" : ""}> ${esc(g)}</label>`).join(" ") : `<span class="hint">no restriction</span>`}</td>
      <td class="actions-cell">${b.key in policyDraft ? `<button class="small" data-save-policy>Save</button>` : ""}</td>
    </tr>`;
  }).join("");

  host.innerHTML = `
    <div class="view-head"><h1>Access</h1>
      <p class="hint">Who may send what to whom. Every node authenticates with the coordinator's own identity provider; the group it belongs to is the one its token carries, never the one it declares.</p></div>

    <section class="card">
      <h2>Identity provider</h2>
      <p class="hint">Embedded in the coordinator, after TransportR's SimpleIdp: OpenIddict, the <code>${esc(rights.idp.grant)}</code> flow, and the node's group as the <code>${esc(rights.idp.scope)}</code> scope TransportR reads.</p>
      <table class="kv-table">
        <tr><th>issuer</th><td><code>${esc(rights.idp.issuer)}</code></td></tr>
        <tr><th>token endpoint</th><td><code>${esc(rights.idp.tokenEndpoint)}</code></td></tr>
        <tr><th>token lifetime</th><td>${esc(rights.idp.tokenLifetime)}</td></tr>
      </table>
      <p class="hint">A node host presents its token on the control channel, and gives each pipeline node a hub URL that carries it (<code>/t/&lt;token&gt;/…</code>): <code>PipelineNode</code> has no credential option of its own.</p>
    </section>

    <section class="card">
      <div class="card-head"><h2>Identities <span class="hint">who</span></h2><button data-add>${adding ? "Cancel" : "Add an identity"}</button></div>
      ${adding ? `<div class="row add-identity">
        <input id="a-id" class="mono" placeholder="client id"><input id="a-name" placeholder="display name">
        <input id="a-secret" type="password" placeholder="secret (12+ characters)"><input id="a-group" class="mono" placeholder="group" list="groups-list">
        <select id="a-node"><option value="">— no node —</option>${nodes.map((n) => `<option>${esc(n)}</option>`).join("")}</select>
        <button class="primary" data-create>Create</button></div>` : ""}
      <datalist id="groups-list">${groups.map((g) => `<option>${esc(g)}</option>`).join("")}</datalist>
      <table><tr><th>client</th><th>group</th><th>may announce node</th><th></th><th>state</th><th></th></tr>${identities}</table>
      <p class="hint">A new group takes effect with the node's next token: reconnect it, then redeploy what runs there. A disabled identity obtains no token and may not announce.</p>
    </section>

    <section class="card">
      <div class="card-head"><h2>Flow matrix <span class="hint">to whom</span></h2>
        <span class="actions">${matrixDraft ? `<button class="ghost" data-matrix-reset>Discard</button><button class="primary" data-matrix-save>Apply</button>` : ""}</span></div>
      <p class="hint">Rows send, columns receive. The hub checks it on every transfer it opens, so a change applies to the next run with no redeployment; every plan is checked against it too.</p>
      <table class="matrix edit"><tr><th>from \\ to</th>${groups.map((g) => `<th>${esc(g)}</th>`).join("")}</tr>
        ${groups.map((g) => `<tr><th>${esc(g)}</th>${groups.map((t) => matrixCell(g, t)).join("")}</tr>`).join("")}</table>
    </section>

    <section class="card">
      <h2>Source bricks <span class="hint">what</span></h2>
      <p class="hint">The groups a brick's rows may be sent to as they leave their node. Checked on every plan: the hub sees groups, not bricks.</p>
      <table>${policies || `<tr><td class="hint">No source brick offered.</td></tr>`}</table>
    </section>

    <section class="card"><h2>Audit</h2>
      ${(rights.audit || []).slice(0, 40).map((a) => `<div class="audit"><span class="hint">${esc(new Date(a.at).toLocaleString())}</span> ${esc(a.change)}</div>`).join("") || `<p class="hint">No change yet.</p>`}
    </section>`;
}

function onChange(e) {
  const t = e.target;
  if (t.dataset.matrix !== undefined) {
    if (!matrixDraft) matrixDraft = Object.fromEntries(rights.groups.map((g) => [g, setOf(rights.matrix[g])]));
    const row = (matrixDraft[t.dataset.matrix] ||= new Set());
    t.checked ? row.add(t.dataset.to) : row.delete(t.dataset.to);
    render();
  } else if (t.dataset.any !== undefined || t.dataset.policy !== undefined) {
    const key = t.closest("[data-brick]").dataset.brick;
    const current = key in policyDraft ? policyDraft[key] : rights.brickPolicies[key] ? setOf(rights.brickPolicies[key]) : null;
    if (t.dataset.any !== undefined) policyDraft[key] = t.checked ? null : new Set(current || []);
    else {
      const next = new Set(current || []);
      t.checked ? next.add(t.dataset.policy) : next.delete(t.dataset.policy);
      policyDraft[key] = next;
    }
    render();
  }
}

async function onClick(e) {
  const b = e.target.closest("button");
  if (!b || b.disabled) return;
  const row = b.closest("[data-identity]");
  try {
    if (b.dataset.add !== undefined) { adding = !adding; render(); }
    else if (b.dataset.create !== undefined) {
      const v = (id) => document.getElementById(id).value.trim();
      await api("/api/rights/identities", { clientId: v("a-id"), displayName: v("a-name") || null, secret: v("a-secret"), group: v("a-group"), node: v("a-node") || null });
      adding = false;
      toast(`${v("a-id")} created: it can obtain tokens now.`, "info");
    } else if (b.dataset.saveIdentity !== undefined) {
      const id = row.dataset.identity;
      const current = rights.identities.find((i) => i.clientId === id);
      await api(`/api/rights/identities/${encodeURIComponent(id)}`, {
        clientId: id, displayName: current.displayName,
        group: row.querySelector('[data-field="group"]').value.trim(),
        node: row.querySelector('[data-field="node"]').value || null,
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
    else if (b.dataset.savePolicy !== undefined) {
      const key = b.closest("[data-brick]").dataset.brick;
      const [node, id] = key.split("/");
      const draft = policyDraft[key];
      await api(`/api/rights/bricks/${encodeURIComponent(node)}/${encodeURIComponent(id)}`, { groups: draft ? [...draft] : null }, "PUT");
      delete policyDraft[key];
    }
    await load();
  } catch (err) {
    toast(err.message);
  }
}
