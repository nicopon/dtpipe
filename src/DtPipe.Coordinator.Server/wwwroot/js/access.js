// Access: who may send what to whom. Identities (who: verified by a key this coordinator makes or by the IdP it trusts, a node's
// name and groups, a person's role), their keys, the hosts waiting for approval, the flow matrix between groups (to whom, enforced
// by the hub on every transfer), and the audit of every change.
import { api, esc, nodeColor, store, toast } from "./core.js";

let host = null;
let rights = null;
let matrixDraft = null;      // group -> Set(targets), while edited
let adding = false;
let activeTab = null;        // node, launcher, monitor, person
const openKeys = new Set();  // the identities whose keys are listed
const keysOf = {};           // client id -> its keys as GET …/keys lists them: ids, last characters and dates, never a key

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
  try {
    rights = await api("/api/rights");
    await Promise.all([...openKeys].map(listKeys));
  } catch (e) { toast(e.message); return; }
  render();
}

async function listKeys(id) {
  if (!rights.identities.some((i) => i.clientId === id)) { openKeys.delete(id); return; }
  keysOf[id] = await api(`/api/rights/identities/${encodeURIComponent(id)}/keys`);
}

const setOf = (list) => new Set(list || []);
const ROLES = ["reader", "designer", "admin"];
const KINDS = [
  { id: "node", label: "Nodes", desc: "A node hosts pipeline fragments and exchanges data. A node sends to another only if every one of its groups may send to every group of the receiver in the flow matrix. A change of name or groups, or disabling a node, drops the control connections of its hosts, which reconnect under the new rights. Reconnect drops connections without changing anything." },
  { id: "launcher", label: "Launchers", desc: "A launcher is a scheduler or CI/CD identity: it starts the runs of the pipelines listed on its row and reads their status via <code>/service/runs</code>, nothing else. With no pipeline listed, it can launch none." },
  { id: "monitor", label: "Monitors", desc: "A monitor is a supervisor identity: it reads <code>/health</code> and nothing else, belongs to no group and hosts nothing." },
  { id: "person", label: "Persons", desc: "A person signs in to these pages and the studio with the role on their row (<code>reader</code>, <code>designer</code>, or <code>admin</code>). One verified by the identity provider is identified by its account (an email by default); one verified by a key signs in with that key." },
];
const groupsOf = (text) => text.split(",").map((g) => g.trim()).filter(Boolean);
const when = (date) => (date ? new Date(date).toLocaleString() : "");

// The groups of every runner the inventory knows, as the rights give them: what the server allows a new node to exchange with
// when asked (RightsEndpoints.RunnerEdges). The page shows them; the server decides.
function runnerGroups() {
  const runners = (store.nodes || []).filter((n) => n.role === "Runner").map((n) => n.name);
  return [...new Set(rights.identities.filter((i) => i.kind === "node" && i.enabled && runners.includes(i.name)).flatMap((i) => i.groups))].sort();
}

// The first group named after the node that no identity or matrix entry holds yet: a node accepted alone in a group of its own.
function freeGroup(name) {
  let group = name;
  for (let n = 2; rights.groups.includes(group); n++) group = `${name}-${n}`;
  return group;
}

function runnersCheck(attributes) {
  const groups = runnerGroups();
  return `<label class="check" title="${groups.length ? `Allows its groups and ${esc(groups.join(", "))} to send to each other, in the same change` : "No runner has announced itself yet"}">
    <input type="checkbox" ${attributes} ${groups.length ? "checked" : "disabled"}> exchange with the runners both ways${groups.length ? ` (${esc(groups.join(", "))})` : ""}</label>`;
}

function matrixCell(from, to) {
  const allowed = matrixDraft ? matrixDraft[from]?.has(to) : (rights.matrix[from] || []).includes(to);
  return `<td class="${allowed ? "yes" : "no"}"><input type="checkbox" data-matrix="${esc(from)}" data-to="${esc(to)}" ${allowed ? "checked" : ""} aria-label="${esc(from)} may send to ${esc(to)}"></td>`;
}

function verification(i) {
  if (i.auth !== "key") return `<span class="chip" title="The identity provider verifies it">idp</span>`;
  return `${i.hasKey ? `<span class="chip ok" title="Verified by a key this coordinator made">key</span>` : `<span class="chip warn" title="Verified by a key, and none is live: it cannot sign in or connect">no live key</span>`}
    <button class="small ghost" data-keys>${openKeys.has(i.clientId) ? "Hide keys" : "Keys"}</button>`;
}

function keysRow(i, colspan = 8) {
  const answer = keysOf[i.clientId];
  if (!openKeys.has(i.clientId) || !answer) return "";
  const rows = answer.keys.map((k) => `
    <tr><td class="mono">${esc(k.id)}</td><td class="mono">…${esc(k.hint)}</td><td>${esc(k.label ?? "")}</td><td>${esc(when(k.created))}</td>
      <td>${k.expires ? esc(when(k.expires)) : `<span class="hint">never</span>`}</td>
      <td>${k.revoked ? `<span class="chip">revoked ${esc(when(k.revoked))}</span>` : k.live ? `<span class="chip ok">live</span>` : `<span class="chip warn">expired</span>`}</td>
      <td>${k.revoked ? "" : `<button class="small ghost danger" data-revoke-key="${esc(k.id)}">Revoke</button>`}</td></tr>`).join("");
  return `<tr class="keys-row" data-keys-of="${esc(i.clientId)}"><td colspan="${colspan}">
    <table><tr><th>key</th><th>ends in</th><th>label</th><th>made</th><th>expires</th><th>state</th><th></th></tr>${rows || `<tr><td colspan="7" class="hint">No key yet.</td></tr>`}</table>
    <div class="row"><input data-key-label placeholder="label (optional)" aria-label="Label of the next key of ${esc(i.clientId)}">
      <label class="check">expires <input type="datetime-local" data-key-expires aria-label="When the next key of ${esc(i.clientId)} expires (optional)"></label>
      <button class="small" data-make-key>${answer.hasKey ? "Rotate: make a second key" : "Make a key"}</button></div>
    <p class="hint">A key is shown once, when it is made: the coordinator keeps its hash only. One made while another is live starts a rotation: both open until the old one is revoked, and a third is refused. Revoking a key disconnects the identity's hosts and ends a person's session at the next request.</p>
  </td></tr>`;
}

function renderIdentityTable(kind, items) {
  if (kind === "node") {
    const rows = items.map((i) => `
      <tr data-identity="${esc(i.clientId)}">
        <td><code>${esc(i.clientId)}</code></td>
        <td>${verification(i)}</td>
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
      </tr>${keysRow(i, 8)}`).join("");
    return `<table><tr><th>client</th><th>verified by</th><th>node name</th><th>groups</th><th>description</th><th></th><th>state</th><th></th></tr>
      ${rows || `<tr><td colspan="8" class="hint">No node configured yet.</td></tr>`}</table>`;
  }
  if (kind === "launcher") {
    const rows = items.map((i) => `
      <tr data-identity="${esc(i.clientId)}">
        <td><code>${esc(i.clientId)}</code></td>
        <td>${verification(i)}</td>
        <td><input class="mono narrow" data-field="name" value="${esc(i.name)}" aria-label="Name of ${esc(i.clientId)}"></td>
        <td><input class="mono" data-field="pipelines" value="${esc((i.pipelines ?? []).join(", "))}" placeholder="none: it launches nothing" aria-label="Pipelines ${esc(i.clientId)} may launch, separated by commas"></td>
        <td><input data-field="description" value="${esc(i.description)}" aria-label="Description of ${esc(i.clientId)}"></td>
        <td><label class="check"><input type="checkbox" data-field="enabled" ${i.enabled ? "checked" : ""}> enabled</label></td>
        <td class="actions-cell">
          <button class="small" data-save-identity>Save</button>
          <button class="small ghost danger" data-remove-identity>Remove</button>
        </td>
      </tr>${keysRow(i, 7)}`).join("");
    return `<table><tr><th>client</th><th>verified by</th><th>name</th><th>pipelines it may launch</th><th>description</th><th></th><th></th></tr>
      ${rows || `<tr><td colspan="7" class="hint">No launcher configured yet.</td></tr>`}</table>`;
  }
  if (kind === "monitor") {
    const rows = items.map((i) => `
      <tr data-identity="${esc(i.clientId)}">
        <td><code>${esc(i.clientId)}</code></td>
        <td>${verification(i)}</td>
        <td><input class="mono narrow" data-field="name" value="${esc(i.name)}" aria-label="Name of ${esc(i.clientId)}"></td>
        <td><input data-field="description" value="${esc(i.description)}" aria-label="Description of ${esc(i.clientId)}"></td>
        <td><label class="check"><input type="checkbox" data-field="enabled" ${i.enabled ? "checked" : ""}> enabled</label></td>
        <td class="actions-cell">
          <button class="small" data-save-identity>Save</button>
          <button class="small ghost danger" data-remove-identity>Remove</button>
        </td>
      </tr>${keysRow(i, 6)}`).join("");
    return `<table><tr><th>client</th><th>verified by</th><th>name</th><th>description</th><th></th><th></th></tr>
      ${rows || `<tr><td colspan="6" class="hint">No monitor configured yet.</td></tr>`}</table>`;
  }
  if (kind === "person") {
    const rows = items.map((i) => `
      <tr data-identity="${esc(i.clientId)}">
        <td><code>${esc(i.clientId)}</code>${i.account ? `<div class="hint">${esc(i.account)}</div>` : ""}</td>
        <td>${verification(i)}</td>
        <td><input class="mono narrow" data-field="name" value="${esc(i.name)}" aria-label="Name of ${esc(i.clientId)}"></td>
        <td><select data-field="role" aria-label="Role of ${esc(i.clientId)}">${ROLES.map((r) => `<option ${r === i.role ? "selected" : ""}>${r}</option>`).join("")}</select></td>
        <td><input data-field="description" value="${esc(i.description)}" aria-label="Description of ${esc(i.clientId)}"></td>
        <td><label class="check"><input type="checkbox" data-field="enabled" ${i.enabled ? "checked" : ""}> enabled</label></td>
        <td class="actions-cell">
          <button class="small" data-save-identity>Save</button>
          <button class="small ghost danger" data-remove-identity>Remove</button>
        </td>
      </tr>${keysRow(i, 7)}`).join("");
    return `<table><tr><th>client</th><th>verified by</th><th>name</th><th>role</th><th>description</th><th></th><th></th></tr>
      ${rows || `<tr><td colspan="7" class="hint">No person configured yet.</td></tr>`}</table>`;
  }
  return "";
}

function render() {
  if (!host || !rights) return;
  const groups = rights.groups;

  if (!activeTab) {
    const firstWithItems = KINDS.find((k) => rights.identities.some((i) => i.kind === k.id));
    activeTab = firstWithItems ? firstWithItems.id : "node";
  }
  const currentKind = KINDS.find((k) => k.id === activeTab) || KINDS[0];
  const items = rights.identities.filter((i) => i.kind === currentKind.id);
  const tabs = KINDS.map((k) => {
    const count = rights.identities.filter((i) => i.kind === k.id).length;
    return `<button class="${k.id === currentKind.id ? "active" : ""}" data-kind-tab="${k.id}">${k.label} <span class="hint">${count}</span></button>`;
  }).join("");

  // What a host proposes for itself is untrusted text: shown escaped, and the administrator decides every field.
  const nameFrom = (proposed) => proposed.toLowerCase().replace(/[^a-z0-9-]+/g, "-").replace(/^-+|-+$/g, "").slice(0, 63);
  const pending = (rights.pending || []).map((p) => {
    const name = nameFrom(p.proposedName || p.subject);
    return `
    <tr data-pending="${esc(p.subject)}">
      <td><code>${esc(p.subject)}</code>${Object.entries(p.labels || {}).map(([k, v]) => `<div class="hint">${esc(k)}: ${esc(v)}</div>`).join("")}</td>
      <td>${esc(p.proposedName)}<div class="hint">${esc(p.description)}</div></td>
      <td class="hint">${p.bricks.length ? p.bricks.map(esc).join(", ") : "no brick"}</td>
      <td><input class="mono narrow" data-field="name" value="${esc(name)}" aria-label="Node name for ${esc(p.subject)}"></td>
      <td><input class="mono" data-field="groups" value="${esc(freeGroup(name))}" placeholder="groups, separated by commas" list="groups-list" aria-label="Groups for ${esc(p.subject)}, separated by commas">
        ${runnersCheck(`data-field="runners"`)}</td>
      <td class="actions-cell">
        <button class="small primary" data-accept>Accept</button>
        <button class="small ghost" data-ignore title="Set it aside for 24 hours: it is not shown again until then">Ignore for 24 h</button>
      </td>
    </tr>`;
  }).join("");

  host.innerHTML = `
    <div class="view-head"><h1>Access</h1>
      <p class="hint">Who may send what to whom. Every identity is verified by a key this coordinator makes or by an identity provider it trusts, which says who it is; the coordinator decides its name, its groups and its role, never the token or the node.</p></div>

    ${rights.idp ? `<section class="card">
      <h2>Identity provider</h2>
      <p class="hint">An external OpenID Connect provider, trusted for the <code>${esc(rights.idp.grant)}</code> tokens it signs for this coordinator's audience. It holds the clients and their secrets; a token names its client (<code>sub</code>) and nothing more.</p>
      <table class="kv-table">
        <tr><th>issuer</th><td><code>${esc(rights.idp.issuer)}</code></td></tr>
        <tr><th>audience</th><td><code>${esc(rights.idp.audience)}</code></td></tr>
      </table>
      <p class="hint">A node host presents its token on the control channel, and gives its pipeline node a token provider (<code>NodeClientOptions.AccessTokenProvider</code>) for the data hub.</p>
    </section>` : `<section class="card">
      <h2>Identity provider</h2>
      <p class="hint">None: this coordinator trusts no external provider, and every identity is verified by a key it makes, exchanged for a token of its own.</p>
    </section>`}

    ${pending ? `<section class="card">
      <h2>Waiting for approval <span class="hint">${rights.pending.length}</span></h2>
      <p class="hint">A host presented a valid token of the identity provider that no identity knows. Its name, description and bricks are what it <i>proposes</i>: you decide its node name and its groups, a group of its own by default. Nothing runs on it, and it receives no group, until you accept it.</p>
      <table><tr><th>client</th><th>proposes</th><th>bricks</th><th>node name</th><th>groups</th><th></th></tr>${pending}</table>
    </section>` : ""}

    <section class="card">
      <div class="card-head"><h2>Identities <span class="hint">who</span></h2><button data-add>${adding ? "Cancel" : "Add an identity"}</button></div>
      ${adding ? `<div class="row add-identity">
        <input id="a-id" class="mono" placeholder="client id">
        <select id="a-kind" aria-label="Kind">
          <option value="node" ${currentKind.id === "node" ? "selected" : ""}>node</option>
          <option value="monitor" ${currentKind.id === "monitor" ? "selected" : ""}>monitor (reads /health)</option>
          <option value="launcher" ${currentKind.id === "launcher" ? "selected" : ""}>launcher (starts runs)</option>
          <option value="person" ${currentKind.id === "person" ? "selected" : ""}>person (signs in)</option>
        </select>
        <select id="a-auth" aria-label="Verified by"><option value="key" ${rights.idp ? "" : "selected"}>verified by a key this coordinator makes</option>${rights.idp ? `<option value="idp" selected>verified by the identity provider</option>` : ""}</select>
        <input id="a-name" class="mono" placeholder="name (the client id by default)">
        <span data-for="node"><input id="a-groups" class="mono" placeholder="groups, separated by commas" list="groups-list">${runnersCheck(`id="a-runners"`)}</span>
        <input id="a-pipelines" class="mono" data-for="launcher" placeholder="pipelines it may launch, separated by commas">
        <select id="a-role" data-for="person" aria-label="Role">${ROLES.map((r) => `<option>${r}</option>`).join("")}</select>
        <input id="a-account" class="mono" data-for="person-idp" placeholder="account at the provider (an email by default)">
        <input id="a-description" placeholder="description">
        <button class="primary" data-create>Create</button></div>` : ""}
      <datalist id="groups-list">${groups.map((g) => `<option>${esc(g)}</option>`).join("")}</datalist>
      <div class="kind-tabs">${tabs}</div>
      ${renderIdentityTable(currentKind.id, items)}
      <p class="hint">${currentKind.desc}</p>
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
  shapeForm();
}

// The fields of the form that apply to the kind and the verification chosen; the others are hidden, never sent.
function shapeForm() {
  const kind = document.getElementById("a-kind")?.value;
  if (!kind) return;
  const auth = document.getElementById("a-auth").value;
  for (const el of host.querySelectorAll(".add-identity [data-for]"))
    el.hidden = el.dataset.for !== kind && !(el.dataset.for === "person-idp" && kind === "person" && auth === "idp");
}

function onChange(e) {
  const t = e.target;
  if (t.dataset.matrix !== undefined) {
    if (!matrixDraft) matrixDraft = Object.fromEntries(rights.groups.map((g) => [g, setOf(rights.matrix[g])]));
    const row = (matrixDraft[t.dataset.matrix] ||= new Set());
    t.checked ? row.add(t.dataset.to) : row.delete(t.dataset.to);
    render();
  } else if (t.id === "a-kind" || t.id === "a-auth") shapeForm();
  else if (t.id === "a-id" || t.id === "a-name") {
    // A node is proposed a group of its own, named after it, until someone types another.
    const groups = document.getElementById("a-groups");
    const name = document.getElementById("a-name").value.trim() || document.getElementById("a-id").value.trim();
    if (groups && (!groups.value || groups.dataset.proposed === groups.value) && name) groups.value = groups.dataset.proposed = freeGroup(name);
  }
}

// How an identity of each kind uses its key, with the key nowhere in it: the key goes into a variable or a store the person
// fills, and every command names that variable (root CLAUDE.md, "No page offers to copy a command that holds a key").
function usageOf(identity) {
  const origin = location.origin;
  if (identity.kind === "node") return [
    "Put the key in the node's environment, never on its command line:",
    "  export DTPIPE_NODE_KEY=…   (paste the key after the =)",
    ...(rights.idp ? [`  export DtPipeNode__Oidc__Authority=${origin}   (the key is this coordinator's, not the provider's)`] : []),
    "Then start the node:",
    `  dtpipe node --name ${identity.clientId} --coordinator ${origin} --client-secret '\${{DTPIPE_NODE_KEY}}'`,
    "Or keep the key in the keyring and give --client-secret keyring://<alias>; a host of its own reads it as Oidc:ClientSecret.",
  ].join("\n");
  if (identity.kind === "person") return [
    `Sign in at ${origin}/account/login with it, or open the studio with the key in a variable:`,
    "  export DTPIPE_KEY=…   (paste the key after the =)",
    `  dtpipe studio --coordinator ${origin} --key '\${{DTPIPE_KEY}}'`,
  ].join("\n");
  return [
    `A ${identity.kind} exchanges it for a token: POST ${origin}/connect/token with grant_type=client_credentials,`,
    `client_id=${identity.clientId} and the key as client_secret, read from its environment or a secret store, never from a command line.`,
  ].join("\n");
}

// The key a call made is shown here once, in the one element the Copy button copies, and nowhere else: not in a command, not in
// storage, not in the address. Closing the window empties that element and removes the window.
function showKey(identity, made) {
  const dialog = document.createElement("dialog");
  dialog.className = "key-window";
  dialog.innerHTML = `
    <h2>Key ${esc(made.id)} of <code>${esc(identity.clientId)}</code></h2>
    <p class="hint">Shown this once: the coordinator keeps its hash only, and closing this window forgets it here. It ends in …${esc(made.hint)}.</p>
    <code class="mono key-secret" data-key-secret></code>
    <div class="row"><button class="primary" data-copy-key>Copy the key</button></div>
    <h3>How to use it</h3>
    <pre class="usage">${esc(usageOf(identity))}</pre>
    <div class="row"><button data-close-key>Close</button></div>`;
  const secret = dialog.querySelector("[data-key-secret]");
  secret.textContent = made.key;
  const close = () => { secret.textContent = ""; dialog.remove(); };
  dialog.addEventListener("close", close);
  dialog.querySelector("[data-close-key]").addEventListener("click", () => dialog.close());
  dialog.querySelector("[data-copy-key]").addEventListener("click", () =>
    navigator.clipboard.writeText(secret.textContent).then(() => toast("Key copied.", "info"), () => toast("The browser refused to copy: select the key and copy it.")));
  document.body.append(dialog);
  dialog.showModal();
}

async function makeKey(identity, label, expires) {
  showKey(identity, await api(`/api/rights/identities/${encodeURIComponent(identity.clientId)}/keys`, { label: label || null, expires: expires || null }));
}

async function onClick(e) {
  const b = e.target.closest("button");
  if (!b || b.disabled) return;
  const row = b.closest("[data-identity]");
  const keys = b.closest("[data-keys-of]");
  const waiting = b.closest("[data-pending]");
  try {
    if (b.dataset.kindTab !== undefined) {
      activeTab = b.dataset.kindTab;
      render();
      return;
    } else if (b.dataset.accept !== undefined) {
      const subject = waiting.dataset.pending;
      await api(`/api/rights/pending/${encodeURIComponent(subject)}/accept`, {
        name: waiting.querySelector('[data-field="name"]').value.trim(),
        groups: groupsOf(waiting.querySelector('[data-field="groups"]').value),
        exchangeWithRunners: waiting.querySelector('[data-field="runners"]').checked,
      });
      activeTab = "node";
      toast(`${subject} accepted: its host joins on its next attempt.`, "info");
    } else if (b.dataset.ignore !== undefined) {
      await api(`/api/rights/pending/${encodeURIComponent(waiting.dataset.pending)}/ignore`, {});
      toast(`${waiting.dataset.pending} set aside for 24 hours.`, "info");
    } else if (b.dataset.add !== undefined) { adding = !adding; render(); return; }
    else if (b.dataset.create !== undefined) {
      const v = (id) => document.getElementById(id).value.trim();
      const identity = { clientId: v("a-id"), name: v("a-name") || null, description: v("a-description"), kind: v("a-kind"), auth: v("a-auth") };
      if (identity.kind === "node") Object.assign(identity, { groups: groupsOf(v("a-groups")), exchangeWithRunners: document.getElementById("a-runners").checked });
      if (identity.kind === "launcher") identity.pipelines = groupsOf(v("a-pipelines"));
      if (identity.kind === "person") Object.assign(identity, { role: v("a-role"), account: identity.auth === "idp" ? v("a-account") : null });
      await api("/api/rights/identities", identity);
      activeTab = identity.kind;
      adding = false;
      if (identity.auth === "key") {
        openKeys.add(identity.clientId);
        await makeKey(identity);
      } else toast(`${identity.clientId} created: the identity provider verifies it.`, "info");
    } else if (b.dataset.keys !== undefined) {
      const id = row.dataset.identity;
      if (openKeys.delete(id)) { render(); return; }
      openKeys.add(id);
      await listKeys(id);
      render();
      return;
    } else if (b.dataset.makeKey !== undefined) {
      const id = keys.dataset.keysOf;
      const expires = keys.querySelector("[data-key-expires]").value;
      await makeKey(rights.identities.find((i) => i.clientId === id), keys.querySelector("[data-key-label]").value.trim(), expires ? new Date(expires).toISOString() : null);
    } else if (b.dataset.revokeKey !== undefined) {
      const id = keys.dataset.keysOf;
      if (!confirm(`Revoke key ${b.dataset.revokeKey} of ${id}? It opens nothing more, its hosts are disconnected and a session it opened ends.`)) return;
      await api(`/api/rights/identities/${encodeURIComponent(id)}/keys/${encodeURIComponent(b.dataset.revokeKey)}/revoke`, {});
      toast(`Key ${b.dataset.revokeKey} of ${id} revoked.`, "info");
    } else if (b.dataset.saveIdentity !== undefined) {
      const id = row.dataset.identity;
      await api(`/api/rights/identities/${encodeURIComponent(id)}`, {
        clientId: id,
        name: row.querySelector('[data-field="name"]').value.trim(),
        description: row.querySelector('[data-field="description"]').value,
        groups: groupsOf(row.querySelector('[data-field="groups"]')?.value ?? ""),
        pipelines: groupsOf(row.querySelector('[data-field="pipelines"]')?.value ?? ""),
        role: row.querySelector('[data-field="role"]')?.value ?? null,
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
      openKeys.delete(id);
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
