// @ts-check
// Connection and workspace dialogs (Agent, Coordinator, Workspace) built on a shared busy-action and connection controller.

import { api, el, fill } from "./dom.js";
import { openDialog } from "./dialog.js";
import { createRecentList } from "./recent-list.js";

const AGENT_PROVIDERS = {
  ollama: { label: "Ollama", url: "http://localhost:11434", provider: "ollama" },
  compatible: { label: "OpenAI-compatible server", url: "", provider: "openai" },
};

const sameUrl = (a, b) => String(a || "").trim().toLowerCase().replace(/\/+$/, "") === String(b || "").trim().toLowerCase().replace(/\/+$/, "");

/**
 * Where a connection's key is, in one line: the model's and the coordinator's dialogs say it the same way.
 * @param {string | undefined} kept `stored`, `flag` or `session` (what the server reads back), absent when none
 * @param {boolean} missing the connection was remembered and its key is not kept
 */
function keyState(kept, missing) {
  return kept === "stored" ? "🔑 In the keyring. Leave empty to keep it, or type another."
    : kept === "flag" ? "🔑 Given on the command line: this session only."
    : kept === "session" ? "🔑 This session only. Type it again to remember it."
    : missing ? "🔑 Missing: type it again."
    : "🔑 Kept per address.";
}

/** What shapes where a connection lives, empty when nothing does: set by the command line, no keyring, or a settings warning. @param {any} connection */
function connectionNote(connection) {
  return [
    connection?.source === "flag" ? "Set by the command line: changes last this session unless Remember is ticked." : null,
    connection?.keyring === false ? "No system keyring: the key lasts this session only." : null,
    connection?.settingsWarning || null,
  ].filter(Boolean).join(" ");
}

function providerChoice(connection) {
  return connection?.provider === "openai" ? "compatible" : "ollama";
}

/**
 * Runs an async dialog action under busy/closed guards, updating the active button text and displaying errors in the banner.
 * @param {{ isBusy: () => boolean, isClosed: () => boolean, setBusy: (v: boolean) => void, clearError: () => void, fail: (msg: string) => void }} ctx
 * @param {() => Promise<any>} action
 * @param {HTMLButtonElement | null} [button]
 * @param {string} [busyText]
 * @param {(err: any) => void} [onError]
 */
async function runBusyAction(ctx, action, button = null, busyText = "", onError = undefined) {
  if (ctx.isBusy() || ctx.isClosed()) return;
  ctx.setBusy(true);
  ctx.clearError();
  const idleText = button ? button.textContent : null;
  if (button && busyText) button.textContent = busyText;
  try {
    return await action();
  } catch (err) {
    if (!ctx.isClosed()) {
      ctx.fail(err.message);
      onError?.(err);
    }
    return undefined;
  } finally {
    if (!ctx.isClosed()) {
      if (button && idleText !== null) button.textContent = idleText;
      ctx.setBusy(false);
    }
  }
}

/**
 * Shared connection dialog controller for Agent and Coordinator endpoints.
 * @param {{
 *   dialogId: string, title: string, prefix: string, recentsLabel: string, recentNoun: string,
 *   disconnectTitle: string, keyPlaceholder: string, connection: any,
 *   onConnect: (body: any) => Promise<any>, onDisconnect: () => Promise<void>, onForgetEntry: (entry: any) => Promise<any>,
 *   setup: (shared: {
 *     connection: () => any, key: HTMLInputElement, keyNote: HTMLElement,
 *     clearTest: () => void, setTestMessage: (msg: string) => void, clearError: () => void,
 *     update: () => void, renderNote: () => void, testBtn: HTMLButtonElement
 *   }) => {
 *     fields: any[], advancedFields?: any[], initialFocus: () => void,
 *     isKeyed: () => boolean, currentAddress: () => string, connectionAddress: (c: any) => string | undefined,
 *     entryText: (entry: any) => string, entryDetail: (entry: any) => string,
 *     applyEntry: (entry: any) => void, buildRequest: (remember: boolean) => any,
 *     runTest: (request: any) => Promise<string>
 *   }
 * }} config
 */
function openConnectionDialog(config) {
  const { prefix, onConnect, onDisconnect, onForgetEntry } = config;
  let connection = config.connection;
  let closed = false;

  openDialog(config.dialogId, config.title, ({ body, close }) => {
    const key = /** @type {HTMLInputElement} */ (el("input", {
      id: `${prefix}-key`, type: "password", autocomplete: "new-password", "aria-label": "Key", placeholder: config.keyPlaceholder,
    }));
    const keyNote = el("p", { id: `${prefix}-key-note`, class: "hint" });
    const remember = /** @type {HTMLInputElement} */ (el("input", {
      id: `${prefix}-remember`, type: "checkbox", checked: connection?.source !== "flag", "aria-label": "Remember on this machine",
    }));
    const note = el("p", { id: `${prefix}-note`, class: "hint" });
    const banner = el("p", { id: `${prefix}-error`, class: "bad dialog-error", hidden: true });
    const connect = /** @type {HTMLButtonElement} */ (el("button", { id: `${prefix}-submit`, type: "submit", class: "primary" }, "Connect"));
    const testBtn = /** @type {HTMLButtonElement} */ (el("button", { id: `${prefix}-test`, type: "button" }, "Test"));
    const disconnectBtn = connection?.connected
      ? /** @type {HTMLButtonElement} */ (el("button", {
        id: `${prefix}-disconnect`, type: "button", class: "ghost", title: config.disconnectTitle,
      }, "Disconnect"))
      : null;

    let busy = false;
    let testMessage = "";
    /** @type {ReturnType<typeof createRecentList>} */
    let recents;
    /** @type {ReturnType<typeof config.setup>} */
    let specific;

    const fail = (message) => { banner.hidden = false; banner.textContent = message; };
    const clearError = () => { banner.hidden = true; };
    const settle = (value) => {
      busy = value;
      connect.disabled = value;
      testBtn.disabled = value;
      if (disconnectBtn) disconnectBtn.disabled = value;
      recents.setDisabled(value);
    };
    const renderNote = () => {
      note.textContent = [testMessage, connectionNote(connection)].filter(Boolean).join(" ");
    };
    const update = () => {
      const keyed = specific.isKeyed();
      key.disabled = !keyed;
      key.parentElement.hidden = !keyed;
      const addr = specific.currentAddress();
      const here = sameUrl(addr, specific.connectionAddress(connection));
      const recent = (connection?.history ?? []).find((h) => sameUrl(specific.connectionAddress(h), addr));
      const kept = here ? connection?.key : recent?.keyKept ? "stored" : undefined;
      keyNote.hidden = !keyed;
      keyNote.textContent = keyState(kept, !!(connection?.needsKey && here));
      renderNote();
    };
    const busyCtx = { isBusy: () => busy, isClosed: () => closed, setBusy: settle, clearError, fail };

    specific = config.setup({
      connection: () => connection,
      key,
      keyNote,
      clearTest: () => { testMessage = ""; },
      setTestMessage: (msg) => { testMessage = msg; },
      clearError,
      update,
      renderNote,
      testBtn,
    });

    recents = createRecentList({
      id: `${prefix}-recents`,
      entries: connection?.history ?? [],
      text: specific.entryText,
      detail: specific.entryDetail,
      kept: (entry) => entry.keyKept,
      note: connection?.connected
        ? "Past connections: × forgets one and its key. Disconnect first to forget the current one."
        : "Past connections: × forgets one and its key.",
      empty: connection?.connected
        ? `No other ${config.recentNoun} is remembered. Disconnect first, then click × here to forget the current ${config.recentNoun} and its key.`
        : `No other ${config.recentNoun} is remembered.`,
      question: (entry) => `Forget ${specific.entryText(entry)}${entry.keyKept ? " and its key" : ""}?`,
      pick: (entry) => {
        specific.applyEntry(entry);
        key.value = "";
        testMessage = "";
        clearError();
        update();
      },
      remove: async (entry) => {
        connection = await onForgetEntry(entry);
        if (!closed) update();
        return connection.history ?? [];
      },
      fail,
      isBusy: () => busy,
      setBusy: settle,
      isClosed: () => closed,
      fallbackFocus: specific.initialFocus,
    });

    if (disconnectBtn) {
      disconnectBtn.addEventListener("click", () => runBusyAction(busyCtx, async () => {
        await onDisconnect();
        if (!closed) close();
      }, disconnectBtn));
    }

    testBtn.addEventListener("click", () => runBusyAction(busyCtx, async () => {
      const msg = await specific.runTest(specific.buildRequest(remember.checked));
      if (closed) return;
      testMessage = msg;
      renderNote();
    }, testBtn, "Testing…"));

    const form = el("form", {
      class: "dialog-form",
      onsubmit: (e) => {
        e.preventDefault();
        runBusyAction(busyCtx, async () => {
          const answer = await onConnect(specific.buildRequest(remember.checked));
          if (answer !== false) {
            key.value = "";
            if (!closed) close();
          }
        }, connect, "Connecting…");
      },
    },
      el("div", { class: "dialog-field" }, el("span", { class: "hint" }, config.recentsLabel), recents.element),
      ...specific.fields,
      el("details", { class: "dialog-details" }, el("summary", {}, "Advanced"),
        ...(specific.advancedFields ?? []),
        el("label", {}, remember, " Remember (key goes to the keyring)")),
      note,
      banner,
      el("div", { class: "row dialog-actions" },
        disconnectBtn,
        el("button", { type: "button", class: "ghost", onclick: close }, "Cancel"),
        connect));

    form.addEventListener("invalid", (e) => {
      const details = /** @type {HTMLElement} */ (e.target).closest("details");
      if (details && !details.open) details.open = true;
    }, true);
    fill(body, form);
    update();
    specific.initialFocus();
  }, { onClose: () => { closed = true; } });
}

/**
 * The agent's connection: provider, address, key, model and, under Advanced, the delay and whether to remember it on this
 * machine. The key is typed here and sent once; the page never reads one back, only whether one is kept and where.
 * @param {{ connection: any, onConnect: (body: any) => Promise<any>, onDisconnect: () => Promise<void>,
 *   onForgetEntry: (entry: any) => Promise<any> }} opts
 */
export function openAgentConnectDialog(opts) {
  openConnectionDialog({
    dialogId: "d-agent-dialog",
    title: "Connect a model",
    prefix: "d-agent",
    recentsLabel: "Recent models",
    recentNoun: "model",
    disconnectTitle: "Disconnect for this session. To forget this model and its key, disconnect first, then reopen and click × in Recent models.",
    keyPlaceholder: "API key, if the server asks for one",
    connection: opts.connection,
    onConnect: opts.onConnect,
    onDisconnect: opts.onDisconnect,
    onForgetEntry: opts.onForgetEntry,
    setup: ({ connection, key, keyNote, clearTest, update, testBtn }) => {
      const conn = connection();
      const choice = providerChoice(conn);
      const provider = /** @type {HTMLSelectElement} */ (el("select", { id: "d-agent-provider", "aria-label": "Provider" },
        Object.entries(AGENT_PROVIDERS).map(([value, p]) => el("option", { value, selected: value === choice }, p.label))));
      const url = /** @type {HTMLInputElement} */ (el("input", {
        id: "d-agent-url", type: "text", class: "mono", autocomplete: "off", "aria-label": "Address",
        value: conn?.url || AGENT_PROVIDERS[choice].url, placeholder: "https://models.example.com/v1",
      }));
      const model = /** @type {HTMLSelectElement} */ (el("select", { id: "d-agent-model", "aria-label": "Model" },
        el("option", { value: conn?.model || "" }, conn?.model || "(Test lists the models)")));
      const timeout = /** @type {HTMLInputElement} */ (el("input", {
        id: "d-agent-timeout", type: "number", min: "1", "aria-label": "Seconds with no token before a call fails", value: String(conn?.timeout || 300),
      }));
      const bounds = conn?.numCtxBounds ?? { min: 4096, max: 1048576, default: 32768 };
      const ctxSize = /** @type {HTMLInputElement} */ (el("input", {
        id: "d-agent-numctx", type: "number", min: String(bounds.min), max: String(bounds.max), step: "1", "aria-label": "Context size in tokens",
        value: String(conn?.numCtx || bounds.default),
      }));

      provider.addEventListener("change", () => { clearTest(); url.value = AGENT_PROVIDERS[provider.value].url; update(); });
      url.addEventListener("input", () => { clearTest(); update(); });

      const syncVisibility = () => {
        const isOllama = provider.value === "ollama";
        ctxSize.disabled = !isOllama;
        ctxSize.parentElement.hidden = !isOllama;
      };
      provider.addEventListener("change", syncVisibility);

      return {
        fields: [
          el("div", { class: "dialog-field" }, el("label", { for: "d-agent-provider" }, "Provider"), provider),
          el("div", { class: "dialog-field" }, el("label", { for: "d-agent-url" }, "Address"), url),
          el("div", { class: "dialog-field" }, el("label", { for: "d-agent-key" }, "Key"), key, keyNote),
          el("div", { class: "dialog-field" }, el("label", { for: "d-agent-model" }, "Model"), el("div", { class: "row" }, model, testBtn)),
        ],
        advancedFields: [
          el("div", { class: "dialog-field" }, el("label", { for: "d-agent-timeout" }, "Seconds with no token before a call fails"), timeout),
          el("div", { class: "dialog-field" }, el("label", { for: "d-agent-numctx" }, "Context size, in tokens"), ctxSize,
            el("p", { class: "hint" }, `The model's window (Ollama's num_ctx), from ${bounds.min} to ${bounds.max}; ${bounds.default} when left as it is.`)),
        ],
        initialFocus: () => { syncVisibility(); url.focus(); },
        isKeyed: () => { syncVisibility(); return provider.value !== "ollama"; },
        currentAddress: () => url.value,
        connectionAddress: (c) => c?.url,
        entryText: (entry) => entry.url,
        entryDetail: (entry) => [AGENT_PROVIDERS[providerChoice(entry)].label, entry.model].filter(Boolean).join(" · "),
        applyEntry: (entry) => {
          provider.value = providerChoice(entry);
          url.value = entry.url;
          fill(model, el("option", { value: entry.model || "" }, entry.model || "(Test lists the models)"));
          timeout.value = String(entry.timeout || 300);
          ctxSize.value = String(entry.numCtx || bounds.default);
          syncVisibility();
        },
        buildRequest: (remember) => ({
          provider: AGENT_PROVIDERS[provider.value].provider,
          url: url.value.trim(),
          model: model.value || undefined,
          timeout: Number(timeout.value) || undefined,
          numCtx: provider.value === "ollama" && ctxSize.value.trim() !== "" ? Number(ctxSize.value) : undefined,
          key: provider.value !== "ollama" && key.value ? key.value : undefined,
          remember,
        }),
        runTest: async (req) => {
          const { models } = await api("/api/agent/connection/test", req);
          const wanted = model.value;
          fill(model, models.length
            ? models.map((name) => el("option", { value: name, selected: name === wanted }, name))
            : el("option", { value: "" }, "(No models listed)"));
          return `${models.length} model${models.length === 1 ? "" : "s"} listed.`;
        },
      };
    },
  });
}

/**
 * The coordinator's connection: its address, how the person signs in (through the browser, or with a key the coordinator made)
 * and, under Advanced, whether to remember it on this machine. The key is typed here and sent once; the page never reads one
 * back, only whether one is kept and where.
 * @param {{ connection: any, onConnect: (body: any) => Promise<any>, onDisconnect: () => Promise<void>,
 *   onForgetEntry: (entry: any) => Promise<any> }} opts
 */
export function openCoordinatorConnectDialog(opts) {
  openConnectionDialog({
    dialogId: "d-coordinator-dialog",
    title: "Connect a coordinator",
    prefix: "d-coordinator",
    recentsLabel: "Recent coordinators",
    recentNoun: "coordinator",
    disconnectTitle: "Disconnect for this session. To forget this coordinator and its key, disconnect first, then reopen and click × in Recent coordinators.",
    keyPlaceholder: "dtp_…",
    connection: opts.connection,
    onConnect: opts.onConnect,
    onDisconnect: opts.onDisconnect,
    onForgetEntry: opts.onForgetEntry,
    setup: ({ connection, key, keyNote, clearTest, update, testBtn }) => {
      const conn = connection();
      const origin = /** @type {HTMLInputElement} */ (el("input", {
        id: "d-coordinator-origin", type: "text", class: "mono", autocomplete: "off", "aria-label": "Address",
        value: conn?.origin || "", placeholder: "https://coordinator.example.com",
      }));
      const auth = /** @type {HTMLSelectElement} */ (el("select", { id: "d-coordinator-auth", "aria-label": "Sign in" },
        el("option", { value: "browser", selected: conn?.auth !== "key" }, "Through the browser"),
        el("option", { value: "key", selected: conn?.auth === "key" }, "With a key the coordinator made for me")));

      auth.addEventListener("change", () => { clearTest(); update(); });
      origin.addEventListener("input", () => { clearTest(); update(); });

      return {
        fields: [
          el("div", { class: "dialog-field" }, el("label", { for: "d-coordinator-origin" }, "Address"), el("div", { class: "row" }, origin, testBtn)),
          el("div", { class: "dialog-field" }, el("label", { for: "d-coordinator-auth" }, "Sign in"), auth),
          el("div", { class: "dialog-field" }, el("label", { for: "d-coordinator-key" }, "Key"), key, keyNote),
        ],
        initialFocus: () => origin.focus(),
        isKeyed: () => auth.value === "key",
        currentAddress: () => origin.value,
        connectionAddress: (c) => c?.origin,
        entryText: (entry) => entry.origin,
        entryDetail: (entry) => (entry.auth === "key" ? "with a key" : "through the browser"),
        applyEntry: (entry) => {
          origin.value = entry.origin;
          auth.value = entry.auth === "key" ? "key" : "browser";
        },
        buildRequest: (remember) => ({
          origin: origin.value.trim(),
          auth: auth.value,
          key: auth.value === "key" && key.value ? key.value : undefined,
          remember,
        }),
        runTest: async (req) => {
          const answer = await api("/api/coordinator/connection/test", req);
          return answer.signedInAs ? `The coordinator answers, and the key signs in as ${answer.signedInAs}.` : "The coordinator answers.";
        },
      };
    },
  });
}

/**
 * Opens another folder: a path typed or one of the recent ones, tested on demand. A folder holding unsaved changes is refused
 * by the server, naming the cause; the dialog then offers to save them or to discard them, and does neither by itself.
 * The recent folders are listed with the one the studio started on, the folder in place left out: a row fills the path, and
 * `onForgetRecent(path)` removes a recent folder from the list (the start folder is never in it, and a folder gone from the disk
 * is removed the same way).
 * @param {{ workspace: { root: string, start: string, recents: string[], currentRemembered?: boolean, canRemember: boolean, settingsWarning?: string | null },
 *   onTest: (path: string) => Promise<{ root: string, jobs: number }>, unsaved: () => boolean,
 *   onOpen: (request: { path: string, remember: boolean }, discard: boolean, isClosed: () => boolean) => Promise<void>, onSave: () => Promise<boolean>,
 *   onForgetRecent: (path: string) => Promise<{ recents: string[], currentRemembered?: boolean, warning?: string | null }> }} opts
 */
export function openWorkspaceDialog(opts) {
  const { workspace, onTest, unsaved, onOpen, onSave, onForgetRecent } = opts;
  let closed = false;
  openDialog("d-workspace-dialog", "Open a folder", ({ body, close }) => {
    const pathInput = /** @type {HTMLInputElement} */ (el("input", {
      id: "d-workspace-path", type: "text", class: "mono", autocomplete: "off", "aria-label": "Folder", value: workspace.root, placeholder: "/path/to/folder",
    }));
    const remember = /** @type {HTMLInputElement} */ (el("input", { id: "d-workspace-remember", type: "checkbox", checked: workspace.canRemember, disabled: !workspace.canRemember, "aria-label": "Remember among the recent folders" }));
    const note = el("p", { id: "d-workspace-note", class: "hint" });
    const banner = el("p", { id: "d-workspace-error", class: "bad dialog-error", hidden: true });
    const choices = el("div", { id: "d-workspace-unsaved", class: "row wrap dialog-unsaved-actions", hidden: true });
    const submit = /** @type {HTMLButtonElement} */ (el("button", { id: "d-workspace-submit", type: "submit", class: "primary" }, "Open"));
    const test = /** @type {HTMLButtonElement} */ (el("button", { id: "d-workspace-test", type: "button" }, "Test"));
    const forgetCurrentBtn = /** @type {HTMLButtonElement} */ (el("button", {
      id: "d-workspace-forget-current", type: "button", class: "small ghost",
      title: "Remove the current folder from recent folders without changing folder",
      "aria-label": "Remove current folder from recent folders",
    }, "Remove from recents"));
    let currentRemembered = !!workspace.currentRemembered;
    const currentRecentRow = el("div", { id: "d-workspace-current-recent", class: "row wrap", hidden: !currentRemembered },
      el("span", { class: "hint" }, "Current folder is remembered."),
      forgetCurrentBtn);
    let busy = false;
    let testMessage = "";

    const renderNote = () => {
      note.textContent = [testMessage, workspace.settingsWarning || ""].filter(Boolean).join(" ");
    };
    const request = () => ({ path: pathInput.value.trim(), remember: remember.checked });
    const fail = (message) => { banner.hidden = false; banner.textContent = message; };
    const clearError = () => { banner.hidden = true; };

    // The folder the studio started on is offered beside the recent ones, never written among them.
    let remembered = workspace.recents;
    const known = () => [...new Set([...remembered, workspace.start])].filter((p) => p && p !== workspace.root);
    let saveBtn = /** @type {HTMLButtonElement | null} */ (null);
    let discardBtn = /** @type {HTMLButtonElement | null} */ (null);
    let recents;
    const settle = (value) => {
      busy = value;
      submit.disabled = value;
      test.disabled = value;
      if (saveBtn) saveBtn.disabled = value;
      if (discardBtn) discardBtn.disabled = value;
      forgetCurrentBtn.disabled = value;
      recents?.setDisabled(value);
    };
    const busyCtx = { isBusy: () => busy, isClosed: () => closed, setBusy: settle, clearError, fail };

    const applyForgetAnswer = (answer) => {
      if (answer?.warning && !closed) fail(answer.warning);
      remembered = answer.recents;
      currentRemembered = !!answer.currentRemembered;
      currentRecentRow.hidden = !currentRemembered;
    };

    recents = createRecentList({
      id: "d-workspace-recents",
      entries: known(),
      text: (path) => path,
      removable: (path) => remembered.includes(path),
      note: "Past folders: × forgets one.",
      empty: "No other folder is remembered.",
      question: (path) => `Forget ${path}? The folder is untouched.`,
      pick: (path) => { pathInput.value = path; clearError(); testMessage = ""; renderNote(); submit.focus(); },
      remove: async (path) => {
        applyForgetAnswer(await onForgetRecent(path));
        return known();
      },
      fail,
      isBusy: () => busy,
      setBusy: settle,
      isClosed: () => closed,
      fallbackFocus: () => pathInput.focus(),
    });

    forgetCurrentBtn.addEventListener("click", async () => {
      if (busy || closed) return;
      if (!confirm(`Forget ${workspace.root}? The folder stays open and untouched.`)) return;
      await runBusyAction(busyCtx, async () => {
        applyForgetAnswer(await onForgetRecent(workspace.root));
        if (closed) return;
        recents.update(known());
        pathInput.focus();
      });
    });

    pathInput.addEventListener("input", () => { testMessage = ""; renderNote(); });

    const open = (discard) => {
      const held = !discard && unsaved();
      choices.hidden = true;
      return runBusyAction(busyCtx, () => onOpen(request(), discard, () => closed), submit, "Opening…", (err) => {
        // Unsaved changes are refused first: a refusal while they are held is theirs, and the person chooses what becomes of them.
        if (err.status === 409 && held) choices.hidden = false;
      });
    };

    saveBtn = /** @type {HTMLButtonElement} */ (el("button", {
      id: "d-workspace-save", type: "button",
      onclick: async () => {
        choices.hidden = true;
        const saved = await runBusyAction(busyCtx, () => onSave());
        if (closed || saved === undefined) return;
        if (saved) await open(false);
        else {
          fail("Job not saved: save it or discard its changes.");
          choices.hidden = false;
        }
      },
    }, "Save, then open"));
    discardBtn = /** @type {HTMLButtonElement} */ (el("button", { id: "d-workspace-discard", type: "button", class: "ghost", onclick: () => open(true) }, "Discard changes and open"));
    fill(choices, saveBtn, discardBtn);

    test.addEventListener("click", () => runBusyAction(busyCtx, async () => {
      const answer = await onTest(request().path);
      if (closed) return;
      testMessage = `${answer.root} holds ${answer.jobs === 1 ? "1 job" : `${answer.jobs} jobs`}.`;
      renderNote();
    }, test, "Testing…"));

    const form = el("form", { class: "dialog-form", onsubmit: (e) => { e.preventDefault(); open(false); } },
      el("div", { class: "dialog-field" }, el("label", { for: "d-workspace-path" }, "Folder"), el("div", { class: "row" }, pathInput, test)),
      el("div", { class: "dialog-field" }, el("span", { class: "hint" }, "Recent folders"), recents.element, currentRecentRow),
      el("details", { class: "dialog-details" }, el("summary", {}, "Advanced"),
        el("label", {}, remember, " Remember among the recent folders on this machine")),
      note,
      banner,
      choices,
      el("p", { class: "hint" }, "Reloads the page and starts a new agent conversation."),
      el("div", { class: "row dialog-actions" },
        el("button", { type: "button", class: "ghost", onclick: close }, "Cancel"),
        submit));
    fill(body, form);
    renderNote();
    pathInput.focus();
    pathInput.select();
  }, { onClose: () => { closed = true; } });
}
