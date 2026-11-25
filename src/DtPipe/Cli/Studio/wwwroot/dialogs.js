// @ts-check
// Modal dialogs for creating, importing, saving, renaming, deleting, inspecting and pulling jobs, and for opening a folder.
// Built with DOM calls only: no HTML strings or inline styles are ever constructed here.

import { api, el, fill, targetOf } from "./dom.js";

/** @type {{ dlg: HTMLDialogElement, opener: HTMLElement | null, onClose?: () => void } | null} */
let active = null;

export const shortCommit = (commit) => String(commit || "").slice(0, 8) || "none";

/** Closes the active modal dialog if one is open, restoring focus when safe. */
export function closeActiveDialog() {
  if (!active) return;
  const { dlg, opener, onClose } = active;
  active = null;
  if (dlg.open) dlg.close();
  dlg.remove();
  onClose?.();
  if (opener?.isConnected && opener.offsetParent !== null) opener.focus();
}

/**
 * Opens a modal dialog over the page. Only one dialog is open at a time.
 * `build` receives `{ dialog, body, close }` and populates `body`.
 * @param {string} id
 * @param {string} title
 * @param {(ctx: { dialog: HTMLDialogElement, body: HTMLElement, close: () => void }) => void} build
 * @param {{ onClose?: () => void }} [options]
 */
export function openDialog(id, title, build, options = {}) {
  closeActiveDialog();
  const opener = document.activeElement instanceof HTMLElement ? document.activeElement : null;
  const dlg = /** @type {HTMLDialogElement} */ (el("dialog", { id, class: "studio-dialog", "aria-labelledby": `${id}-title` }));
  const body = el("div", { class: "dialog-body" });
  const close = () => {
    if (active?.dlg === dlg) closeActiveDialog();
  };
  fill(dlg,
    el("div", { class: "dialog-head" },
      el("h3", { id: `${id}-title` }, title),
      el("button", { type: "button", class: "small ghost", "aria-label": "Close dialog", title: "Close", onclick: close }, "×")),
    body);
  dlg.addEventListener("cancel", (e) => { e.preventDefault(); close(); });
  document.body.append(dlg);
  active = { dlg, opener, onClose: options.onClose };
  dlg.showModal();
  build({ dialog: dlg, body, close });
  return { dialog: dlg, body, close };
}

/**
 * Splits a command line into process arguments as a shell would: whitespace separates, quotes group,
 * backslash escapes outside single quotes, and `\<newline>` continues lines outside single quotes.
 * @param {string} text
 * @returns {string[]}
 */
export function splitCommand(text) {
  const args = [];
  let word = null;
  let quote = null;
  for (let i = 0; i < text.length; i++) {
    const c = text[i];
    if (quote) {
      if (c === quote) quote = null;
      else if (c === "\\" && quote === "\"" && i + 1 < text.length) {
        if (text[i + 1] === "\n") i++;
        else if (text[i + 1] === "\r" && text[i + 2] === "\n") i += 2;
        else word += text[++i];
      } else word += c;
    } else if (c === "'" || c === "\"") {
      quote = c;
      word ??= "";
    } else if (c === "\\" && i + 1 < text.length) {
      if (text[i + 1] === "\n") i++;
      else if (text[i + 1] === "\r" && text[i + 2] === "\n") i += 2;
      else word = (word ?? "") + text[++i];
    } else if (/\s/.test(c)) {
      if (word !== null) { args.push(word); word = null; }
    } else {
      word = (word ?? "") + c;
    }
  }
  if (quote) throw new Error("A quote is not closed.");
  if (word !== null) args.push(word);
  return args;
}

/**
 * Combines a chosen subfolder and file name into a normalized workspace relative path,
 * automatically adding `.yaml` when no YAML extension was typed.
 * @param {string} folder
 * @param {string} rawName
 */
export function normalizeJobPath(folder, rawName) {
  let name = String(rawName || "").trim().replace(/\\/g, "/");
  if (!name) return "";
  const last = name.split("/").pop() || "";
  if (!name.endsWith("/") && last !== "." && last !== ".." && !/\.ya?ml$/i.test(name)) name += ".yaml";
  return folder ? `${folder}/${name}` : name;
}

/**
 * Checks whether `path` is a valid job path in the workspace and whether it collides with an existing file.
 * @param {string} path
 * @param {string[]} folders
 * @param {string[]} existingPaths
 * @param {{ allowExisting?: boolean, allowPath?: string | null }} [options]
 */
export function validateJobPath(path, folders, existingPaths, options = {}) {
  const { allowExisting = false, allowPath = null } = options;
  if (!path) return { valid: false, exists: false, error: "Enter a file name." };
  if (path.startsWith("/") || path.endsWith("/")) return { valid: false, exists: false, error: "Enter a file name relative to the workspace." };
  const parts = path.split("/");
  if (parts.some((s) => !s || s.startsWith("."))) return { valid: false, exists: false, error: "A path cannot leave the folder or name a hidden file." };
  if (!/\.ya?ml$/i.test(path)) return { valid: false, exists: false, error: "A job file must end in .yaml or .yml." };
  if (parts.length > 1) {
    const parent = parts.slice(0, -1).join("/");
    if (!folders.includes(parent)) return { valid: false, exists: false, error: `Folder '${parent}/' does not exist in this workspace.` };
  }
  const exists = existingPaths.includes(path);
  if (exists && !allowExisting && path !== allowPath) {
    return { valid: false, exists: true, error: `'${path}' already exists.` };
  }
  return { valid: true, exists, error: null };
}

/**
 * Builds a subfolder + filename picker with live `.yaml` normalization and collision validation.
 * @param {{ idPrefix: string, folders: string[], existingPaths: string[], initialPath: string, allowExisting?: boolean, allowPath?: string | null, label?: string, onChange?: () => void }} opts
 */
export function createPathPicker(opts) {
  const { idPrefix, folders, existingPaths, initialPath, allowExisting = false, allowPath = null, label = "File path", onChange } = opts;
  const knownFolders = folders?.length ? folders : [""];
  const slash = initialPath.lastIndexOf("/");
  const initialFolder = slash > 0 && knownFolders.includes(initialPath.slice(0, slash)) ? initialPath.slice(0, slash) : "";
  const initialName = initialFolder ? initialPath.slice(initialFolder.length + 1) : initialPath;

  const folderSelect = el("select", { id: `${idPrefix}-folder`, "aria-label": "Folder" },
    knownFolders.map((f) => el("option", { value: f, selected: f === initialFolder }, f ? `${f}/` : "(root)")));
  const nameInput = /** @type {HTMLInputElement} */ (el("input", {
    id: `${idPrefix}-name`,
    type: "text",
    class: "mono",
    value: initialName,
    placeholder: "job-name.yaml",
    "aria-label": label,
  }));
  const hint = el("div", { class: "hint mono dialog-path-preview" });
  const errorBox = el("div", { class: "bad dialog-inline-error", hidden: true });

  const container = el("div", { class: "dialog-field" },
    el("label", { for: `${idPrefix}-name` }, label),
    el("div", { class: "dialog-path-row" },
      knownFolders.length > 1 ? folderSelect : null,
      nameInput),
    hint,
    errorBox);

  function state() {
    const folder = knownFolders.length > 1 ? /** @type {HTMLSelectElement} */ (folderSelect).value : "";
    const path = normalizeJobPath(folder, nameInput.value);
    const check = validateJobPath(path, knownFolders, existingPaths, { allowExisting, allowPath });
    return { path, ...check };
  }

  function refresh(notify = true) {
    const s = state();
    hint.textContent = s.path ? `Path: ${s.path}` : "";
    errorBox.hidden = !s.error;
    errorBox.textContent = s.error || "";
    if (notify) onChange?.();
  }

  folderSelect.addEventListener("change", () => refresh(true));
  nameInput.addEventListener("input", () => refresh(true));
  refresh(false);

  return {
    element: container,
    state,
    setPath(nextPath) {
      const nextSlash = nextPath.lastIndexOf("/");
      const nextFolder = nextSlash > 0 && knownFolders.includes(nextPath.slice(0, nextSlash)) ? nextPath.slice(0, nextSlash) : "";
      if (knownFolders.length > 1) /** @type {HTMLSelectElement} */ (folderSelect).value = nextFolder;
      nameInput.value = nextFolder ? nextPath.slice(nextFolder.length + 1) : nextPath;
      refresh();
    },
    focus() { nameInput.focus(); nameInput.select(); },
  };
}

/** Suggests a non-colliding copy filename derived from `basePath`. @param {string} basePath @param {string[]} existingPaths */
export function nextFreePath(basePath, existingPaths) {
  const taken = new Set(existingPaths);
  const extMatch = /\.ya?ml$/i.exec(basePath);
  const ext = extMatch ? extMatch[0] : ".yaml";
  const stem = extMatch ? basePath.slice(0, extMatch.index) : basePath;
  let candidate = `${stem}-copy${ext}`;
  for (let n = 2; taken.has(candidate); n++) candidate = `${stem}-copy-${n}${ext}`;
  return candidate;
}

/**
 * Computes a compact line-by-line diff view between two YAML texts using pure DOM nodes.
 * @param {string} beforeText
 * @param {string} afterText
 * @param {string} beforeLabel
 * @param {string} afterLabel
 */
export function renderDiff(beforeText, afterText, beforeLabel, afterLabel) {
  const a = String(beforeText || "").replace(/\r\n/g, "\n").split("\n");
  const b = String(afterText || "").replace(/\r\n/g, "\n").split("\n");
  const header = (summary) => el("div", { class: "diff-head" },
    el("span", { class: "mono" }, `${beforeLabel} → ${afterLabel}`),
    el("span", { class: "hint" }, summary));

  if (a.join("\n") === b.join("\n")) {
    return el("div", { class: "diff-box" }, header("Identical content"));
  }

  if (a.length > 400 || b.length > 400) {
    return el("div", { class: "diff-box" },
      header(`${a.length} lines → ${b.length} lines (preview truncated for size)`),
      el("pre", { class: "diff-pre mono" }, afterText.slice(0, 8000)));
  }

  const m = a.length;
  const n = b.length;
  const dp = Array.from({ length: m + 1 }, () => new Uint16Array(n + 1));
  for (let i = m - 1; i >= 0; i--) {
    for (let j = n - 1; j >= 0; j--) {
      dp[i][j] = a[i] === b[j] ? dp[i + 1][j + 1] + 1 : Math.max(dp[i + 1][j], dp[i][j + 1]);
    }
  }

  /** @type {{ kind: "ctx" | "add" | "del", line: string }[]} */
  const ops = [];
  let i = 0;
  let j = 0;
  let added = 0;
  let removed = 0;
  while (i < m && j < n) {
    if (a[i] === b[j]) { ops.push({ kind: "ctx", line: a[i] }); i++; j++; }
    else if (dp[i + 1][j] >= dp[i][j + 1]) { ops.push({ kind: "del", line: a[i] }); removed++; i++; }
    else { ops.push({ kind: "add", line: b[j] }); added++; j++; }
  }
  while (i < m) { ops.push({ kind: "del", line: a[i++] }); removed++; }
  while (j < n) { ops.push({ kind: "add", line: b[j++] }); added++; }

  const prefix = { ctx: "  ", add: "+ ", del: "- " };
  return el("div", { class: "diff-box" },
    header(`+${added} / -${removed} lines`),
    el("pre", { class: "diff-pre mono" },
      ops.map((op) => el("span", { class: `diff-line diff-${op.kind}` }, `${prefix[op.kind]}${op.line}\n`))));
}

/**
 * Dialog to create a blank job with path collision checking and optional title.
 * @param {{ folders: string[], existingPaths: string[], defaultTitle?: string, initialPath?: string, title?: string, hideTitle?: boolean, onCreate: (args: { path: string, title: string }) => Promise<void> | void, onCancel?: () => void }} opts
 */
export function openNewJobDialog(opts) {
  const { folders, existingPaths, defaultTitle = "", initialPath, title = "New blank job", hideTitle = false, onCreate, onCancel } = opts;
  const defaultName = initialPath || (existingPaths.includes("new-job.yaml") ? nextFreePath("new-job.yaml", existingPaths) : "new-job.yaml");
  let submitted = false;
  openDialog("d-new-dialog", title, ({ body, close }) => {
    const createBtn = /** @type {HTMLButtonElement} */ (el("button", { id: "d-new-submit", type: "submit", class: "primary" }, "Create"));
    const titleInput = /** @type {HTMLInputElement} */ (el("input", { id: "d-new-title", type: "text", value: defaultTitle, placeholder: "Optional job title", "aria-label": "Job title" }));
    const errorBanner = el("p", { id: "d-new-error", class: "bad dialog-error", hidden: true });
    let busy = false;

    const picker = createPathPicker({
      idPrefix: "d-new",
      folders,
      existingPaths,
      initialPath: defaultName,
      label: "File path",
      onChange: () => { createBtn.disabled = busy || !picker.state().valid; },
    });

    const form = el("form", {
      class: "dialog-form",
      onsubmit: async (e) => {
        e.preventDefault();
        const s = picker.state();
        if (!s.valid || busy) return;
        busy = true;
        createBtn.disabled = true;
        errorBanner.hidden = true;
        try {
          submitted = true;
          close();
          await onCreate({ path: s.path, title: hideTitle ? "" : titleInput.value.trim() });
        } catch (err) {
          submitted = false;
          busy = false;
          createBtn.disabled = !picker.state().valid;
          errorBanner.hidden = false;
          errorBanner.textContent = err.message;
        }
      },
    },
      picker.element,
      hideTitle ? null : el("div", { class: "dialog-field" },
        el("label", { for: "d-new-title" }, "Title (optional)"),
        titleInput),
      errorBanner,
      el("div", { class: "row dialog-actions" },
        el("button", { type: "button", class: "ghost", onclick: close }, "Cancel"),
        createBtn));

    fill(body, form);
    createBtn.disabled = !picker.state().valid;
    picker.focus();
  }, { onClose: () => { if (!submitted) onCancel?.(); } });
}

/**
 * Dialog to create a job from a dtpipe CLI command line (multi-line supported).
 * Preserves the typed command if the server rejects it.
 * @param {{ folders: string[], existingPaths: string[], onImport: (args: { path: string, yaml: string }) => Promise<void> | void }} opts
 */
export function openFromCliDialog(opts) {
  const { folders, existingPaths, onImport } = opts;
  const defaultName = existingPaths.includes("imported.yaml") ? nextFreePath("imported.yaml", existingPaths) : "imported.yaml";
  openDialog("d-cli-dialog", "New job from CLI command", ({ body, close }) => {
    const submitBtn = /** @type {HTMLButtonElement} */ (el("button", { id: "d-cli-submit", type: "submit", class: "primary" }, "Import command"));
    const cmdInput = /** @type {HTMLTextAreaElement} */ (el("textarea", {
      id: "d-cli-command",
      class: "mono",
      rows: "5",
      spellcheck: "false",
      "aria-label": "dtpipe command line",
      placeholder: "dtpipe --input csv:orders.csv \\\n  --output csv:out.csv",
    }));
    const errorBanner = el("p", { id: "d-cli-error", class: "bad dialog-error", hidden: true });
    let busy = false;

    const update = () => {
      submitBtn.disabled = busy || !cmdInput.value.trim() || !picker.state().valid;
    };

    const picker = createPathPicker({
      idPrefix: "d-cli",
      folders,
      existingPaths,
      initialPath: defaultName,
      label: "Save as file",
      onChange: update,
    });
    cmdInput.addEventListener("input", update);

    const form = el("form", {
      class: "dialog-form",
      onsubmit: async (e) => {
        e.preventDefault();
        const s = picker.state();
        if (!s.valid || busy || !cmdInput.value.trim()) return;
        busy = true;
        submitBtn.disabled = true;
        submitBtn.textContent = "Importing…";
        errorBanner.hidden = true;
        try {
          const args = splitCommand(cmdInput.value);
          if (args[0] === "dtpipe") args.shift();
          if (!args.length) throw new Error("Enter a dtpipe command line.");
          const { yaml } = await api("/api/import", { args });
          close();
          await onImport({ path: s.path, yaml });
        } catch (err) {
          busy = false;
          submitBtn.textContent = "Import command";
          update();
          errorBanner.hidden = false;
          errorBanner.textContent = err.message;
        }
      },
    },
      el("div", { class: "dialog-field" },
        el("label", { for: "d-cli-command" }, "Command line"),
        cmdInput,
        el("p", { class: "hint" }, "Multi-line commands with trailing \\ are supported. ${{…}} references stay as typed; --bind-input and --bind-output are refused.")),
      picker.element,
      errorBanner,
      el("div", { class: "row dialog-actions" },
        el("button", { type: "button", class: "ghost", onclick: close }, "Cancel"),
        submitBtn));

    fill(body, form);
    update();
    cmdInput.focus();
  });
}

/**
 * Dialog to import an external YAML file (upload or paste) into the workspace as a new unsaved draft.
 * @param {{ folders: string[], existingPaths: string[], onImport: (args: { path: string, yaml: string }) => Promise<void> | void }} opts
 */
export function openImportYamlDialog(opts) {
  const { folders, existingPaths, onImport } = opts;
  const defaultName = existingPaths.includes("imported.yaml") ? nextFreePath("imported.yaml", existingPaths) : "imported.yaml";
  openDialog("d-yaml-import-dialog", "Import YAML file", ({ body, close }) => {
    const submitBtn = /** @type {HTMLButtonElement} */ (el("button", { id: "d-yaml-import-submit", type: "submit", class: "primary" }, "Import YAML"));
    const fileInput = /** @type {HTMLInputElement} */ (el("input", {
      id: "d-yaml-import-file",
      type: "file",
      accept: ".yaml,.yml,text/yaml,application/x-yaml",
      "aria-label": "Choose a YAML file from your computer",
    }));
    const textInput = /** @type {HTMLTextAreaElement} */ (el("textarea", {
      id: "d-yaml-import-text",
      class: "mono",
      rows: "8",
      spellcheck: "false",
      "aria-label": "YAML content",
      placeholder: "Paste job YAML here, or choose a .yaml / .yml file above…",
    }));
    const errorBanner = el("p", { id: "d-yaml-import-error", class: "bad dialog-error", hidden: true });
    let busy = false;

    const update = () => {
      submitBtn.disabled = busy || !textInput.value.trim() || !picker.state().valid;
    };

    const picker = createPathPicker({
      idPrefix: "d-yaml-import",
      folders,
      existingPaths,
      initialPath: defaultName,
      label: "Destination file in workspace",
      onChange: update,
    });

    fileInput.addEventListener("change", async () => {
      const file = fileInput.files?.[0];
      if (!file) return;
      try {
        textInput.value = await file.text();
        const safeName = file.name.replace(/^.*[/\\]/, "");
        if (safeName) picker.setPath(safeName);
        errorBanner.hidden = true;
      } catch (err) {
        errorBanner.hidden = false;
        errorBanner.textContent = err.message;
      }
      update();
    });
    textInput.addEventListener("input", update);

    const form = el("form", {
      class: "dialog-form",
      onsubmit: async (e) => {
        e.preventDefault();
        const s = picker.state();
        const yaml = textInput.value;
        if (!s.valid || busy || !yaml.trim()) return;
        busy = true;
        submitBtn.disabled = true;
        submitBtn.textContent = "Checking…";
        errorBanner.hidden = true;
        try {
          const check = await api("/api/design/decompose", { yaml, layout: null });
          const parseError = check.errors?.find((msg) => msg.startsWith("The job is not readable:"));
          if (parseError) throw new Error(parseError);
          close();
          await onImport({ path: s.path, yaml });
        } catch (err) {
          busy = false;
          submitBtn.textContent = "Import YAML";
          update();
          errorBanner.hidden = false;
          errorBanner.textContent = err.message;
        }
      },
    },
      el("div", { class: "dialog-field" },
        el("label", { for: "d-yaml-import-file" }, "Choose a file (optional)"),
        fileInput),
      el("div", { class: "dialog-field" },
        el("label", { for: "d-yaml-import-text" }, "YAML content"),
        textInput),
      picker.element,
      errorBanner,
      el("div", { class: "row dialog-actions" },
        el("button", { type: "button", class: "ghost", onclick: close }, "Cancel"),
        submitBtn));

    fill(body, form);
    update();
    fileInput.focus();
  });
}

/**
 * Dialog to save the currently open job under a new non-colliding path.
 * @param {{ folders: string[], existingPaths: string[], currentPath: string, suggestedPath: string, onSaveAs: (path: string) => Promise<void> }} opts
 */
export function openSaveAsDialog(opts) {
  const { folders, existingPaths, currentPath, suggestedPath, onSaveAs } = opts;
  openDialog("d-save-as-dialog", "Save job as…", ({ body, close }) => {
    const submitBtn = /** @type {HTMLButtonElement} */ (el("button", { id: "d-save-as-submit", type: "submit", class: "primary" }, "Save copy"));
    const errorBanner = el("p", { id: "d-save-as-error", class: "bad dialog-error", hidden: true });
    let busy = false;

    const picker = createPathPicker({
      idPrefix: "d-save-as",
      folders,
      existingPaths,
      initialPath: suggestedPath,
      label: "New file path",
      onChange: () => { submitBtn.disabled = busy || !picker.state().valid; },
    });

    const form = el("form", {
      class: "dialog-form",
      onsubmit: async (e) => {
        e.preventDefault();
        const s = picker.state();
        if (!s.valid || busy) return;
        busy = true;
        submitBtn.disabled = true;
        submitBtn.textContent = "Saving…";
        errorBanner.hidden = true;
        try {
          await onSaveAs(s.path);
          close();
        } catch (err) {
          busy = false;
          submitBtn.textContent = "Save copy";
          submitBtn.disabled = !picker.state().valid;
          errorBanner.hidden = false;
          errorBanner.textContent = err.message;
        }
      },
    },
      el("p", { class: "hint" }, "Save ", el("code", {}, currentPath), " under another path in this workspace."),
      picker.element,
      errorBanner,
      el("div", { class: "row dialog-actions" },
        el("button", { type: "button", class: "ghost", onclick: close }, "Cancel"),
        submitBtn));

    fill(body, form);
    submitBtn.disabled = !picker.state().valid;
    picker.focus();
  });
}

/**
 * Dialog to rename or move a job file inside the workspace.
 * @param {{ folders: string[], existingPaths: string[], path: string, hasEdits: boolean, onRename: (to: string) => Promise<void> }} opts
 */
export function openRenameDialog(opts) {
  const { folders, existingPaths, path, hasEdits, onRename } = opts;
  openDialog("d-rename-dialog", "Rename or move job", ({ body, close }) => {
    const submitBtn = /** @type {HTMLButtonElement} */ (el("button", { id: "d-rename-submit", type: "submit", class: "primary" }, "Rename"));
    const errorBanner = el("p", { id: "d-rename-error", class: "bad dialog-error", hidden: true });
    let busy = false;

    const update = () => {
      const s = picker.state();
      submitBtn.disabled = busy || hasEdits || !s.valid || s.path === path;
    };

    const picker = createPathPicker({
      idPrefix: "d-rename",
      folders,
      existingPaths,
      initialPath: path,
      allowPath: path,
      label: "New file path",
      onChange: update,
    });

    const form = el("form", {
      class: "dialog-form",
      onsubmit: async (e) => {
        e.preventDefault();
        const s = picker.state();
        if (hasEdits || !s.valid || s.path === path || busy) return;
        busy = true;
        submitBtn.disabled = true;
        submitBtn.textContent = "Renaming…";
        errorBanner.hidden = true;
        try {
          await onRename(s.path);
          close();
        } catch (err) {
          busy = false;
          submitBtn.textContent = "Rename";
          update();
          errorBanner.hidden = false;
          errorBanner.textContent = err.message;
        }
      },
    },
      el("p", { class: "hint" }, "Moves ", el("code", {}, path), " and its studio metadata (layout, launch values, library link, checkpoints)."),
      hasEdits ? el("p", { class: "warn" }, "This file has unsaved changes. Save or revert them before renaming.") : null,
      picker.element,
      errorBanner,
      el("div", { class: "row dialog-actions" },
        el("button", { type: "button", class: "ghost", onclick: close }, "Cancel"),
        submitBtn));

    fill(body, form);
    update();
    picker.focus();
  });
}

/**
 * Confirmation dialog to delete a local job file and its studio metadata.
 * @param {{ path: string, isOpen: boolean, hasEdits: boolean, publicationId: string | null, onDelete: () => Promise<void> }} opts
 */
export function openDeleteDialog(opts) {
  const { path, isOpen, hasEdits, publicationId, onDelete } = opts;
  openDialog("d-delete-dialog", "Delete local job", ({ body, close }) => {
    const deleteBtn = /** @type {HTMLButtonElement} */ (el("button", { id: "d-delete-submit", type: "button", class: "danger" }, "Delete file"));
    const errorBanner = el("p", { id: "d-delete-error", class: "bad dialog-error", hidden: true });
    let busy = false;

    deleteBtn.addEventListener("click", async () => {
      if (busy) return;
      busy = true;
      deleteBtn.disabled = true;
      deleteBtn.textContent = "Deleting…";
      errorBanner.hidden = true;
      try {
        await onDelete();
        close();
      } catch (err) {
        busy = false;
        deleteBtn.disabled = false;
        deleteBtn.textContent = "Delete file";
        errorBanner.hidden = false;
        errorBanner.textContent = err.message;
      }
    });

    fill(body,
      el("p", {}, "Delete ", el("code", {}, path), " and its local studio metadata from this folder? This cannot be undone."),
      isOpen && hasEdits ? el("p", { class: "warn" }, "Unsaved changes in the open editor will be lost.") : null,
      publicationId ? el("p", { class: "hint" }, "Only the local file is removed: pipeline ", el("code", {}, publicationId), " remains in the coordinator's library.") : null,
      errorBanner,
      el("div", { class: "row dialog-actions" },
        el("button", { type: "button", class: "ghost", onclick: close }, "Cancel"),
        deleteBtn));
    deleteBtn.focus();
  });
}

/**
 * Read-only inspection modal for a coordinator library pipeline without writing anything to disk.
 * @param {{ pipeline: any, linkedPath: string | null, syncState: string | null, onPull: () => void, onOpenLocal: (path: string) => void }} opts
 */
export function openInspectLibraryDialog(opts) {
  const { pipeline, linkedPath, syncState, onPull, onOpenLocal } = opts;
  let closed = false;
  openDialog("d-library-inspect-dialog", pipeline.title || pipeline.id, ({ body, close }) => {
    const content = el("div", { class: "dialog-form" },
      el("p", { class: "hint" }, "Reading ", el("code", {}, pipeline.id), " from the coordinator…"));
    fill(body, content);

    api(`/api/library/job?id=${encodeURIComponent(pipeline.id)}`)
      .then((remote) => {
        if (closed) return;
        const primaryLabel = !linkedPath ? "Pull to workspace…"
          : syncState === "outdated" || syncState === "diverged" ? "Pull update…"
          : "Open local";
        const copyBtn = el("button", {
          type: "button",
          class: "ghost",
          onclick: async () => {
            try { await navigator.clipboard.writeText(remote.yaml); copyBtn.textContent = "Copied"; }
            catch { copyBtn.textContent = "Copy failed"; }
          },
        }, "Copy YAML");

        fill(content,
          el("p", { class: "hint mono" }, `${remote.id} · commit ${shortCommit(remote.commit)}`),
          pipeline.description ? el("p", {}, pipeline.description) : null,
          linkedPath ? el("p", { class: "hint" }, "Linked in workspace: ", el("code", {}, linkedPath)) : null,
          el("pre", { class: "dialog-pre mono" }, remote.yaml),
          el("div", { class: "row dialog-actions" },
            copyBtn,
            linkedPath && (syncState === "outdated" || syncState === "diverged")
              ? el("button", { type: "button", class: "ghost", onclick: () => { close(); onOpenLocal(linkedPath); } }, "Open local")
              : null,
            el("button", { type: "button", class: "ghost", onclick: close }, "Close"),
            el("button", {
              type: "button",
              class: "primary",
              onclick: () => {
                close();
                if (linkedPath && syncState !== "outdated" && syncState !== "diverged") onOpenLocal(linkedPath);
                else onPull();
              },
            }, primaryLabel)));
      })
      .catch((err) => {
        if (closed) return;
        fill(content,
          el("p", { class: "bad" }, err.message),
          el("div", { class: "row dialog-actions" },
            el("button", { type: "button", class: "ghost", onclick: close }, "Close")));
      });
  }, { onClose: () => { closed = true; } });
}

/**
 * Dialog to pull a pipeline from the coordinator library into the workspace:
 * creates a new local file, updates a linked local file with a line diff, or resolves an unlinked filename collision.
 * @param {{ pipeline: any, folders: string[], existingPaths: string[], linkedPath: string | null, syncState: string | null, openJobPath: string | null, openJobHasEdits: boolean, onPulled: (path: string) => Promise<void>, onOpenLocal: (path: string) => Promise<void> }} opts
 */
export function openPullDialog(opts) {
  const { pipeline, folders, existingPaths, linkedPath, syncState, openJobPath, openJobHasEdits, onPulled, onOpenLocal } = opts;
  let closed = false;

  openDialog("d-pull-dialog", `Pull ${pipeline.id}`, ({ body, close }) => {
    const content = el("div", { class: "dialog-form" },
      el("p", { class: "hint" }, "Fetching ", el("code", {}, pipeline.id), " from the coordinator…"));
    fill(body, content);

    const defaultNewPath = `${pipeline.id}.yaml`;
    const localToLoad = linkedPath || (existingPaths.includes(defaultNewPath) ? defaultNewPath : null);
    Promise.all([
      api(`/api/library/job?id=${encodeURIComponent(pipeline.id)}`),
      localToLoad ? api(`/api/job?path=${encodeURIComponent(localToLoad)}`).catch(() => null) : Promise.resolve(null),
    ]).then(([remote, localDoc]) => {
      if (closed) return;
      /** @type {Map<string, { yaml: string, hash: string }>} */
      const localCache = new Map();
      if (localToLoad && localDoc) localCache.set(localToLoad, { yaml: localDoc.yaml, hash: localDoc.hash });

      const submitBtn = /** @type {HTMLButtonElement} */ (el("button", { id: "d-pull-submit", type: "submit", class: "primary" }, "Pull"));
      const errorBanner = el("p", { id: "d-pull-error", class: "bad dialog-error", hidden: true });
      let busy = false;

      if (linkedPath && localDoc) {
        let mode = syncState === "outdated" || syncState === "diverged" ? "update" : "copy";
        const copyDefault = nextFreePath(linkedPath, existingPaths);
        const modeArea = el("div");
        const picker = createPathPicker({
          idPrefix: "d-pull-copy",
          folders,
          existingPaths,
          initialPath: copyDefault,
          label: "Copy file path",
          onChange: syncUi,
        });

        const radioUpdate = /** @type {HTMLInputElement} */ (el("input", { type: "radio", name: "pull-mode", value: "update", checked: mode === "update" }));
        const radioCopy = /** @type {HTMLInputElement} */ (el("input", { type: "radio", name: "pull-mode", value: "copy", checked: mode === "copy" }));
        radioUpdate.addEventListener("change", () => { if (radioUpdate.checked) { mode = "update"; syncUi(); } });
        radioCopy.addEventListener("change", () => { if (radioCopy.checked) { mode = "copy"; syncUi(); } });

        function syncUi() {
          if (mode === "update") {
            const divergedWarn = syncState === "diverged" || (linkedPath === openJobPath && openJobHasEdits);
            fill(modeArea,
              divergedWarn ? el("p", { class: "warn" }, "Local changes in ", el("code", {}, linkedPath), " will be replaced by the coordinator's version.") : null,
              renderDiff(localDoc.yaml, remote.yaml, linkedPath, `library (${shortCommit(remote.commit)})`));
            submitBtn.textContent = `Update ${linkedPath}`;
            submitBtn.disabled = busy;
          } else {
            fill(modeArea, picker.element);
            submitBtn.textContent = "Pull as new copy";
            submitBtn.disabled = busy || !picker.state().valid;
          }
        }

        const form = el("form", {
          class: "dialog-form",
          onsubmit: async (e) => {
            e.preventDefault();
            if (busy) return;
            const targetPath = mode === "update" ? linkedPath : picker.state().path;
            const overwriteHash = mode === "update" ? localDoc.hash : null;
            if (mode === "copy" && !picker.state().valid) return;
            busy = true;
            submitBtn.disabled = true;
            submitBtn.textContent = "Pulling…";
            errorBanner.hidden = true;
            try {
              const saved = await api("/api/library/open", { id: pipeline.id, path: targetPath, overwriteHash });
              close();
              await onPulled(saved.path);
            } catch (err) {
              busy = false;
              errorBanner.hidden = false;
              errorBanner.textContent = err.message;
              syncUi();
            }
          },
        },
          el("p", { class: "hint" }, "Pipeline ", el("code", {}, pipeline.id), " · remote commit ", el("code", {}, shortCommit(remote.commit))),
          el("div", { class: "dialog-radio-group" },
            el("label", {}, radioUpdate, " Update linked file (", el("code", {}, linkedPath), ")"),
            el("label", {}, radioCopy, " Save as a new copy")),
          modeArea,
          errorBanner,
          el("div", { class: "row dialog-actions" },
            el("button", { type: "button", class: "ghost", onclick: close }, "Cancel"),
            submitBtn));

        fill(content, form);
        syncUi();
        submitBtn.focus();
        return;
      }

      // Unlinked pull: allow picking path; if chosen path already exists locally, let user open it, rename, or opt in to overwrite with diff.
      let replaceExisting = false;
      const collisionArea = el("div");
      const picker = createPathPicker({
        idPrefix: "d-pull",
        folders,
        existingPaths,
        initialPath: defaultNewPath,
        allowExisting: true,
        label: "Save to workspace as",
        onChange: () => { replaceExisting = false; syncCollision(); },
      });

      function syncCollision() {
        const s = picker.state();
        if (!s.exists) {
          fill(collisionArea);
          submitBtn.textContent = "Pull & open";
          submitBtn.disabled = busy || !s.valid;
          return;
        }
        const cached = localCache.get(s.path);
        const replaceCheck = /** @type {HTMLInputElement} */ (el("input", { id: "d-pull-overwrite", type: "checkbox", checked: replaceExisting }));
        replaceCheck.addEventListener("change", async () => {
          replaceExisting = replaceCheck.checked;
          if (replaceExisting && !localCache.has(s.path)) {
            try {
              const doc = await api(`/api/job?path=${encodeURIComponent(s.path)}`);
              if (closed) return;
              localCache.set(s.path, { yaml: doc.yaml, hash: doc.hash });
            } catch (err) {
              errorBanner.hidden = false;
              errorBanner.textContent = err.message;
              replaceExisting = false;
            }
          }
          syncCollision();
        });

        fill(collisionArea,
          el("div", { class: "dialog-collision" },
            el("p", { class: "warn" }, el("code", {}, s.path), " already exists in this workspace (not linked to ", el("code", {}, pipeline.id), ")."),
            el("div", { class: "row wrap" },
              el("button", { type: "button", class: "small", onclick: async () => { close(); await onOpenLocal(s.path); } }, `Open existing ${s.path}`),
              el("label", {}, replaceCheck, ` Replace existing ${s.path}`)),
            replaceExisting && cached ? renderDiff(cached.yaml, remote.yaml, s.path, `library (${shortCommit(remote.commit)})`) : null));

        submitBtn.textContent = replaceExisting ? `Replace ${s.path}` : "Pull & open";
        submitBtn.disabled = busy || !s.valid || !replaceExisting || !localCache.has(s.path);
      }

      const form = el("form", {
        class: "dialog-form",
        onsubmit: async (e) => {
          e.preventDefault();
          const s = picker.state();
          if (busy || !s.valid) return;
          if (s.exists && !replaceExisting) return;
          const overwriteHash = s.exists ? localCache.get(s.path)?.hash : null;
          if (s.exists && !overwriteHash) return;
          busy = true;
          submitBtn.disabled = true;
          submitBtn.textContent = "Pulling…";
          errorBanner.hidden = true;
          try {
            const saved = await api("/api/library/open", { id: pipeline.id, path: s.path, overwriteHash });
            close();
            await onPulled(saved.path);
          } catch (err) {
            busy = false;
            errorBanner.hidden = false;
            errorBanner.textContent = err.message;
            syncCollision();
          }
        },
      },
        el("p", { class: "hint" }, pipeline.title ? [el("b", {}, pipeline.title), " · "] : null,
          el("code", {}, pipeline.id), " · commit ", el("code", {}, shortCommit(remote.commit))),
        el("details", { class: "dialog-details" },
          el("summary", {}, "Preview YAML"),
          el("pre", { class: "dialog-pre mono" }, remote.yaml)),
        picker.element,
        collisionArea,
        errorBanner,
        el("div", { class: "row dialog-actions" },
          el("button", { type: "button", class: "ghost", onclick: close }, "Cancel"),
          submitBtn));

      fill(content, form);
      syncCollision();
      picker.focus();
    }).catch((err) => {
      if (closed) return;
      fill(content,
        el("p", { class: "bad" }, err.message),
        el("div", { class: "row dialog-actions" },
          el("button", { type: "button", class: "ghost", onclick: close }, "Close")));
    });
  }, { onClose: () => { closed = true; } });
}

const AGENT_PROVIDERS = {
  ollama: { label: "Ollama", url: "http://localhost:11434", provider: "ollama" },
  openai: { label: "OpenAI", url: "https://api.openai.com", provider: "openai" },
  compatible: { label: "OpenAI-compatible server", url: "", provider: "openai" },
};

/** The choice a connection stands for: an openai provider at another address than OpenAI's is a compatible server. @param {any} connection */
function providerChoice(connection) {
  if (connection?.provider === "ollama") return "ollama";
  if (connection?.provider === "openai") return connection.url && connection.url !== AGENT_PROVIDERS.openai.url ? "compatible" : "openai";
  return "ollama";
}

/**
 * The agent's connection: provider, address, key, model and, under Advanced, the delay and whether to remember it on this
 * machine. The key is typed here and sent once; the page never reads one back, only whether one is kept and where.
 * `onConnect(body)` makes the connection (false: the person declined) and `onForget()` forgets what is remembered.
 * @param {{ connection: any, onConnect: (body: any) => Promise<any>, onForget: () => Promise<void> }} opts
 */
export function openAgentConnectDialog(opts) {
  const { connection, onConnect, onForget } = opts;
  openDialog("d-agent-dialog", "Connect a model", ({ body, close }) => {
    const choice = providerChoice(connection);
    const provider = /** @type {HTMLSelectElement} */ (el("select", { id: "d-agent-provider", "aria-label": "Provider" },
      Object.entries(AGENT_PROVIDERS).map(([value, p]) => el("option", { value, selected: value === choice }, p.label))));
    const url = /** @type {HTMLInputElement} */ (el("input", {
      id: "d-agent-url", type: "text", class: "mono", autocomplete: "off", "aria-label": "Address",
      value: connection?.url || AGENT_PROVIDERS[choice].url, placeholder: "https://models.example.com/v1",
    }));
    const key = /** @type {HTMLInputElement} */ (el("input", { id: "d-agent-key", type: "password", autocomplete: "off", "aria-label": "Key", placeholder: "API key, if the server asks for one" }));
    const keyNote = el("p", { id: "d-agent-key-note", class: "hint" });
    const removeKey = /** @type {HTMLInputElement} */ (el("input", { id: "d-agent-remove-key", type: "checkbox", "aria-label": "Remove the key kept for this address" }));
    const model = /** @type {HTMLSelectElement} */ (el("select", { id: "d-agent-model", "aria-label": "Model" },
      el("option", { value: connection?.model || "" }, connection?.model || "(Test lists the models)")));
    const timeout = /** @type {HTMLInputElement} */ (el("input", { id: "d-agent-timeout", type: "number", min: "1", "aria-label": "Seconds with no token before a call fails", value: String(connection?.timeout || 300) }));
    const bounds = connection?.numCtxBounds ?? { min: 4096, max: 1048576, default: 32768 };
    const ctxSize = /** @type {HTMLInputElement} */ (el("input", {
      id: "d-agent-numctx", type: "number", min: String(bounds.min), max: String(bounds.max), step: "1", "aria-label": "Context size in tokens",
      value: String(connection?.numCtx || bounds.default),
    }));
    const remember = /** @type {HTMLInputElement} */ (el("input", { id: "d-agent-remember", type: "checkbox", checked: true, "aria-label": "Remember on this machine" }));
    const note = el("p", { id: "d-agent-note", class: "hint" });
    const banner = el("p", { id: "d-agent-error", class: "bad dialog-error", hidden: true });
    const connect = /** @type {HTMLButtonElement} */ (el("button", { id: "d-agent-submit", type: "submit", class: "primary" }, "Connect"));
    const test = /** @type {HTMLButtonElement} */ (el("button", { id: "d-agent-test", type: "button" }, "Test"));
    let busy = false;

    const same = (a, b) => String(a || "").toLowerCase().replace(/\/+$/, "") === String(b || "").toLowerCase().replace(/\/+$/, "");
    const request = () => ({
      provider: AGENT_PROVIDERS[provider.value].provider,
      url: url.value.trim(),
      model: model.value || undefined,
      timeout: Number(timeout.value) || undefined,
      numCtx: provider.value === "ollama" && ctxSize.value.trim() !== "" ? Number(ctxSize.value) : undefined,
      key: key.value || undefined,
      removeKey: removeKey.checked,
      remember: remember.checked,
    });
    const fail = (message) => { banner.hidden = false; banner.textContent = message; };
    const settle = (value) => { busy = value; connect.disabled = value; test.disabled = value; };
    const update = () => {
      const keyed = provider.value !== "ollama";
      key.disabled = !keyed;
      key.parentElement.hidden = !keyed;
      ctxSize.parentElement.hidden = provider.value !== "ollama";
      const here = same(url.value, connection?.url);
      const kept = here && connection?.key;
      keyNote.hidden = !keyed;
      keyNote.textContent = kept === "stored" ? "A key is kept in the keyring for this address: leave the field empty to keep it, or type another to replace it."
        : kept === "flag" ? "The key was given on the command line: it holds for this session only."
        : kept === "session" ? "A key typed earlier is held for this session only: type it again to remember it."
        : connection?.needsKey && here ? "This model was remembered, but its key is not kept: type it again."
        : "A key is kept only for the address it is given at: another address asks for it again.";
      removeKey.parentElement.hidden = !(keyed && kept === "stored");
      note.textContent = connection?.source === "flag" ? "This model is set by the command line: a change holds for this session, and is remembered only when ticked."
        : connection?.keyring === false ? "The system keyring cannot be used: a key lasts for this session only." : "";
    };
    provider.addEventListener("change", () => { url.value = AGENT_PROVIDERS[provider.value].url; update(); });
    url.addEventListener("input", update);

    test.addEventListener("click", async () => {
      if (busy) return;
      settle(true);
      banner.hidden = true;
      test.textContent = "Testing…";
      try {
        const { models } = await api("/api/agent/connection/test", request());
        const wanted = model.value;
        fill(model, models.map((name) => el("option", { value: name, selected: name === wanted }, name)));
        note.textContent = `${models.length} model${models.length === 1 ? "" : "s"} listed.`;
      } catch (err) {
        fail(err.message);
      }
      test.textContent = "Test";
      settle(false);
    });

    const form = el("form", {
      class: "dialog-form",
      onsubmit: async (e) => {
        e.preventDefault();
        if (busy) return;
        settle(true);
        banner.hidden = true;
        connect.textContent = "Connecting…";
        try {
          const answer = await onConnect(request());
          key.value = "";
          if (answer !== false) { close(); return; }
        } catch (err) {
          fail(err.message);
        }
        connect.textContent = "Connect";
        settle(false);
      },
    },
      el("div", { class: "dialog-field" }, el("label", { for: "d-agent-provider" }, "Provider"), provider),
      el("div", { class: "dialog-field" }, el("label", { for: "d-agent-url" }, "Address"), url),
      el("div", { class: "dialog-field" }, el("label", { for: "d-agent-key" }, "Key"), key, keyNote,
        el("label", {}, removeKey, " Remove the key kept for this address")),
      el("div", { class: "dialog-field" }, el("label", { for: "d-agent-model" }, "Model"), el("div", { class: "row" }, model, test)),
      el("details", { class: "dialog-details" }, el("summary", {}, "Advanced"),
        el("div", { class: "dialog-field" }, el("label", { for: "d-agent-timeout" }, "Seconds with no token before a call fails"), timeout),
        el("div", { class: "dialog-field" }, el("label", { for: "d-agent-numctx" }, "Context size, in tokens"), ctxSize,
          el("p", { class: "hint" }, `The model's window (Ollama's num_ctx), from ${bounds.min} to ${bounds.max}; ${bounds.default} when left as it is.`)),
        el("label", {}, remember, " Remember on this machine (the key goes to the system keyring, never to a file)")),
      note,
      banner,
      el("div", { class: "row dialog-actions" },
        connection?.source && connection.source !== "none"
          ? el("button", {
            id: "d-agent-forget", type: "button", class: "ghost", title: "Forget the remembered model and the key kept for its address",
            onclick: async () => {
              if (busy) return;
              try { await onForget(); close(); } catch (err) { fail(err.message); }
            },
          }, "Forget what is remembered")
          : null,
        el("button", { type: "button", class: "ghost", onclick: close }, "Cancel"),
        connect));
    fill(body, form);
    update();
    url.focus();
  });
}

/**
 * The coordinator's connection: its address, how the person signs in (through the browser, or with a key the coordinator made)
 * and, under Advanced, whether to remember it on this machine. The key is typed here and sent once; the page never reads one
 * back, only whether one is kept and where. `onConnect(body)` makes the connection (false: the person declined) and
 * `onForget()` disconnects and forgets what is remembered.
 * @param {{ connection: any, onConnect: (body: any) => Promise<any>, onForget: () => Promise<void> }} opts
 */
export function openCoordinatorConnectDialog(opts) {
  const { connection, onConnect, onForget } = opts;
  openDialog("d-coordinator-dialog", "Connect a coordinator", ({ body, close }) => {
    const origin = /** @type {HTMLInputElement} */ (el("input", {
      id: "d-coordinator-origin", type: "text", class: "mono", autocomplete: "off", "aria-label": "Address",
      value: connection?.origin || "", placeholder: "https://coordinator.example.com",
    }));
    const auth = /** @type {HTMLSelectElement} */ (el("select", { id: "d-coordinator-auth", "aria-label": "Sign in" },
      el("option", { value: "browser", selected: connection?.auth !== "key" }, "Through the browser"),
      el("option", { value: "key", selected: connection?.auth === "key" }, "With a key the coordinator made for me")));
    const key = /** @type {HTMLInputElement} */ (el("input", { id: "d-coordinator-key", type: "password", autocomplete: "off", "aria-label": "Key", placeholder: "dtp_…" }));
    const keyNote = el("p", { id: "d-coordinator-key-note", class: "hint" });
    const removeKey = /** @type {HTMLInputElement} */ (el("input", { id: "d-coordinator-remove-key", type: "checkbox", "aria-label": "Forget the key kept for this address" }));
    const remember = /** @type {HTMLInputElement} */ (el("input", { id: "d-coordinator-remember", type: "checkbox", checked: true, "aria-label": "Remember on this machine" }));
    const note = el("p", { id: "d-coordinator-note", class: "hint" });
    const banner = el("p", { id: "d-coordinator-error", class: "bad dialog-error", hidden: true });
    const connect = /** @type {HTMLButtonElement} */ (el("button", { id: "d-coordinator-submit", type: "submit", class: "primary" }, "Connect"));
    const test = /** @type {HTMLButtonElement} */ (el("button", { id: "d-coordinator-test", type: "button" }, "Test"));
    let busy = false;

    const same = (a, b) => String(a || "").toLowerCase().replace(/\/+$/, "") === String(b || "").toLowerCase().replace(/\/+$/, "");
    const request = () => ({
      origin: origin.value.trim(),
      auth: auth.value,
      key: key.value || undefined,
      removeKey: removeKey.checked,
      remember: remember.checked,
    });
    const fail = (message) => { banner.hidden = false; banner.textContent = message; };
    const settle = (value) => { busy = value; connect.disabled = value; test.disabled = value; };
    const update = () => {
      const keyed = auth.value === "key";
      key.parentElement.hidden = !keyed;
      const here = same(origin.value, connection?.origin);
      const kept = here && connection?.key;
      keyNote.textContent = kept === "stored" ? "A key is kept in the keyring for this address: leave the field empty to keep it, or type another to replace it."
        : kept === "flag" ? "The key was given on the command line: it holds for this session only."
        : kept === "session" ? "A key typed earlier is held for this session only: type it again to remember it."
        : connection?.needsKey && here ? "This coordinator was remembered, but its key is not kept: type it again."
        : "A key is kept only for the address it is given at: another address asks for it again.";
      removeKey.parentElement.hidden = !(!keyed && kept === "stored");
      note.textContent = connection?.source === "flag" ? "This coordinator is set by the command line: a change holds for this session, and is remembered only when ticked."
        : connection?.keyring === false ? "The system keyring cannot be used: a key lasts for this session only." : "";
    };
    auth.addEventListener("change", update);
    origin.addEventListener("input", update);

    test.addEventListener("click", async () => {
      if (busy) return;
      settle(true);
      banner.hidden = true;
      test.textContent = "Testing…";
      try {
        const answer = await api("/api/coordinator/connection/test", request());
        note.textContent = answer.signedInAs ? `The coordinator answers, and the key signs in as ${answer.signedInAs}.` : "The coordinator answers.";
      } catch (err) {
        fail(err.message);
      }
      test.textContent = "Test";
      settle(false);
    });

    const form = el("form", {
      class: "dialog-form",
      onsubmit: async (e) => {
        e.preventDefault();
        if (busy) return;
        settle(true);
        banner.hidden = true;
        connect.textContent = "Connecting…";
        try {
          const answer = await onConnect(request());
          key.value = "";
          if (answer !== false) { close(); return; }
        } catch (err) {
          fail(err.message);
        }
        connect.textContent = "Connect";
        settle(false);
      },
    },
      el("div", { class: "dialog-field" }, el("label", { for: "d-coordinator-origin" }, "Address"), el("div", { class: "row" }, origin, test)),
      el("div", { class: "dialog-field" }, el("label", { for: "d-coordinator-auth" }, "Sign in"), auth),
      el("div", { class: "dialog-field" }, el("label", { for: "d-coordinator-key" }, "Key"), key, keyNote),
      el("label", {}, removeKey, " Forget the key kept for this address"),
      el("details", { class: "dialog-details" }, el("summary", {}, "Advanced"),
        el("label", {}, remember, " Remember on this machine (the key goes to the system keyring, never to a file)")),
      note,
      banner,
      el("div", { class: "row dialog-actions" },
        connection?.source && connection.source !== "none"
          ? el("button", {
            id: "d-coordinator-forget", type: "button", class: "ghost", title: "Disconnect, and forget the remembered coordinator and the key kept for its address",
            onclick: async () => {
              if (busy) return;
              try { await onForget(); close(); } catch (err) { fail(err.message); }
            },
          }, "Disconnect and forget")
          : null,
        el("button", { type: "button", class: "ghost", onclick: close }, "Cancel"),
        connect));
    fill(body, form);
    update();
    origin.focus();
  });
}

/**
 * Opens another folder: a path typed or one of the recent ones, tested on demand. A folder holding unsaved changes is refused
 * by the server, naming the cause; the dialog then offers to save them or to discard them, and does neither by itself.
 * @param {{ workspace: { root: string, start: string, recents: string[], canRemember: boolean, settingsWarning?: string | null },
 *   onTest: (path: string) => Promise<{ root: string, jobs: number }>, unsaved: () => boolean,
 *   onOpen: (request: { path: string, remember: boolean }, discard: boolean) => Promise<void>, onSave: () => Promise<boolean> }} opts
 */
export function openWorkspaceDialog(opts) {
  const { workspace, onTest, unsaved, onOpen, onSave } = opts;
  openDialog("d-workspace-dialog", "Open a folder", ({ body }) => {
    const pathInput = /** @type {HTMLInputElement} */ (el("input", {
      id: "d-workspace-path", type: "text", class: "mono", autocomplete: "off", "aria-label": "Folder", value: workspace.root, placeholder: "/path/to/folder",
    }));
    const remember = /** @type {HTMLInputElement} */ (el("input", { id: "d-workspace-remember", type: "checkbox", checked: workspace.canRemember, disabled: !workspace.canRemember, "aria-label": "Remember among the recent folders" }));
    const note = el("p", { id: "d-workspace-note", class: "hint" }, workspace.settingsWarning || "");
    const banner = el("p", { id: "d-workspace-error", class: "bad dialog-error", hidden: true });
    const choices = el("div", { id: "d-workspace-unsaved", class: "row dialog-actions", hidden: true });
    const submit = /** @type {HTMLButtonElement} */ (el("button", { id: "d-workspace-submit", type: "submit", class: "primary" }, "Open"));
    const test = /** @type {HTMLButtonElement} */ (el("button", { id: "d-workspace-test", type: "button" }, "Test"));
    let busy = false;

    const request = () => ({ path: pathInput.value.trim(), remember: remember.checked });
    const fail = (message) => { banner.hidden = false; banner.textContent = message; };
    const settle = (value) => { busy = value; submit.disabled = value; test.disabled = value; };

    // The folder the studio started on is offered beside the recent ones, never written among them.
    const known = [...new Set([...workspace.recents, workspace.start])].filter((p) => p && p !== workspace.root);
    const recents = known.length
      ? el("ul", { id: "d-workspace-recents", class: "dialog-list" }, known.map((p) => el("li", {},
        el("button", {
          type: "button", class: "small ghost mono", title: p, "aria-label": `Choose ${p}`,
          onclick: () => { pathInput.value = p; banner.hidden = true; note.textContent = ""; submit.focus(); },
        }, p))))
      : el("p", { class: "hint" }, "No recent folder yet: a folder you open and remember comes here.");

    const open = async (discard) => {
      if (busy) return;
      settle(true);
      banner.hidden = true;
      choices.hidden = true;
      submit.textContent = "Opening…";
      const held = !discard && unsaved();
      try {
        await onOpen(request(), discard);
        return;
      } catch (err) {
        fail(err.message);
        // Unsaved changes are refused first: a refusal while they are held is theirs, and the person chooses what becomes of them.
        if (err.status === 409 && held) choices.hidden = false;
      }
      submit.textContent = "Open";
      settle(false);
    };

    fill(choices,
      el("button", {
        id: "d-workspace-save", type: "button",
        onclick: async () => {
          if (busy) return;
          if (await onSave()) await open(false);
          else fail("The job is still not saved: save it from the page, or discard its changes.");
        },
      }, "Save, then open"),
      el("button", { id: "d-workspace-discard", type: "button", class: "ghost", onclick: () => open(true) }, "Discard changes and open"));

    test.addEventListener("click", async () => {
      if (busy) return;
      settle(true);
      banner.hidden = true;
      test.textContent = "Testing…";
      try {
        const answer = await onTest(request().path);
        note.textContent = `${answer.root} holds ${answer.jobs === 1 ? "1 job" : `${answer.jobs} jobs`}.`;
      } catch (err) {
        fail(err.message);
      }
      test.textContent = "Test";
      settle(false);
    });

    const form = el("form", { class: "dialog-form", onsubmit: (e) => { e.preventDefault(); open(false); } },
      el("div", { class: "dialog-field" }, el("label", { for: "d-workspace-path" }, "Folder"), el("div", { class: "row" }, pathInput, test)),
      el("div", { class: "dialog-field" }, el("span", { class: "hint" }, "Recent folders"), recents),
      el("details", { class: "dialog-details" }, el("summary", {}, "Advanced"),
        el("label", {}, remember, " Remember among the recent folders on this machine")),
      note,
      banner,
      choices,
      el("p", { class: "hint" }, "The page opens the folder's jobs, and the agent starts a new conversation."),
      el("div", { class: "row dialog-actions" },
        el("button", { type: "button", class: "ghost", onclick: () => closeActiveDialog() }, "Cancel"),
        submit));
    fill(body, form);
    pathInput.focus();
    pathInput.select();
  });
}
