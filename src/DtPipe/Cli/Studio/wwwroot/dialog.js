// @ts-check
// Modal dialog shell and focus capture/restoration helpers.
// Built with DOM calls only: no markup strings or inline styling are ever constructed here.

import { el, fill } from "./dom.js";

/** @type {{ dlg: HTMLDialogElement, opener: HTMLElement | null, onClose?: () => void } | null} */
let active = null;

export const shortCommit = (commit) => String(commit || "").slice(0, 8) || "none";

/** Captures the currently focused element when it is an HTMLElement. @returns {HTMLElement | null} */
export function captureFocus() {
  return document.activeElement instanceof HTMLElement ? document.activeElement : null;
}

/** Restores focus to a captured opener element if it is still connected and visible. @param {HTMLElement | null} opener */
export function restoreFocus(opener) {
  if (opener?.isConnected && opener.offsetParent !== null) opener.focus();
}

/** Closes the active modal dialog if one is open, restoring focus when safe. */
export function closeActiveDialog() {
  if (!active) return;
  const { dlg, opener, onClose } = active;
  active = null;
  if (dlg.open) dlg.close();
  dlg.remove();
  onClose?.();
  restoreFocus(opener);
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
  const opener = captureFocus();
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
