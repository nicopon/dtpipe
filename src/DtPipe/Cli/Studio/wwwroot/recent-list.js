// @ts-check
// Reusable recent-entries list component with pick, key-kept pill, and delete confirmation.

import { el, fill } from "./dom.js";

/**
 * The recent values a dialog offers, most recent first: a row fills the form and does nothing else, and the × deletes the entry
 * after a confirmation. `note` says where forgetting is, above the list whatever it holds, and `empty` stands for no entry.
 * `remove(entry)` answers the entries that remain, and the list is drawn again without closing the dialog. The list grows with
 * what is kept and scrolls inside the dialog.
 * @template T
 * @param {{ id: string, entries: T[], text: (entry: T) => string, detail?: (entry: T) => string, kept?: (entry: T) => boolean,
 *   removable?: (entry: T) => boolean, note: string, empty: string, question: (entry: T) => string, pick: (entry: T) => void,
 *   remove: (entry: T) => Promise<T[]>, fail: (message: string) => void, isBusy?: () => boolean, setBusy?: (busy: boolean) => void,
 *   isClosed?: () => boolean, fallbackFocus?: () => void }} opts
 */
export function createRecentList(opts) {
  const host = el("div", { id: opts.id });
  let entries = opts.entries;
  let disabled = false;
  const draw = () => {
    const note = el("p", { class: "hint" }, opts.note);
    if (entries.length === 0) {
      fill(host, note, el("p", { class: "hint" }, opts.empty));
      return;
    }
    fill(host, note, el("ul", { class: "dialog-list" }, entries.map((entry, index) => {
      const text = opts.text(entry);
      const detail = opts.detail?.(entry);
      return el("li", {},
        el("button", { type: "button", class: "small ghost mono pick", title: text, "aria-label": `Choose ${text}`, onclick: () => opts.pick(entry) }, text),
        detail ? el("span", { class: "hint" }, detail) : null,
        opts.kept?.(entry) ? el("span", { class: "pill good", title: "A key is kept in the system keyring for this address" }, "key kept") : null,
        opts.removable?.(entry) ?? true
          ? el("button", {
            type: "button", class: "small ghost forget", disabled, title: "Delete this entry", "aria-label": `Delete ${text} from the recent ones`,
            onclick: async () => {
              if (opts.isBusy?.() || opts.isClosed?.()) return;
              if (!confirm(opts.question(entry))) return;
              if (opts.isBusy?.() || opts.isClosed?.()) return;
              opts.setBusy?.(true);
              try {
                entries = await opts.remove(entry);
                if (opts.isClosed?.()) return;
                draw();
                const picks = host.querySelectorAll("button.pick");
                if (picks.length) /** @type {HTMLElement} */ (picks[Math.min(index, picks.length - 1)]).focus();
                else if (opts.fallbackFocus) opts.fallbackFocus();
                else /** @type {HTMLElement | null} */ (host.closest("dialog")?.querySelector("input:not([disabled]), select:not([disabled]), button:not([disabled])"))?.focus();
              } catch (err) {
                if (!opts.isClosed?.()) opts.fail(err.message);
              } finally {
                if (!opts.isClosed?.()) opts.setBusy?.(false);
              }
            },
          }, "×")
          : null);
    })));
  };
  draw();
  return {
    element: host,
    update: (/** @type {T[]} */ next) => { entries = next; draw(); },
    setDisabled: (/** @type {boolean} */ value) => {
      disabled = value;
      for (const btn of host.querySelectorAll("button.forget")) /** @type {HTMLButtonElement} */ (btn).disabled = value;
    },
  };
}
