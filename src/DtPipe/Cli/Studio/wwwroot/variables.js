// @ts-check
// The Variables tab: the ${{…}} the composed job reads, and what the studio launches it with.

import { api, el, status } from "./dom.js";
import { brickBranches, compose } from "./designer.js";
import { invalidate } from "./render.js";
import { sequence, state } from "./state.js";

const loading = sequence();

/** Reads the variables of a composed job. @param {{ yaml?: string } | null} result */
export async function loadVariables(result) {
  const ticket = loading.begin();
  try {
    const answer = await api("/api/variables", { path: state.job?.path || null, yaml: result?.yaml || "", bricks: brickBranches() });
    if (loading.stale(ticket)) return;
    state.vars = answer;
  } catch (e) {
    if (!loading.stale(ticket)) state.vars = { variables: [], aliases: [], error: e.message };
  }
  invalidate("drawer");
}

/** How many variables the job cannot run without. */
export const unreadyCount = () => state.vars.variables.filter((v) => !v.ready).length;

const launchOf = () => Object.fromEntries(state.vars.variables.filter((v) => v.kind === "env" && v.override).map((v) => [v.name, v.override]));

/** What follows a change of launch values: the rows held depend on them, so the places are asked again. @type {() => void} */
let afterLaunch = () => {};

/** @param {() => void} fn */
export function onLaunchChanged(fn) { afterLaunch = fn; }

/** @param {string} name @param {any} chosen */
async function launch(name, chosen) {
  const overrides = launchOf();
  if (chosen) overrides[name] = chosen; else delete overrides[name];
  try {
    await api("/api/launch", { path: state.job?.path, overrides }, "PUT");
    await loadVariables(await compose());
    afterLaunch();
  } catch (e) { status(e.message, "bad"); }
}

function describe(v) {
  if (v.kind === "keyring") return v.defined ? "in the keyring" : `not in the keyring: dtpipe secret set ${v.name}`;
  if (v.kind === "cursor") return "a cursor's state file: the run resolves it";
  if (v.override?.keyring) return `launched with the keyring alias ${v.override.keyring}${v.ready ? "" : " (missing)"}`;
  if (v.override) return "launched with a text typed here";
  return v.defined ? "set in the environment" : "not set: choose a value";
}

function variableRow(v) {
  const state_ = v.ready ? (v.kind === "cursor" ? "info" : "good") : "bad";
  const control = [];
  if (v.kind === "env") {
    const choice = v.override?.keyring ? `keyring:${v.override.keyring}` : v.override ? "text" : "";
    const select = el("select", { "aria-label": `Launch ${v.name} with` },
      el("option", { value: "", selected: choice === "" }, v.defined ? "the environment" : "nothing yet"),
      el("option", { value: "text", selected: choice === "text" }, "a text…"),
      state.vars.aliases.map((a) => el("option", { value: `keyring:${a}`, selected: choice === `keyring:${a}` }, `keyring: ${a}`)));
    const text = el("input", { type: "password", class: "mono", value: v.override?.value ?? "", placeholder: "value", hidden: choice !== "text", "aria-label": `Value of ${v.name}` });
    const reveal = el("button", { class: "small ghost", hidden: choice !== "text", title: "Show or hide the text" }, "show");
    select.addEventListener("change", () => {
      if (select.value === "text") { text.hidden = false; reveal.hidden = false; text.focus(); return; }
      launch(v.name, select.value ? { keyring: select.value.slice("keyring:".length) } : null);
    });
    text.addEventListener("change", () => launch(v.name, { value: text.value }));
    reveal.addEventListener("click", () => { text.type = text.type === "password" ? "text" : "password"; reveal.textContent = text.type === "password" ? "show" : "hide"; });
    control.push(select, text, reveal);
  }
  return el("tr", {},
    el("td", {}, el("span", { class: `dot ${state_}` }), " ", el("code", {}, `\${{${v.expression}}}`)),
    el("td", { class: "wrap" }, describe(v)),
    el("td", {}, control));
}

export function variablesView() {
  const vars = state.vars;
  if (vars.error) return el("p", { class: "bad" }, vars.error);
  if (!vars.variables.length) return el("p", { class: "hint" }, "This job reads no ${{…}} variable.");
  return [
    el("p", { class: "hint" }, "The job keeps each ${{NAME}}; what the studio launches it with stays on this machine, under .dtpipe/, outside git. A keyring value is read when a sample runs and never shown."),
    state.job ? null : el("p", { class: "warn" }, "Open or create a job to keep launch values."),
    el("table", { class: "vars" }, el("tbody", {}, vars.variables.map(variableRow))),
  ];
}
