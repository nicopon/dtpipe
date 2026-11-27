// The two short indicators of a pipeline, one rule for every page that shows them: what its deployment is, and whether it can be
// launched. `e` carries the pipeline's `id`, the `commit` of its job and its `bindingProblems`, as the library's list and
// `GET /api/library/{id}` both give them.
import { ago, deploymentOf, esc, healthOf, infoChip } from "./core.js";
// Why the bindings of the library's version do not hold: a node it names is absent or does not offer the brick, a brick has
// none, or its text is no longer the brick it was bound to. Shown for a pipeline that is not deployed; a deployed one speaks
// through its health.
function bindingsChip(e) {
  const problems = e.bindingProblems || [];
  if (!problems.length) return "";
  const blocking = problems.some((p) => p.blocking);
  return `<span class="chip ${blocking ? "bad" : "warn"}" title="${esc(problems.map((p) => p.message).join(" "))}">Bindings: ${problems.length} problem${problems.length > 1 ? "s" : ""}</span>`;
}

const HEALTH_WORD = { ok: "Healthy", warning: "Degraded", critical: "Blocked" };

// The one status chip of a row. A deployed pipeline says whether its armed version can be launched, as the coordinator's health
// read does (what a supervisor reads at /health); a pipeline that is not deployed says whether its bindings hold.
export function statusChip(e) {
  const d = deploymentOf(e.id);
  if (!d) return bindingsChip(e);
  const h = healthOf(e.id);
  if (!h) return "";
  const told = h.causes.filter((c) => c.severity !== "info").map((c) => c.message);
  const tone = h.status === "ok" ? "ok" : h.status === "warning" ? "warn" : "bad";
  return `<span class="chip ${tone}" data-health="${esc(h.status)}" title="${esc(told.join(" ") || "Can be launched.")}">${esc(HEALTH_WORD[h.status] ?? h.status)}</span>`;
}

// Whether the library holds another job or other bindings than the deployed ones, judged by content as the health read does
// (`library-newer`); before the first health read, the commit stands in.
function libraryIsNewer(e, d) {
  const h = healthOf(e.id);
  if (h) return h.causes.some((c) => c.code === "library-newer");
  return !!(d.commit && e.commit && d.commit !== e.commit);
}

const shortCommit = (commit) => (commit || "").slice(0, 8);

const DEPLOYMENT_WORD = { ready: "Deployed", deploying: "Deploying", failed: "Failed" };

// What a deployment is: the pipeline is armed at a commit, and a run uses that version, not the latest one in the library. The
// chip says when the library has moved on from it; the reason of a failed deployment is the server's.
export function deploymentChip(e) {
  const d = deploymentOf(e.id);
  if (!d) return `<span class="chip">Not deployed</span>`;
  const word = DEPLOYMENT_WORD[d.state] ?? d.state;
  const tone = d.state === "failed" ? "bad" : d.state === "ready" ? "ok" : "warn";
  const newer = libraryIsNewer(e, d);
  const armed = shortCommit(d.commit);
  const current = shortCommit(e.commit);
  const text = !newer ? word
    : armed && current && armed !== current ? `${word} ${armed} · library has ${current}`
      : `${word} · library has other bindings`;
  const problems = e.bindingProblems || [];
  const bindings = problems.length
    ? ` ${problems.some((p) => p.blocking) ? "Deploying the library version would be refused" : "Library bindings report"}: ${problems.map((p) => p.message).join(" ")}`
    : "";
  const title = d.state === "deploying" ? "The pipeline is being armed."
    : d.state === "failed" ? "The deployment failed."
    : `${armed ? `Armed at version ${armed}${d.deployedAt ? `, ${ago(d.deployedAt)}` : ""}. ` : ""}Runs use this version.${newer ? ` A later version is in the library: deploy again to use it.${bindings}` : ""}`;
  return d.state === "failed" && d.message
    ? infoChip(text, `chip ${tone}`, d.message)
    : `<span class="chip ${newer ? "warn" : tone}" title="${esc(title)}">${esc(text)}</span>`;
}
