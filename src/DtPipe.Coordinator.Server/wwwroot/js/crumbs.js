// The path at the head of a sub-page: each step but the last links to the page above it, the last is the page itself. A step is
// `{ html, href }` with its `html` already escaped; one with no `href` is the page. The result is the page's `<h1>`, `tail` being
// what follows the path in it (a state chip, say).
export function crumbs(steps, tail = "") {
  const last = steps.length - 1;
  const parts = steps.map((s, i) => i === last || !s.href ? `<span class="here">${s.html}</span>` : `<a href="${s.href}">${s.html}</a>`);
  return `<h1 class="crumbs">${parts.join(`<span class="sep" aria-hidden="true">›</span>`)}${tail ? ` ${tail}` : ""}</h1>`;
}
