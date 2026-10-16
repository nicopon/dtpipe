// @ts-check
// What the page knows, in one object with one part per screen. A function changes a part and asks for the
// zones it touches to be drawn again (render.js); nothing outside a screen's module writes its part. The
// server holds the pins, the checkpoints and their validity: the page keeps what it was told and compares
// strings, it never decides what is still true.

/** @typedef {{ x: number, y: number }} Point */
/** @typedef {{ alias: string, kind: string, from: string[], ref: string[], yaml?: string, query?: string, brick?: string, transformers?: any[] }} Step */
/** @typedef {{ title: string, description: string, directives: string[], steps: Step[], layout: Record<string, Point> }} Model */
/** @typedef {{ yaml: string, steps: any[], errors: string[], warnings: string[] }} Composed */
/** @typedef {{ alias?: string, t?: number, edge?: { from: string, to: string, port: string } }} Selection */
/** @typedef {{ path: string, hash: string | null, rewrites: boolean }} Job */
/** @typedef {{ branch: string, at: string, card?: string, key?: string }} Watch */

/**
 * The cards of the open job. `selected` is a card, a transformer of the zoomed card (`t`) or a wire; `zoom` is
 * the alias of the transform step shown as the chain of its transformers.
 * @typedef {{ model: Model | null, result: Composed | null, selected: Selection | null, zoom: string | null, dirty: boolean }} Cards
 */

/**
 * The drawer's tab and the panes' sizes. A tab is yaml, problems, edit, preview, or one a host adds.
 * @typedef {{ drawer: string, sizes: Record<string, number> }} View
 */

/**
 * What a preview, and a real run left suspended, show.
 *   rows: source rows asked; loaded: the server's answer for this job was read; real: a real run is suspended before
 *   its target is written, holding every row it read; watch: one pin per card, `card` being the card it sits on, which
 *   keeps its name when the branch that runs it is renamed; branches: { branch, transformers, cuttable } as the server
 *   plans them; places: the digest of what each (branch, read|end) depends on, by `branch#at`; shown: what each card
 *   showed last, by card; edited: the cards whose rows an edit made untrue; failed: the cards a failed branch runs;
 *   running: a preview is under way; ranYaml: the text the last preview was for; draft: the name a job not saved yet
 *   keeps its pins under, handed to the job at its first save. queue: while a real run is suspended, one entry per branch
 *   that writes, `{ branch, key, rows }`, the key and the count being null when the run holds none of its rows; held: the rows
 *   of those keys, by branch, as the server gave them.
 * @typedef {{ rows: number, loaded: boolean, real: boolean, watch: Watch[], branches: any[], places: Map<string, string>,
 *   shown: Map<string, { stage: any, digest: string | undefined, key?: string }>, edited: Set<string>, failed: Set<string>,
 *   running: boolean, notice: string | null, report: any, ranYaml: string | null, draft: string | null,
 *   queue: { branch: string, key: string | null, rows: number | null }[], held: Map<string, { stage: any, key: string }> }} Preview
 */

/** The real run of the saved job. phase: "confirm" (what it would write, to confirm), "running", "done". @typedef {any} Run */

/** How the targets are written: `review` stops the real run before every writer, to be read before it writes. @typedef {{ review: boolean }} Writing */

/**
 * The last publication's outcome ({ id, saved }, { id, conflict }, { id, refused } or, while the person chooses the
 * node of the bricks several nodes offer, { id, choosing }); the id and message being published; the node chosen for
 * each brick branch, by alias.
 * @typedef {{ outcome: any, request: { id: string, message: string } | null, bindings: Record<string, string> }} Publication
 */

/** @returns {Preview} */
export const freshPreview = () => ({
  rows: 1000, loaded: false, real: false, watch: [], branches: [], places: new Map(), shown: new Map(), edited: new Set(),
  failed: new Set(), running: false, notice: null, report: null, ranYaml: null, draft: null, queue: [], held: new Map(),
});

export const state = {
  /** The job open: its path, the hash a save must find on disk (null for a job not written yet), and whether saving rewrites a text written by hand. @type {Job | null} */
  job: null,
  /** @type {Cards} */
  cards: { model: null, result: null, selected: null, zoom: null, dirty: false },
  /** @type {View} */
  view: { drawer: "yaml", sizes: { pal: 220, insp: 330, drawer: 260 } },
  preview: freshPreview(),
  /** @type {Run | null} */
  run: null,
  /** @type {Writing} */
  writing: { review: false },
  /** @type {Publication} */
  publication: { outcome: null, request: null, bindings: {} },
  /** The variables of the composed job, by name, and the keyring's aliases: never a value the person did not type. @type {{ variables: any[], aliases: string[], error?: string }} */
  vars: { variables: [], aliases: [] },
  /** The components the palette offers. */
  components: { readers: /** @type {any[]} */ ([]), writers: /** @type {any[]} */ ([]) },
  /** The coordinator's catalog read last (its nodes and bricks) and this studio's version; empty with none. */
  catalog: { nodes: /** @type {any[]} */ ([]), bricks: /** @type {any[]} */ ([]), studioVersion: "" },
  /** The jobs of the folder, and the coordinator's library when a person is signed in. @type {{ folder: string, paths: string[], library: { shown: boolean, pipelines: any[] | null, error: string | null } }} */
  jobs: { folder: "", paths: [], library: { shown: false, pipelines: null, error: null } },
  /** The coordinator the command line named, or null: { url, state, reason, me, studioVersion, contract }. @type {any} */
  coordinator: null,
};

/**
 * One counter per kind of request. `begin` hands out a ticket; `stale` says a later one was handed out, so the
 * answer in hand is dropped. Two requests of different natures never share a counter: a stale answer of one must
 * not drop the answer of the other.
 */
export function sequence() {
  let last = 0;
  return {
    begin: () => ++last,
    /** @param {number} ticket */
    stale: (ticket) => ticket !== last,
    /** Makes every ticket handed out so far stale. */
    cancel: () => { last++; },
  };
}
