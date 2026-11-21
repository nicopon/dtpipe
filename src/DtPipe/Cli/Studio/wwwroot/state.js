// @ts-check
// What the page knows, in one object with one part per screen. A function changes a part and asks for the
// zones it touches to be drawn again (render.js); nothing outside a screen's module writes its part. The
// server holds the pins, the checkpoints and their validity: the page keeps what it was told and compares
// strings, it never decides what is still true.

/** @typedef {{ x: number, y: number }} Point */
/** @typedef {{ alias: string, kind: string, from: string[], ref: string[], yaml?: string, query?: string, brick?: string, transformers?: any[] }} Step */
/** @typedef {{ title: string, description: string, directives: string[], steps: Step[], layout: Record<string, Point> }} Model */
/** @typedef {{ yaml: string, steps: any[], errors: string[], warnings: string[], unanswered?: boolean }} Composed `unanswered`: a result a request got back when the studio gave no verdict */
/** @typedef {{ alias?: string, t?: number, edge?: { from: string, to: string, port: string } }} Selection */
/** @typedef {{ path: string, hash: string | null, rewrites: boolean }} Job */
/** @typedef {{ branch: string, at: string, card?: string, key?: string }} Watch */

/**
 * The cards of the open job. `selected` is a card, a transformer of the zoomed card (`t`) or a wire; `zoom` is
 * the alias of the transform step shown as the chain of its transformers. `history` is what Undo puts back, last change
 * last: the cards before a card was deleted or a text replaced them (`label`), whether they were unsaved and the hash the job
 * had then, and the cards as that change left them (`after`, layout aside). It holds job values, so it stays in the page:
 * no request carries it and nothing keeps it. `text` is what the person typed in the YAML tab, or null when it shows the
 * composed job: while it differs from the composed text the cards are held, so the two never say different things.
 * @typedef {{ label: string, model: Model, dirty: boolean, hash: string | null, after: string }} Change
 * @typedef {{ model: Model | null, result: Composed | null, selected: Selection | null, zoom: string | null, dirty: boolean, history: Change[], text: string | null }} Cards
 */

/**
 * The drawer's tab, the panes' sizes and the side panel the person chose: jobs, steps or none (closed); null until they choose,
 * and the panel then follows the page. A tab is yaml, problems, edit, preview, or one a host adds.
 * @typedef {{ drawer: string, sizes: Record<string, number>, side: "jobs" | "steps" | "none" | null }} View
 */

/**
 * What a preview, and a real run left suspended, show.
 *   rows: source rows asked; loaded: the server's answer for this job was read; real: a real run is suspended before
 *   its target is written, holding every row it read; watch: one pin per card, `card` being the card it sits on, which
 *   keeps its name when the branch that runs it is renamed; branches: { branch, transformers, cuttable } as the server
 *   plans them; places: the digest of what each (branch, read|end) depends on, by `branch#at`; shown: what each card
 *   showed last, by card; edited: the cards whose rows an edit made untrue; failed: the cards a failed branch runs;
 *   running: a preview is under way; rowCap: the most rows the engine reads of a source, as the last preview answered (0 before
 *   one); ranYaml: the text the last preview was for; draft: the name a job not saved yet
 *   keeps its pins under, handed to the job at its first save. queue: while a real run is suspended, one entry per branch
 *   that writes, `{ branch, key, rows }`, the key and the count being null when the run holds none of its rows; held: the rows
 *   of those keys, by branch, as the server gave them.
 * @typedef {{ rows: number, rowCap: number, loaded: boolean, real: boolean, watch: Watch[], branches: any[], places: Map<string, string>,
 *   shown: Map<string, { stage: any, digest: string | undefined, key?: string }>, edited: Set<string>, failed: Set<string>,
 *   running: boolean, notice: string | null, report: any, ranYaml: string | null, draft: string | null,
 *   queue: { branch: string, key: string | null, rows: number | null }[], held: Map<string, { stage: any, key: string }> }} Preview
 */

/** The real run of the saved job. phase: "confirm" (what it would write, to confirm), "running", "done". @typedef {any} Run */

/** How the targets are written: `review` stops the real run before every writer, to be read before it writes. @typedef {{ review: boolean }} Writing */

/**
 * The last publication's outcome ({ id, saved }, { id, conflict } or { id, refused }); the id and message being published;
 * and the last verdict of the runner's dry run, for the composed text it judged (`yaml`): `pending` while it runs, then
 * `ok` with the runner's `report`, or `error` when it could not run.
 * @typedef {{ yaml: string, pending: boolean, ok?: boolean, report?: string | null, error?: string }} PublicationCheck
 * @typedef {{ outcome: any, request: { id: string, message: string, previous?: string | null } | null, check: PublicationCheck | null,
 *   edges: EdgeVerdict | null }} Publication
 */

/**
 * What the coordinator's nodes would do with the edges of the composed text `yaml`, as it worded it: `pending` while it is asked,
 * then `deployable`, its `refusal` when the plan is not, and each judged edge (`protection` and `refusal` both null when it cannot
 * tell), or `error` when it did not answer. The page reads which fields are null and compares no level.
 * @typedef {{ producerFragment: string, consumerFragment: string, protection: string | null, refusal: string | null }} EdgeJudgment
 * @typedef {{ yaml: string, pending: boolean, deployable?: boolean, refusal?: string | null, edges?: EdgeJudgment[], error?: string }} EdgeVerdict
 */

/**
 * The rows a brick card asked its node for: the brick's key, `loading` while the node reads, then `done` (csv and
 * columns) or `failed` (error). One slot, replaced by the next read; the catalog keeps no schema, which depends on the rows.
 * @typedef {{ key: string, phase: "loading" | "done" | "failed", csv?: string | null, columns?: { name: string, type: string, isNullable: boolean }[] | null, error?: string }} BrickRows
 */

/** @returns {Preview} */
export const freshPreview = () => ({
  rows: 1000, rowCap: 0, loaded: false, real: false, watch: [], branches: [], places: new Map(), shown: new Map(), edited: new Set(),
  failed: new Set(), running: false, notice: null, report: null, ranYaml: null, draft: null, queue: [], held: new Map(),
});

export const state = {
  /** The job open: its path, the hash a save must find on disk (null for a job not written yet), and whether saving rewrites a text written by hand. @type {Job | null} */
  job: null,
  /** @type {Cards} */
  cards: { model: null, result: null, selected: null, zoom: null, dirty: false, history: [], text: null },
  /** @type {View} */
  view: { drawer: "yaml", sizes: { pal: 220, insp: 330, drawer: 260, agent: 360 }, side: null },
  preview: freshPreview(),
  /** @type {Run | null} */
  run: null,
  /** @type {Writing} */
  writing: { review: false },
  /** @type {Publication} */
  publication: { outcome: null, request: null, check: null, edges: null },
  /** @type {BrickRows | null} */
  brickRows: null,
  /** The variables of the composed job, by name, and the keyring's aliases: never a value the person did not type. @type {{ variables: any[], aliases: string[], error?: string, pending?: boolean }} */
  vars: { variables: [], aliases: [] },
  /** The components the palette offers. */
  components: { readers: /** @type {any[]} */ ([]), writers: /** @type {any[]} */ ([]) },
  /** The coordinator's catalog read last (its nodes and bricks) and this studio's version; empty with none. */
  catalog: { nodes: /** @type {any[]} */ ([]), bricks: /** @type {any[]} */ ([]), studioVersion: "" },
  /** The jobs of the folder, and the coordinator's library when a person is signed in. @type {{ folder: string, paths: string[], library: { shown: boolean, pipelines: any[] | null, error: string | null } }} */
  jobs: { folder: "", paths: [], library: { shown: false, pipelines: null, error: null } },
  /** The coordinator the command line named, or null: { url, state, reason, me, studioVersion, contract }. @type {any} */
  coordinator: null,
  /** The agent's panel: whether the command line opened one, the server's last answer (GET /api/agent), and what the agent gave
   * that waits for the person's Apply because they changed the cards since the agent read them (a plan, or the file it saved),
   * and the cards the agent's last plan adds, changes and removes against the cards it came to (`unknown` when either side does
   * not take apart into cards). `problem` is what the provider's list of models says is wrong with the agent before a first
   * message: its server does not answer, or the model the command line names is not among those it lists.
   * @type {{ enabled: boolean, status: any, behind: { yaml?: string, saved?: string } | null,
   *   marks: { yaml: string, unknown?: boolean, added: string[], changed: string[], removed: string[] } | null,
   *   problem: { short: string, text: string } | null }} */
  agent: { enabled: false, status: null, behind: null, marks: null, problem: null },
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
