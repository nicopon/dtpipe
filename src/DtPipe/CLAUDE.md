# DtPipe (CLI) — parsing, options, sample mode, checkpoints, MCP, agent and studio

Mechanics for editing the CLI project. The rules themselves are in the root `CLAUDE.md`.

## From args to a DAG

1. `JobService.BuildSubcommands()` registers the named subcommands into `System.CommandLine`.
2. `FlagRegistryFactory.Build(serviceProvider)` assembles a `FlagRegistry` from `[ComponentOption]`
   providers plus stream-processor trigger flags. `PipelineLexer.Parse(args)` → `ParsedPipeline`
   (`BranchSpec[]` with `ReaderArgs` / `PipelineArgs` / `WriterArgs`).
   `PipelineToJobConverter.Convert(parsed, …)` → `(Dictionary<string, JobDefinition>,
   JobDagDefinition)`.
3. Linear: `LinearPipelineService` → `ExportService.RunExportAsync()` → `PipelineExecutor`.

Implicit branch split — exactly three tokens:
- `-i` / `--input` — when an input or job file was already seen in the current branch;
- `--from <alias[,alias...]>` — when a `--from`, `--input` or `--job` was already seen (the first
  `--from` in a fresh branch stays in it);
- `--job` / `-j <file>` — when a job file or input was already seen.

Neither `--sql` nor boolean processor flags (`--merge`) split. Each processor declares its trigger
flags via `IStreamTransformerFactory.CliTriggerFlags`. `--job` loads a YAML job and applies extra CLI
flags as overrides; `--export-job <file>` serialises the CLI pipeline through `JobFileWriter` and
exits without running.

## Options

Provider wiring: `RegisterReader<T>()` / `RegisterWriter<T>()` / `RegisterStreamTransformer<T>()` in
`Program.cs`. `CliProviderFactory<T>` wraps descriptors; `CliOptionBuilder.GenerateFlagDefsForType`
reflects on `[ComponentOption]`; `FlagBinder.Bind(options, args, registry)` binds at execution.
Options live scoped in `OptionsRegistry`, keyed by type.

`TransformerPipelineBuilder.CollectGroups` is the single source of transformer steps, shared by the
live run and `--export-job`. Consecutive flags of one factory are **one step**, so an option reaches
every trigger value before and after it; a trigger that is not repeatable opens a new step. Two
values for one scalar option in a step are refused unless equal. A flag several factories declare
(`--skip-null`) binds only through the factory in context. Derive each rule from `FlagArity` and the
declared owners, never from a transformer's name. `[CI: OrderedPipelineTests]`
`[local: validate_doc_examples.sh]`

A flag that binds nothing must fail, never exit 0 with no effect. Two causes look identical from
outside — tell them apart first:
- **The component does not own the option**: `PipelineToJobConverter.RejectFlagsThatBindToNothing`
  refuses it and names the options the component accepts.
- **The option is owned but not written**: `OptionBinder.ConvertValue` has no conversion for its
  type. A new option CLR type goes into `ConvertValue` **and** into the sample generator of
  `CliOptionBindingTests`, which fails on a type it cannot sample.

`[CI: CliOptionBindingTests — catalogue-wide]`

## Sample mode — mechanics

- **`ISampleTap`** — an observation point offered each stage's output where `ReportTransform` is
  already called. Read-only; never disposes or retains a `RecordBatch`. Stage 0 is the reader, 1..n
  the transformers in pipeline order.
- **`SampleModeSink`** — two decorators selected by the real writer's **capability**, the shape
  `CursorTracking{Row,Columnar}Decorator` uses. The engine reads row-vs-columnar mode off
  `writer is IColumnarDataWriter`, so a sink of the wrong kind changes segmentation and the bridge
  count. A sink taking both shapes does not escape this: `PipelineExecutor` tests the columnar
  contract first, at the entry and at the writer boundary, so `NullDataWriter` pulls a row-mode
  pipeline into Arrow and back out.
- **`SampleRun`** — the capture, read by both the renderer and the checkpoint store.
- **The sink replaces a writer that leaves the process, never the channel between two branches.**
  `ExportRunState.NeutralisesWriter` reads `WriterFactory.CapabilityKind`, the capability the
  orchestrator routes by: a channel writer is initialised, fed and completed as in the real run, or
  the branch reading it waits on a stream nobody closes. Its sample still keeps the tap and the
  safety gate, and still advances no cursor.
- **`--dry-run N` bounds a source only.** `IStreamReaderFactory.IsSource` is false for a channel
  reader, a processor and a checkpoint; `EffectiveLimit` leaves those at `--limit`, so a one-to-many
  expansion upstream arrives whole. The tap sits behind the bound in every path, direct transfer
  included: the reader stage counts what the run read.

Sample mode also suppresses `ValidateAndMigrateAsync` (it can CREATE/ALTER the target), **all four
hooks** (SQL on the target connection), the cursor and the metrics file.

### Safety is a read-side problem

A reader can mutate: `DELETE … RETURNING`, `… OUTPUT`, `--duck-init`, an `ATTACH` inside `--sql`.
`--limit` bounds what the client reads, never what the server already destroyed.
`SampleModeSafetyGate` classifies the **resolved** pipeline's source SQL (so a `@file` query is
covered), and `ISqlDialect.ReadOnlySessionSql` lets the server refuse
instead of a regex guessing.

Never classify writer hooks: they are already suppressed, so refusing them adds no safety and trains
users to pass `--allow-destructive` by reflex, which unlocks the source side too.

SQL Server returns `null` for `ReadOnlySessionSql`: `ApplicationIntent=ReadOnly` routes to a replica
and does not make a session read-only. The report states which guarantee the run had.

A real run (`execute-yaml-job`, the studio's run) is checked by `JobSqlSafety` on the **parsed** job,
with the same rule and policy: the SQL of each provider block (a reader's query, a writer's hooks, a
stream processor's text values) and the network reach of each endpoint, never the YAML text, a key, a
comment or a transformer option. A real run executes the writer's hooks, so they are classified there.
The "what is SQL" rule lives once, in `SampleModeSafetyGate.SqlBearingValues`.

`[CI: SampleModeSafetyGateTests, SampleModeDagTests, JobSqlSafetyTests, validate_sample_safety.sh]` `[unchecked: a query that writes
through a function — SELECT my_function() passes a verb scan, hence the server-enforced form]`

## Checkpoints — mechanics

`--checkpoint` tees the columnar stream into the session store; `--from-checkpoint` reads it back.
The key is a hash of the branch prefix's **definition**, never the alias: two pipelines in one
directory cannot collide, and an unchanged prefix is reused. `ExportRunState.ComputeCheckpointKey`
passes what the resolved run knows (component, sanitised connection, query, `EffectiveLimit`,
sampling) and `PipelineOptions.DefinitionFingerprint`, computed by `BranchFingerprint` from the job as
written: transformer configuration, a processor's options and, recursively, the fingerprint of
each branch it reads. A plain `from:` takes its upstream's fingerprint alone, a processor takes
(alias, fingerprint) pairs because its query names them. `JobService` computes the whole DAG
before it starts, since branches run concurrently and an upstream without `--checkpoint` computes
no key. A non-source reader (`IStreamReaderFactory.IsSource`) contributes no connection: its route
is a channel alias. Pass a new part of a branch's definition through the fingerprint, never through
a runtime handoff between branches.

Encryption (AES-GCM) does not buy confidentiality at rest — the key is on the same disk. It buys two
properties of the **store as a whole**: an inert copy, and a purge made reliable by destroying the
key. One cleartext session would void both for every session, retroactively.

**A row-mode pipeline gets a bridge, not a refusal.** When materialising and the last segment is not
columnar, `ExecuteSegmentedPipelineAsync` appends an *empty* columnar segment — the device used for a
row reader feeding a columnar writer — so the chain reaches Arrow at the writer boundary, tees, and
bridges back. Add that segment **only** when `materialise is not null`, so an ordinary run is
unchanged.

`[CI: CheckpointCipherTests, CheckpointKeyTests, CheckpointRoundTripTests; CheckpointKeyIdentityTests
for the caller of `Compute`, BranchFingerprintTests]`
`[local: validate_checkpoint.sh]`

## MCP server and agent

`dtpipe mcp` (STDIO) exposes schema discovery, validation and execution (`execute-yaml-job` is
dry-run by default). Tool table: `REFERENCE.md#mcp-server`; options: `REFERENCE.md#agent-guardrails`.
`dtpipe agent` runs an interactive loop against Ollama/OpenAI (`AgentExecutor`, `AgentTui`,
`OllamaClient`). Agent classes live under `Cli/Agent/`.

Hardening invariants — fail-closed, non-negotiable:
- **F1 Planner/Executor split** — `--mode plan` (default) hides every tool marked `[WritesToTarget]`.
  The set is reflected off the attribute, never listed. `dry-run` stays unmarked: it runs the real
  pipeline with the writer neutralised, and the planner's role prompt tells it to call it.
  `[CI: AgentModeTests — whole catalogue]`
- **F2 Guardrails** — `ISqlSafetyPolicy` (destructive verbs / network) + `IApprovalGate`; a write
  needs `apply` + approval + a clean check.
- **F3 Determinism** — available, not default: `--temperature 0 --seed N --repeat N`;
  `DeterminismReport` variance = distinct YAML − 1. Default temperature stays `1`: greedy decoding
  degenerates weak quantised models. `--seed` keeps a run replayable at any temperature.
- **F4 Non-destructive context** — fact cache + `ConversationWindowManager.Compact`.
- **F5 Parallel tools** — every `ToolCall` of a turn runs (`Task.WhenAll`; `--sequential` forces
  serial).
- **F6 Single YAML path** — the `yamlContent` tool argument is the sole plan source.
- **F7 CI gate** — `tests/agentic/analyze-traces.sh --gate` fails on unhandled MCP errors or a failed
  mission. Its variance criterion applies only when `variance_results.jsonl` holds real replication
  data, which the shipped missions do not produce. **Never record a placeholder variance**: a
  criterion that cannot fire is worse than none. The authoritative signal for F1–F7 is the unit
  suite (`Unit/Cli/Agent*Tests`, `Unit/Cli/Mcp*Tests`).

Mandatory MCP directives:
1. Reflect help from `[Description]` / `[ComponentHelp]`; never hardcode it. `[unchecked — nothing
   tests GetGeneralHelp]`
2. Execute in memory via `JobFileParser` + `JobService.ExecutePipelineAsync()`; no temp files or
   shell proxies. `[unchecked]`
3. Discover tables on `inspect` without a query; give actionable hints on validation errors.
   `[unchecked]`
4. Fail closed: default `apply=false`, reject on ambiguity. `[CI: ExecuteYamlJobGuardrailTests]`

### One run, one report collection

The agent runs the tools of a turn in parallel (F5), so two runs overlap. `JobRunner` is the only
caller-facing way to run a resolved job (MCP, `split`, `contract`, the studio): it arms a `RunReports` for the run
through `SampleReportCollector.Begin`, around the whole `ExecutePipelineAsync`, and returns a
`RunOutcome` (exit code, branch failures, reports, checkpoint keys). `SampleReportCollector` is the
singleton the engine and the observer publish to; it forwards to the collection the calling async flow
armed, branches included, and drops everything when none is armed. Never read it from a caller, and
never arm it from an `async` method that returns before the run ends: the value is gone with the frame.
`dtpipe mcp` serves until it is stopped, so a signal is its only end and it exits 130.

`JobAnalysis` (`Cli/`) is the matching path for a job text: parse, validate, judge the SQL it carries
(`JobSqlSafety`), sample, resolve for a run. MCP and the studio call it; `DtPipeMcpTools` keeps only the tool
surface and the wording written for a model. A refusal that names a tool of this server goes through `JobHints`, so
a caller that has none gets the refusal without the pointer. Never copy a validator into a caller.
`[CI: ConcurrentRunCollectionTests, SampleReportCollectorTests, SampleModeDagTests, McpCommandTests]`

### The session trace

`dtpipe agent --trace <path>` (or `$DTPIPE_AGENT_TRACE`) records a session as JSON lines: what the
model was **given** (the role prompt the mode selected, the tool catalogue as offered), every step,
each turn's verdict, and any `/note` left by the person watching. Keep the first two: without them a
wrong tool call cannot be told apart from a tool never offered, or one whose description misled.

Never gate on its verdict: a fail-closed criterion over an LLM loop needs an attributable signal this
file does not provide. Arguments and results pass through `ConnectionStringSanitizer`, which blanks
only the shapes it recognises; the file header says so rather than claiming the file is safe.

To diagnose a failing session: read the trace, verify the claim in the source, run the fix before
publishing it. Look first at what dtpipe tells the model about itself.

## Studio

`dtpipe studio [folder]` (`StudioCommand`, `Cli/Studio/`) serves a local page over a folder's YAML
jobs and runs them through `IStudioEngine` (`StudioTools.Engine`, never null: a test double implements all of
it). `StudioEngine` samples and validates through `JobAnalysis`, the path the `dry-run` tool shares: never a
second sample path, and runs through `JobAnalysis.ResolveForRun` and `JobRunner`. `StudioServer` runs
one engine action at a time (409 otherwise), as the debug state and the quit require. Kestrel is referenced
for this server alone.

The page is the designer: `DesignService` (`DtPipe.Jobs`, the coordinator's own model) composes the
cards and takes a job apart, never the page. `ComponentEndpointRules` gives it dtpipe's readers and
writers through `JobCutter.OwnersOf`, so a source or sink card holds the blocks a cut would put on
that side, and `BranchSides` names the keys of each side for both.

The page runs dtpipe with the person's access, so the server is guarded like a shell:
- **No cookie.** The token rides in the URL fragment, the page keeps it in `sessionStorage` and sends
  it in `X-Dtpipe-Studio` on every `/api` call; cookies are shared by every port of a host.
- **Only its own address.** `Host` must be `127.0.0.1` or `localhost` at its port (DNS rebinding);
  another `Origin`, or `Sec-Fetch-Site` other than same-origin, is refused; no CORS header, ever.
- **No inline script or style**, and every value from a job or a source set as text in the page,
  never as HTML: a sampled value is untrusted. The page builds nodes (`el`, `fill`, `svg` in
  `dom.js`) and positions them through the CSSOM; `replaceChildren` takes nodes, never a list.
- **The folder only.** `StudioFolder` reads `.yaml`/`.yml` files under it: no segment starting with
  a dot, no symbolic link below it. The empty builder reads no `appsettings.json` from the folder.
- **The server writes, from cards.** `PUT /api/job` composes the page's cards and writes that text
  (`WriteJob`): whole and atomic, only where a job could be read, into a folder that exists, against
  the hash the page opened (409 otherwise), never over a file with a new job. What the studio keeps
  for a job (`StudioLocalFiles`) lives under `.dtpipe/studio/`, which `SessionStore.EnsureIgnored`
  keeps out of git; nothing there is reached through a link, and a link refuses the save before the
  job is written.
- **A variable's name is the contract.** `StudioVariables` reads a job's `${{…}}` with the run's own
  grammar (`VariableTokens` in `DtPipe.Jobs`), and an action arms each chosen value for
  its own call chain (`LaunchValues.Arm`, an `AsyncLocal` like `NonInteractiveGuard`) which
  `LaunchValueInterpolator` gives the real resolver ahead of the environment, in the one chain
  `CompositeStringContentResolver.Standard` builds for the DI and for `JobFileParser`. Nothing writes the process environment, so a request beside the action never
  resolves its values; the Variables tab reads the real environment only.
  `[CI: StudioServerTests, StudioSampleTests — the values reach every branch of a DAG]` The
  launch file holds an alias or a typed text, never a keyring value; the page sees names and
  aliases only. The keyring is opened only for a job that needs it: opening it can ask the person.
- **The command line names the coordinator, never the page.** `--coordinator <url>` gives the studio
  a `CoordinatorLink` to that origin: the coordinator's metadata (RFC 9728) and contract number, its
  provider's discovery (issuer equal, no HTTPS downgrade), then authorization code with PKCE through
  a new tab, back to `/callback`, the one path reached without the session's token: a GET, the
  `Host` check kept, a single-use `state` that a wrong one leaves waiting, `iss` checked when sent.
  The tokens live in the process only, never in a response or the page; the access token is renewed
  two minutes ahead, a 401 renews once and sends once, a refused renewal signs out with its reason.
  The link's client sends to the coordinator's origin only, follows no redirect, and always carries
  the CSRF header.
- **Without `--coordinator` the studio is whole, minus the bricks, the library and the publication.**
  `StudioTools.Coordinator` is null: `/api/coordinator` has no address, `/api/bricks` an empty catalogue,
  `GET /api/publish` no publication, and `/api/coordinator/login`, `/api/library`, `/api/library/open` and
  `POST /api/publish` answer 404 "opened without --coordinator". Jobs, cards, variables and samples never
  ask for the link; a route that needs it refuses the same way.
  `[CI: StudioServerTests — jobs, cards, variables, samples and the four refusals]`
  `[unchecked: import, debug and run without a coordinator; that a new route needing the link refuses]`
- **The coordinator decides what a brick is; the studio reads its answer.** Before taking a job
  apart, the studio sends it to `/api/bricks/recognize` and gives `DesignService` a `StudioBricks`
  that finds each answer again by the top-level branch's YAML, a key and never a matching rule
  (`Match`/`Contained` see each top-level branch whole: `StudioBrickTests` pins it). Nobody signed
  in, or no answer: no branch is a brick, and each opens as a local card. `Find`/`BranchOf` read the
  catalog read last, kept after a sign-out, so a brick card still composes and saves. **Only a
  branch whose input or output names a variable a node's brick announces is sent** (`ForRecognition`):
  a branch of this machine, a connection string included, never leaves it, and a brick it misses
  opens as a local card. `[CI: StudioBrickTests]`
- **A brick samples here through the run's own binding.** `BrickSampling` turns a source brick's
  branch into `input: arrow:-` and a sink brick's into `output: arrow:-`; `SampleAsync` binds them as
  `--bind-input`/`--bind-output` do (`AliasBindingApplier`, before validation): never a second
  binding path. A source's rows come from its node through the coordinator, at most
  `CoordinatorApi.MaxSampleRows`; a sink's end in a throwaway file. Those files live under
  `.dtpipe/studio/samples/`, created owner-only, deleted when the sample ends and purged when the
  studio starts, never reached through a link. A brick's branch is out of the Variables tab: its node
  resolves those.
- **A publication binds a brick to a node only when one node offers it or the person chose it.** `/api/publish` asks the
  coordinator to recognise the saved job (`CoordinatorLink.RecognizeAsync`) and sends `LibrarySave.Bindings`, a `Binding` that
  names only the node (the brick is the coordinator's to resolve from the job's text), for each branch that is a brick offered
  by exactly one node, then the nodes the page sends as `bindings`, each of which must offer that brick (400 otherwise). A brick
  several nodes offer and nobody chose is left unbound, since the studio never picks a node for a person, and the coordinator's
  answer lists it. `GET /api/bindings?path=` gives the page the nodes that offer each brick branch of the saved job; the publish
  tab shows one selector per brick when some brick has several. `[CI: StudioPublishTests]` `[unchecked: the page's selector]`
- **A publication sends the job saved in the folder, nothing beside it.** `/api/publish` reads the
  file again and refuses one whose hash is not the one the page saved; the layout and the launch
  file stay on this machine. The first publication of an id creates only (`If-None-Match: *`); a
  later one replaces only the commit published last from here (`<job>-publish.json`, under
  `.dtpipe/studio/`), and a 412 is replaced only when the person names the commit in place. A 422 is
  the coordinator's check refusing the job: the report is shown and nothing was published.
- **A command line imports through `--export-job`'s own path.** `POST /api/import` takes the
  arguments as the array the page split (never one string to cut here) and answers the text
  `CommandLineJob.Export` gives, which `Program.cs` shares for its refusal of `--bind-input`/
  `--bind-output`; a `${{…}}` stays as typed, and nothing is saved until the person saves the new
  job. `[CI: CommandLineJobTests, StudioServerTests]`
- **A library pipeline opens as a new file, never over one.** `POST /api/library/open` writes
  `<id>.yaml` through `WriteJob` (create only, 409 when the file exists) and keeps the version read in
  `<job>-publish.json`, so the next publication sends `If-Match` for it; a link under `.dtpipe/`
  refuses before the job is written. `[CI: StudioPublishTests]`
- **A run is the saved file, confirmed first, one engine action at a time.** `POST /api/run/prepare`
  re-reads the file (its hash is the page's), refuses a job that reads a brick (its rows would
  recreate a real target), resolves every variable and lists what each branch writes: the output
  through `ConnectionStringSanitizer.Redact`, the writer's block as the file holds it (never
  interpreted), a sink brick as a throwaway file. Its digest, an HMAC of the text and the launch
  values by name under the session's token, is what `POST /api/run` must carry: another text or
  another value is a 409. The run goes through `IStudioEngine.RunAsync`: `ResolveForRun` validates the
  job, then the SQL safety policy judges it, and only then `JobRunner` runs it. It holds the sample's
  semaphore (409 for a sample, a run or a quit beside it), and reports each branch's failure through `ConnectionStringSanitizer.Sanitize`.
  **A stop asked while the run was unfinished ends in 130**, whether the engine threw (a linear
  job) or answered 1 or 0 (a DAG); a run finished before the stop keeps its code and a later stop
  is a 409. `[CI: StudioRunTests]` `[local: studio_check.py]`
- **A preview is the real sample of the job, cut only at the pinned cards, never the job's run.** A
  watcher (the page's pin) sits at a card's read (level 0) or at the end of its branch. `DebugCutter` cuts a branch
  there into mini-branches joined by `from:` (`__level` is reserved), each with a checkpoint in the
  studio's own session, through `JobCutter.Cut` and `BranchSides`: the head keeps the reader and what
  bounds it, the last piece keeps the branch's alias, output and writer side. With no watcher nothing
  is cut; a branch with a processor, no transformer or one `JobCutter` refuses runs whole and keeps
  an end checkpoint only. A run stops at a card (`upTo`) and drops what the stop does not need (`Dropped`),
  reading each branch it needs back from the highest level held (`Unread`: no input left to bind).
  **A stop is for its run only**: it is not added to the watchers, and the checkpoint it wrote is deleted
  unless a level held has the same key. It runs through `SampleCoreAsync` (bricks bound as a sample binds them),
  the bound applies to each source alone (at most `DebugMaxRows`, a thousand rows shown per stage) and a
  checkpoint read back is not bounded again. With no stop the run reaches the end of the job: only the branches
  that write or that nothing reads start it, so a branch whose reader reads a checkpoint back is left out.
- **The server says where a card's rows are; the page never rebuilds the cut's names.** The view lists
  `places`, one per branch read and end, with the digest of what its rows depend on and, after a run, the `alias` of
  the piece of the report that holds it (first stage for a read, last for an end) or null when the run did not
  produce it (read back from a level held, upstream of a stop). A null place keeps the rows it showed, while
  its digest is unchanged. `POST /api/debug/plan` answers the places of a text and nothing else: pure, no engine
  slot, no file. After an edit the page compares digests as strings and clears the rows they no longer
  support. `[CI: StudioDebugTests]`
- **A staged run is the job's own run, and nothing leaves the process before its last stage.**
  `POST /api/run/prepare` and `POST /api/run` take `debug` and `review`; the saved file only, and the
  digest also binds the mode, the watchers and the keys held. `DebugCutter.Cut(write: true)` makes the
  cut at the pinned cards. With `review` (`beforeWriters`) every branch that has an output ends at its
  end with a checkpoint and no writer (upstream branches included; one that runs whole keeps its processor and
  loses only the writer's keys and blocks, `WithoutWriter`; a job no branch of which writes is refused), because the engine neutralises nothing
  outside a sample. The cursor and its state file move with the piece that writes the branch
  (`JobCutter` puts them on the head, which would advance them for rows no target received). The
  engine reports the key of each committed checkpoint through `RunOutcome.Checkpoints`. A level of a real run is keyed to every row and to a salt (an HMAC
  under the session's token of the launch values and the state files a cursor reads), so it serves no sample and no
  other values; a write that reached the end of the job deletes the levels that fed it, one that stopped
  or failed keeps what it completed as `real` (`DebugState.Real`), and until it is continued or abandoned
  (`resetAll`) a sample, a change of watchers and any run but its continuation are a 409.
  **A review's holds are the run's, never pins**: they are watchers for the run only, leave `state.Watch`
  before it is saved, and `Judge` keeps a held end of a writing branch without a pin while `Real`. The
  continuation is refused (409) unless every writing branch holds its end, so no row is written unread.
  `view.queue` lists, while `real`, every writing branch of the saved file with its key and the rows
  counted once when the review ended (`HeldLevel.Rows`, a count in the clear file, never a row).
  `POST /api/debug/rows` reads the first rows of a held key, the only way a person sees them before the
  write. A failed last stage leaves the target as its writer does in a plain run, and the held rows for a
  retry. `[CI: StudioStagedRunTests]` `[unchecked: the page, a writer other than the local ones tried]`
- **A stop is a new run, never a paused process.** A review is a run that ends before its writers; the
  continuation reads the checkpoints back from the highest level held and writes. A watcher has a branch,
  a place and the card it sits on, nothing else.
- **A level's digest comes from the job as written, never from the cut.** It hashes the branches the
  level reads, the reader side, the transformers up to it and the rows asked, so pinning another card
  invalidates nothing. A launch value is not in a sample's, but is in a real run's (the salt).
- **A checkpoint is valid or does not exist.** `POST /api/debug` and `POST /api/debug/state` settle the
  watchers and held levels against the text under the engine slot, delete from the session what the
  text, the rows or a reset no longer supports (a reset takes everything downstream with it), and
  answer what is held; the page never decides validity. `.dtpipe/studio/<job>-debug.json` keeps the
  watchers and each held level's key and digest (and, for the end of a branch a review holds, its row count), never the text, and does not exist for a job that
  holds none; a job not saved yet keeps it under
  `drafts/<id>`, adopted at its first save and purged when the studio starts. The cut is never saved
  or published. `[CI: StudioDebugTests]`
- **One state, one drawing, one counter per request; nothing the browser keeps is true.** The page
  keeps what it knows in `state` (`wwwroot/state.js`), one part per screen, and only the module of a
  screen writes its part. A change names the zones it touches (`invalidate`) and `render.js` draws
  them once, in a fixed order; a zone holding what the person is doing (the inspector's focused field,
  the drawer's scroll) is invalidated by what makes it untrue, never by its own events. Each kind of
  request has its own `sequence()`, so a stale answer of one never drops the answer of another, and an
  answer about a job is dropped once another job is open. The page keeps nothing in `localStorage`:
  how it was left (tab, rows, selected card, pane sizes) goes through `PUT /api/view` to
  `.dtpipe/studio/view.json`, whose values the server checks and rewrites, and which is read only as a
  preference. A new script needs its route in `StudioServer.Assets`.
  `[CI: StudioServerTests (every module is served, every import resolves; the view's bounds)]`
  `[local: tests/studio/page_smoke.mjs - Node 22+, Chrome and a built dtpipe; types are checked by the
  editor alone (// @ts-check), no tool runs them]`
- Quit from the page exits 0, Ctrl+C 130. A quit is refused while a run goes on; Ctrl+C cancels
  the run and the studio then exits.

`[CI: StudioServerTests (no script writes HTML or an inline style), StudioSampleTests,
StudioCoordinatorTests, StudioBrickTests, StudioPublishTests, StudioRunTests, StudioDebugTests, DesignServiceTests, LocalEndpointCardTests]` `[unchecked: the page's behaviour beyond page_smoke.mjs - no CI test drives a
browser]`
