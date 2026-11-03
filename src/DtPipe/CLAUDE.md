# DtPipe (CLI) — parsing, engine and host, options, sample mode, checkpoints, MCP, agent, studio

Mechanics for editing the CLI project. The rules themselves are in the root `CLAUDE.md` and `.claude/rules/engine-invariants.md`; those of MCP
and the agent are in `.claude/rules/mcp-and-agent.md`.

## From args to a DAG

1. `JobService.BuildSubcommands()` registers the named subcommands into `System.CommandLine`.
2. `FlagRegistryFactory.Build(serviceProvider)` assembles a `FlagRegistry` from `[ComponentOption]`
   providers plus stream-processor trigger flags. `PipelineLexer.Parse(args)` → `ParsedPipeline`
   (`BranchSpec[]` with `ReaderArgs` / `PipelineArgs` / `WriterArgs`).
   `PipelineToJobConverter.Convert(parsed, …)` → `(Dictionary<string, JobDefinition>,
   JobDagDefinition)`.
3. Linear: `LinearPipelineService` → `ExportService.RunExportAsync()` → `PipelineExecutor`.

Implicit branch split — exactly two tokens:
- `-i` / `--input` — when an input was already seen in the current branch;
- `--from <alias[,alias...]>` — when a `--from` or `--input` was already seen (the first `--from` in
  a fresh branch stays in it).

Neither `--sql` nor boolean processor flags (`--merge`) split. Each processor declares its trigger
flags via `IStreamTransformerFactory.CliTriggerFlags`.

`--job` / `-j` opens no branch: it is a global flag, and the lexer refuses a second one, naming both
files, since a command line runs one job file (a job file itself holds as many branches as it
declares). The file is the source of the branches: `PipelineToJobConverter.ConvertFromJobFile` never
reads the branches of the line, so the lexer refuses, naming it as typed, every flag of a branch given
beside `--job` that `EngineOverrideFlags.All` does not list. That list is what the conversion applies
to the loaded job; a control joins it only when the conversion reads it, never the other way round.
`--export-job <file>` serialises the CLI pipeline through `JobFileWriter` and exits without running.
`[CI: PipelineLexerTests]`

## The engine and its host

`Engine/` (namespace `DtPipe.Engine`) runs a parsed job; `Cli/` hosts it.
`JobService.ExecutePipelineAsync` hands the run to `JobExecutor.ExecuteAsync`, which takes the
one-run gate, applies alias binding, validates, fingerprints the DAG and drives `DagOrchestrator`.
Each branch is a `BranchRun` that `LinearPipelineService` resolves once, binds and hands to
`ExportService`. What a run needs of its process (the interrupt, logging, a choice, a display) it
asks through `IRunHost`, which `JobService` implements: read the interface for its members.

A message is plain text: the orchestrator raises a `DagLogEvent`, the engine calls
`IExportObserver.LogMessage` with a `MessageTone` (Core, `Abstractions/`), and the host picks glyph
and colour and escapes. Never write markup into a message's text, and never render one as markup in
a host: a `[` in a path, a value or an exception message is then read as a tag and throws inside
the branch. `validate_engine_boundary.sh` greps `Engine/` for the three libraries; it does not see
one of their types reached through another assembly's signature.

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
  the transformers in pipeline order. `ReportTransform` takes the same stage number and counts per
  stage, never per transformer name, so two transformers of one type keep their own count.
  `[CI: ProgressReporterTests]`
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
- **A sample inspects a target only where the real run does.** `PrepareSampleWriterAsync` reads `InspectTargetAsync` only from a
  writer whose `RequiresTargetInspection` is true; any other target reads as one that does not exist yet. The sample therefore
  judges what the real run judges, and a sink that has already served (a file whose header says text) keeps its verdict.
  `contract check` is such a sample. `[CI: ContractCheckTargetTests]`

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

## `dtpipe node`

`NodeCommand` is a node whose bricks are the branches of the pipeline grammar, read by the path `--export-job` uses
(`CommandLineJob.Export`), so `-i … --alias a` and the same branch in a `--job` file give the same text and the same version.
`Export` loads a `--job` file without interpolating, as `--export-job` does: a `${{NAME}}`, a `keyring://` alias and a
`${{cursor://|default}}` stay in the announced text, the version and the fragment's job, and only the child resolves them. A value
resolved at start would enter the version and the announcement. `[CI: CommandLineJobTests]` `[CI: PipelineNode.Tests NodeCommandTests]`
System.CommandLine matches only the options of the node (`--runner`, `--name`, `--coordinator`, `--description`, `--client-secret`)
and leaves every other token to the lexer (`TreatUnmatchedTokensAsErrors = false`). `NodeBricks` takes a branch for a brick
only if it reads (`input`) or writes (`output`), never both, never `from`; the alias is the id, a branch without one has none.
`--runner` makes the node a runner (`LaunchSettings.Runner`: `RunAnyFragment()` and a default description): it hosts no data, so any
token left to the lexer is refused by name and no brick is read, while without `--runner` a node with no brick is refused.
`[CI: PipelineNode.Tests NodeCommandTests]`

`KeygenCommand` (`dtpipe keygen <id>`) makes a key through `CoordinatorApi.NewKey`, the one place a key is made: the server's `ClearKey.New`
calls it too, so the shape the coordinator reads and the one the command prints cannot drift. It refuses a client id `ClientIdOfKey`
would not read back, prints nothing else on stdout, and with `--out` creates the file `CreateNew` with mode 0600, never replacing one.
`[CI: KeygenCommandTests]`

- **The CLI reaches the node host through one file.** `Cli/Node/NodeHostEntry.cs` is the only source that names
  `DtPipe.PipelineNode.Launcher`; the method that does is `NoInlining`, so the runtime compiles it, and loads the assembly and
  the transport library behind it, when a node starts. Never name the assembly from another file, never from a constructor or
  from help. `[CI: validate_distributed_boundary.sh, NodeCommandTests]`
- **A secret is a reference.** `--client-secret` takes `keyring://alias`, `${{NAME}}` or `@file` and refuses a value; the
  environment's `DtPipeNode__Oidc__ClientSecret` may hold a reference too. It is resolved once, at start, by
  `CompositeStringContentResolver.Standard`. The host is built without the command line, and a token that names a node setting
  (`DtPipeNode:…`, `DtPipeNode__…`) is refused, so no argument reaches a setting the options do not name. A reference the
  resolver leaves as written (a variable not set, an alias the keyring lacks) stops the start by its name, never passing for the
  secret, and a file that cannot be read is named by its exception's type, never its path, which may be a key pasted after the
  `@`. `NodeBricks.ReadSecretAsync` is the one reading, which `dtpipe studio --key` shares.
  `[CI: PipelineNode.Tests NodeCommandTests; DtPipe.Tests StudioKeyTests]` `[unchecked: the refusals of an unset variable, an absent alias and an unreadable file]`
- **A brick holds no credential as a value.** Its version is the hash of its text; `ConnectionStringSanitizer.CarriesLiteralSecret`
  judges every scalar of the branch as written, with its key, and a hit stops the start by naming the branch. Best-effort by
  construction, in both directions (the vocabulary is the one `Sanitize` uses): an unknown key passes, a query comparing a
  column named like a credential is refused. The rule a brick's owner keeps is the reference.
  `[CI: NodeCommandTests — a literal refused, a reference passing, a query]` `[unchecked: a credential under a key the vocabulary does not know]`

## Checkpoints — mechanics

`--checkpoint` tees the columnar stream into the session store; `--from-checkpoint` reads it back.
The key is a hash of the branch prefix's **definition**, never the alias: two pipelines in one
directory cannot collide, and an unchanged prefix is reused. `ExportRunState.ComputeCheckpointKey`
passes what the resolved run knows (component, sanitised connection, query, `EffectiveLimit`,
sampling) and `PipelineOptions.DefinitionFingerprint`, computed by `BranchFingerprint` from the job as
written: transformer configuration, a processor's options and, recursively, the fingerprint of
each branch it reads. A plain `from:` takes its upstream's fingerprint alone, a processor takes
(alias, fingerprint) pairs because its query names them. `JobExecutor` computes the whole DAG
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
- **F5 Every call of a turn runs** — the calls that run no pipeline in parallel (`Task.WhenAll`), then
  the tools marked `[RunsPipeline]` one at a time, in the order the model emitted them; results keep
  the turn's order, and `--sequential` forces everything serial. The set is reflected off the
  attribute, never listed; a tool that only parses, validates or reads a source stays unmarked.
  `[CI: AgentParallelToolsTests, AgentModeTests — whole catalogue]`
- **F6 Single YAML path** — the `yamlContent` tool argument is the sole plan source.
- **F7 CI gate** — `tests/agentic/analyze-traces.sh --gate` fails on unhandled MCP errors or a failed
  mission. Its variance criterion applies only when `variance_results.jsonl` holds real replication
  data, which the shipped missions do not produce. **Never record a placeholder variance**: a
  criterion that cannot fire is worse than none. The authoritative signal for F1–F7 is the unit
  suite (`Unit/Cli/Agent*Tests`, `Unit/Cli/Mcp*Tests`).

Mandatory MCP directives:
1. Reflect help from `[Description]` / `[ComponentHelp]`; never hardcode it. `[unchecked — nothing
   tests GetGeneralHelp]`
2. Execute in memory via `JobFileParser` + `JobRunner`; no temp files or shell proxies.
   `[unchecked]`
3. Discover tables on `inspect` without a query; give actionable hints on validation errors.
   `[unchecked]`
4. Fail closed: default `apply=false`, reject on ambiguity. `[CI: ExecuteYamlJobGuardrailTests]`

### One run, one report collection

A process runs one pipeline at a time (`.claude/rules/engine-invariants.md`), so a run started beside another waits for
it at `JobExecutor.ExecuteAsync`, behind `JobService.ExecutePipelineAsync`. The agent orders its own runs before they reach that gate
(F5); `dtpipe mcp` relies on the gate alone, since the SDK handles a client's requests concurrently. `JobRunner` is the only caller-facing way to run a resolved job
(MCP's sample and real write, `split`, `contract`, the studio): it arms a `RunReports` for the run
through `SampleReportCollector.Begin`, around the whole `ExecutePipelineAsync`, and returns a
`RunOutcome` (exit code, branch failures, reports, checkpoint keys). A caller that answers a model words
a failed run through `RunOutcome.FailureText` alone (the sample and the real write of `execute-yaml-job`
share it), since a failed branch throws nothing and the console that would show its reason is silenced. `SampleReportCollector` is the
singleton the engine and the observer publish to; it forwards to the collection the calling async flow
armed, branches included, and drops everything when none is armed. Never read it from a caller, and
never arm it from an `async` method that returns before the run ends: the value is gone with the frame.
`dtpipe mcp` serves until it is stopped, so a signal is its only end and it exits 130.

`JobAnalysis` (`Cli/`) is the matching path for a job text: parse, validate, judge the SQL it carries
(`JobSqlSafety`), sample, resolve for a run. MCP and the studio call it; `DtPipeMcpTools` keeps only the tool
surface and the wording written for a model. A refusal that names a tool of this server goes through `JobHints`, so
a caller that has none gets the refusal without the pointer. Never copy a validator into a caller.
`[CI: ConcurrentRunCollectionTests, SampleReportCollectorTests, SampleModeDagTests, McpCommandTests,
ExecuteYamlJobRealRunTests]`

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

`dtpipe studio` (`Cli/Studio/`): its mechanics and the guards of its page are in
`src/DtPipe/Cli/Studio/CLAUDE.md`, loaded when you read a file under it.
