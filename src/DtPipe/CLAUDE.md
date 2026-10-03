# DtPipe (CLI) — parsing, options, sample mode, checkpoints, MCP and agent

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

Sample mode also suppresses `ValidateAndMigrateAsync` (it can CREATE/ALTER the target), **all four
hooks** (SQL on the target connection), the cursor and the metrics file.

### Safety is a read-side problem

A reader can mutate: `DELETE … RETURNING`, `… OUTPUT`, `--duck-init`, an `ATTACH` inside `--sql`.
`--limit` bounds what the client reads, never what the server already destroyed.
`SampleModeSafetyGate` classifies the **resolved** pipeline's source SQL (so a `@file` query is
covered, unlike the YAML text scan), and `ISqlDialect.ReadOnlySessionSql` lets the server refuse
instead of a regex guessing.

Never classify writer hooks: they are already suppressed, so refusing them adds no safety and trains
users to pass `--allow-destructive` by reflex, which unlocks the source side too.

SQL Server returns `null` for `ReadOnlySessionSql`: `ApplicationIntent=ReadOnly` routes to a replica
and does not make a session read-only. The report states which guarantee the run had.

`[CI: SampleModeSafetyGateTests, validate_sample_safety.sh]` `[unchecked: a query that writes
through a function — SELECT my_function() passes a verb scan, hence the server-enforced form]`

## Checkpoints — mechanics

`--checkpoint` tees the columnar stream into the session store; `--from-checkpoint` reads it back.
The key is a hash of the branch prefix's **definition** (sanitised connection, query, transformers,
sampling parameters), never the alias: two pipelines in one directory cannot collide, and an
unchanged prefix is reused.

Encryption (AES-GCM) does not buy confidentiality at rest — the key is on the same disk. It buys two
properties of the **store as a whole**: an inert copy, and a purge made reliable by destroying the
key. One cleartext session would void both for every session, retroactively.

**A row-mode pipeline gets a bridge, not a refusal.** When materialising and the last segment is not
columnar, `ExecuteSegmentedPipelineAsync` appends an *empty* columnar segment — the device used for a
row reader feeding a columnar writer — so the chain reaches Arrow at the writer boundary, tees, and
bridges back. Add that segment **only** when `materialise is not null`, so an ordinary run is
unchanged.

`[CI: CheckpointCipherTests, CheckpointKeyTests, CheckpointRoundTripTests]`
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
