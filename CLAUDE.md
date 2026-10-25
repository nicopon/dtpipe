# CLAUDE.md

Guidance for Claude Code in this repository: **internals only** (call chains, ownership,
invariants). User-facing syntax lives in [README.md](./README.md) and [REFERENCE.md](./REFERENCE.md);
[docs/](./docs/) is the task-oriented site. Link out, never restate.

Every rule that must hold is stated here or in `.claude/rules/`: a rule of one area (the distributed
family, the docs site, MCP and the agent) is in `.claude/rules/<area>.md`, which loads when you work
on a file of that area. Each area also has a nested `CLAUDE.md` with the mechanics you need when
editing it, loaded when you read a file under it: `src/DtPipe/`, `src/DtPipe/Cli/Studio/`,
`src/DtPipe.Core/`, `src/DtPipe.Transformers/`, `src/DtPipe.Adapters/`,
`src/DtPipe.Adapters.Shared/`, `src/DtPipe.Processors/`, `src/Apache.Arrow.Serialization/`,
`src/DtPipe.Coordinator/`, `src/DtPipe.PipelineNode/`, `src/DtPipe.PipelineNode.Hosting/`,
`src/DtPipe.Coordinator.Server/`.

Each rule ends with its guard: `[CI: check]`, `[local: check]` (runs only when someone runs it), or
`[unchecked]` (this file is the only defence).

## Closed decisions — do not reopen without the user

- `docs/` stays plain Markdown: no site generator, no build step, no published artefact.
- No `COOKBOOK.md`; walkthroughs live in `docs/guides/`.
- No root `CHANGELOG.md`; the changelog is kept outside the repository.
- No `DtPipe.ArrowBridge` package; `Core/Infrastructure/Arrow/` stays in Core behind its three-type
  surface.
- The micro perf gate stays out of CI: shared runners are no stable measure.
- Checkpoint encryption has no opt-out.
- The agent session trace (`--trace`) is a diagnostic, never a gate.
- TransportR stays a transport library: no dtpipe concept (plan, fragment, edge, node) enters it;
  `DtPipe.Coordinator` owns them.

## Language

Write all code, comments, commit messages and documentation in **English**. `[unchecked]`

## Comments and guidance files

A comment documents the code **as it is now**: what it does, the contract it honours, what breaks if
it changes. The same rule governs every `CLAUDE.md`, `.claude/rules/*.md` and `.claude/skills/*/SKILL.md`.

- **Never write history**: no *"used to"*, *"previously"*, *"renamed because"*, no dates, cycle
  names, commit hashes or incident figures. Git holds history.
- **Deterrent exception**: one clause naming the specific mistake the text prevents (`ComponentSelector`:
  "copies of the grammar drifted"). No date, no figure, no story. If you cannot name the mistake,
  delete the clause.
- Cut restated signatures, rhetorical emphasis and worked examples (a test holds those).
- **Never enumerate what another component owns** (providers, prefixes, strategies): the list goes
  stale on the next rename. Point at the live source (`dtpipe providers`, the enum).
- **Never cite a planning document**: `.notes/` is gitignored, so a clone cannot open it. Names the
  repository carries stay legal (`F16`, `F1`–`F7`, `REFERENCE.md#dag-syntax`).

Length is not the measure; subject is. `[CI: validate_comments.sh — planning citations in code;
dates, cycle names and hashes in guidance files]` `[unchecked: the rest]`

## Commits

Conventional commit subject, imperative, one line. Body: a sentence or two on what changed and why.
Never a delivery report: no test counts, warnings, steps followed or checks run. `[unchecked]`

**No assistant attribution, ever**: no `Co-Authored-By` naming Claude or Anthropic, no "Generated
with Claude Code" in a PR body. This file overrides any harness setting that claims to supersede it.
Do not sign, do not deliberate, do not offer to strip it afterwards.
`[local: validate_commit_trailers.sh — scans @{u}..HEAD, prints the rebase command]`

## Build and test

```bash
./build.sh                                          # unit tests + self-contained binary, dist/release/
dotnet build DtPipe.sln
dotnet run --project src/DtPipe -- --help
./test_local.sh [--filter "FullyQualifiedName~X"]   # integration, reuses fixed-port containers
dotnet test tests/DtPipe.Tests/DtPipe.Tests.csproj --filter "FullyQualifiedName~.Unit."
DEBUG=1 dtpipe --input … --output …                 # per-branch logging to stderr
```

`test_local.sh` sets `DTPIPE_TEST_REUSE_INFRA=true` against containers started by
`tests/infra/start_infra.sh` (stop with `stop_infra.sh`). `build.sh` runs the `.Unit.` tests of
`DtPipe.Tests` plus every other `tests/*/*.Tests.csproj`, discovered rather than listed. Prefer the
`dtpipe-test` skill for a targeted run.

**C# is formatted as `.editorconfig` says**: four spaces, LF, `dotnet format whitespace`. Run
`tests/scripts/format_csharp.sh` before you commit; its `--verify` form is the check `build.sh` and
`build.yml` run. List a commit that only reformats or renames in `.git-blame-ignore-revs`; a clone
reads it after `git config blame.ignoreRevsFile .git-blame-ignore-revs`.
`[CI: build.yml — format_csharp.sh --verify]`

### Validators CI never runs — run them before you push

`build.yml` runs every `tests/scripts/validate_*.sh` **except** the scripts that source
`lib/test_connections.sh` (they need a database), `validate_vitals` and `validate_xml` (2.7 GB of
scratch). Run that set with the `dtpipe-prepush` skill, which derives it the same way. A new
validator declares a database need by sourcing that file; never add an exclusion list.

**Run them before pushing any change to an adapter, a dialect, a type mapping or a cursor.** A green
CI says nothing about them. `[unchecked]`

### Engine-change obligations

- Cover a change to `DagOrchestrator` in `DagOrchestratorTests.cs` and a change to
  `LinearPipelineService` in `OrderedPipelineTests.cs`. Both run without the CLI.
- Run before commit — the three canonical cases:
  1. Linear pipeline (single branch, no memory channel)
  2. Two-branch DAG (independent branches)
  3. DAG with SQL processor (`--from` + `--sql`)
- For a new topology, add a golden definition to `GoldenDagDefinitions.cs` and a round-trip test to
  `JobDagDefinition_JsonTests.cs`.

`[CI: DagOrchestratorTests, ChannelInjectionTests, EngineInvariantsTests, JobDagDefinition_JsonTests
over the golden shapes; PipelineLexerTests, PipelineToJobConverterTests for args → DAG]`
`[unchecked: no test ties CLI arguments to the golden shapes; nothing checks that an engine change
arrives with a test]`

### Performance

Local only. Use the `dtpipe-perf-gate` skill before stating any figure. **Below ~10 % between two
macro runs there is no result.** Update the micro baseline only on the reference machine, for a
deliberate, understood shift.

## The docs/ site

What the site shows, what `REFERENCE.md` enumerates and the checks its pages pass:
`.claude/rules/docs-site.md`, loaded when you work on `docs/` or a root document.

## Architecture

| Project | Role |
|---|---|
| `src/DtPipe` | CLI entry, DI wiring, the engine (`Engine/`, namespace `DtPipe.Engine`), `JobService` (its CLI host), MCP server, AI agent, studio |
| `src/DtPipe.Core` | Abstractions, DAG engine, pipeline models, helpers |
| `src/DtPipe.Adapters` | Readers and writers (`Adapters/<Name>/`) |
| `src/DtPipe.Adapters.Shared` | Infrastructure shared by adapters and processors (DuckDB read path) |
| `src/DtPipe.Transformers` | Row and columnar transformers (one subdirectory each) |
| `src/DtPipe.Processors` | SQL stream processors (DuckDB, Merge) |
| `src/Apache.Arrow.Ado` | Standalone ADO.NET → Arrow; depends on `Apache.Arrow.Serialization` only |
| `src/Apache.Arrow.Serialization` | Standalone CLR↔Arrow type map + POCO serializer |
| `src/DtPipe.Jobs` | A job file as a YAML tree (`JobYaml`), the designer's cards (`DesignService`, used by `dtpipe studio`), the coordinator's API the studio calls (`Coordinator/`) |
| `src/DtPipe.Coordinator*`, `src/DtPipe.PipelineNode*`, `deploy` | The distributed family: coordinator, nodes, their hosts and images (`.claude/rules/distributed-pipelines.md`) |
| `tests/DtPipe.Tests` | xunit.v3 unit and integration tests |

`DtPipe.Core` holds abstractions, models and the DAG orchestrator only; the engine that runs a
branch lives in the CLI's assembly, under `src/DtPipe/Engine/`. The distributed family's projects
and rules are in `.claude/rules/distributed-pipelines.md`.

Data flow: `args` → `PipelineLexer.Parse` → `PipelineToJobConverter` → `DagOrchestrator` →
`LinearPipelineService` → `ExportService.RunExportAsync` → `PipelineExecutor` → `IDataWriter`.
`DagOrchestrator` runs this chain once per branch, concurrently; a linear run goes through it once.
Fundamental shape: `IStreamReader` → `IDataTransformer[]` → `IDataWriter`.

Key interfaces: `IStreamReader` / `IColumnarStreamReader`; `IDataWriter` / `IRowDataWriter` /
`IColumnarDataWriter`; `IDataTransformer` / `IDataTransformerFactory`; `IStreamTransformerFactory`
(multi-input, receives `BranchChannelContext`); `ICliContributor` / `OptionsRegistry`.

Providers implement `IProviderDescriptor<TService>`, registered in `Program.cs`; options come from
`[ComponentOption]` via reflection. Detail: `src/DtPipe/CLAUDE.md`.

### DAG grammar

```
--from <alias[,alias...]> [--ref <alias[,alias...]>] (--sql "<query>" | --<processor>) [--alias <name>] [-o <dest>]
```

`-i`, `--from` and `--job` open a new branch (exact rules: `src/DtPipe/CLAUDE.md`). **An alias list
is always comma-separated, and repeating a flag never accumulates**: repetition already means "new
branch", so every other value flag is scalar and rejects a second occurrence in the same stage. How
many aliases `--from` accepts is the processor's business, not the grammar's. Semantics:
`REFERENCE.md#dag-syntax`.

## Invariants

### Connection selectors are invisible to providers

`ComponentSelector` is the **single authority** on `{component}[+{variant}]:`.
- `CanHandle` judges the raw string by **content** (extension, connection-string keywords), never by
  its own prefix.
- Route every site through `ComponentSelector`; never hand-roll `StartsWith(name + ":")`.
- A remote URI (`s3://…`) is never a selector.
- An internal channel is never a selector: a transport (`IInternalChannelCapable`) is registered
  apart from the reader and writer factories (`InternalChannelTransports`) and picked by capability
  for a typed endpoint, so no routing site sees its name.
- Variants reach providers as data (`IVariantAwareOptions`), never as text to re-parse.

`[CI: ComponentSelectorTests, RemoteUriClaimTests, LinearPipelineServiceTests — an internal channel
prefix]` `[unchecked: a site that bypasses the
selector]` Detail: `src/DtPipe.Adapters/CLAUDE.md`.

### One way to read DuckDB

`duck:` and `--sql` both read through `DuckDbArrowResultReader`. **Never reimplement the fetch loop
in one consumer**: a copy is a second P/Invoke surface onto the same library, and it drifts. Init
SQL follows the same rule: `DuckInitSqlRunner` (Core) is the single runner for reader, writer and
processor. `[unchecked]` Detail: `src/DtPipe.Adapters.Shared/CLAUDE.md`.

### No magic conversions in the engine core

Core, Processors and the DAG orchestrator never convert types implicitly to work around an adapter.
On a type mismatch, prefer in order: (1) adapter parameterisation (`--column-type "Id:uuid"`), (2) a
transformer (`--compute`), (3) the SQL processor (`CAST(... AS UUID)`). Forbidden:
- detecting a source format and silently converting in a type mapper or schema factory;
- changing `ArrowTypeMapper` / `PipeColumnInfo` to compensate for an adapter;
- branching in `ExportService` / `PipelineExecutor` / `DagOrchestrator` on adapter identity.

`[CI: validate_core_boundary.sh — keeps concrete SQL/dialect/cursor classes out of Core]`
`[unchecked: the three bullets]`

Canonical UUID: `FixedSizeBinaryType(16)` + field metadata `ARROW:extension:name = arrow.uuid`,
RFC 4122 big-endian.

### Arrow ↔ CLR mapping: no heuristics

`GetClrType(IArrowType)` / `GetValue(array, i)` are storage-only and never infer semantics
(`FixedSizeBinary` → `byte[]`). Use the metadata-aware `GetClrTypeFromField(Field)` and
`GetValueForField(array, field, i)`, and `GetField(name, clrType, nullable)` instead of
`new Field(...)`. `[unchecked]`

Representation rules live under their own names in `Apache.Arrow.Serialization/Mapping/`:
`Rfc4122Guid` (byte order) and `TemporalNormalization` (zone-less `DateTime`). **Change both
directions of `TemporalNormalization` together**; never call `new DateTimeOffset(dt)`, which resolves
against the local zone. `[CI: validate_core_boundary.sh]` `[local: validate_temporal.sh]`

`Core/Infrastructure/Arrow/` exposes exactly three DtPipe types: `PipeColumnInfo`,
`IRowToColumnarBridge`, `IColumnarToRowBridge`. The standalone Arrow libraries never reference
DtPipe. `[CI: validate_core_boundary.sh]`

### RecordBatch ownership

**Every `RecordBatch` has exactly one owner. The owner disposes it exactly once, then never touches
it. Ownership moves downstream when a batch is yielded, returned or written.**
- The segment runner disposes its input after the chain, unless the transformer returned the same
  reference (pass-through).
- A transformer returning a **new** batch that reuses an input column **must** wrap it in
  `ArrowOwnership.RetainArray(...)`; without it the output points at freed buffers.
- A reader handing over Arrow IPC batches re-homes them with `ArrowOwnership.TakeOwnership`.
- Fan-out gives each consumer its own batch via `ArrowOwnership.RetainAll`.

`[CI: ArrowOwnershipTests, CDataOwnershipTests]` `[unchecked: a transformer that aliases a column
without retaining it]` Detail: `src/DtPipe.Core/CLAUDE.md`.

### The engine formats nothing

**The engine names neither Spectre.Console, System.CommandLine nor Serilog: what it and
`DagOrchestrator` tell a person is plain text, which the host formats.**
`[CI: validate_engine_boundary.sh — a grep of src/DtPipe/Engine/]`
`[unchecked: markup written into a message's text; a host that renders a message as markup]`
Detail: `src/DtPipe/CLAUDE.md`.

### One run at a time per process

**A process runs one pipeline at a time.** `JobExecutor.ExecuteAsync` holds a static gate for the
whole run, and every run reaches it through `JobService.ExecutePipelineAsync` (the command line,
`JobRunner`): a run that finds it held
waits, and one cancelled while it waits returns 130. A run owns state the process holds once (the
channel registry `DagOrchestrator` clears at its start, the console's Ctrl-C); two runs over it
trade rows, and one can still exit 0. **Never start a run from inside a run**: it would wait on
itself. Callers that overlap wait: MCP requests, which the SDK handles concurrently; the agent
issues a turn's runs one after the other (F5). **A DAG returns its verdict only once every branch has
ended**, a failed one included: the gate is never released under a branch that still completes its
channel by alias.
`[CI: ConcurrentRunCollectionTests, DagOrchestratorTests — a failing branch's verdict waits for a slow
sibling]` `[unchecked: an entry that runs the engine without JobExecutor]` Detail: `src/DtPipe/CLAUDE.md`.

**A value of one run is armed for its call chain, never written to the process environment**
(`LaunchValues`, `CursorOverride`): the environment reaches every branch, every run beside it and
every child. `--cursor-from` is armed before the job is read, so a `--job` file resolves it too;
`DTPIPE_CURSOR_OVERRIDE`, which a node gives its child, is read once for the process, under the flag.
`[CI: validate_process_environment.sh, CursorFromTests]`

### Adding an adapter

Full patterns: `EXTENDING.md`.
- Row writers build `ColumnConverterFactory.Build(source, target)` once per column at init, never
  per-cell `ValueConverter.ConvertValue()`.
- Columnar writers implement `IColumnarDataWriter` and read with `GetValueForField`.
- Text readers implement `IColumnTypeInferenceCapable` for `--auto-column-types`.

`[unchecked]` Help attributes are a contract for models: `src/DtPipe.Adapters/CLAUDE.md`.

### Sample mode — there is no second engine

`--dry-run N` is **the real execution over N source rows with the writer neutralised**: same reader,
transformers, segmentation and bridges. Never add an analyser beside `PipelineExecutor`; a second
engine disagrees with the real run. The sink mirrors the real writer's capability (row or
columnar), never a fixed sink. Sample mode suppresses schema migration, all four hooks, the cursor
and the metrics file. The neutralisation aims at writers that leave the process: the channel between
two branches carries its rows as in the real run, and the bound `N` applies to a source reader
only (`IStreamReaderFactory.IsSource`), never to a branch reading another, a processor or a
checkpoint.

Safety is a **read-side** problem: a source can mutate (`DELETE … RETURNING`, `--duck-init`,
`ATTACH`). `SampleModeSafetyGate` classifies the resolved source SQL; `ReadOnlySessionSql` lets the
server refuse. **Never report a guarantee that is sometimes absent as always there.**

`[CI: SampleModeEquivalenceTests, SampleModeDagTests, SampleModeSafetyGateTests, validate_single_engine.sh,
validate_sample_safety.sh]` Detail: `src/DtPipe/CLAUDE.md`.

### Checkpoints

The key is a hash of the branch prefix's **definition**, never the alias. **Always encrypt, no
opt-out**: one cleartext session voids the store-wide guarantees. `--from-checkpoint` resolves by
capability and **never** through `ComponentSelector`, so a hex key is never read as a prefix.
`[CI: Checkpoint*Tests]` `[local: validate_checkpoint.sh]` Detail: `src/DtPipe/CLAUDE.md`.

### Redaction

Use `ConnectionStringSanitizer.Redact` for anything that *is* a connection string: it parses it and
masks every key not on the safe list (fail-closed). Use `Sanitize` only for prose with no grammar
(best-effort). **No site sits on the other side of that line.** **Never anchor the sensitive-key
pattern on `\b`**: it finds no boundary at `_` or a capital, so `SecretAccessKey` passes. A
coordinator's identity key (`dtp_…`) is a secret by its shape: both entry points mask it under any key
or none, and `CarriesLiteralSecret` refuses a text that holds one.
`[CI: ConnectionStringSanitizerTests, validate_secret_redaction.sh]` Detail:
`src/DtPipe.Core/CLAUDE.md`.

### Distributed pipelines

The family's rules: `.claude/rules/distributed-pipelines.md`, loaded under the family, its tests,
`deploy/` and the lab.

### The designer writes the job, the page never does

`DesignService.Compose` (`DtPipe.Jobs`) is the only writer of a designed job's YAML, and `dtpipe
studio` the only host that designs: a page sends cards and reads back the text. The coordinator's
pages design nothing; they read a job and show it, and what a person writes there arrives published
from a studio. **One model, never a copy per host**: a host adds what only it knows behind
`IBrickLookup` (the studio, the coordinator's bricks) and maps its page's fields, never a second
composer.
`[CI: DesignServiceTests]` `[unchecked: a page that builds YAML itself]`

### Exit codes

`0` success · `1` fault · `130` user cancellation. Cancellation never masks as success (F16): a
branch reporting 130 cancels the DAG, which returns 130. `[local: validate_cancellation.sh]`
Detail: `src/DtPipe.Core/CLAUDE.md`.

## MCP server and agent

`dtpipe mcp` and `dtpipe agent`: their hardening invariants (F1–F7) and directives are in
`.claude/rules/mcp-and-agent.md`, loaded under `src/DtPipe/Cli/Mcp/` and `src/DtPipe/Cli/Agent/`.
