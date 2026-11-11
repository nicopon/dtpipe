---
paths:
  - "src/DtPipe/**"
  - "src/DtPipe.Core/**"
  - "src/DtPipe.Processors/**"
  - "src/Apache.Arrow.*/**"
  - "tests/DtPipe.Tests/**"
---

# Engine invariants

Rules of the engine and its core. The root `CLAUDE.md` holds the rules every task needs.

## Engine-change obligations

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

## Performance

Local only. Use the `dtpipe-perf-gate` skill before stating any figure. **Below ~10 % between two
macro runs there is no result.** Update the micro baseline only on the reference machine, for a
deliberate, understood shift.

## No magic conversions in the engine core

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

## Arrow ↔ CLR mapping: no heuristics

`GetClrType(IArrowType)` / `GetValue(array, i)` are storage-only and never infer semantics
(`FixedSizeBinary` → `byte[]`). Use the metadata-aware `GetClrTypeFromField(Field)` and
`GetValueForField(array, field, i)`, and `GetField(name, clrType, nullable)` instead of
`new Field(...)`. `[unchecked]`

**A cell reads alike cell by cell and column by column.** `ArrowTypeMap.CompileValueReader(array,
field)` is `GetValue`'s switch resolved once per column, for a caller that walks a column; it returns
what `GetValue` returns for every index, null and exceptions included. Change the two together, and
never write a third reading: a per-type copy in a consumer drifted from the map unseen.
`[CI: CompileValueReaderTests — every case of the conformance catalogue, whole and sliced]`

Representation rules live under their own names in `Apache.Arrow.Serialization/Mapping/`:
`Rfc4122Guid` (byte order) and `TemporalNormalization` (zone-less `DateTime`). **Change both
directions of `TemporalNormalization` together**; never call `new DateTimeOffset(dt)`, which resolves
against the local zone. `[CI: validate_core_boundary.sh]` `[local: validate_temporal.sh]`

`Core/Infrastructure/Arrow/` exposes exactly three DtPipe types: `PipeColumnInfo`,
`IRowToColumnarBridge`, `IColumnarToRowBridge`. The standalone Arrow libraries never reference
DtPipe. `[CI: validate_core_boundary.sh]`

## Routing a writer

`PipelineExecutor` routes by the writer's contracts. A writer with a columnar contract pulls the
pipeline to Arrow, row contract or not: that is a database's bulk path. **Only `IEitherShapeWriter` gets
what the chain ends in**, with no bridge, and a sink or decorator in front of one mirrors it, or the
sample is not the run. **`IEitherShapeWriter` needs a reader that reads a cell alike on both paths**, so
a divergence of the read paths reaches `checksum:`. `[CI: PipelineExecutorTests, WriteConformanceTests]`

## RecordBatch ownership

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

## The engine formats nothing

**The engine names neither Spectre.Console, System.CommandLine nor Serilog: what it and
`DagOrchestrator` tell a person is plain text, which the host formats.**
`[CI: validate_engine_boundary.sh — a grep of src/DtPipe/Engine/]`
`[unchecked: markup written into a message's text; a host that renders a message as markup]`
Detail: `src/DtPipe/CLAUDE.md`.

## One run at a time per process

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

## Sample mode — there is no second engine

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

## Checkpoints

The key is a hash of the branch prefix's **definition**, never the alias. **Always encrypt, no
opt-out**: one cleartext session voids the store-wide guarantees. `--from-checkpoint` resolves by
capability and **never** through `ComponentSelector`, so a hex key is never read as a prefix.
`[CI: Checkpoint*Tests]` `[local: validate_checkpoint.sh]` Detail: `src/DtPipe/CLAUDE.md`.

## Redaction

Use `ConnectionStringSanitizer.Redact` for anything that *is* a connection string: it parses it and
masks every key not on the safe list (fail-closed). Use `Sanitize` only for prose with no grammar
(best-effort). **No site sits on the other side of that line.** **Never anchor the sensitive-key
pattern on `\b`**: it finds no boundary at `_` or a capital, so `SecretAccessKey` passes. A
coordinator's identity key (`dtp_…`) is a secret by its shape: both entry points mask it under any key
or none, and `CarriesLiteralSecret` refuses a text that holds one.
`[CI: ConnectionStringSanitizerTests, validate_secret_redaction.sh]` Detail:
`src/DtPipe.Core/CLAUDE.md`.
