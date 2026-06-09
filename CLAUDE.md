# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Language

All code, comments, commit messages, and documentation **must** be written in English. This is a hard requirement — no exceptions.

## Build & Run

Prefer `./build.sh` for a full build (runs unit tests + produces a self-contained binary in `dist/release/`):

```bash
./build.sh
```

For targeted builds during development:

```bash
dotnet build DtPipe.sln
dotnet run --project src/DtPipe -- --help
```

```bash
dtpipe --help
```

## Testing

Prefer `./test_local.sh` for integration tests — it reuses persistent Docker containers instead of spinning up new ones via Testcontainers (much faster):

```bash
./test_local.sh
./test_local.sh --filter "FullyQualifiedName~SomeTest"
```

For unit tests only (no Docker required):

```bash
dotnet test tests/DtPipe.Tests/DtPipe.Tests.csproj --filter "FullyQualifiedName~.Unit."

# Run a single test by name
dotnet test tests/DtPipe.Tests/ --filter "FullyQualifiedName~CliDagParserTests"
```

`test_local.sh` sets `DTPIPE_TEST_REUSE_INFRA=true` to tell tests to connect to the fixed-port containers started by `tests/infra/start_infra.sh`. Use `tests/infra/stop_infra.sh` to tear them down. Shell-based integration scripts are also available in `tests/scripts/`.

### Engine Change Obligations

Any change to `DagOrchestrator` **must** be covered by a unit test in `DagOrchestratorTests.cs`, and any change to `LinearPipelineService` **must** be covered in `OrderedPipelineTests.cs`. Both validate behavior without going through the CLI.

Before committing engine changes, verify the three canonical cases pass:
1. Linear pipeline (single branch, no memory channel)
2. Two-branch DAG (independent branches)
3. DAG with SQL processor (`--from` + `--sql`)

The golden DAG fixtures in `GoldenDagDefinitions.cs` are the canonical reference shapes. `CliDagParser_GoldenTests.cs` verifies that `CliDagParser.Parse(args)` produces exactly those structures. If you add a new DAG topology, add a corresponding golden definition and a round-trip JSON test in `JobDagDefinition_JsonTests.cs`.

## Architecture Overview

### Solution Structure

| Project | Role |
|---|---|
| `src/DtPipe` | CLI entry point, DI wiring, `JobService`, `ExportService` |
| `src/DtPipe.Core` | Abstractions, DAG engine, pipeline models, helpers |
| `src/DtPipe.Adapters` | Readers and writers for all data sources/targets |
| `src/DtPipe.Transformers` | Row and columnar data transformers |
| `src/DtPipe.Processors` | C# side of SQL stream processors (DuckDB, factories) |
| `src/Apache.Arrow.Ado` | Standalone ADO.NET → Arrow library; zero DtPipe deps (depends on `Apache.Arrow.Serialization` only) |
| `src/Apache.Arrow.Serialization` | Standalone CLR↔Arrow type map + POCO serializer; zero DtPipe deps, no external deps beyond `Apache.Arrow` |
| `tests/DtPipe.Tests` | xunit.v3 unit and integration tests |

#### File placement conventions

- `DtPipe.Core` contains **only** abstractions, models, and the generic DAG/pipeline engine — no concrete implementations.
- Each concrete transformer in `DtPipe.Transformers` lives in its own subdirectory (`Row/Expand/`, `Arrow/Filter/`…) with a matching sub-namespace (`DtPipe.Transformers.Row`, `DtPipe.Transformers.Arrow`…).
- Each concrete stream processor in `DtPipe.Processors` follows the same pattern: one subdirectory per processor (`DuckDB/`, `Merge/`, `Sql/`…) with a matching sub-namespace (`DtPipe.Processors.DuckDB`, `DtPipe.Processors.Merge`, `DtPipe.Processors.Sql`…).
- Readers and writers in `DtPipe.Adapters` are grouped by technology under `Adapters/<Name>/`.

### Core Data Flow

The fundamental pipeline: `IStreamReader` → `IDataTransformer[]` → `IDataWriter`.

1. `JobService.BuildSubcommands()` registers named subcommands (`inspect`, `providers`, `completion`, `secret`) into the `System.CommandLine` root command. Named subcommands are dispatched before pipeline parsing.
2. On pipeline execution, `FlagRegistryFactory.Build(serviceProvider)` assembles a `FlagRegistry` from all registered providers (via `[ComponentOption]` attributes) and stream processor trigger flags. `PipelineLexer.Parse(args)` then splits raw args into a `ParsedPipeline` (a list of `BranchSpec` records). `PipelineToJobConverter.Convert(parsed, streamTransformerFactories)` maps that to a `(Dictionary<string, JobDefinition>, JobDagDefinition)` pair.
3. For linear pipelines, `LinearPipelineService` drives execution through `ExportService.RunExportAsync()`.
4. For DAG pipelines, `DagOrchestrator` spawns concurrent `Task`s per branch, wiring them via in-memory `Channel<T>` for zero-copy data flow.

`PipelineEngine` (`DtPipe.Core`) is a headless, CLI-free engine for programmatic use. It accepts `IStreamReader + IRowDataWriter + IDataTransformer[]` and drives the full pipeline without DI or CLI dependencies.

### Provider Pattern

Every adapter implements `IProviderDescriptor<TService>` and is registered in `Program.cs` via `RegisterReader<T>()` / `RegisterWriter<T>()` / `RegisterStreamTransformer<T>()`. The `CliProviderFactory<T>` wraps descriptors into CLI contributors: `CliOptionBuilder.GenerateFlagDefsForType(OptionsType)` reflects on `[ComponentOption]` attributes and produces `FlagDef` entries for the `FlagRegistry`. At execution time, `FlagBinder.Bind(optionsInstance, args, registry)` maps the raw CLI args to the options object. Provider-specific options are stored in `OptionsRegistry` (keyed by type) and scoped per DI scope.

### DAG Pipeline

`PipelineLexer` (`DtPipe.Cli.Pipeline`) tokenises raw args into a `ParsedPipeline` whose `Branches` list contains `BranchSpec` records (each carrying stage-scoped arg slices: `ReaderArgs`, `PipelineArgs`, `WriterArgs`). `PipelineToJobConverter` then maps each `BranchSpec` to a `BranchDefinition` (the core DAG model). Three tokens trigger an implicit branch split:
- `-i` / `--input` — when an input or a job file was already seen in the current branch (new data source)
- `--from <alias[,alias...]>` — when a `--from`, `--input`, or `--job` was already seen in the current branch. The first `--from` in a completely fresh branch (no prior input) stays in the current branch.
- `--job` / `-j <file>` — when a job file or an input was already seen in the current branch (new YAML job)

Neither `--sql` nor boolean processor flags (e.g. `--merge`) trigger a split. Each stream processor registers its trigger flags via `IStreamTransformerFactory.CliTriggerFlags`, which `FlagRegistryFactory` uses to populate the `FlagRegistry`. The canonical processor syntax is:

```
--from <alias[,alias...]> [--ref <alias[,alias...]>] (--sql "<query>" | --<processor>) [--alias <name>] [-o <dest>]
```

- `--from a,b,c` declares one or more streaming main sources (comma-separated). Fan-out consumers use a single alias; multi-stream processors (e.g. merge) use multiple aliases.
- `--ref a,b` declares materialized reference sources (preloaded before query execution, comma-separated). Used by SQL JOIN branches.
- `--sql "<query>"` runs an inline SQL query. Default engine: DuckDB (standard SQL, no build step).
- `--merge` (and future boolean flags) declares the processor explicitly by name.
- `--job <file>` / `-j <file>` loads a YAML pipeline job file; `PipelineToJobConverter` reads it and applies any additional CLI flags as overrides.
- `--export-job <file>` serialises the current CLI pipeline to a YAML job file via `JobFileWriter` and exits without running the pipeline.

Branches communicate via `IMemoryChannelRegistry` (either native `Channel<IReadOnlyList<object?[]>>` or Arrow `Channel<RecordBatch>`). The `MappedMemoryChannelRegistry` handles logical-to-physical alias resolution for fan-out (broadcast/tee) scenarios. A `BranchChannelContext` injected per branch carries an `AliasMap` that translates logical aliases (as written in CLI args) to physical channel names (including fan-out sub-channels like `s__fan_0`). This mapping is populated by `DagOrchestrator` and is transparent to all downstream components including processors.

Canonical topologies (Linear, SQL, JOIN, Merge, Fan-out, Diamond, Join→fan-out) are documented with full CLI patterns in `REFERENCE.md`.

### SQL Processors

`CompositeSqlTransformerFactory` is the entry point registered in DI. The primary engine is:

**DuckDB (default)** — `DuckDBSqlTransformerFactory` / `DuckDBSqlProcessor`:
- Pure C# via DuckDB.NET. No native bridge build required.
- Input (`--from`): zero-copy Arrow C Data Interface via `duckdb_arrow_scan`.
- Output: lazy streaming via `duckdb_execute_prepared_streaming` + `duckdb_fetch_chunk` + `duckdb_data_chunk_to_arrow`. Arrow extension types (UUID, etc.) preserved via `arrow_lossless_conversion = true`.
- Schema inferred from prepared statement before execution — no extra query round-trip.
- Standard SQL dialect, rich function library (window functions, CTEs, JSON, etc.). Queries testable externally with the DuckDB CLI.
- `--duck-init "SQL"` (optional): runs on the in-memory connection after the two hardcoded `SET` statements and before Arrow stream registration. Use to load extensions (`LOAD httpfs`), set session variables (S3/Azure credentials), or define macros/views that the query depends on. Extracted from `branchArgs` by `DuckDBSqlTransformerFactory` and passed as the `initSql` optional parameter of `DuckDBSqlProcessor`.

The same `--duck-init` option is available on `DuckDataSourceReader` (runs after `PRAGMA memory_limit/threads`) and `DuckDbDataWriter` (runs once after connection open, guarded by `_initSqlApplied`). Each DuckDB component has its own connection — `--duck-init` must be specified separately on each branch that needs it. The helper logic is in `DuckInitSqlHelper` (Adapters) and a private static `RunInitSqlAsync` (Processor).

**`--duck-init` value resolution** is handled by `IStringContentResolver` (`DtPipe.Core.Security`). The CLI uses `CliStringContentResolver` (`DtPipe/Cli/Security/`); headless contexts use `DefaultStringContentResolver`. Resolution order: (1) `@file` or `keyring://` replaces the whole value, then (2) `${{ENV_VAR}}` and `${{keyring://alias}}` are substituted inline. Steps are composable. This resolver is also used by `--compute` and `--expand` (via `DefaultStringContentResolver.Instance`, env vars + `@file` only). Full syntax documented in `REFERENCE.md`.

Logical SQL table names come from the branch args (`--from`/`--ref`). Physical channel aliases are resolved by `DagOrchestrator` before the processor is created, via `BranchChannelContext.AliasMap` — processors never need to know about fan-out sub-channel naming (`__fan_N` suffixes).

**`--ref` materialization is intentional.** Secondary sources declared via `--ref` are fully read into memory before query execution. This is required for both engines to build a cost-based execution plan — streaming both main and reference tables simultaneously prevents join optimization. Only the source declared via `--from` (the main) uses a true streaming path. Keep `--ref` tables at manageable size; for large lookups, pre-filter upstream.

### Transformer Pipeline

Transformers implement `IDataTransformer` with three methods: `InitializeAsync` (schema propagation), `Transform` (per-row), and `Flush` (end-of-stream for stateful transformers). `PipelineSegmenter` groups consecutive columnar-capable transformers into segments to enable Arrow zero-copy bridging between row and columnar processing modes.

### Key Interfaces

- `IStreamReader` / `IColumnarStreamReader` — open + stream batches
- `IDataWriter` / `IRowDataWriter` / `IColumnarDataWriter` — write contracts (row-based and columnar)
- `IDataTransformer` / `IDataTransformerFactory` — transform rows; factory creates from CLI config or YAML
- `IStreamTransformerFactory` — multi-input stream processors; declares `MinStreams`/`MaxStreams`, `MinLookups`/`MaxLookups`, and `CliTriggerFlags` (used by `FlagRegistryFactory`); `Create(branchArgs, ctx, serviceProvider)` receives `BranchChannelContext` for alias resolution
- `ICliContributor` — contributes CLI options and can intercept command handling
- `OptionsRegistry` — scoped key-value store for provider-specific parsed options

## Debug Mode

Set `DEBUG=1` to enable verbose branch-level logging to stderr:

```bash
DEBUG=1 dtpipe --input pg:"..." --output csv:out.csv
```

## Pipeline Design Principles

### No magic conversions in the engine core

The DtPipe engine (Core, Processors, DAG orchestrator) must **never** perform implicit type conversions to work around limitations or specificities of a particular adapter (reader or writer). Adapter-specific behavior belongs in the adapter, not the engine.

When a type mismatch arises between two pipeline stages (e.g. a CSV source produces a generic `string` column for what is logically a UUID, while a downstream SQL step joining it with a Parquet or database source expects a typed UUID column), the correct remediation is one of the following — in order of preference:

1. **Adapter parameterization** — Add configuration to the source adapter that allows the user to declare the logical type of one or more columns and apply explicit parsing at read time. Example: a `--column-type "Id:uuid"` option on the CSV reader that parses Base64 or UUID-formatted strings into `byte[16]` during ingestion.

2. **Pipeline transformer** — Insert an existing or new DtPipe transformer between the source and the SQL step to convert the column to the expected format. Example: a `--compute` expression that parses a string column into a typed UUID representation.

3. **SQL processor capabilities** — Use the SQL engine's native casting and formatting functions directly in the query. Example: `CAST(base64_decode(id) AS UUID)` in the `--sql` expression.

What is explicitly **forbidden**:
- Detecting a specific source format (e.g. "this string looks like a Base64-encoded UUID") and silently converting it inside a consumer, type mapper, or schema factory.
- Changing the Arrow schema or type mapping in `AdoToArrow`, `ArrowTypeMapper`, or `PipeColumnInfo` to compensate for a specific adapter's output format.
- Inserting conditional branches in `ExportService`, `PipelineExecutor`, or `DagOrchestrator` based on adapter identity.

The canonical Arrow representation of a UUID in DtPipe is `FixedSizeBinaryType(16)` with Field metadata `ARROW:extension:name = arrow.uuid` and RFC 4122 big-endian byte order (use `ArrowTypeMapper.ToArrowUuidBytes` / `FromArrowUuidBytes`). Any adapter that produces UUID values must emit them in this format, or the user must insert an explicit conversion step.

## Apache.Arrow.Serialization

`src/Apache.Arrow.Serialization/` is a **standalone library** with no DtPipe dependencies (only `Apache.Arrow`). Dependency graph:

```
Apache.Arrow.Serialization   ← standalone, no DtPipe deps
       ↑
Apache.Arrow.Ado             ← uses ArrowTypeResult
       ↑
DtPipe.Core                  ← ArrowTypeMapper is a facade over ArrowTypeMap
       ↑
DtPipe.Adapters, DtPipe.Processors, …
```

`ArrowTypeMap` (`Mapping/ArrowTypeMap.cs`) is the canonical CLR↔Arrow mapping; `ArrowTypeMapper` in `DtPipe.Core` is a pure facade.

`FixedSizeBinaryArrayBuilder` (`Reflection/FixedSizeBinaryArrayBuilder.cs`) is an intentional copy of the same class in `DtPipe.Core` — kept separate to avoid a circular dependency. **Keep both files in sync.**

See `EXTENDING.md` for `ArrowSerializer`, `ArrowDeserializer`, and `ArrowReflectionEngine` usage.

---

## Adding a New Adapter

See `EXTENDING.md` for the full adapter and transformer patterns. Key rules:

- **Row writers**: use `ColumnConverterFactory.Build(sourceClrType, targetClrType)` once per column during init; never call `ValueConverter.ConvertValue()` per-cell.
- **Columnar writers**: implement `IColumnarDataWriter`; use `ArrowTypeMapper.GetValueForField(array, field, i)` when a `Field` is available.
- **Text readers**: implement `IColumnTypeInferenceCapable` so `--auto-column-types` works automatically.

### Arrow ↔ CLR mapping: no heuristics

**Firm rule: `ArrowTypeMapper.GetClrType(IArrowType)` never infers a semantic CLR type from the storage type alone.** Ambiguous types (e.g. `FixedSizeBinary`) map to the most generic CLR type (`byte[]`). Semantic resolution requires `ArrowTypeMapper.GetClrTypeFromField(Field)` (checks extension metadata).

Key APIs:
- `GetLogicalType(Type)` → `ArrowTypeResult` (`.ArrowType` + `.Metadata`)
- `GetField(name, clrType, nullable)` → `Field` with metadata embedded — use instead of `new Field(...)`
- `GetClrTypeFromField(Field)` → `Type` — use wherever a `Field` is available
- `GetValueForField(array, field, i)` → `object?` — respects extension metadata (e.g. `arrow.uuid` → `Guid`)
- `GetClrType(IArrowType)` / `GetValue(array, i)` — storage-only, no metadata

UUID canonical representation: `FixedSizeBinaryType(16)` + Field metadata `ARROW:extension:name = arrow.uuid`, RFC 4122 big-endian. Use `ArrowTypeMapper.ToArrowUuidBytes` / `FromArrowUuidBytes`.
