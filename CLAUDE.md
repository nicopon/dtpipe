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
| `src/DtPipe.Processors` | C# side of SQL stream processors (DuckDB.NET + DataFusion P/Invoke wrappers, factories) |
| `src/DtPipe.Processors.DataFusion` | Rust/Cargo native library — DataFusion engine, Arrow IPC bridge |
| `src/Apache.Arrow.Ado` | Standalone ADO.NET → Arrow library; zero DtPipe deps (depends on `Apache.Arrow.Serialization` only) |
| `src/Apache.Arrow.Serialization` | Standalone CLR↔Arrow type map + POCO serializer; zero DtPipe deps, no external deps beyond `Apache.Arrow` |
| `tests/DtPipe.Tests` | xunit.v3 unit and integration tests |

#### File placement conventions

- `DtPipe.Core` contains **only** abstractions, models, and the generic DAG/pipeline engine — no concrete implementations.
- Each concrete transformer in `DtPipe.Transformers` lives in its own subdirectory (`Row/Expand/`, `Columnar/Filter/`…) with a matching sub-namespace (`DtPipe.Transformers.Row`, `DtPipe.Transformers.Columnar`…).
- Each concrete stream processor in `DtPipe.Processors` follows the same pattern: one subdirectory per processor (`DataFusion/`, `Merge/`…) with a matching sub-namespace (`DtPipe.Processors.DataFusion`, `DtPipe.Processors.Merge`…).
- Readers and writers in `DtPipe.Adapters` are grouped by technology under `Adapters/<Name>/`.

### Core Data Flow

The fundamental pipeline: `IStreamReader` → `IDataTransformer[]` → `IDataWriter`.

1. `JobService.Build()` constructs the `System.CommandLine` root command and wires all providers as CLI contributors.
2. On execution, `CliDagParser.Parse()` inspects raw args to detect multi-branch DAG syntax (multiple `--input` / `--from` flags).
3. For linear pipelines, `LinearPipelineService` drives execution through `ExportService.RunExportAsync()`.
4. For DAG pipelines, `DagOrchestrator` spawns concurrent `Task`s per branch, wiring them via in-memory `Channel<T>` for zero-copy data flow.

`PipelineEngine` (`DtPipe.Core`) is a headless, CLI-free engine for programmatic use. It accepts `IStreamReader + IRowDataWriter + IDataTransformer[]` and drives the full pipeline without DI or CLI dependencies.

### Provider Pattern

Every adapter implements `IProviderDescriptor<TService>` and is registered in `Program.cs` via `RegisterReader<T>()` / `RegisterWriter<T>()` / `RegisterStreamTransformer<T>()`. The `CliProviderFactory<T>` wraps descriptors into CLI contributors that auto-generate `System.CommandLine` options from the descriptor's `OptionsType` (a class decorated with `[ComponentOption]` attributes). Provider-specific options are stored in `OptionsRegistry` (keyed by type) and scoped per DI scope.

### DAG Pipeline

`CliDagParser` splits args into `BranchDefinition` records. Only two tokens trigger a split:
- `-i` / `--input` — when an input was already seen in the current branch (new data source)
- `--from <alias[,alias...]>` — always (fan-out consumer **or** processor main source)

Neither `--sql` nor boolean processor flags (e.g. `--merge`) trigger a split. The canonical processor syntax is:

```
--from <alias[,alias...]> [--ref <alias[,alias...]>] (--sql "<query>" | --<processor>) [--alias <name>] [-o <dest>]
```

- `--from a,b,c` declares one or more streaming main sources (comma-separated). Fan-out consumers use a single alias; multi-stream processors (e.g. merge) use multiple aliases.
- `--ref a,b` declares materialized reference sources (preloaded before query execution, comma-separated). Used by SQL JOIN branches.
- `--sql "<query>"` runs an inline SQL query. Default engine: DataFusion (native Rust, fast analytics). Use `--sql-engine duckdb` (or `DTPIPE_SQL_ENGINE=duckdb`) to switch to the DuckDB engine.
- `--merge` (and future boolean flags) declares the processor explicitly by name. Each processor is registered as a `BooleanProcessorFlag` in `CliPipelineRules`.

Branches communicate via `IMemoryChannelRegistry` (either native `Channel<IReadOnlyList<object?[]>>` or Arrow `Channel<RecordBatch>`). The `MappedMemoryChannelRegistry` handles logical-to-physical alias resolution for fan-out (broadcast/tee) scenarios. A `BranchChannelContext` injected per branch carries an `AliasMap` that translates logical aliases (as written in CLI args) to physical channel names (including fan-out sub-channels like `s__fan_0`). This mapping is populated by `DagOrchestrator` and is transparent to all downstream components including processors.

#### Canonical DAG topologies

In the patterns below, `{reader:cfg}` represents any reader and its full provider configuration (e.g. `pg:"host=...;Database=mydb" --query "SELECT ..."` or `parquet:data.parquet`). Similarly, `{writer:cfg}` represents any writer configuration (e.g. `pg:"..." --table t` or `csv:out.csv`).

| Topology | CLI pattern | Branches |
|---|---|---|
| **Linear** | `-i {reader:cfg} -o {writer:cfg}` | 1 |
| **Two independent sources** | `-i {reader1:cfg} -o {writer1:cfg}  -i {reader2:cfg} -o {writer2:cfg}` | 2 |
| **SQL (single source)** | `-i {reader:cfg} --alias a  --from a --sql "SELECT * FROM a"` | 2 |
| **SQL JOIN (main + ref)** | `-i {main:cfg} --alias m  -i {ref:cfg} --alias r  --from m --ref r --sql "SELECT * FROM m JOIN r ON ..."` | 3 |
| **Merge (UNION ALL)** | `-i {readerA:cfg} --alias a  -i {readerB:cfg} --alias b  --from a,b --merge -o {writer:cfg}` | 3 |
| **Fan-out (tee)** | `-i {reader:cfg} --alias s  --from s -o {writerA:cfg}  --from s -o {writerB:cfg}` | 3 |
| **Fan-out + SQL** | `-i {reader:cfg} --alias s  --from s -o {writerA:cfg}  --from s --sql "SELECT ..."` | 3 |
| **Diamond (fan-out → filter → join)** | `-i {reader:cfg} --alias s  --from s --filter '...' --alias hi  --from s --filter '...' --alias lo  --from hi --ref lo --sql "SELECT * FROM hi JOIN lo ON ..."` | 4 |
| **Join → fan-out** | `... --from m --ref r --sql "SELECT ..." --alias joined  --from joined -o {writerA:cfg}  --from joined -o {writerB:cfg}` | 5 |

### SQL Processors

`CompositeSqlTransformerFactory` is the entry point registered in DI; it selects the engine based on `--sql-engine` (or `DTPIPE_SQL_ENGINE`). Two engines are available:

**DataFusion (default)** — `DataFusionSqlTransformerFactory` / `DataFusionProcessor`:
- **C# layer** (`DtPipe.Processors`): `DataFusionSqlTransformerFactory` / `DataFusionProcessor` implement `IStreamTransformerFactory` / `IColumnarStreamReader` and call into the native library via P/Invoke (`DataFusionBridge`).
- **Rust layer** (`DtPipe.Processors.DataFusion`): Cargo library (`libdatafusion_bridge`) hosting a Tokio runtime and a DataFusion session context. Receives Arrow data via the C Data Interface and streams results back through an anonymous pipe using Arrow IPC.

**DuckDB (alternative, `--sql-engine duckdb`)** — `DuckDBSqlTransformerFactory` / `DuckDBSqlProcessor`:
- Pure C# using DuckDB.NET. No native bridge build required.
- Streaming input (`--from`): `RegisterTableFunction` + `CREATE VIEW alias AS SELECT * FROM __dtpipe_stream_alias()`.
- Materialized input (`--ref`): `CREATE TABLE alias (...)` + `DuckDBAppender` row by row.

Logical SQL table names come from the branch args (`--from`/`--ref`). Physical channel aliases are resolved by `DagOrchestrator` before the processor is created, via `BranchChannelContext.AliasMap` — processors never need to know about fan-out sub-channel naming (`__fan_N` suffixes).

**`--ref` materialization is intentional.** Secondary sources declared via `--ref` are fully read into memory before query execution. This is required for both engines to build a cost-based execution plan — streaming both main and reference tables simultaneously prevents join optimization. Only the source declared via `--from` (the main) uses a true streaming path. Keep `--ref` tables at manageable size; for large lookups, pre-filter upstream.

### Transformer Pipeline

Transformers implement `IDataTransformer` with three methods: `InitializeAsync` (schema propagation), `Transform` (per-row), and `Flush` (end-of-stream for stateful transformers). `PipelineSegmenter` groups consecutive columnar-capable transformers into segments to enable Arrow zero-copy bridging between row and columnar processing modes.

### Key Interfaces

- `IStreamReader` / `IColumnarStreamReader` — open + stream batches
- `IDataWriter` — initialize schema + complete (base contract for all writers)
- `IRowDataWriter` — extends `IDataWriter`; adds `WriteBatchAsync` for row-based output
- `IDataTransformer` / `IDataTransformerFactory` — transform rows; factory creates from CLI config or YAML
- `IStreamTransformerFactory` — factory for multi-input stream processors (SQL joins, merges); `Create(branchArgs, ctx, serviceProvider)` receives `BranchChannelContext` for alias resolution
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

The canonical Arrow representation of a UUID in DtPipe is `BinaryType` with `.NET Guid.ToByteArray()` byte order. Any adapter that produces UUID values must emit them in this format, or the user must insert an explicit conversion step.

## Apache.Arrow.Serialization

`src/Apache.Arrow.Serialization/` is a **standalone library** with no DtPipe dependencies (only `Apache.Arrow`). It provides:

### Dependency graph

```
Apache.Arrow.Serialization   ← standalone, no DtPipe deps
       ↑
Apache.Arrow.Ado             ← uses ArrowTypeResult
       ↑
DtPipe.Core                  ← ArrowTypeMapper is a facade over ArrowTypeMap
       ↑
DtPipe.Adapters, DtPipe.Processors, …
```

### ArrowTypeMap (`Mapping/ArrowTypeMap.cs`)
Canonical CLR↔Arrow mapping. All mapping logic lives here; `ArrowTypeMapper` in `DtPipe.Core` is a pure facade that delegates to it.

`ArrowTypeResult` is a struct carrying both the Arrow type and any required Field metadata:
```csharp
public readonly struct ArrowTypeResult
{
    public IArrowType ArrowType { get; }
    public IReadOnlyDictionary<string, string>? Metadata { get; }
}
```
`GetLogicalType(Type)` returns `ArrowTypeResult` — callers that only need the `IArrowType` access `.ArrowType`.
`TryGetLogicalType(Type, out ArrowTypeResult)` is the exception-free variant for contexts with a string fallback.

### ArrowSerializer / ArrowDeserializer
Reflection-based round-trip for strongly-typed POCOs and dynamic types (`ExpandoObject`, `JsonObject`):
```csharp
RecordBatch batch = await ArrowSerializer.SerializeAsync(myList);
IEnumerable<MyPoco> items = ArrowDeserializer.Deserialize<MyPoco>(batch);
```
Supports: primitives, `Guid`, `DateTime`, `DateTimeOffset`, `TimeSpan`, `decimal`, `byte[]`, nullable wrappers, enums, collections (`List<T>`, arrays), dictionaries, nested structs.
Schema is inferred from property types via `ArrowReflectionEngine`; compiled expression delegates are cached per type.

### ArrowReflectionEngine (`Reflection/ArrowReflectionEngine.cs`)
Derives an Arrow `Schema` from a .NET `Type` or a runtime `IDictionary` (dynamic/ExpandoObject).
Calls `ArrowTypeMap.TryGetLogicalType` for scalars and handles complex types (List → `ListType`, Dictionary → `MapType`, nested objects → `StructType`).

### FixedSizeBinaryArrayBuilder (`Reflection/FixedSizeBinaryArrayBuilder.cs`)
Builds a `FixedSizeBinaryArray` of arbitrary byte width. An intentional copy of the same class in `DtPipe.Core` — kept separate to avoid a circular dependency. **Keep both files in sync** when changing build logic.

---

## Adding a New Adapter

1. Create a descriptor class implementing `IProviderDescriptor<IStreamReader>` (or `IDataWriter`).
2. Define an options class implementing `IOptionSet` with `[ComponentOption]` attributes.
3. Register in `Program.cs` with `RegisterReader<YourDescriptor>()` or `RegisterWriter<YourDescriptor>()`.
4. The CLI options are auto-generated from the options type — no manual `System.CommandLine` wiring needed.

### Type Handling in Adapters

#### Row-mode writers (`IDataWriter`)
Row-mode writers receive `object?[]` rows. The CLR type at each cell is declared in `PipeColumnInfo.ClrType`.
Writers must NOT assume source and target types match. Use `ColumnConverterFactory.Build(sourceClrType, targetClrType)` (`DtPipe.Core.Helpers`) to compile a typed converter **once per column** during initialization, then invoke it in the write loop:

```csharp
// During initialization — build once
_converters = columns.Select(col =>
    ColumnConverterFactory.Build(col.ClrType, targetType)).ToArray();

// In write loop — invoke per cell
var convertedVal = _converters[i](row[i]);
```

Do **not** call `ValueConverter.ConvertValue()` per-cell directly — it performs reflection-based dispatch on every call.

`SqliteDataWriter` is exempt: `SqliteParameter` accepts `object` and coerces natively.

#### Columnar writers (`IColumnarDataWriter`)
Columnar writers receive `RecordBatch` directly (zero intermediate `object?[]`). When a Field context is available, extract values using `ArrowTypeMapper.GetValueForField(array, field, rowIndex)` (resolves extension types such as `arrow.uuid` → `Guid`). Without Field context, `ArrowTypeMapper.GetValue(array, rowIndex)` returns raw storage values (e.g. `byte[]` for `FixedSizeBinaryArray`). No `ValueConverter` call needed. This is the preferred path for all new DB writers.

Implementing `IColumnarDataWriter` enables `PipelineExecutor` to route columnar sources (Parquet, PostgreSQL, DuckDB output) directly to the writer without any row-level bridging.

`DuckDbDataWriter` and `ParquetDataWriter` implement only `IColumnarDataWriter` — they have no row-mode fallback. When the pipeline source is row-based, `PipelineExecutor` bridges rows→Arrow automatically via `BridgeRowsToColumnarAsync`.

#### Text sources (CSV, JSON, etc.)
Text readers emit `string` for every column by default. Downstream writers will hit the conversion path in `ColumnConverterFactory`. Two remediation options in order of preference:
1. User declares `--column-types "Col:type"` — reader emits typed values directly (zero conversion downstream).
2. `--auto-column-types` — inference runs automatically on the first 100 rows and applies types before the main pipeline opens.

When implementing a new text reader, implement `IColumnTypeInferenceCapable` so the dry-run suggestion and `--auto-column-types` work automatically.

#### Arrow ↔ CLR mapping: no heuristics

The conversion between Arrow types and CLR types is limited to direct, unambiguous mappings
defined in `Apache.Arrow.Serialization.Mapping.ArrowTypeMap` (the canonical source) and
exposed via the `ArrowTypeMapper` facade in `DtPipe.Core`.

For Arrow types whose CLR semantics depend on context (e.g. `FixedSizeBinary(n)` which could be
a UUID, a hash, a key, etc.), the mapping to a specific CLR type must be driven by an
**Arrow extension type** (Field metadata), not by a heuristic on the data structure.

**Firm rule: `ArrowTypeMapper.GetClrType(IArrowType)` never infers a semantic CLR type from
the storage type alone.** If the Arrow type is ambiguous (e.g. `FixedSizeBinary`), it maps to
the most generic CLR type (`byte[]`). Semantic resolution requires `ArrowTypeMapper.GetClrTypeFromField(Field)`.

**Key APIs** (`ArrowTypeMapper` wraps all of these):
- `GetLogicalType(Type clrType)` → `ArrowTypeResult` — preferred way to get the Arrow type + any required metadata for a CLR type. Returns a struct with `.ArrowType` and `.Metadata`.
- `GetField(string name, Type clrType, bool isNullable)` → `Field` — creates an Arrow Field with metadata embedded. Use this instead of `new Field(...)` to guarantee metadata is always present.
- `GetClrTypeFromField(Field)` → `Type` — checks extension metadata before falling through to storage type.
- `GetClrType(IArrowType)` → `Type` — storage-type-only mapping, no metadata, returns `byte[]` for `FixedSizeBinary`.
- `GetValueForField(array, field, i)` → `object?` — respects extension metadata (e.g. `arrow.uuid` → `Guid`).
- `ToArrowUuidBytes(guid)` / `FromArrowUuidBytes(span)` — byte-level RFC 4122 conversion helpers.

Use `GetClrTypeFromField(field)` wherever a `Field` is available (reading schemas from Arrow IPC,
Parquet, channel schemas). Use `GetValueForField(array, field, i)` for value extraction when the
Field is available. `GetClrType(IArrowType)` and `GetValue(array, i)` are for contexts without Field.

If an external source omits Arrow extension metadata, users must insert an explicit transform/cast
step in the pipeline. DtPipe does not infer semantic types from data shape.

**Example — UUID columns:**
- Storage: `FixedSizeBinaryType(16)` + Field metadata `ARROW:extension:name = arrow.uuid`
- `ArrowSchemaFactory.Create()` emits this automatically for `typeof(Guid)` columns via `GetField()`
- `GetLogicalType(typeof(Guid))` → `ArrowTypeResult { ArrowType = FixedSizeBinaryType(16), Metadata = { "ARROW:extension:name": "arrow.uuid" } }`
- `GetClrType(FixedSizeBinaryType(16))` → `typeof(byte[])` (generic, no inference)
- `GetClrTypeFromField(field with arrow.uuid)` → `typeof(Guid)` (explicit via metadata)
- `GetValueForField(array, field with arrow.uuid, i)` → `Guid`; without metadata → `byte[]`
