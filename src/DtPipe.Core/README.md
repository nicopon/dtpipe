# DtPipe.Core

The core pipeline engine for DtPipe. Contains all abstractions, models, and pipeline logic with **no external dependencies** beyond `Microsoft.Extensions.Logging.Abstractions`.

> Suitable for use as a standalone NuGet package in custom ETL pipelines.

---

## Package

```xml
<PackageReference Include="DtPipe.Core" Version="1.0.0" />
```

---

## What's Inside

```
DtPipe.Core/
├── Abstractions/       # Core interfaces (row and columnar)
├── Attributes/         # [ComponentOption] attribute
├── Dialects/           # ISqlDialect, SQL generation helpers
├── Helpers/            # Shared utility helpers
├── Infrastructure/     # Arrow type mapping, schema factory, row↔columnar bridges
│   └── Arrow/
├── Models/             # Shared data models (PipeColumnInfo, etc.)
├── Options/            # IOptionSet, IQueryAwareOptions, IKeyAwareOptions, OptionsRegistry
├── Pipelines/          # PipelineExecutionPlan, PipelineSegment, DAG orchestration
│   └── Dag/
├── Security/           # SQL query validator
├── Validation/         # Schema and constraint validators
└── PipelineEngine.cs   # Headless pipeline engine for library use
```

---

## Key Abstractions

| Interface | Purpose |
|---|---|
| `IStreamReader` | Reads rows as async batches from a source |
| `IStreamReaderFactory` | Creates `IStreamReader` instances |
| `IColumnarStreamReader` | Reads Apache Arrow `RecordBatch` streams (zero-copy columnar path) |
| `IDataWriter` | Base contract for all writers |
| `IRowDataWriter` | Writes `object?[]` rows to a destination |
| `IColumnarDataWriter` | Writes Arrow `RecordBatch` directly (no row conversion) |
| `IDataWriterFactory` | Creates `IDataWriter` instances |
| `IDataTransformer` | Transforms a batch of rows in the pipeline |
| `IDataTransformerFactory` | Creates `IDataTransformer` instances |
| `IColumnarTransformer` | Columnar (Arrow-level) variant of `IDataTransformer` |
| `IMultiRowTransformer` | Transformer that may produce multiple rows per input row |
| `IColumnTypeInferenceCapable` | Reader that supports `--auto-column-types` inference |
| `IProviderDescriptor<T>` | Describes a provider: name, options type, factory method |
| `ISchemaInspector` | Introspects target schema |
| `ISchemaMigrator` | Applies schema migrations (auto-migrate) |
| `ISqlDialect` | Generates provider-specific DDL/DML SQL |

## Key Options Interfaces

| Interface | Purpose |
|---|---|
| `IOptionSet` | Base contract for all Options classes |
| `IQueryAwareOptions` | Implement on reader options to receive the global `--query` |
| `IKeyAwareOptions` | Implement on writer options to receive the global `--key` |

---

## Implementing a Custom Reader

```csharp
// 1. Define options
public record MyReaderOptions : IProviderOptions, IQueryAwareOptions
{
    public static string Prefix => "mydb";
    public static string DisplayName => "MyDB Reader";
    public string? Query { get; set; }
}

// 2. Implement IStreamReader
public class MyStreamReader : IStreamReader
{
    public IReadOnlyList<PipeColumnInfo>? Columns { get; private set; }
    public Task OpenAsync(CancellationToken ct) { ... }
    public IAsyncEnumerable<ReadOnlyMemory<object?[]>> ReadBatchesAsync(int batchSize, CancellationToken ct) { ... }
    public ValueTask DisposeAsync() { ... }
}

// 3. Implement IProviderDescriptor<IStreamReader>
public class MyReaderDescriptor : IProviderDescriptor<IStreamReader>
{
    public string ProviderName => "mydb";
    public Type OptionsType => typeof(MyReaderOptions);
    public bool RequiresQuery => true;
    public bool CanHandle(string cs) => cs.StartsWith("mydb:");
    public IStreamReader Create(string cs, object options, IServiceProvider sp)
        => new MyStreamReader(cs, ((MyReaderOptions)options).Query!);
}
```

---

## Running the Pipeline Directly

```csharp
var engine = new PipelineEngine(logger);
long rowsWritten = await engine.RunAsync(
    reader,
    writer,
    pipeline: transformers,   // optional
    batchSize: 50_000,
    ct: cancellationToken);
```

---

## License
MIT
