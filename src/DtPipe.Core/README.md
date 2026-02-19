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
├── Abstractions/       # Core interfaces
├── Models/             # Shared data models (PipeColumnInfo, etc.)
├── Options/            # IOptionSet, IQueryAwareOptions, IKeyAwareOptions, OptionsRegistry
├── Attributes/         # [CliOption] attribute
├── Dialects/           # ISqlDialect, SQL generation helpers
├── Helpers/            # Shared utility helpers
├── Pipelines/          # TransformerPipelineBuilder
├── Resilience/         # Retry policy
├── Security/           # SQL query validator
└── PipelineEngine.cs   # Main export orchestration engine
```

---

## Key Abstractions

| Interface | Purpose |
|---|---|
| `IStreamReader` | Reads rows as async batches from a source |
| `IStreamReaderFactory` | Creates `IStreamReader` instances |
| `IDataWriter` | Writes rows to a destination |
| `IDataWriterFactory` | Creates `IDataWriter` instances |
| `IDataTransformer` | Transforms a batch of rows in the pipeline |
| `IDataTransformerFactory` | Creates `IDataTransformer` instances |
| `IProviderDescriptor<T>` | Describes a provider: name, options type, factory method |
| `ISchemaInspector` | Introspects target schema |
| `ISchemaMigrator` | Applies schema migrations (auto-migrate) |
| `ISqlDialect` | Generates provider-specific DDL/DML SQL |
| `ITypeMapper` | Maps CLR types to provider-specific column types |

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
var engine = new PipelineEngine(loggerFactory);
await engine.RunExportAsync(options, ct, transformers, readerFactory, writerFactory, registry);
```

---

## License
MIT
