# Extending DtPipe: adding an Adapter or a Transformer

[![Open in GitHub Codespaces](https://github.com/codespaces/badge.svg)](https://github.com/codespaces/new?repo=nicopon/DtPipe)
This guide explains where and how to add a new *adapter* (reader/writer) or a *transformer* in DtPipe.

---

## Architecture Overview

The solution is split into three layers:

| Project | Role |
|---|---|
| `DtPipe.Core` | Interfaces, models, pipeline engine — **no external deps** |
| `DtPipe.Adapters` | All provider implementations (readers/writers) |
| `DtPipe` | CLI, DI wiring, Descriptors, Transformers |

### Where to put new code

| What | Implementation | Descriptor / Registration |
|---|---|---|
| **New reader/writer** | `src/DtPipe.Adapters/Adapters/<Provider>/` | `src/DtPipe/Adapters/<Provider>/` |
| **New transformer** | `src/DtPipe/Transformers/<Name>/` | `src/DtPipe/Program.cs` |

---

## Adding an Adapter (Reader or Writer)

### Step 1 — Options class (in DtPipe.Adapters)

Create `src/DtPipe.Adapters/Adapters/MyProvider/MyProviderReaderOptions.cs`.

For SQL readers, implement **`IQueryAwareOptions`** so the CLI propagates `--query` automatically:

```csharp
using DtPipe.Core.Options;

public record MyProviderReaderOptions : IProviderOptions, IQueryAwareOptions
{
    public static string Prefix => "myprovider";
    public static string DisplayName => "MyProvider Reader";

    public string? Query { get; set; }  // propagated by CliStreamReaderFactory
}
```

For SQL writers, implement **`IKeyAwareOptions`** so the CLI propagates `--key` automatically:

```csharp
public class MyProviderWriterOptions : IWriterOptions, IKeyAwareOptions
{
    public static string Prefix => "myprovider";
    public static string DisplayName => "MyProvider Writer";

    public string? Key { get; set; }  // propagated by JobService

    [CliOption(Description = "Target table name", Hidden = true)]
    public string Table { get; set; } = "export";

    [CliOption(Description = "Write strategy", Hidden = true)]
    public MyProviderWriteStrategy? Strategy { get; set; }
}
```

> **Why `Hidden = true`?** Generic options (`--table`, `--strategy`, `--insert-mode`) appear once in the CLI help under "Core". Provider-specific versions of these options are hidden to keep help output clean while retaining backward compatibility.

### Step 2 — IStreamReader / IDataWriter (in DtPipe.Adapters)

```csharp
public class MyProviderStreamReader : IStreamReader
{
    private readonly string _connectionString;
    private readonly string _query;

    public MyProviderStreamReader(string connectionString, string query)
    {
        _connectionString = connectionString;
        _query = query;
    }

    public IReadOnlyList<PipeColumnInfo>? Columns { get; private set; }

    public async Task OpenAsync(CancellationToken ct = default)
    {
        // Open connection, execute query, read schema → populate Columns
    }

    public async IAsyncEnumerable<ReadOnlyMemory<object?[]>> ReadBatchesAsync(
        int batchSize, [EnumeratorCancellation] CancellationToken ct = default)
    {
        // Yield batches of rows as object?[][] wrapped in ReadOnlyMemory
    }

    public ValueTask DisposeAsync() { ... }
}
```

### Step 3 — Descriptor (in DtPipe/Adapters)

Create `src/DtPipe/Adapters/MyProvider/MyProviderReaderDescriptor.cs`.

The descriptor is the only coupling point between `DtPipe` (CLI) and `DtPipe.Adapters`. It must **not** use `DumpOptions` — the query is received via the casted options object:

```csharp
using DtPipe.Core.Abstractions;

public class MyProviderReaderDescriptor : IProviderDescriptor<IStreamReader>
{
    public string ProviderName => "myprovider";
    public Type OptionsType => typeof(MyProviderReaderOptions);
    public bool RequiresQuery => true;

    public bool CanHandle(string connectionString)
        => connectionString.StartsWith("myprovider:", StringComparison.OrdinalIgnoreCase);

    public IStreamReader Create(string connectionString, object options, IServiceProvider serviceProvider)
    {
        var o = (MyProviderReaderOptions)options;
        return new MyProviderStreamReader(connectionString, o.Query!);
        // o.Query is set by CliStreamReaderFactory via IQueryAwareOptions — never null here
    }
}
```

### Step 4 — Register in DI

In `src/DtPipe/Program.cs`:

```csharp
RegisterReader<MyProviderReaderDescriptor>(services);
RegisterWriter<MyProviderWriterDescriptor>(services);
```

### Step 5 — (Optional) Type mapping

Implement `ITypeMapper` in `MyProviderTypeConverter.cs` for custom CLR → column type mapping.

### Step 6 — Tests

Place unit tests under `tests/DtPipe.Tests/Unit/` and integration tests under `tests/DtPipe.Tests/Integration/`. If the provider requires a database, provide a Docker Compose service in `tests/infra/`.

---

## Adding a Transformer

### Step 1 — Options class

```csharp
// src/DtPipe/Transformers/MyTransformer/MyTransformerOptions.cs
public class MyTransformerOptions : IOptionSet
{
    public static string Prefix => "my";  // CLI flags: --my-column, etc.
    public static string DisplayName => "My Transformer";

    [Description("Description shown in --help")]
    public string? Column { get; set; }
}
```

### Step 2 — IDataTransformer

```csharp
public class MyTransformer : IDataTransformer
{
    public ValueTask<IReadOnlyList<PipeColumnInfo>> InitializeAsync(
        IReadOnlyList<PipeColumnInfo> columns, CancellationToken ct)
    {
        // Optionally add/remove columns from schema
        return ValueTask.FromResult(columns);
    }

    public object?[] Transform(object?[] row)
    {
        // Transform and return the row (avoid excessive allocations in hot path)
        return row;
    }
}
```

### Step 3 — IDataTransformerFactory

The factory must implement **`ICliContributor`** so the CLI discovers its options:

```csharp
public class MyTransformerFactory : IDataTransformerFactory, ICliContributor
{
    private readonly OptionsRegistry _registry;

    public MyTransformerFactory(OptionsRegistry registry) => _registry = registry;

    public string Category => "Transformers";

    public IEnumerable<Option> GetCliOptions()
        => CliOptionBuilder.GenerateOptions<MyTransformerOptions>();

    public void BindOptions(ParseResult parseResult, OptionsRegistry registry)
        => CliOptionBuilder.BindOptions<MyTransformerOptions>(parseResult, registry);

    public IDataTransformer CreateFromConfiguration(IReadOnlyList<(string Key, string Value)> config)
    {
        var options = _registry.Get<MyTransformerOptions>();
        // Apply config overrides if needed
        return new MyTransformer(options);
    }
}
```

### Step 4 — Register in DI

```csharp
// src/DtPipe/Program.cs
services.AddSingleton<IDataTransformerFactory, MyTransformerFactory>();
```

### Step 5 — YAML syntax

```yaml
transformers:
  - my:
      column: MyColumn
```

The key (`my`) must match your `Prefix`.

---

## Build & Test

```bash
# Build + unit tests
./build.sh

# Integration tests (requires Docker)
dotnet test DtPipe.sln

# Quick smoke test
./dist/release/dtpipe -i "sqlite:sample.db" -q "SELECT 1" -o out.csv
```

---

## Contribution rules

- Follow the Descriptor/Factory pattern. Use existing adapters (PostgreSQL, DuckDB) as reference.
- Implement `IQueryAwareOptions` for SQL readers, `IKeyAwareOptions` for SQL writers.
- Factory **must** implement `ICliContributor` or its options will be silently ignored.
- Respect the batching model (`ReadBatchesAsync`) to keep memory usage constant.
- Add tests and a minimal usage example.

---

## Using ExportService Programmatically (Library Mode)

When consuming DtPipe as a library (without the CLI), drive the pipeline through `ExportService` directly using `PipelineOptions` — a CLI-neutral DTO that lives in `DtPipe.Core.Models`.

```csharp
var pipelineOptions = new PipelineOptions
{
    BatchSize    = 10_000,
    MaxRetries   = 3,
    RetryDelayMs = 1000,
    // Schema options, sampling, dry-run, hooks — all optional
};

await exportService.RunExportAsync(
    options:       pipelineOptions,
    providerName:  "myprovider",    // display only — used in logs/UI
    outputPath:    "output.parquet", // display only
    ct:            cancellationToken,
    pipeline:      new List<IDataTransformer>(),
    readerFactory: myReaderFactory,
    writerFactory: myWriterFactory,
    registry:      optionsRegistry);
```

> [!NOTE]
> `providerName` and `outputPath` are display-only strings forwarded to the `IExportObserver` for logging/UI. They do not affect the pipeline execution itself.
>
> Connection strings, queries, and strategies are passed via the `OptionsRegistry` (populated from your own options classes that implement `IQueryAwareOptions` / `IKeyAwareOptions`).
