# Extending DtPipe: adding an Adapter or a Transformer

[![Open in GitHub Codespaces](https://github.com/codespaces/badge.svg)](https://github.com/codespaces/new?repo=nicopon/DtPipe)
This guide explains where and how to add a new *adapter* (reader/writer) or a *transformer* in DtPipe.

---

## Architecture Overview

| Project | Role |
|---|---|
| `DtPipe.Core` | Interfaces, models, pipeline engine — **no external deps** |
| `DtPipe.Adapters` | All row-based and columnar provider implementations |
| `DtPipe.Transformers` | Row and columnar data transformers |
| `DtPipe.Processors` | C# side of SQL stream processors (DuckDB) |
| `DtPipe` | CLI entry point, DI wiring |

### Where to put new code

| What | Implementation | Descriptor / Registration |
|---|---|---|
| **New reader/writer** | `src/DtPipe.Adapters/Adapters/<Provider>/` | `src/DtPipe.Adapters/Adapters/<Provider>/` |
| **New transformer** | `src/DtPipe.Transformers/<Category>/<Name>/` | `src/DtPipe/Program.cs` |

---

## Adding an Adapter (Reader or Writer)

### Step 1 — Options class (in DtPipe.Adapters)

Create `src/DtPipe.Adapters/Adapters/MyProvider/MyProviderReaderOptions.cs`.

For SQL readers, implement **`IQueryAwareOptions`** so the CLI propagates `--query` automatically:

```csharp
using DtPipe.Core.Attributes;
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
public class MyProviderWriterOptions : IProviderOptions, IKeyAwareOptions
{
    public static string Prefix => "myprovider";
    public static string DisplayName => "MyProvider Writer";

    public string? Key { get; set; }  // propagated by JobService

    [ComponentOption(Description = "Target table name", Hidden = true)]
    public string Table { get; set; } = "export";

    [ComponentOption(Description = "Write strategy", Hidden = true)]
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

### Step 3 — Descriptor (in DtPipe.Adapters)

Create `src/DtPipe.Adapters/Adapters/MyProvider/MyProviderReaderDescriptor.cs`.

The descriptor is the only coupling point between `DtPipe` (CLI) and `DtPipe.Adapters`. It must **not** use dump options — the query is received via the cast options object:

```csharp
using DtPipe.Core.Abstractions;

public class MyProviderReaderDescriptor : IProviderDescriptor<IStreamReader>
{
    public string ComponentName => "myprovider";
    public string Category => "Readers";
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

Options live in `src/DtPipe.Transformers/<Category>/<Name>/`. Implement `ITransformerOptions` and
annotate properties with `[ComponentOption]` — CLI flags are auto-generated from these attributes.

```csharp
// src/DtPipe.Transformers/Row/MyTransformer/MyTransformerOptions.cs
using DtPipe.Core.Attributes;
using DtPipe.Core.Options;

public class MyTransformerOptions : ITransformerOptions
{
    public static string Prefix => "my";       // CLI prefix: --my-column, --my-mode, etc.
    public static string DisplayName => "My Transformer";

    [ComponentOption("--my-column", Description = "Column(s) to process (repeatable)")]
    public IEnumerable<string> Columns { get; set; } = Array.Empty<string>();

    [ComponentOption("--my-mode", Description = "Processing mode")]
    public string? Mode { get; set; }
}
```

`[ComponentOption]` supports `Name`, `Description`, `Aliases`, `Hidden`, and `Required`.
If `Name` is omitted, the flag name is derived automatically from the property name and the prefix
(e.g. property `Mode` with prefix `my` → `--my-mode`).

### Step 2 — IDataTransformer

```csharp
public class MyTransformer : IDataTransformer
{
    private readonly MyTransformerOptions _options;

    public MyTransformer(MyTransformerOptions options) => _options = options;

    public ValueTask<IReadOnlyList<PipeColumnInfo>> InitializeAsync(
        IReadOnlyList<PipeColumnInfo> columns, CancellationToken ct)
    {
        // Optionally add, remove, or rename columns in the schema.
        return ValueTask.FromResult(columns);
    }

    public object?[] Transform(object?[] row)
    {
        // Transform and return the row.
        // Avoid allocations in the hot path — reuse the input array when possible.
        return row;
    }

    public ValueTask<IReadOnlyList<object?[]>> FlushAsync()
    {
        // Called at end-of-stream. Return any buffered rows (stateful transformers only).
        return ValueTask.FromResult<IReadOnlyList<object?[]>>(Array.Empty<object?[]>());
    }
}
```

### Step 3 — IDataTransformerFactory

The factory has three creation paths:

- `CreateFromOptions(object)` — called by the CLI pipeline builder (main path since refactoring).
- `CreateFromConfiguration(...)` — legacy path kept for compatibility; can delegate to `CreateFromOptions`.
- `CreateFromYamlConfig(TransformerConfig)` — YAML job file path.

```csharp
using DtPipe.Configuration;
using DtPipe.Core.Abstractions;
using DtPipe.Core.Options;
using DtPipe.Core.Pipelines;

public class MyTransformerFactory : IDataTransformerFactory
{
    private readonly OptionsRegistry _registry;

    public MyTransformerFactory(OptionsRegistry registry) => _registry = registry;

    public string ComponentName => "my";
    public string Category => "Transformers";
    public Type OptionsType => typeof(MyTransformerOptions);
    public bool CanHandle(string connectionString) => false;

    // Main CLI path: options pre-bound by TransformerArgsBinder
    public IDataTransformer? CreateFromOptions(object options) =>
        options is MyTransformerOptions o ? CreateFromOptions(o) : null;

    public IDataTransformer CreateFromOptions(MyTransformerOptions options) =>
        new MyTransformer(options);

    // Legacy path — kept for backward compatibility
    public IDataTransformer CreateFromConfiguration(IEnumerable<(string Option, string Value)> config)
    {
        var options = new MyTransformerOptions
        {
            Columns = config.Select(x => x.Value).ToArray()
        };
        return new MyTransformer(options);
    }

    // YAML path
    public IDataTransformer? CreateFromYamlConfig(TransformerConfig config)
    {
        if (config.Mappings == null || config.Mappings.Count == 0) return null;
        var options = new MyTransformerOptions
        {
            Columns = config.Mappings.Keys.ToArray()
        };
        if (config.Options != null) ConfigurationBinder.Bind(options, config.Options);
        return new MyTransformer(options);
    }
}
```

> **Note on `ICliContributor`:** Transformers do **not** need to implement `ICliContributor`.
> CLI flags are auto-generated from `[ComponentOption]` attributes by `CliOptionBuilder.GenerateFlagDefsForType(OptionsType)`.
> Implement `ICliContributor` only if you need to customize flag definitions beyond what `[ComponentOption]` provides
> (e.g. non-standard arity or complex aliases).

### Step 4 — Register in DI

In `src/DtPipe/Program.cs`, add one line in `ConfigureServices`:

```csharp
RegisterTransformer<MyTransformerFactory>(services);
```

This wraps the factory in `CliDataTransformerFactory` and registers it as `IDataTransformerFactory`.

### Step 5 — YAML syntax

The YAML key must match `ComponentName`. The `mappings` dictionary structure varies by transformer.

```yaml
transformers:
  - my:
      mappings:
        ColumnA: ~         # key = column name; value semantics depend on transformer
        ColumnB: someValue
      options:
        my-mode: fast      # matches [ComponentOption] names (hyphenated)
```

---

## Build & Test

```bash
# Build + unit tests
./build.sh

# Integration tests (requires Docker)
./test_local.sh

# Quick smoke test
./dist/release/dtpipe -i "generate:5" --my-column "GenerateIndex" -o /dev/null --dry-run 1
```

---

## Contribution rules

- Follow the Descriptor/Factory pattern. Use existing adapters (PostgreSQL, DuckDB) as reference.
- Implement `IQueryAwareOptions` for SQL readers, `IKeyAwareOptions` for SQL writers.
- Use `[ComponentOption]` for all CLI-visible options — no manual `System.CommandLine` wiring.
- Respect the batching model (`ReadBatchesAsync`) to keep memory usage constant.
- Add tests and a minimal usage example.

---

## Advanced

DtPipe uses the **Arrow C Data Interface** for zero-copy data exchange between pipeline stages.

### IColumnarStreamReader
Adapters that can emit Arrow `RecordBatch` directly should implement `IColumnarStreamReader`.
This enables the columnar fast-path: when source and destination both support it, data passes
through without row conversion.

### Using ExportService Programmatically (Library Mode)

```csharp
var pipelineOptions = new PipelineOptions
{
    BatchSize    = 10_000,
    // Schema options, sampling, dry-run — all optional
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

> `providerName` and `outputPath` are display-only strings forwarded to the `IExportObserver`.
> Connection strings, queries, and strategies are passed via the `OptionsRegistry`.
