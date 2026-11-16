# Extending DtPipe: adding an Adapter or a Transformer

[![Open in GitHub Codespaces](https://github.com/codespaces/badge.svg)](https://github.com/codespaces/new?repo=nicopon/DtPipe)
This guide explains where and how to add a new *adapter* (reader/writer) or a *transformer* in
DtPipe.

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

| What | Implementation | Descriptor / factory |
|---|---|---|
| **New reader/writer** | `src/DtPipe.Adapters/Adapters/<Provider>/` | beside it |
| **New transformer** | `src/DtPipe.Transformers/<Category>/<Name>/` | beside it |

Nothing is registered: at start, the CLI finds every descriptor and factory of its four component
assemblies by the interface it implements.

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

    public string? Key { get; set; }  // propagated by ProviderConfigurationService

    [ComponentOption("--table", Aliases = new[] { "-t" }, Description = "Target table name", Required = true)]
    public string Table { get; set; } = string.Empty;

    [ComponentOption("--strategy", Aliases = new[] { "-s" }, Description = "Write strategy")]
    public MyProviderWriteStrategy? Strategy { get; set; }
}
```

> Each writer declares its own `--table` and `--strategy`: the help lists every option under the
> heading of the provider that takes it.

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

The descriptor is the only coupling point between `DtPipe` (CLI) and `DtPipe.Adapters`. It must
**not** use dump options — the query is received via the cast options object:

```csharp
using DtPipe.Core.Abstractions;

public class MyProviderReaderDescriptor : IProviderDescriptor<IStreamReader>
{
    public string ComponentName => "myprovider";
    public string Category => "Readers";
    public Type OptionsType => typeof(MyProviderReaderOptions);
    public bool RequiresQuery => true;
    // Content check ONLY — never test for your own "myprovider:" prefix here. CanHandle receives
    // the RAW connection string, and selector routing ("myprovider:", "myprovider+variant:") is
    // ComponentSelector's job. Judge by file extension or connection-string keywords instead.
    public bool CanHandle(string connectionString)
        => connectionString.EndsWith(".myext", StringComparison.OrdinalIgnoreCase);

    public IStreamReader Create(string connectionString, object options, IServiceProvider serviceProvider)
    {
        var o = (MyProviderReaderOptions)options;
        return new MyProviderStreamReader(connectionString, o.Query!);
        // o.Query is set by CliStreamReaderFactory via IQueryAwareOptions — never null here
    }
}
```

### Step 4 — Nothing to register

The descriptor is found at start by `ComponentCatalog`, which scans `DtPipe.Adapters` and the other
component assemblies. It needs a parameterless constructor, and its `ComponentName` must be unique
among readers (or among writers): a duplicate stops the start.

### Step 5 — (Optional) Type mapping

Implement `ITypeMapper` in `MyProviderTypeConverter.cs` for custom CLR → column type mapping.

### Step 6 — Tests

Place unit tests under `tests/DtPipe.Tests/Unit/` and integration tests under
`tests/DtPipe.Tests/Integration/`. If the provider requires a database, provide a Docker Compose
service in `tests/infra/`.

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

`[ComponentOption]` supports `Name`, `Description`, `Aliases` and `Required`.
If `Name` is omitted, the flag name is derived automatically from the property name and the prefix
(e.g. property `Mode` with prefix `my` → `--my-mode`).

### Step 2 — IDataTransformer

```csharp
public class MyTransformer : IDataTransformer
{
    private readonly MyTransformerOptions _options;

    public MyTransformer(MyTransformerOptions options) => _options = options;

    public ValueTask<IReadOnlyList<PipeColumnInfo>> InitializeAsync(
        IReadOnlyList<PipeColumnInfo> columns, CancellationToken ct = default)
    {
        // Optionally add, remove, or rename columns in the schema.
        return ValueTask.FromResult(columns);
    }

    public object?[]? Transform(IReadOnlyList<object?> row)
    {
        // Transform and return the row, or return null to drop it.
        // Avoid allocations in the hot path — the row is usually already an array.
        return row as object?[] ?? row.ToArray();
    }

    // Stateful transformers also implement Flush(): called at end-of-stream, it returns the buffered
    // rows. The default returns none.
    // public IEnumerable<object?[]> Flush() { ... }
}
```

### Step 3 — IDataTransformerFactory

Derive from `TransformerFactoryBase<TOptions>` (`DtPipe.Transformers.Abstract`), which supplies
`CanHandle` and `CreateFromOptions`. Three members remain:

- `CreateFromTypedOptions(options)` — a transformer from options already bound, whether they came
  from the command line or a job file.
- `CreateFromConfiguration(...)` — the `(flag, value)` pairs of one step, in the order written.
- `CreateOptionsFromYaml(TransformerConfig)` — the options a job file's `mappings:` describe, or
  `null` when there is nothing to build. The caller binds the block's `options:` onto the object
  you return, by property name.

```csharp
using DtPipe.Core.Abstractions;
using DtPipe.Core.Pipelines;
using DtPipe.Transformers.Abstract;

public class MyTransformerFactory : TransformerFactoryBase<MyTransformerOptions>
{
    public override string ComponentName => "my";
    public override string Category => "Transformers";

    protected override IDataTransformer? CreateFromTypedOptions(MyTransformerOptions options) =>
        new MyTransformer(options);

    public override IDataTransformer CreateFromConfiguration(
        IEnumerable<(string Option, string Value)> configuration) =>
        new MyTransformer(new MyTransformerOptions
        {
            Columns = configuration.Where(x => x.Option == "--my-column")
                                   .Select(x => x.Value).ToArray()
        });

    public override object? CreateOptionsFromYaml(TransformerConfig config) =>
        config.Mappings is { Count: > 0 }
            ? new MyTransformerOptions { Columns = config.Mappings.Keys.ToArray() }
            : null;
}
```

> **Note on `ICliContributor`:** Transformers do **not** need to implement `ICliContributor`. CLI
> flags are auto-generated from `[ComponentOption]` attributes by
> `CliOptionBuilder.GenerateFlagDefsForType(OptionsType)`. Implement `ICliContributor` only if you
> need to customize flag definitions beyond what `[ComponentOption]` provides (e.g. non-standard
> arity or complex aliases).

### Step 4 — Nothing to register

`ComponentCatalog` finds the factory at start, and the CLI wraps it in `CliDataTransformerFactory`.
Its `ComponentName` must be unique among transformers (a duplicate stops the start). The catalog
reads the name by constructing the factory without services, so a factory whose constructor needs
anything beyond what `ComponentCatalog.TryGetComponentName` supplies is skipped without a message.

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

### Driving the pieces from your own code

The loop that runs a reader through the transformers into a writer (`ExportService`) is part of
the `dtpipe` tool, not of a package you can reference. To run your adapter or transformer from
.NET code, write that loop: [dtpipe and .NET](docs/dotnet.md).
