# Extending DtPipe: adding an Adapter or a Transformer

This quick guide explains where and how to add a new *adapter* (reader/writer) or a *transformer* in DtPipe.

## Recommended locations
- Adapters: `src/DtPipe/Adapters/<Provider>/`
- Transformers: `src/DtPipe/Transformers/<Name>/`
- Documentation: `EXTENDING.md` (this file)

## Adding an Adapter (Reader or Writer)
1. Create a folder `src/DtPipe/Adapters/<YourProvider>/`.
2. Add an options class (e.g. `MyProviderOptions.cs`) for provider-specific settings.
3. Implement the appropriate interface:
   - Reader: `IStreamReader` (key methods: `OpenAsync`, `ReadBatchesAsync` returning `IAsyncEnumerable`, `DisposeAsync`).
   - Writer: `IDataWriter` (key methods: `InitializeAsync`, `WriteBatchAsync`, `CompleteAsync`, `DisposeAsync`).
4. Add a `Descriptor` implementing `IProviderDescriptor<T>` (e.g. `MyProviderReaderDescriptor.cs`).
5. (Optional) Add a `ConnectionHelper` / `TypeMapper` if your driver requires specific conversions.
6. Register the provider in DI: open `src/DtPipe/Program.cs` and call `RegisterReader<MyProviderReaderDescriptor>(services);` or `RegisterWriter<MyProviderWriterDescriptor>(services);`.
7. Add unit/integration tests under `tests/DtPipe.Tests/Unit` or `tests/DtPipe.Tests/Integration`. Provide a small dataset or docker-compose if needed (see `tests/infra`).
8. Add a usage example to the README or scripts in `scripts/` if relevant.

Practical tips:
- Follow the structure and class names of existing adapters (DuckDB, PostgreSQL, Csv) for consistency.
- Respect the batching model (`ReadBatchesAsync`) to benefit from the streaming pipeline and keep memory usage low.

### Adapter Descriptor (Skeleton)

The descriptor bridges your implementation (Reader/Writer) and its options to the application.

```csharp
public class MyProviderReaderDescriptor : IProviderDescriptor<IStreamReader>
{
    // Unique identifier for this provider (used in CLI/YAML, e.g. "myprovider")
    public string Id => "myprovider";

    // How to create the Reader instance
    public IStreamReader Create(string connectionString, object options, DumpOptions context, IServiceProvider serviceProvider)
    {
        var myOptions = (MyProviderOptions)options;
        
        // Validate options if necessary
        if (string.IsNullOrEmpty(myOptions.ConnectionString))
            throw new InvalidOperationException("Missing connection string for MyProvider.");

        // Create the reader
        return new MyProviderReader(myOptions, context.BatchSize);
    }

    // Register your options so the CLI knows them
    public IEnumerable<Option> GetOptions()
    {
        return CliOptionBuilder.GenerateOptions<MyProviderOptions>();
    }
}
```

## Adding a Transformer
1. Create `src/DtPipe/Transformers/<YourTransformer>/`.
2. Add an options class `YourTransformerOptions.cs`.
   - Implement `IOptionSet`.
   - Define a `Prefix` (e.g. `fake` -> flags will be `--fake-seed`, `--fake-locale`).
   - Use `[Description]` or `[CliOption]` on properties to expose them to the CLI.

   ```csharp
   public class MyOptions : IOptionSet
   {
       public static string Prefix => "my"; // Flags: --my-property

       [Description("Description shown in --help")]
       public string Property { get; set; } = "default";
   }
   ```
3. Implement `IDataTransformer` (or follow existing classes):
   - `ValueTask<IReadOnlyList<ColumnInfo>> InitializeAsync(IReadOnlyList<ColumnInfo> columns, CancellationToken ct)` — prepare the target schema.
   - `object?[] Transform(object?[] row)` — transform a single row (should be fast and avoid excessive allocations when possible).
4. Provide an `IDataTransformerFactory` (e.g. `YourTransformerFactory`) that reads options and creates transformer instances.
5. Register the factory in DI: in `Program.cs` add `services.AddSingleton<IDataTransformerFactory, YourTransformerFactory>();`.
6. Add CLI/YAML support:
   - To expose CLI flags, see `src/DtPipe/Cli/CliOptionBuilder.cs` and `OptionsRegistry` for how options are declared and bound.
   - Document YAML syntax in `README.md` or here (see example below).
7. Add unit tests for the transformation logic.

Minimal transformer skeleton (simplified):
```csharp
public class MyTransformer : IDataTransformer
{
    public ValueTask<IReadOnlyList<ColumnInfo>> InitializeAsync(IReadOnlyList<ColumnInfo> columns, CancellationToken ct)
    {
        // return modified schema
        return new ValueTask<IReadOnlyList<ColumnInfo>>(columns);
    }

    public object?[] Transform(object?[] row)
    {
        // return transformed row
    }
}
```

## YAML — Example for adding a transformer
transformers:
  - mytransformer:
      options:
        foo: bar

The name `mytransformer` must match the identifier handled by your `IDataTransformerFactory`/`OptionsRegistry`.

## Registration and debugging
- To register: modify `src/DtPipe/Program.cs` (see `RegisterReader` / `RegisterWriter` and the `IDataTransformerFactory` registrations already present).
- Quick local build & run:
```bash
./build.sh
./dist/release/dtpipe --input "sqlite:sample.db" --query "SELECT 1" --output out.csv
```
- Run tests:
```bash
dotnet test DtPipe.sln
```

## Tests & CI
- Add unit tests for logic (transformers) and integration tests for adapters that depend on databases.
- If the provider requires native components (Oracle clients, etc.), document prerequisites in `README.md` and/or provide Docker images for tests in `tests/infra`.

## Disabling the TUI (non-interactive / CI)

DtPipe uses a live terminal UI for progress reporting. In CI or non-interactive environments you can disable the live TUI by setting the environment variable `DTPIPE_NO_TUI=1`, or by ensuring output is redirected. The reporter also disables itself when a `CI` environment variable is present.

## Contribution rules (quick)
- Follow naming conventions and the Descriptor/Factory pattern.
- Add tests and a minimal usage example.
- Document options exposed in the `Options` classes and the README if noteworthy.
