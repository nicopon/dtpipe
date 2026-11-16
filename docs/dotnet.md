# dtpipe and .NET

[← Documentation](README.md)

The parts dtpipe is built from are published to NuGet with each release, so a .NET application can
use them directly.

```bash
dotnet add package DtPipe.Adapters
dotnet add package DtPipe.Transformers
```

| Package | What it gives you |
|:---|:---|
| `DtPipe.Core` | The contracts (`IStreamReader`, `IDataWriter`, `IDataTransformer`) and the pipeline models |
| `DtPipe.Adapters` | The readers and writers |
| `DtPipe.Transformers` | The row and columnar transformers |
| `DtPipe.Processors` | The SQL stream processors |
| `DtPipe.Arrow.Ado` | ADO.NET to Arrow, standalone: `AdoToArrow.ReadToArrowBatchesAsync(reader)` turns any `DbDataReader` into record batches, `RecordBatchDataReader` goes back |
| `DtPipe.Arrow.Serialization` | The CLR/Arrow type map and a POCO serializer, standalone: it depends on `Apache.Arrow` only |
| `DtPipe.PipelineNode.Hosting` | Makes a .NET service a node of a [distributed pipeline](distributed.md): `AddDtPipeNode`, and `AddArrowSource` to serve the service's own data |

The two Arrow packages have no dtpipe dependency and are useful on their own.

## A service as a node

```bash
dotnet add package DtPipe.PipelineNode.Hosting
```

```xml
<PackageDownload Include="dtpipe" Version="[1.16.0]" />
```

```csharp
var node = builder.Services.AddDtPipeNode(o => { /* name, coordinator, key file */ });
node.AddArrowSource("orders", "Orders of the shop API", OrdersTransform.Schema, (ctx, ct) => OrdersTransform.ReadAsync(api, ct));
node.AddBrick("numbers", BrickKind.Source, "Generated numbers", "input: generate:1000");
```

A coordinator sees the service as a data node offering two bricks: `orders`, read from the service's
own code, and `numbers`, a standard dtpipe source. Each run of a fragment is a `dtpipe` child
process, taken from the `dtpipe` tool package. A tool package cannot be a `PackageReference`, so it
is downloaded, and the `PackageDownload` line stays in the service's own project. The machine needs
the .NET and ASP.NET Core runtimes.

A service you can run is in
[samples/arrow-socket-source](../samples/arrow-socket-source/README.md).

## What you write yourself

The loop that drives a reader through the transformers into a writer ships inside the `dtpipe`
tool, not in a library. Embedding the pieces means writing that loop, and two details matter:

- A transformer that turns one row into several implements `IMultiRowTransformer`. A loop that
  only calls `Transform` drops the extra rows.
- A stateful transformer holds rows until `Flush()`, and those rows still go through the steps
  after it.

A working loop, with both details handled, is in
[dtpipe-sandbox](https://github.com/nicopon/dtpipe-sandbox) under `src/DtPipe.Sample`. It also shows
a reader consumed by hand, a writer fed from a `DataTable`, a custom transformer, and a LINQ
sequence used as a source.

The `name:` prefix belongs to the command line, not to the connection string: from code, pass what
follows it, as in `new GenerateReader("5", …)` rather than `"generate:5"`.

---

Writing a new adapter or transformer: [EXTENDING.md](../EXTENDING.md). The connection string of
each driver: [Connection catalog](connections/README.md). The common flags:
[REFERENCE.md](../REFERENCE.md).
