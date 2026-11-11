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

The two Arrow packages have no dtpipe dependency and are useful on their own.

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
