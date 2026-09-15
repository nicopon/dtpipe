# DtPipe in a .NET estate

[← Documentation](README.md)

dtpipe is a .NET application, and that shows up in four places on a corporate network: **what you
install**, **what a connection string looks like**, **which databases it knows well**, and **what
you can reference from your own code**.

This page is about fitting into an existing estate. Nothing here is a claim about other tools —
if what you use already does these things, that is one less reason to change.

## One file

A release archive holds a single self-contained executable. The .NET runtime, the DuckDB engine
and every database driver are inside it.

| Carried in the binary | What that saves |
|:---|:---|
| The .NET runtime | Nothing to install on the host first |
| `Microsoft.Data.SqlClient` | Speaks TDS directly — no ODBC driver manager, no `DSN` |
| `Oracle.ManagedDataAccess` | Fully managed — no Instant Client, no `LD_LIBRARY_PATH`, no architecture-matched package |
| DuckDB, Npgsql, MySqlConnector, Microsoft.Data.Sqlite | The engines and drivers the pipelines use |

Unpacking needs no admin rights and no installer. Six platforms per release — Linux, macOS and
Windows, x64 and arm64. On a locked-down workstation or a hardened build agent, that can be the
deciding constraint. Because the drivers travel with the binary, a database-to-database or
database-to-file pipeline also runs on an isolated network as-is.

The cost of that choice is size: a self-contained binary is around 240 MB on disk, because it
carries all of the above whether a given run needs it or not.

## The connection strings you already have

Sources and targets take **ADO.NET connection strings** — the same ones your applications keep in
`appsettings.json`:

```bash
dtpipe -i "mssql:Server=prod;Database=sales;Integrated Security=true" \
  --query "SELECT * FROM dbo.Invoices" -o invoices.parquet
```

The key vocabulary belongs to the driver, so what your applications already rely on works here:
Windows authentication, `Encrypt` / `TrustServerCertificate`, `ApplicationIntent`,
`MultiSubnetFailover`, pooling, `Application Name` — which is worth setting, because it is what
your DBA sees in `sys.dm_exec_sessions` when they ask who is running the query.

| Provider | Driver |
|:---|:---|
| `mssql:` | Microsoft.Data.SqlClient |
| `ora:` | Oracle.ManagedDataAccess |
| `pg:` | Npgsql |
| `mysql:` | MySqlConnector |
| `sqlite:` | Microsoft.Data.Sqlite |

There is no ODBC layer and no DSN anywhere in that list: the connection string you paste is the one
the driver parses.

## Oracle and SQL Server

Both read and write, with bulk loading (`SqlBulkCopy`, Oracle direct-path), upsert and schema
migration. Identifier casing follows each engine: Oracle folds unquoted identifiers up, PostgreSQL
folds them down, the rest store what they are given — dtpipe hands each engine the spelling it
reads unquoted, so an object it creates is addressable from every other tool on the same database.

See [SQL Server](connections/sql-server.md) and [Oracle](connections/oracle.md), including the two
engine-specific traps that cost the most time: SQL Server has no read-only session for previews,
and an Oracle cursor comparison needs an explicit format mask.

## Referencing dtpipe from your own code

Every release publishes libraries to NuGet alongside the tool, so the parts dtpipe is built from
are available to a .NET application:

| Package | What it gives you |
|:---|:---|
| `DtPipe.Core` | The contracts — `IStreamReader`, `IDataWriter`, `IDataTransformer` — and the pipeline models |
| `DtPipe.Adapters` | The readers and writers, constructed directly |
| `DtPipe.Transformers` | The row and columnar transformers |
| `DtPipe.Processors` | The SQL stream processors |
| `DtPipe.Arrow.Ado` | **ADO.NET to Arrow, standalone.** No dtpipe dependency: `AdoToArrow.ReadToArrowBatchesAsync(reader)` turns any `DbDataReader` into record batches, and `RecordBatchDataReader` goes back the other way |
| `DtPipe.Arrow.Serialization` | **The CLR/Arrow type map and a POCO serializer, standalone.** Its only dependency is `Apache.Arrow` |

```bash
dotnet add package DtPipe.Adapters
dotnet add package DtPipe.Transformers
```

The two Arrow packages are the ones that stand on their own. They were written so the conversion
layer could be replaced without touching the engine, and that same property makes them useful to a
project that has nothing to do with dtpipe.

> [!IMPORTANT]
> **The pieces are published; the orchestration is not.** The loop that drives a reader through the
> transformers into a writer ships inside the `dtpipe` tool package, not in a library. Embedding
> means owning that loop.
>
> It is short, but two details are not optional. A transformer that turns one row into several
> implements `IMultiRowTransformer`, a separate interface — a loop that only calls `Transform`
> drops the extra rows silently. And a stateful transformer holds its result until `Flush()`, whose
> rows still have to travel through the stages that come after it.
>
> A working version of that loop, with both details handled, is in
> [dtpipe-sandbox](https://github.com/nicopon/dtpipe-sandbox) — `src/DtPipe.Sample`, which also
> shows a reader consumed by hand, a writer fed from a `DataTable`, a custom C# transformer, and a
> LINQ sequence wrapped as a source.

One more thing a direct consumer has to know: **the `name:` prefix is command-line routing, not part
of the connection string.** It is stripped before the adapter is constructed, so from your own code
you pass what is left — `new GenerateReader("5", …)`, not `"generate:5"`.

## Credentials, automation, production data

Connection strings live in the OS credential store and are referenced by alias; on a build agent,
`${{ENV_VAR}}` covers the same ground from the pipeline's own secret store. See
[Secrets](guides/secrets.md).

Exit codes, `--metrics-path`, logs to a file, and what a preview does and does not guarantee against
a production source: see [YAML jobs](guides/yaml-jobs.md) and
[Preview and checkpoints](guides/preview-and-checkpoints.md).

## Running without internet access

dtpipe collects no telemetry and contacts no service of its own. Two things still need access:

- **DuckDB extensions** (`httpfs`, `azure`, `spatial`…) are installed from DuckDB's repository on
  first use. Pre-populate the extension directory, or avoid the extensions.
- **Object storage** needs to reach its endpoint — including a MinIO on your own network, via
  `--s3-endpoint`.

---

See also: [Install](install.md) · [SQL Server](connections/sql-server.md) ·
[Oracle](connections/oracle.md) · [YAML jobs](guides/yaml-jobs.md)
