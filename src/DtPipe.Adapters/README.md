# DtPipe.Adapters

All built-in provider adapters (readers and writers) for DtPipe. Depends only on **DtPipe.Core**.

---

## Supported Providers

| Provider | Reader | Writer | Prefix |
|---|:---:|:---:|---|
| **DuckDB** | ✅ | ✅ | `duck:` |
| **SQLite** | ✅ | ✅ | `sqlite:` |
| **PostgreSQL** | ✅ | ✅ | `pg:` |
| **MySQL / MariaDB** | ✅ | ✅ | `mysql:` |
| **SQL Server** | ✅ | ✅ | `mssql:` |
| **Oracle** | ✅ | ✅ | `ora:` |
| **CSV** | ✅ | ✅ | `csv:` / `.csv` |
| **JsonL** | ✅ | ✅ | `jsonl:` / `.jsonl` |
| **XML** | ✅ | — | `xml:` / `.xml` |
| **Apache Arrow** | ✅ | ✅ | `arrow:` / `.arrow` |
| **Parquet** | ✅ | ✅ | `parquet:` / `.parquet` |
| **S3 object storage** | ✅ | ✅ | `s3://` |
| **Azure Blob** | ✅ | ✅ | `azure://` |
| **Generate** | ✅ | — | `generate:N` |
| **Null** | — | ✅ | `null:` |
| **Checksum** | — | ✅ | `checksum:` |

`dtpipe providers` lists what a build carries.

---

## Structure

Each adapter family lives in its own folder under `Adapters/`:

```
DtPipe.Adapters/
└── Adapters/
    ├── Common/          Shared helpers (GuidAsBytesConsumer, …)
    ├── DuckDB/          DuckDbReaderOptions, DuckDbWriterOptions, DuckDbStreamReader, DuckDbDataWriter, ...
    ├── Sqlite/          SqliteReaderOptions, SqliteWriterOptions, SqliteStreamReader, SqliteDataWriter, ...
    ├── PostgreSQL/      PostgreSqlReaderOptions, PostgreSqlWriterOptions, PostgreSqlReader, PostgreSqlDataWriter, ...
    ├── SqlServer/       SqlServerReaderOptions, SqlServerWriterOptions, SqlServerStreamReader, SqlServerDataWriter, ...
    ├── Oracle/          OracleReaderOptions, OracleWriterOptions, OracleStreamReader, OracleDataWriter, ...
    ├── MySql/           MySqlReaderOptions, MySqlWriterOptions, MySqlReader, MySqlDataWriter, ...
    ├── Csv/             CsvStreamReader, CsvDataWriter, ...
    ├── JsonL/           JsonLStreamReader, JsonLDataWriter, ...
    ├── Xml/             XmlStreamReader, ...
    ├── Arrow/           ArrowStreamReader, ArrowDataWriter, ...
    ├── Parquet/         ParquetStreamReader, ParquetDataWriter, ...
    ├── S3/, Azure/      Object-storage readers and writers
    ├── Generate/        GenerateStreamReader (synthetic data)
    ├── Null/            NullDataWriter
    ├── Checksum/        ChecksumDataWriter
    └── MemoryChannel/   In-process Arrow channels for DAG branch wiring
```

---

## Adapter Contracts

Every adapter implements `DtPipe.Core` interfaces. A descriptor, in the adapter's own folder, tells
the CLI how to build it; the CLI finds descriptors at start, so none is registered by hand:

```
IProviderDescriptor<IStreamReader>  →  found by the CLI's ComponentCatalog
IProviderDescriptor<IDataWriter>    →  found by the CLI's ComponentCatalog
```

### Options Interfaces
All SQL reader options implement `IQueryAwareOptions` — the CLI propagates `--query` automatically.  
All SQL writer options implement `IKeyAwareOptions` — the CLI propagates `--key` automatically.

```csharp
public record PostgreSqlReaderOptions : IProviderOptions, IQueryAwareOptions
{
    public static string Prefix => PostgreSqlMetadata.ComponentName;
    public static string DisplayName => "PostgreSQL Reader";
    public string? Query { get; set; }  // set by CliStreamReaderFactory
}
```

---

## DuckDB-specific Options

The DuckDB reader and writer both support `--duck-init`, which runs SQL on the connection immediately after it opens — before any query execution or schema initialization.

```bash
# Reader: load an extension before querying
dtpipe -i "duck:warehouse.duckdb" --query "SELECT * FROM tbl" \
       --duck-init "LOAD spatial" -o result.parquet

# Writer: load credentials from a file
dtpipe -i data.parquet \
       -o "duck:output.duckdb" --table t \
       --duck-init "@/secret/azure_init.sql"
```

`--duck-init` accepts inline SQL or a file path prefixed with `@`. See [REFERENCE.md](../../REFERENCE.md#duckdb) for details.

---

## Write Strategies

All SQL writers support the same 6 standardized strategies via `--strategy`:

| Strategy | Behaviour |
|---|---|
| `Append` | INSERT new rows |
| `Truncate` | TRUNCATE + INSERT |
| `DeleteThenInsert` | DELETE + INSERT |
| `Recreate` | DROP + CREATE + INSERT |
| `Upsert` | UPDATE existing, INSERT new (requires PK) |
| `Ignore` | INSERT only for missing rows (requires PK) |

---

## Adding a New Adapter

1. Add a new folder under `src/DtPipe.Adapters/Adapters/MyProvider/`
2. Create `MyProviderMetadata.cs` to hold `ComponentName`, `CanHandle`, and `SupportsStdio`.
3. Implement `MyProviderStreamReader : IStreamReader` and/or `MyProviderDataWriter : IDataWriter`.
4. Create `MyProviderReaderOptions : IProviderOptions, IQueryAwareOptions` (if SQL-based).
5. Add a `MyProviderReaderDescriptor : IProviderDescriptor<IStreamReader>` in
   `src/DtPipe.Adapters/Adapters/MyProvider/`. It is found at start; there is nothing to register.

### Standard I/O Support (`SupportsStdio`)

Providers explicitly declare if they support standard input/output pipes via the `SupportsStdio` metadata property.

- **Supported**: CSV, Parquet, Arrow, JsonL, XML (reader), Null, Checksum.
- **Unsupported**: PostgreSql, SqlServer, Oracle, DuckDB (DataSource), etc.

To use standard I/O, you **must** use the `-` character in the connection string:
- `csv:-` (Explicit)
- `csv` (Shorthand for `csv:-`)

If you attempt to use `-` with an unsupported provider (e.g., `pg:-`), DtPipe will throw an error early.

> [!TIP]
> Always prefer using the explicit `{ComponentName}:` prefix in connection strings to avoid ambiguity.

See [EXTENDING.md](../../EXTENDING.md) for a full walkthrough.

---

## License
MIT
