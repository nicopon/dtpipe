# DtPipe.Adapters

All built-in provider adapters (readers and writers) for DtPipe. Depends only on **DtPipe.Core**.

---

## Package

```xml
<PackageReference Include="DtPipe.Core" Version="1.0.0" />
<PackageReference Include="DtPipe.Adapters" Version="1.0.0" />
```

---

## Supported Providers

| Provider | Reader | Writer | Prefix |
|---|:---:|:---:|---|
| **DuckDB** | ✅ | ✅ | `duck:` |
| **SQLite** | ✅ | ✅ | `sqlite:` |
| **PostgreSQL** | ✅ | ✅ | `pg:` |
| **SQL Server** | ✅ | ✅ | `mssql:` |
| **Oracle** | ✅ | ✅ | `ora:` |
| **CSV** | ✅ | ✅ | `csv:` / `.csv` |
| **JsonL** | ✅ | ✅ | `jsonl:` / `.jsonl` |
| **Apache Arrow** | ✅ | ✅ | `arrow:` / `.arrow` |
| **Parquet** | ✅ | ✅ | `parquet:` / `.parquet` |
| **Generate** | ✅ | — | `generate:N` |
| **Null** | — | ✅ | `null:` |
| **Checksum** | — | ✅ | `checksum:` |

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
    ├── Csv/             CsvStreamReader, CsvDataWriter, ...
    ├── JsonL/           JsonLStreamReader, JsonLDataWriter, ...
    ├── Arrow/           ArrowStreamReader, ArrowDataWriter, ...
    ├── Parquet/         ParquetStreamReader, ParquetDataWriter, ...
    ├── Generate/        GenerateStreamReader (synthetic data)
    ├── Null/            NullDataWriter
    ├── Checksum/        ChecksumDataWriter
    └── MemoryChannel/   In-process channels for DAG branch wiring (row and Arrow modes)
```

---

## Adapter Contracts

Every adapter implements `DtPipe.Core` interfaces. Descriptors (in `src/DtPipe/Adapters/`) wire them into the DI container:

```
IProviderDescriptor<IStreamReader>  →  registered in DtPipe CLI
IProviderDescriptor<IDataWriter>    →  registered in DtPipe CLI
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

### Standard I/O Support (`SupportsStdio`)

Providers explicitly declare if they support standard input/output pipes via the `SupportsStdio` metadata property.

- **Supported**: CSV, Parquet, Arrow, JsonL, Null.
- **Unsupported**: PostgreSql, SqlServer, Oracle, DuckDB (DataSource), etc.

To use standard I/O, you **must** use the `-` character in the connection string:
- `csv:-` (Explicit)
- `csv` (Shorthand for `csv:-`)

If you attempt to use `-` with an unsupported provider (e.g., `pg:-`), DtPipe will throw an error early.
3. Implement `MyProviderStreamReader : IStreamReader` and/or `MyProviderDataWriter : IDataWriter`.
4. Create `MyProviderReaderOptions : IProviderOptions, IQueryAwareOptions` (if SQL-based).
5. Add a `MyProviderReaderDescriptor : IProviderDescriptor<IStreamReader>` in `src/DtPipe.Adapters/Adapters/MyProvider/`.
6. Register in DI in `src/DtPipe/Program.cs`.

> [!TIP]
> Always prefer using the explicit `{ComponentName}:` prefix in connection strings to avoid ambiguity.

See [EXTENDING.md](../../EXTENDING.md) for a full walkthrough.

---

## License
MIT
