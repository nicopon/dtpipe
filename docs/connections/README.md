# Connection catalog

[← Documentation](../README.md)

Every source and target is named the same way: a **prefix**, then a connection string or a path.

```bash
dtpipe -i "pg:Host=db;Database=app;Username=app"  -o "sales.parquet"
       ^^                                            ^^^^^^^^^^^^^^
       prefix + ADO.NET connection string            path, format from the extension
```

## What this build supports

| System | Read | Write | Prefix | Needs a query | stdin/stdout |
|:---|:---:|:---:|:---|:---:|:---:|
| [SQL Server](sql-server.md) | ✅ | ✅ | `mssql:` | ✅ | — |
| [Oracle](oracle.md) | ✅ | ✅ | `ora:` | ✅ | — |
| [PostgreSQL](postgresql.md) | ✅ | ✅ | `pg:` | ✅ | — |
| [MySQL / MariaDB](mysql.md) | ✅ | ✅ | `mysql:` | ✅ | — |
| [DuckDB](duckdb.md) | ✅ | ✅ | `duck:` | ✅ | — |
| SQLite | ✅ | ✅ | `sqlite:` | ✅ | — |
| [CSV](files.md) | ✅ | ✅ | `csv:` / `.csv` | — | ✅ |
| [JSONL](files.md) | ✅ | ✅ | `jsonl:` / `.jsonl` | — | ✅ |
| [Parquet](files.md) | ✅ | ✅ | `parquet:` / `.parquet` | — | ✅ |
| [Apache Arrow](files.md) | ✅ | ✅ | `arrow:` / `.arrow` | — | ✅ |
| [XML](files.md) | ✅ | — | `xml:` / `.xml` | — | ✅ |
| [S3-compatible storage](object-storage.md) | ✅ | ✅ | `s3://`, `s3a://` | — | — |
| [Azure Blob](object-storage.md) | ✅ | ✅ | `azure://`, `az://` | — | — |

Plus three endpoints that exist to make pipelines testable: `generate:N` produces N rows from
nothing, `null:` accepts and discards everything, `checksum:` accepts rows and prints a digest.

> [!TIP]
> `dtpipe providers` lists what *your* binary carries. It is derived from the registered
> components, so it cannot be out of date the way this table can.

## The three rules that apply everywhere

**1. The prefix decides, the content never does.** `mssql:Server=host;Database=db` and
`mysql:Server=host;Database=db` are the same string after the prefix — no amount of sniffing could
tell them apart, so dtpipe does not try. For databases the prefix is required; for files the
extension is enough (`sales.parquet`), and a prefix is still accepted when you want to be explicit
(`parquet:sales.parquet`).

**2. Connection strings are ADO.NET, and the keys belong to the driver.** Not a URI —
`Key=Value;Key=Value`. dtpipe fixes the form; the vocabulary comes from the .NET driver behind the
provider, which is where to look for anything these pages do not list:

| Provider | Driver |
|:---|:---|
| `pg:` | [Npgsql](https://www.npgsql.org/doc/connection-string-parameters.html) |
| `mssql:` | [Microsoft.Data.SqlClient](https://learn.microsoft.com/en-us/sql/connect/ado-net/connection-string-syntax) |
| `ora:` | [Oracle.ManagedDataAccess](https://docs.oracle.com/en/database/oracle/oracle-database/23/odpnt/ConnectionConnectionString.html) |
| `mysql:` | [MySqlConnector](https://mysqlconnector.net/connection-options/) |
| `sqlite:` | [Microsoft.Data.Sqlite](https://learn.microsoft.com/en-us/dotnet/standard/data/sqlite/connection-strings) |

Coming from a Python or SQLAlchemy URI? The translation table is in
[REFERENCE.md](../../REFERENCE.md#conversion-reference-table).

**3. A database source needs to be told what to read.** Either a query or a table:

```bash
dtpipe -i "pg:…" --query "SELECT id, email FROM users WHERE active"   -o users.csv
dtpipe -i "pg:…" --table users                                        -o users.csv
dtpipe -i "pg:…" --query "@queries/active_users.sql"                  -o users.csv
```

`--table` builds `SELECT * FROM <table>`, quoting each part the way that engine spells it —
`eshop.customers` becomes `"eshop"."customers"` on PostgreSQL and `[eshop].[customers]` on SQL
Server. A file source ignores both: it has no query language.

## Look before you load

```bash
dtpipe inspect -i "mssql:Server=…;Database=app"                     # tables in the database
dtpipe inspect -i "mssql:Server=…;Database=app" --query "SELECT …"  # the shape of a result
dtpipe inspect -i sales.parquet --format json                       # machine-readable
```

## Keep credentials out of the command line

Any connection string can come from the OS credential store instead of your shell history:

```bash
dtpipe secret set prod-db "pg:Host=prod;Database=app;Username=app;Password=…"
dtpipe -i keyring://prod-db --query "SELECT * FROM users" -o users.parquet
```

See [Secrets](../guides/secrets.md) for inline interpolation, environment variables and `@file`.

---

Per-system pages: [SQL Server](sql-server.md) · [Oracle](oracle.md) ·
[PostgreSQL](postgresql.md) · [MySQL](mysql.md) · [DuckDB](duckdb.md) · [Files](files.md) ·
[Object storage](object-storage.md)
