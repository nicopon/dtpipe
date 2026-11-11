# Connection catalog

[← Documentation](../README.md)

A source or a target is a **prefix**, then a connection string or a path:

```bash
dtpipe -i "pg:Host=db;Database=app;Username=app"  -o "sales.parquet"
       ^^                                            ^^^^^^^^^^^^^^
       prefix + connection string                    path: the format comes from the extension
```

| System | Read | Write | Prefix |
|:---|:---:|:---:|:---|
| [SQL Server](sql-server.md) | ✅ | ✅ | `mssql:` |
| [Oracle](oracle.md) | ✅ | ✅ | `ora:` |
| [PostgreSQL](postgresql.md) | ✅ | ✅ | `pg:` |
| [MySQL / MariaDB](mysql.md) | ✅ | ✅ | `mysql:` |
| [DuckDB](duckdb.md) | ✅ | ✅ | `duck:` |
| SQLite | ✅ | ✅ | `sqlite:` |
| [CSV, JSONL, Parquet, Arrow](files.md) | ✅ | ✅ | `csv:`, `jsonl:`, `parquet:`, `arrow:`, or the extension |
| [XML](files.md) | ✅ | — | `xml:` or `.xml` |
| [S3-compatible storage](object-storage.md) | ✅ | ✅ | `s3://`, `s3a://` |
| [Azure Blob](object-storage.md) | ✅ | ✅ | `azure://`, `az://` |

Three endpoints help with testing: `generate:N` produces N rows, `null:` discards everything, and
`checksum:` prints a digest of what it receives. `dtpipe providers` lists what your binary carries.

## Three rules

**The prefix decides.** `mssql:Server=host;Database=db` and `mysql:Server=host;Database=db` look
the same after the prefix, so a database always needs one. A file only needs its extension.

**Connection strings are ADO.NET** (`Key=Value;Key=Value`), not URIs, and the keys belong to the
driver:

| Prefix | Driver, and its list of keys |
|:---|:---|
| `pg:` | [Npgsql](https://www.npgsql.org/doc/connection-string-parameters.html) |
| `mssql:` | [Microsoft.Data.SqlClient](https://learn.microsoft.com/en-us/sql/connect/ado-net/connection-string-syntax) |
| `ora:` | [Oracle.ManagedDataAccess](https://docs.oracle.com/en/database/oracle/oracle-database/23/odpnt/ConnectionConnectionString.html) |
| `mysql:` | [MySqlConnector](https://mysqlconnector.net/connection-options/) |
| `sqlite:` | [Microsoft.Data.Sqlite](https://learn.microsoft.com/en-us/dotnet/standard/data/sqlite/connection-strings) |

Converting from a URI such as `postgresql://user@host/db`: see
[REFERENCE.md](../../REFERENCE.md#conversion-reference-table).

**A database source needs a query or a table:**

```bash
dtpipe -i "pg:…" --query "SELECT id, email FROM users WHERE active"   -o users.csv
dtpipe -i "pg:…" --table users                                        -o users.csv
dtpipe -i "pg:…" --query "@queries/active_users.sql"                  -o users.csv
```

`--table eshop.customers` reads `SELECT * FROM "eshop"."customers"`, quoted the way each engine
expects. A file source has no query.

## Look before you load

```bash
dtpipe inspect -i "mssql:Server=…;Database=app"                     # the tables
dtpipe inspect -i "mssql:Server=…;Database=app" --query "SELECT …"  # the shape of a result
dtpipe inspect -i sales.parquet --format json                       # for a script
```

To keep credentials out of the command line: [Secrets](../guides/secrets.md).

---

Provider options: [REFERENCE.md](../../REFERENCE.md#providers).
