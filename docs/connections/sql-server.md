# SQL Server

[← Connections](README.md) · [Documentation](../README.md)

Read and write. Prefix `mssql:`. Driver:
[Microsoft.Data.SqlClient](https://learn.microsoft.com/en-us/sql/connect/ado-net/connection-string-syntax),
compiled into the binary — there is nothing to install on the host, no ODBC manager and no driver
package to match against the server version.

## Connection string

```
mssql:Server=host,1433;Database=app;User Id=sa;Password=…;TrustServerCertificate=True
```

The keys are the driver's, not dtpipe's, so anything Microsoft.Data.SqlClient accepts works here:

| Key | For |
|:---|:---|
| `Integrated Security=true` | Windows authentication — the process's own identity, no password in the string |
| `Encrypt=true`, `TrustServerCertificate=True` | TLS, and whether an untrusted certificate is accepted |
| `ApplicationIntent=ReadOnly` | Route to a readable secondary in an availability group |
| `Application Name=dtpipe-nightly` | What your DBA sees in `sys.dm_exec_sessions` — worth setting |
| `Connect Timeout`, `Max Pool Size`, `MultiSubnetFailover` | The usual driver knobs |

## Reading

```bash
dtpipe -i "mssql:Server=prod;Database=sales;Integrated Security=true" \
  --query "SELECT * FROM dbo.Invoices WHERE Modified >= '2026-01-01'" \
  -o invoices.parquet
```

`--table dbo.Invoices` is the shorthand for `SELECT * FROM [dbo].[Invoices]` — each part of a
qualified name is quoted the way SQL Server spells it.

A stored procedure or any non-`SELECT` statement needs `--unsafe-query`, which says out loud that
dtpipe can no longer tell whether the source is read-only.

## Writing

```bash
dtpipe -i orders.parquet \
  -o "mssql:Server=dwh;Database=staging;Integrated Security=true" \
  --table stg_orders --strategy Truncate
```

| Option | Effect |
|:---|:---|
| `--table` | Target table (default: `export`) |
| `--strategy` | `Append`, `Truncate`, `DeleteThenInsert`, `Recreate`, `Upsert`, `Ignore` |
| `--key "Id"` | Key columns for `Upsert`/`Ignore`; read from the target's primary key when omitted |
| `--auto-migrate` | `ALTER TABLE ADD COLUMN` for columns the target does not have yet |

Rows are loaded with `SqlBulkCopy`, under a table lock and its own transaction — the same path a
.NET application would use for a bulk load.

Identifiers are stored as given: `--table StockMoves` creates `StockMoves`, addressable unquoted,
because SQL Server compares case-insensitively. See
[Write strategies](../guides/write-strategies.md) for the full matrix, and for the four schema
regimes (`discard`, `freeze`, `evolve`, `off`) that every run states in one line.

## Preview, and the one caveat that is SQL Server's

`--dry-run N` neutralises the writer everywhere. On the **source** side it also classifies the
query and, where the engine supports it, sets the session read-only so the server itself refuses a
write.

> [!IMPORTANT]
> SQL Server has no read-only session. `ApplicationIntent=ReadOnly` routes to a replica; it does
> not make a session read-only. So on `mssql:` the guarantee is the verb scan alone — and the run
> says which of the two guarantees it had, rather than presenting the weaker one as the stronger.

## Common pitfalls

| Symptom | Cause |
|:---|:---|
| `A connection was successfully established … certificate chain` | Add `TrustServerCertificate=True`, or install the server certificate |
| `Login failed for user ''` | `Integrated Security=true` uses the *process* identity — check which account the scheduler or service runs as |
| Upsert inserts duplicates | The key columns are not covered by a unique index on the target |
| A `datetimeoffset` loses its offset on another engine | The offset survives to any target that has an offset-carrying type; check the target's type mapping |

---

See also: [dtpipe and .NET](../dotnet.md) ·
[Write strategies](../guides/write-strategies.md) ·
[Incremental loading](../guides/incremental.md) ·
[REFERENCE.md](../../REFERENCE.md#providers)
