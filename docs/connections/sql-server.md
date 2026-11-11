# SQL Server

[← Connections](README.md) · [Documentation](../README.md)

Read and write, prefix `mssql:`. The driver,
[Microsoft.Data.SqlClient](https://learn.microsoft.com/en-us/sql/connect/ado-net/connection-string-syntax),
is inside the binary: no ODBC and nothing to install.

```
mssql:Server=host,1433;Database=app;User Id=sa;Password=…;TrustServerCertificate=True
```

| Key | For |
|:---|:---|
| `Integrated Security=true` | Windows authentication, as the account running dtpipe |
| `Encrypt=true`, `TrustServerCertificate=True` | TLS, and accepting an untrusted certificate |
| `ApplicationIntent=ReadOnly` | Reading from a secondary of an availability group |
| `Application Name=dtpipe-nightly` | Naming the session your DBA sees |

## Read

```bash
dtpipe -i "mssql:Server=prod;Database=sales;Integrated Security=true" \
  --query "SELECT * FROM dbo.Invoices WHERE Modified >= '2026-01-01T00:00:00'" \
  -o invoices.parquet
```

A stored procedure or any statement other than `SELECT` needs `--unsafe-query`.

## Write

```bash
dtpipe -i orders.parquet \
  -o "mssql:Server=dwh;Database=staging;Integrated Security=true" \
  --table stg_orders --strategy Truncate
```

Rows are loaded in bulk with `SqlBulkCopy`. `CHECK` and `FOREIGN KEY` constraints are enforced: a
row that breaks one fails the run, naming the constraint. Strategies and schema modes:
[Writing to a database](../guides/write-strategies.md).

## Pitfalls

| Symptom | Cause |
|:---|:---|
| `'2026-01-02'` reads as 1 February | A bare date follows the session language. Write `'2026-01-02T00:00:00'` |
| `… certificate chain` at connection | Add `TrustServerCertificate=True`, or install the server certificate |
| `Login failed for user ''` | `Integrated Security` uses the account running dtpipe: check the scheduler's account |
| A `datetimeoffset` reads at `+00:00` | dtpipe reads it in UTC, the same instant. To keep the offset, select it as text |
| A preview does not protect the source | SQL Server has no read-only session: a preview only checks the query's verbs |

---

See also: [REFERENCE.md](../../REFERENCE.md#providers)
