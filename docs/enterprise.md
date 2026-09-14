# DtPipe in a .NET estate

[← Documentation](README.md)

dtpipe is a .NET application, and that shows up in three places that matter on a corporate
network: **what you have to install**, **what a connection string looks like**, and **which
databases are first-class**.

## One file, nothing to install around it

A release archive holds a single self-contained executable. The .NET runtime, the DuckDB engine
and every database driver are inside it.

| You do **not** need | Because |
|:---|:---|
| A .NET runtime on the host | The binary is self-contained |
| A Python environment | There is none |
| An ODBC driver manager or a `DSN` | `Microsoft.Data.SqlClient` speaks TDS directly |
| Oracle Instant Client, `LD_LIBRARY_PATH`, a matching architecture package | `Oracle.ManagedDataAccess` is fully managed |
| Admin rights | Unpack it anywhere and run it |

Six platforms per release — Linux, macOS and Windows, x64 and arm64. On a locked-down workstation
or a hardened build agent, "copy one file" is often the difference between a tool you can use and
one you cannot.

## The connection strings you already have

Sources and targets take **ADO.NET connection strings** — the ones in your `appsettings.json`, not
a URI dialect invented for the tool:

```bash
dtpipe -i "mssql:Server=prod;Database=sales;Integrated Security=true" \
  --query "SELECT * FROM dbo.Invoices" -o invoices.parquet
```

The key vocabulary belongs to the driver, so anything your applications already rely on works
here: Windows authentication, `Encrypt` / `TrustServerCertificate`, `ApplicationIntent`,
`MultiSubnetFailover`, pooling, `Application Name` — which is worth setting, because it is what
your DBA sees in `sys.dm_exec_sessions` when they ask who is running the query.

| Provider | Driver |
|:---|:---|
| `mssql:` | Microsoft.Data.SqlClient |
| `ora:` | Oracle.ManagedDataAccess |
| `pg:` | Npgsql |
| `mysql:` | MySqlConnector |
| `sqlite:` | Microsoft.Data.Sqlite |

## Oracle and SQL Server are not an afterthought

Both read and write, with bulk loading (`SqlBulkCopy`, Oracle direct-path), upsert, schema
migration and per-engine identifier casing handled rather than approximated: Oracle folds
identifiers up, PostgreSQL down, the rest store what they are given, and dtpipe hands each the
spelling it reads unquoted so the object it creates is addressable from every other tool.

See [SQL Server](connections/sql-server.md) and [Oracle](connections/oracle.md), including the two
engine-specific traps that cost the most time: SQL Server has no read-only session for previews,
and an Oracle cursor comparison needs an explicit format mask.

## Credentials

Connection strings live in the OS credential store — Windows Credential Manager, macOS Keychain,
Linux Secret Service — and are referenced by alias:

```bash
dtpipe secret set prod-db "mssql:Server=prod;Database=sales;Integrated Security=true"
dtpipe -i keyring://prod-db --table dbo.Invoices -o invoices.parquet
```

On a build agent, `${{ENV_VAR}}` interpolation covers the same ground from the pipeline's secret
store. Either way, nothing sensitive needs to sit in a job file or a shell history. See
[Secrets](guides/secrets.md).

## Automation contract

| | |
|:---|:---|
| Exit codes | `0` success · `1` failure · `130` cancelled (Ctrl-C). A cancellation never reports as success |
| `--metrics-path run.json` | Structured result: row counts read and written, throughput, peak memory, start/end, duration |
| `--log run.log` | The run's log to a file |
| `--strict-bindings` | An unrecognised flag or a failed binding becomes a non-zero exit instead of a warning |
| `--job pipeline.yaml` | The pipeline itself, in version control, reviewable in a pull request |

```json
{
  "StartTime": "2026-09-13T16:22:52.322362Z",
  "EndTime": "2026-09-13T16:22:52.329564Z",
  "ReadCount": 1000,
  "WriteCount": 1000,
  "OverallThroughputRowsPerSec": 173532.78,
  "PeakMemoryWorkingSetMb": 76.67,
  "Duration": "00:00:00.0072020"
}
```

## Handling production data

Three things make the difference between a tool that touches production and one that is allowed
to:

- **Anonymization happens in transit.** The source is read, never written; no intermediate file
  holds clear values. Seeded faking keeps joins working across tables, so an anonymized extract is
  still usable. See [Anonymization](guides/anonymization.md).
- **A preview writes nothing**, and on PostgreSQL, Oracle, MySQL and SQLite the source session is
  set read-only so the *server* enforces it. On SQL Server no such session exists and the run says
  so rather than implying a guarantee it does not have. See
  [Preview and checkpoints](guides/preview-and-checkpoints.md).
- **Materialised data is always encrypted** (AES-GCM), with no opt-out, so a checkpoint left on a
  laptop is inert and a purge is reliable.

## Running without internet access

dtpipe collects no telemetry and contacts no service of its own, and the binary carries its
drivers — so a database-to-database or database-to-file pipeline runs on an isolated network
as-is. Two things do need access:

- **DuckDB extensions** (`httpfs`, `azure`, `spatial`…) are installed from DuckDB's repository on
  first use. Pre-populate the extension directory, or avoid the extensions.
- **Object storage** obviously needs to reach the endpoint — including a MinIO on your own
  network, via `--s3-endpoint`.

---

See also: [Install](install.md) · [SQL Server](connections/sql-server.md) ·
[Oracle](connections/oracle.md) · [YAML jobs](guides/yaml-jobs.md)
