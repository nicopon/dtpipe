# DtPipe in a .NET estate

[← Documentation](README.md)

dtpipe is a .NET application, and that shows up in three places on a corporate network: **what you
install**, **what a connection string looks like**, and **which databases it knows well**.

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
deciding constraint.

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

## Oracle and SQL Server

Both read and write, with bulk loading (`SqlBulkCopy`, Oracle direct-path), upsert and schema
migration. Identifier casing follows each engine: Oracle folds unquoted identifiers up, PostgreSQL
folds them down, the rest store what they are given — dtpipe hands each engine the spelling it
reads unquoted, so an object it creates is addressable from every other tool on the same database.

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
| `--strict-bindings` | The two tolerated binding faults become a non-zero exit: an undeclared flag that consumes no value, and an unknown `provider-options` key |
| `--job pipeline.yaml` | The pipeline itself, in version control, reviewable in a pull request |

```json
{
  "StartTime": "2026-09-14T16:59:08.834614Z",
  "EndTime": "2026-09-14T16:59:08.842365Z",
  "ReadCount": 1000,
  "WriteCount": 1000,
  "OverallThroughputRowsPerSec": 161563.9389288311,
  "PeakMemoryWorkingSetMb": 76.734375,
  "TransformerStats": {},
  "TransformerCountsByIndex": null,
  "Duration": "00:00:00.0077510"
}
```

`ReadCount` and `WriteCount` differing is the number to alert on: a row read and not written was
dropped by a filter, or refused by the target.

There is no scheduler and no daemon: a run starts, streams and exits. `cron`, a CI job or an
orchestrator decides *when*.

## Working with production data

- **Anonymization happens in transit.** The source is read, never written; no intermediate file
  holds clear values. Seeded faking keeps joins working across tables, so an anonymized extract is
  still usable. See [Anonymization](guides/anonymization.md).
- **A preview writes nothing**, and on PostgreSQL, Oracle, MySQL and SQLite the source session is
  set read-only so the *server* enforces it. On SQL Server no such session exists, and the run
  reports the weaker guarantee rather than implying the stronger one. See
  [Preview and checkpoints](guides/preview-and-checkpoints.md).
- **Materialised data is always encrypted** (AES-GCM), with no opt-out, so a checkpoint left on a
  laptop is inert and a purge is reliable.

Worth being explicit about what none of that covers: masking and faking are not anonymity proofs,
and choosing which columns are sensitive is yours.
[Anonymization](guides/anonymization.md#what-this-is-and-what-it-is-not) says where the limits are.

## Running without internet access

dtpipe collects no telemetry and contacts no service of its own, and the binary carries its
drivers — so a database-to-database or database-to-file pipeline runs on an isolated network
as-is. Two things do need access:

- **DuckDB extensions** (`httpfs`, `azure`, `spatial`…) are installed from DuckDB's repository on
  first use. Pre-populate the extension directory, or avoid the extensions.
- **Object storage** needs to reach its endpoint — including a MinIO on your own network, via
  `--s3-endpoint`.

---

See also: [Install](install.md) · [SQL Server](connections/sql-server.md) ·
[Oracle](connections/oracle.md) · [YAML jobs](guides/yaml-jobs.md)
