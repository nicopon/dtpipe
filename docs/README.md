# DtPipe documentation

DtPipe reads from a source, transforms rows in flight, and writes to a target. It moves data between
databases, files and object storage, and can anonymize, join, filter and reshape it on the way.

**Start:** [Install](install.md) · [Quickstart](quickstart.md) · [Concepts](concepts.md)

## I want to…

| | |
|:---|:---|
| Copy a table into a database | [Writing to a database](guides/write-strategies.md) |
| Hand out realistic data that is not real | [Anonymization](guides/anonymization.md) |
| Join two sources, merge them, or fan one out | [DAG pipelines](guides/dag.md) |
| Reshape rows with SQL or JavaScript | [SQL and JavaScript](guides/sql-and-javascript.md) |
| Transfer only what changed | [Incremental loading](guides/incremental.md) |
| See what a pipeline will do before it does it | [Preview and checkpoints](guides/preview-and-checkpoints.md) |
| Save a pipeline and run it from CI or every night | [Jobs and production](guides/production.md) |
| Keep credentials out of the command line | [Secrets](guides/secrets.md) |
| Let an AI assistant design a pipeline | [The agent](guides/agent.md) |
| Use dtpipe's libraries from .NET code | [dtpipe and .NET](dotnet.md) |
| Understand an error | [Troubleshooting](troubleshooting.md) |

## Connections

[Catalog](connections/README.md) · [SQL Server](connections/sql-server.md) ·
[Oracle](connections/oracle.md) · [PostgreSQL](connections/postgresql.md) ·
[MySQL / MariaDB](connections/mysql.md) · [DuckDB](connections/duckdb.md) ·
[Files](connections/files.md) · [Object storage](connections/object-storage.md)

## Experimental

[Experimentation](experimentation.md): one pipeline across several hosts, and `dtpipe studio`.

---

The common flags, the YAML schema and provider options: [REFERENCE.md](../REFERENCE.md). Every
flag: `dtpipe --help`. Writing an adapter or a transformer: [EXTENDING.md](../EXTENDING.md).
