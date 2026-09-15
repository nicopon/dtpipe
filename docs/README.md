# DtPipe documentation

DtPipe is a single self-contained binary that reads from a source, transforms rows in flight, and
writes to a destination with no intermediate staging. It moves data between databases, files and
object storage — and, on the way, it anonymizes, joins, filters and reshapes.

These pages are plain Markdown: they render as you browse the repository, there is nothing to
build and nothing to publish. Every link below is a relative link inside this folder.

## Start here

| | |
|:---|:---|
| [**Install**](install.md) | A binary for your platform, or `dotnet tool install -g dtpipe` |
| [**Quickstart**](quickstart.md) | A working pipeline in five commands, no database required |
| [**Concepts**](concepts.md) | The model: source → transformers → target, branches, batches |

## By task

| I want to… | Page |
|:---|:---|
| Copy a table from one database to another | [Write strategies](guides/write-strategies.md) |
| Give a developer realistic data without giving them real data | [Anonymization](guides/anonymization.md) |
| Join two sources, fan one source out to several targets | [DAG pipelines](guides/dag.md) |
| Reshape rows with SQL or JavaScript | [SQL and JavaScript](guides/sql-and-javascript.md) |
| Transfer only what changed since the last run | [Incremental loading](guides/incremental.md) |
| See what a pipeline will do before it does it | [Preview and checkpoints](guides/preview-and-checkpoints.md) |
| Save a pipeline and run it from CI | [YAML jobs](guides/yaml-jobs.md) |
| Run it every night, unattended, against production | [Running in production](guides/production.md) |
| Keep credentials out of the command line | [Secrets](guides/secrets.md) |
| Run it in a .NET estate, or call it from .NET code | [dtpipe and .NET](dotnet.md) |
| Understand an error message | [Troubleshooting](troubleshooting.md) |

## Connections

Start at the [**connection catalog**](connections/README.md) for the full list, the connection
string format and the rules that apply to every provider.

| Databases | Files & storage |
|:---|:---|
| [SQL Server](connections/sql-server.md) · [Oracle](connections/oracle.md) · [PostgreSQL](connections/postgresql.md) · [MySQL / MariaDB](connections/mysql.md) · [DuckDB](connections/duckdb.md) | [Files](connections/files.md) (CSV, JSONL, Parquet, Arrow, XML) · [Object storage](connections/object-storage.md) (S3, Azure Blob) |

## Reference

These pages explain and show. When you need the exhaustive list — every flag, every option, every
YAML key — go to the reference documents at the root of the repository:

| Document | What it holds |
|:---|:---|
| [REFERENCE.md](../REFERENCE.md) | Every CLI flag, the YAML job schema, provider-specific options, the MCP tool table |
| [COOKBOOK.md](../COOKBOOK.md) | End-to-end recipes, including ones these pages only summarise |
| [EXTENDING.md](../EXTENDING.md) | Writing a new adapter or transformer |
