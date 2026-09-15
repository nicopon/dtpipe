# DtPipe

A self-contained CLI for streaming, transforming, and anonymizing data across databases and file
formats.

DtPipe reads from a source, applies row and columnar transformations in batches, and writes to a
destination with no intermediate staging. It is designed for automation and CI/CD workflows where
repeatable, observable data pipelines matter.

---

### 📚 [Start with the documentation → docs/](./docs/README.md)

Install, a first pipeline, the concepts, and a guide per task — anonymization, joins and fan-out,
incremental loading, YAML jobs, secrets, and running in a .NET estate.

---

## Install

```bash
dotnet tool install -g dtpipe        # or download a binary from the releases page
dtpipe --help
```

Six platforms per release — Linux, macOS and Windows, x64 and arm64 — each a single executable with
the .NET runtime, the DuckDB engine and every database driver inside it. Nothing else to install.
Full instructions, including building from source: [docs/install.md](./docs/install.md).

## One example

Anonymize a table on the way out, without ever writing the clear values to disk:

```bash
dtpipe \
  -i "pg:Host=localhost;Database=prod;Username=postgres" \
  --query "SELECT * FROM users" \
  --fake "email:internet.email" \
  --fake "name:name.fullName" \
  --mask "phone:###-****" \
  --null "ssn" \
  -o anonymized_users.parquet
```

More, from a first pipeline to a DAG: [docs/quickstart.md](./docs/quickstart.md).

## What it connects to

| Family | Examples | Prefix |
|:---|:---|:---|
| **Databases** | PostgreSQL, MySQL, SQLite, DuckDB, SQL Server, Oracle | `pg:`, `mysql:`, `sqlite:`, `duck:`, `mssql:`, `ora:` |
| **Files** | CSV, JsonL, Parquet, Arrow, XML | `csv:`, `jsonl:`, `parquet:`, `arrow:`, `xml:` |
| **Object storage** | S3-compatible, Azure Blob | `s3://bucket/key.parquet`, `azure://container/blob.csv` |
| **Special** | Data generator (source), null / checksum (sink) | `generate:N`, `null:`, `checksum:` |

`dtpipe providers` lists what your binary actually carries. The connection catalog, with the string
format and the rules that apply to every provider, is at
[docs/connections/](./docs/connections/README.md).

> **Is DuckDB alone enough?** Often, yes — and then use it directly. DtPipe embeds it as its SQL
> engine and adds what a transfer needs around it: anonymization in transit, concurrent fan-out,
> write strategies (upsert, auto-migrate, bulk), Oracle / SQL Server / XML sources, and repeatable
> YAML jobs with secret management.

## Documentation

| Where | What it holds |
|:---|:---|
| [**docs/**](./docs/README.md) | **Start here.** Install, quickstart, concepts, a guide per task, a page per connection |
| [REFERENCE.md](./REFERENCE.md) | The exhaustive lists: every CLI flag, the YAML job schema, provider options, the MCP tools |
| [COOKBOOK.md](./COOKBOOK.md) | End-to-end recipes |
| [EXTENDING.md](./EXTENDING.md) | Writing a new adapter or transformer |

## Contributing

See [EXTENDING.md](./EXTENDING.md) for the adapter and transformer patterns.

## License
MIT
