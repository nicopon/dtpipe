# DuckDB

[← Connections](README.md) · [Documentation](../README.md)

DuckDB has two roles in dtpipe, and it is worth keeping them apart.

| Role | How you reach it |
|:---|:---|
| **A provider** — read from and write to a `.duckdb` file, or an in-memory database | `-i duck:analytics.duckdb`, `-o duck:analytics.duckdb` |
| **The SQL engine** behind `--sql` branches — joins, unions, CTEs, window functions | `--from a --ref b --sql "SELECT …"` |

Both run in the same process. Nothing is installed, and nothing is downloaded, until you ask for
an extension.

## As a provider

```bash
dtpipe -i duck:analytics.duckdb --query "SELECT * FROM sales WHERE year = 2026" -o sales.parquet
dtpipe -i sales.parquet -o duck:analytics.duckdb --table sales
dtpipe -i "duck::memory:" --query "SELECT 1 AS id" -o out.csv
```

`duck::memory:` is the prefix `duck:` followed by DuckDB's own `:memory:` — a database that never
touches disk, useful for a one-line query or a test.

Reads go through the Arrow C Data interface, so a batch crosses from DuckDB into the pipeline
without being copied or re-encoded.

## `--duck-init`: extensions and credentials

`--duck-init` runs SQL on the DuckDB connection right after it opens — before any read, write or
query. It is how an extension gets loaded:

```bash
dtpipe -i duck::memory: \
  --duck-init "INSTALL httpfs; LOAD httpfs; SET s3_region='eu-west-1'" \
  --query "SELECT * FROM read_parquet('s3://bucket/dt=2026-*/part-*.parquet')" \
  -o report.csv
```

This is the escape hatch for anything dtpipe has no provider for — Iceberg, spatial formats, an
HTTP endpoint, Excel — because DuckDB's extension ecosystem becomes reachable without a new
adapter.

> [!NOTE]
> `--duck-init` applies per connection, and a DAG gives each branch its own. Pass it on every
> branch that needs it. It also accepts `keyring://alias`, `${{ENV}}` and `@file`, so credentials
> in an extension's `SET` statement never have to appear on the command line — see
> [Secrets](../guides/secrets.md).

> [!IMPORTANT]
> `--duck-init` is not `--pre-exec`. `--duck-init` runs on the DuckDB connection before reading;
> `--pre-exec` runs on the **target** database before the pipeline starts.

Extensions are installed from DuckDB's repository on first use, so the host needs access to it
once — or an extension directory that already holds them. That is the one thing to check before
running on an air-gapped machine.

## As the SQL engine

See [SQL and JavaScript](../guides/sql-and-javascript.md) and [DAG pipelines](../guides/dag.md).
The short version: any source becomes a table you can query, whatever it was on disk.

```bash
dtpipe -i orders.csv --auto-column-types --alias o \
       -i customers.csv --alias c \
       --from o --ref c \
       --sql "SELECT c.name, sum(o.amount) AS total FROM o JOIN c ON o.customer_email = c.email GROUP BY c.name" \
       -o revenue.csv
```

## Hub prefixes (`duck+provider:`) are retired

`duck+mysql:` and friends parse, and then fail with a message naming the native provider to use
instead. The rule was always *no hub route where a native provider exists*: `ATTACH` reaches a
catalog but not `COPY`, bulk load or upsert, so the native route is strictly more capable.

---

See also: [SQL and JavaScript](../guides/sql-and-javascript.md) ·
[Object storage](object-storage.md) · [REFERENCE.md](../../REFERENCE.md#duckdb)
