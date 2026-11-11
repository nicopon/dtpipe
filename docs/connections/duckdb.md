# DuckDB

[← Connections](README.md) · [Documentation](../README.md)

DuckDB plays two roles in dtpipe:

- **a provider**: read from or write to a `.duckdb` file, or an in-memory database, with `duck:`;
- **the SQL engine** behind `--sql`: see [SQL and JavaScript](../guides/sql-and-javascript.md).

## As a provider

```bash
dtpipe -i duck:analytics.duckdb --query "SELECT * FROM sales WHERE year = 2026" -o sales.parquet
dtpipe -i sales.parquet -o duck:analytics.duckdb --table sales
dtpipe -i "duck::memory:" --query "SELECT 1 AS id" -o out.csv      # in memory, no file
```

## Writing into an existing table

A value its column cannot hold stops the run with exit `1`, naming the column: a number out of
range, or a decimal with more digits than the column. Rows already flushed to the table stay;
rows the writer still buffers are dropped.

| Source | What happens |
|:---|:---|
| A type the writer does not convert (`HUGEINT` into `BIGINT`, `DATE` into `TIMESTAMP`) | Refused: cast it in the query, as in `CAST(total AS BIGINT)` |
| `UHUGEINT`, `STRUCT`, `MAP`, `UNION`, `INTERVAL`, a list of these | Refused before the table is touched: cast to `VARCHAR` in a SQL step |
| A list or an array of scalars | Written as a list |
| `ENUM` | Written as text |
| A decimal with no declared width | Creates a `DECIMAL(38,18)` |

The full rules: [Types a writer cannot write](../../REFERENCE.md#types-a-writer-cannot-write).

## Extensions: `--duck-init`

`--duck-init` runs SQL on the DuckDB connection right after it opens. It loads extensions, which
reach what dtpipe has no provider for: HTTP, Iceberg, spatial formats, Excel.

```bash
dtpipe -i duck::memory: \
  --duck-init "INSTALL httpfs; LOAD httpfs; SET s3_region='eu-west-1'" \
  --query "SELECT * FROM read_parquet('s3://bucket/dt=2026-*/part-*.parquet')" \
  -o report.csv
```

- Each branch of a DAG has its own connection: give `--duck-init` to every branch that needs it.
- It accepts `keyring://`, `${{ENV}}` and `@file`, so credentials stay out of the command line.
- It is not `--pre-exec`, which runs on the target database.
- Extensions are downloaded on first use: an air-gapped host needs them installed beforehand.

---

See also: [Object storage](object-storage.md) · [REFERENCE.md](../../REFERENCE.md#duckdb)
