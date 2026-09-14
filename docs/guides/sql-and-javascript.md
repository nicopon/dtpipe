# SQL and JavaScript

[← Documentation](../README.md)

Two ways to reshape a stream, with different jobs.

| | `--sql` (DuckDB) | `--compute` / `--filter` / `--expand` (JavaScript) |
|:---|:---|:---|
| Works on | Sets — the whole stream is a table | One row at a time |
| Good at | Joins, aggregates, window functions, deduplication, sorting | Parsing a string, deriving a field, calling logic that has no SQL form |
| Costs | A branch, and Arrow batches | A function call per row |
| Needs | An alias per source | Nothing |

Rule of thumb: **if it involves more than one row, it belongs in SQL.**

## SQL over any source

Any source becomes a table named by its alias — a CSV, an Oracle query, a Parquet file on S3:

```bash
dtpipe -i sales.csv --auto-column-types --alias s \
       --from s --sql "SELECT region, sum(amount) AS total
                       FROM s GROUP BY region HAVING sum(amount) > 10000" \
       -o report.csv
```

It is real DuckDB SQL: CTEs, window functions, `QUALIFY`, JSON functions, `list_*`, everything the
engine has. Deduplication, for instance, needs no dedicated flag — and DuckDB spills to disk, so
it works on volumes that would not fit in memory:

```bash
dtpipe -i dup.csv --alias src \
  --from src --sql "SELECT * FROM src
                    QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY updated_at DESC) = 1" \
  -o dedup.csv
```

```
id,updated_at,status        id,updated_at,status
1,2026-01-01,new            1,2026-02-01,paid
2,2026-01-01,new     →      2,2026-03-01,cancelled
1,2026-02-01,paid           3,2026-01-15,new
3,2026-01-15,new
2,2026-03-01,cancelled
```

Loading an extension first — `httpfs`, `spatial`, `excel`, `iceberg` — is what `--duck-init` is
for; see [DuckDB](../connections/duckdb.md).

> [!IMPORTANT]
> A text file has no types: every CSV column arrives as a string, and `sum(VARCHAR)` has no
> meaning. Add `--auto-column-types`, or declare the ones that matter with
> `--column-types "amount:decimal"`. See [Files](../connections/files.md).

## JavaScript per row

```bash
dtpipe -i emp.csv \
  --compute "domain:row.email.split('@')[1]" \
  --compute "band:Math.floor(row.salary/10000)*10000" \
  --compute-types "band:int32" \
  --filter "row.salary > 55000" \
  -o js.csv
```

```
id,name,email,phone,iban,salary,domain,band
2,Bob Durand,bob@corp.com,…,61000,corp.com,60000
```

| Flag | What it does |
|:---|:---|
| `--compute "col:expr"` | Sets or creates a column. A single expression returns implicitly; use `return` with statements |
| `--compute-types "col:int32"` | Declares the type of a computed column — otherwise it is whatever the expression produced |
| `--filter "expr"` | Drops rows where the expression is falsy. A column the schema does not carry is an **error**, not an empty result |
| `--expand "expr"` | Turns one row into several. Must return an array of **row objects** |
| `--expand-types "tag:string"` | Declares a column `--expand` creates; undeclared keys are dropped |
| `--window-count`, `--window-script` | A sliding window of rows, for logic that needs neighbours |

```bash
# One row per tag
dtpipe -i products.jsonl \
  --expand "row.tags.map(t => ({ ...row, tag: t }))" --expand-types "tag:string" \
  --drop tags -o product_tags.parquet
```

Scripts can live in files rather than in the shell: `--compute "col:@scripts/derive.js"`.

## Which one runs where

A `--compute` step runs on the **row** path, and a `--sql` branch on the **Arrow** path. Every run
prints the mode per branch (`● row`, `◈ Arrow`) and a `--dry-run` names the bridges between them,
so a pipeline that pays for a conversion says so. Mixing them is normal — anonymize with row
transformers, aggregate with SQL:

```bash
dtpipe -i "pg:…" --query "SELECT * FROM orders" --fake "email:internet.email" --alias o \
       --from o --sql "SELECT date_trunc('month', ordered_at) AS m, count(*) FROM o GROUP BY 1" \
       -o monthly.csv
```

---

See also: [DAG pipelines](dag.md) · [Files](../connections/files.md) ·
[COOKBOOK.md](../../COOKBOOK.md#sql-processors-and-joins) ·
[REFERENCE.md](../../REFERENCE.md#data-transformations)
