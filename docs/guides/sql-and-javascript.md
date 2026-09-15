# SQL and JavaScript

[← Documentation](../README.md)

Two ways to reshape a stream, with different jobs.

| | `--sql` (DuckDB engine) | `--compute` / `--filter` / `--expand` (JavaScript) |
|:---|:---|:---|
| Works on | Sets — the whole stream is a table | One row at a time |
| Good at | Joins, aggregates, window functions, deduplication, sorting | Parsing a string, deriving a field, calling logic that has no SQL form |
| Costs | A branch, and Arrow batches | A function call per row, and the row path |
| Needs | An alias per source | Nothing |

Rule of thumb: **if it involves more than one row, it belongs in SQL.**

## Nothing is loaded into a database

`--sql` runs on a DuckDB engine embedded in the dtpipe process, on a connection that has no file
behind it. Your data is never written to a DuckDB database and read back: each branch is
**registered as a view over the Arrow batches already travelling through the pipeline**, and the
query reads from there.

The two ways in differ, and it is the only part worth knowing:

| | What happens | Memory |
|:---|:---|:---|
| `--from <alias>` | the branch is scanned **as it streams**, zero-copy | bounded by the batch size |
| `--ref <alias>` | the branch is **collected first**, then queried | the whole branch is held |

That is why `--from` takes the large side of a join and `--ref` the small one: the engine needs a
materialised side to plan against, not because either is a table on disk. Nothing survives the
run — the engine goes away with the process, and there is no file to clean up.

`duck:` is a different thing: that one *is* a DuckDB database you name, read or write like any
other target. See [DuckDB](../connections/duckdb.md).

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
                    QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY updated_at DESC) = 1
                    ORDER BY id" \
  -o dedup.csv
```

> [!NOTE]
> The trailing `ORDER BY id` is not decoration. DuckDB evaluates in parallel and promises no row
> order unless you ask for one — the same query without it returns the same three rows in a
> different arrangement from one run to the next. Order the output whenever something downstream,
> a diff or a test compares it.

`dup.csv`

```
id,updated_at,status
1,2026-01-01,new
2,2026-01-01,new
1,2026-02-01,paid
3,2026-01-15,new
2,2026-03-01,cancelled
```

`dedup.csv` — the last version of each key, one row per `id`:

```
id,updated_at,status
1,2026-02-01,paid
2,2026-03-01,cancelled
3,2026-01-15,new
```

Loading an extension first — `httpfs`, `spatial`, `excel`, `iceberg` — is what `--duck-init` is
for; see [DuckDB](../connections/duckdb.md).

> [!IMPORTANT]
> A text file has no types: every CSV column arrives as a string, and `sum(VARCHAR)` has no
> meaning. Add `--auto-column-types`, or declare the ones that matter with
> `--column-types "amount:decimal"`. See [Files](../connections/files.md).

## JavaScript per row

`emp.csv` is used for the rest of this page:

```
id,name,email,salary
1,Alice Martin,alice@corp.com,52000
2,Bob Durand,bob@corp.com,61000
3,Carla Neri,carla@corp.com,74000
4,Dan Petit,dan@corp.com,
```

A single expression returns implicitly. `row` is the current row, by column name:

```bash
dtpipe -i emp.csv --auto-column-types \
  --compute "domain:row.email.split('@')[1]" \
  --filter "row.salary > 55000" \
  -o js.csv
```

```
id,name,email,salary,domain
2,Bob Durand,bob@corp.com,61000,corp.com
3,Carla Neri,carla@corp.com,74000,corp.com
```

### When one expression is not enough

As soon as you need a guard or a branch, write statements and `return` explicitly. A NULL is the
usual reason — it arrives as `null`, and arithmetic on it silently produces nothing useful:

```bash
dtpipe -i emp.csv --auto-column-types \
  --compute "band:const s = row.salary; if (s === null) return 'unknown'; if (s >= 70000) return 'C'; if (s >= 55000) return 'B'; return 'A';" \
  -o banded.csv
```

That works, and it is already hard to read on one line — which is what `@file` is for. The same
script in `scripts/band.js`:

```js
const s = row.salary;
if (s === null) return "unknown";
if (s >= 70000) return "C";
if (s >= 55000) return "B";
return "A";
```

```bash
dtpipe -i emp.csv --auto-column-types --compute "band:@scripts/band.js" -o banded.csv
```

```
id,name,email,salary,band
1,Alice Martin,alice@corp.com,52000,A
2,Bob Durand,bob@corp.com,61000,B
3,Carla Neri,carla@corp.com,74000,C
4,Dan Petit,dan@corp.com,,unknown
```

Prefer the file past the first line or two: it is reviewable in a pull request, it keeps quoting
out of the shell, and the job file that `--export-job` writes carries the reference rather than a
wall of escaped JavaScript.

### The flags

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

## What a JavaScript step costs, and when to pay it

A `--compute` step runs on the **row** path; a `--sql` branch runs on the **Arrow** path. A
pipeline whose reader and writer are both columnar — Parquet to DuckDB, say — travels as Arrow
batches from end to end, and inserting a JavaScript step pulls it out to rows and back:

```
╭─Pipeline Execution Plan──────────────────────╮
│  Reader  csv                  ▼ row          │
│  Step    Compute              ▼ row-only     │
│  Sink    duck                 ▲ columnar sink│
│                                              │
│  Strategy: Columnar · 1 bridge               │
╰──────────────────────────────────────────────╯
```

That is a function call per row plus the conversions, against a set operation DuckDB runs on whole
batches. The order to try things in:

1. **`--sql`**, if the work can be expressed over the set — including per-row expressions:
   `split_part(email, '@', 2)` is the SQL form of the `domain` example above.
2. **A dedicated transformer** — `--mask`, `--format`, `--overwrite`, `--null`, `--project` all
   run columnar, with no JavaScript engine involved. See [Anonymization](anonymization.md).
3. **`--compute`**, for what neither can express.

Mixing paths is normal, and a `--dry-run` names every bridge so the cost is visible rather than
guessed:

```bash
dtpipe -i "pg:…" --query "SELECT * FROM orders" --fake "email:internet.email" --alias o \
       --from o --sql "SELECT date_trunc('month', ordered_at) AS m, count(*) FROM o GROUP BY 1" \
       -o monthly.csv
```

---

See also: [DAG pipelines](dag.md) · [Files](../connections/files.md) ·
[Anonymization](anonymization.md) ·
[COOKBOOK.md](../../COOKBOOK.md#sql-processors-and-joins) ·
[REFERENCE.md](../../REFERENCE.md#data-transformations)
