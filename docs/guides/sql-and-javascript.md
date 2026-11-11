# SQL and JavaScript

[← Documentation](../README.md)

| | `--sql` (DuckDB) | `--compute`, `--filter`, `--expand` (JavaScript) |
|:---|:---|:---|
| Works on | The whole stream, as a table | One row at a time |
| Good at | Joins, aggregates, window functions, deduplication, sorting | Parsing a string, deriving a field |
| Speed | Fast: whole batches at once | A function call per row |

If it involves more than one row, use SQL.

## SQL over any source

Give a branch an alias, and the alias is a table in the query:

```bash
dtpipe -i sales.csv --auto-column-types --alias s \
       --from s --sql "SELECT region, sum(amount) AS total
                       FROM s GROUP BY region HAVING sum(amount) > 10000" \
       -o report.csv
```

This is full DuckDB SQL: CTEs, window functions, `QUALIFY`, JSON and list functions. Nothing is
loaded into a database file: the query reads the rows as they stream, and nothing is left on disk
after the run. DuckDB spills to disk when a query needs more memory than it has.

Deduplication, keeping the last version of each key:

`dup.csv`

```
id,updated_at,status
1,2026-01-01,new
2,2026-01-01,new
1,2026-02-01,paid
3,2026-01-15,new
2,2026-03-01,cancelled
```

```bash
dtpipe -i dup.csv --alias src \
  --from src --sql "SELECT * FROM src
                    QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY updated_at DESC) = 1
                    ORDER BY id" \
  -o dedup.csv
```

`dedup.csv`

```
id,updated_at,status
1,2026-02-01,paid
2,2026-03-01,cancelled
3,2026-01-15,new
```

Pitfalls:

- **Row order is not guaranteed** without an `ORDER BY`: DuckDB works in parallel.
- **A CSV column is text.** `sum()` on it fails: add `--auto-column-types`, or declare the types
  with `--column-types "amount:decimal"`. See [Files](../connections/files.md).
- **A join reads its lookup with `--ref`**, which loads it in full: see [DAG pipelines](dag.md).
- **Extensions** (`httpfs`, `spatial`, `excel`) are loaded with `--duck-init`: see
  [DuckDB](../connections/duckdb.md).

## JavaScript, one row at a time

`emp.csv`

```
id,name,email,salary
1,Alice Martin,alice@corp.com,52000
2,Bob Durand,bob@corp.com,61000
3,Carla Neri,carla@corp.com,74000
4,Dan Petit,dan@corp.com,
```

`row` is the current row. A single expression returns its value:

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

Past one expression, write statements with an explicit `return`, in a file. A null arrives as
`null`, so guard against it. `scripts/band.js`:

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

| Flag | What it does |
|:---|:---|
| `--compute "col:expr"` | Sets or creates a column |
| `--compute-types "col:int32"` | Declares the type of a computed column |
| `--filter "expr"` | Keeps the rows where the expression is true. An unknown column is an error |
| `--expand "expr"` | Turns one row into several: returns an array of row objects |
| `--expand-types "tag:string"` | Declares a column `--expand` creates; undeclared ones are dropped |
| `--window-count`, `--window-key`, `--window-script` | Runs a script over a group of rows |

```bash
# One row per tag
dtpipe -i products.jsonl \
  --expand "row.tags.map(t => ({ ...row, tag: t }))" --expand-types "tag:string" \
  --drop tags -o product_tags.parquet
```

## Which one to use

1. **`--sql`**, whenever the work can be written in SQL, even per row: `split_part(email, '@', 2)`
   is the SQL form of the `domain` example.
2. **A dedicated step**: `--mask`, `--format`, `--overwrite`, `--null`, `--project` are fast and
   need no script. See [Anonymization](anonymization.md).
3. **`--compute`**, for the rest.

A JavaScript step switches the pipeline to row mode, and back. `--dry-run` shows these switches as
bridges in its plan: see [Preview and checkpoints](preview-and-checkpoints.md).

---

See also: [DAG pipelines](dag.md) · [REFERENCE.md](../../REFERENCE.md#data-transformations)
