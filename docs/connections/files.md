# Files: CSV, JSONL, Parquet, Arrow, XML

[← Connections](README.md) · [Documentation](../README.md)

```bash
dtpipe -i sales.csv      -o sales.parquet
dtpipe -i csv:sales.data -o parquet:sales.out      # a prefix, when the extension does not say
```

| Format | Read | Write | Options |
|:---|:---:|:---:|:---|
| CSV | ✅ | ✅ | `--csv-separator`, `--csv-has-header` (reader), `--csv-header` (writer), `--encoding` |
| JSONL | ✅ | ✅ | One JSON object per line; `--path` selects a sub-document |
| Parquet | ✅ | ✅ | Columnar, with list columns |
| Arrow IPC | ✅ | ✅ | `--arrow-compression` |
| XML | ✅ | — | `--path "//Product"` selects the record element |

## Standard input and output

Use `-` as the path:

```bash
cat export.csv | dtpipe -i csv:- -o jsonl:- --mask "email:###****" | jq .
dtpipe -i "pg:…" --query "SELECT * FROM users" -o jsonl:- --no-stats > users.jsonl
```

## Several files at once

A `*` or `?` in the file name reads every match as one stream, in path order:

```bash
dtpipe -i "daily/*.csv" -o events.parquet
```

The first file sets the schema; a file with other columns stops the run. Matching nothing is an
error. Locally, the wildcard must be in the file name, not in a folder name; object storage allows
both (see [Object storage](object-storage.md)).

## Text columns are text until you say otherwise

A CSV has no types: every column is a string. Summing one fails.

```bash
# ✗ Binder Error: No function matches sum(VARCHAR)
dtpipe -i orders.csv --alias o --from o --sql "SELECT sum(amount) FROM o" -o total.csv

# ✓ infer the types from a sample of rows
dtpipe -i orders.csv --auto-column-types --alias o --from o --sql "SELECT sum(amount) FROM o" -o total.csv

# ✓ or declare the ones that matter
dtpipe -i orders.csv --column-types "amount:decimal,order_id:int32" --alias o …
```

This applies to CSV, JSONL and XML. `--max-sample` sets how many rows inference reads.

- A number uses `.` as its decimal point and no thousands separator: `1,5` is refused.
- A value that does not fit its type stops the run, naming the line and the column.
- Inference never rounds: a number that a `double` cannot hold exactly stays a `decimal` or text.
  Declare `decimal` for exact arithmetic, since `4.29` is inferred as a `double`.

The type names and the rules: [REFERENCE.md](../../REFERENCE.md#type-hints).

## Nested values

A list or a nested object keeps its shape where the target has one (a Parquet list, a PostgreSQL
array), and is written as JSON text elsewhere. To turn a nested field into its own column:

```bash
dtpipe -i events.jsonl \
  --compute "severity:row.payload.severity" --compute-types "severity:string" --drop payload \
  -o "pg:…" --table events
```

---

See also: [SQL and JavaScript](../guides/sql-and-javascript.md) ·
[REFERENCE.md](../../REFERENCE.md#source-reader-options)
