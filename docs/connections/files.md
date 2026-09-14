# Files: CSV, JSONL, Parquet, Arrow, XML

[← Connections](README.md) · [Documentation](../README.md)

The format comes from the extension, or from an explicit prefix when you want to be sure:

```bash
dtpipe -i sales.csv      -o sales.parquet
dtpipe -i csv:sales.data -o parquet:sales.out      # explicit, when the extension lies
```

| Format | Read | Write | Notes |
|:---|:---:|:---:|:---|
| CSV | ✅ | ✅ | `--csv-separator`, `--csv-has-header`, `--encoding` |
| JSONL / NDJSON | ✅ | ✅ | one JSON object per line; `--path` selects a sub-document |
| Parquet | ✅ | ✅ | columnar end to end, native `LIST` columns |
| Arrow IPC | ✅ | ✅ | the in-memory format, written as-is |
| XML | ✅ | — | streaming reader, `--path "//Product"` selects the record node |

## Standard input and output

Use `-` as the location, and dtpipe composes with the rest of your shell:

```bash
cat export.csv | dtpipe -i csv:- -o jsonl:- --mask "email:###****" | jq .
dtpipe -i "pg:…" --query "SELECT * FROM users" -o jsonl:- --no-stats > users.jsonl
```

`--no-stats` keeps the progress panel out of a piped stream.

## Globs

A local input whose **file name** carries `*` or `?` is read as one stream over every match, in
ordinal path order:

```bash
dtpipe -i "daily/*.csv"        -o events.parquet
dtpipe -i "exports/part-?.jsonl" -o "pg:…" --table events
```

- **The schema is the first file's.** A later file with different columns stops the run and names
  it, rather than dropping or misaligning columns.
- **Matching nothing is an error**, not an empty read.
- The wildcard stays in the file name — a pattern spanning directories is not supported locally.
  Object storage does glob across prefixes; see [Object storage](object-storage.md).

## Text columns are text until you say otherwise

A CSV has no types. Every column arrives as a string, which is correct and occasionally
surprising: a `--sql` branch summing such a column fails, because `sum(VARCHAR)` has no meaning.

```bash
# ✗ Binder Error: No function matches sum(VARCHAR)
dtpipe -i orders.csv --alias o --from o --sql "SELECT sum(amount) FROM o" -o total.csv

# ✓ infer the types from the first 100 rows
dtpipe -i orders.csv --auto-column-types --alias o --from o --sql "SELECT sum(amount) FROM o" -o total.csv

# ✓ or declare exactly the ones that matter
dtpipe -i orders.csv --column-types "amount:decimal,order_id:int32" --alias o …
```

`--column-types` and `--auto-column-types` apply to the text readers (CSV, JSONL, XML). A hint
that names no known type is refused rather than silently ignored — the vocabulary is in
[REFERENCE.md](../../REFERENCE.md#type-hints).

## Nested and repeated values

A list or a nested object keeps its shape wherever the target has one — a Parquet `LIST`, an Arrow
`ListType`, a PostgreSQL array — and is rendered as JSON text wherever it does not (`[10,20,30]`,
`{"severity":"info"}`). The declared schema decides; the rendering never changes it.

To land a nested field as its own typed column, name it in the pipeline:

```bash
dtpipe -i events.jsonl \
  --compute "severity:row.payload.severity" --compute-types "severity:string" --drop payload \
  -o "pg:…" --table events
```

---

See also: [SQL and JavaScript](../guides/sql-and-javascript.md) ·
[Object storage](object-storage.md) · [REFERENCE.md](../../REFERENCE.md#source-reader-options)
