# Quickstart

[← Documentation](README.md)

Six commands, no database to set up. Everything below runs against a generated dataset and two
local files, and every output shown was produced by running it.

## 1. Make a dataset

`generate:N` is a source that produces N rows out of nothing. `--fake` fills a column with
realistic values — and **creates the column when the incoming rows do not have one**, which is
what makes a generator useful on its own.

```bash
dtpipe -i generate:1000 \
  --fake "name:name.fullName" \
  --fake "email:internet.email" \
  --fake "city:address.city" \
  --drop GenerateIndex \
  -o people.csv
```

```
╭─ Pipeline ──────────────────╮
│   ◉  generate:1000  ◈ Arrow │
│      → fake                 │
│      → fake                 │
│      → fake                 │
│      → project              │
│      ──▶  people.csv        │
╰─────────────────────────────╯
```

```
name,email,city
Rory Hane,Audie.Schaden@yahoo.com,Stantonton
Andreane Wyman,Brielle_McClure@hotmail.com,Samanthaton
Denis Koch,Blanca2@gmail.com,Ryanborough
```

Every run prints that panel: the source, each transformation in order, the target, and whether the
stream travelled row by row (`● row`) or as Arrow batches (`◈ Arrow`).

The same flag anonymizes an existing column instead of inventing one, and can keep a join working
across two anonymized tables — see [Anonymization](guides/anonymization.md).

## 2. Look at what you have

```bash
dtpipe inspect -i people.csv
```

```
📋 Schema for people.csv
3 columns

┌───┬────────┬────────┬──────────┐
│ # │ Column │ Type   │ Nullable │
├───┼────────┼────────┼──────────┤
│ 1 │ name   │ String │ ✓        │
│ 2 │ email  │ String │ ✓        │
│ 3 │ city   │ String │ ✓        │
└───┴────────┴────────┴──────────┘
```

`inspect` works on any source, including a database — with `--query` to describe the shape of a
query rather than a table.

## 3. Convert a file

The format comes from the extension or the prefix; nothing else changes.

```bash
dtpipe -i people.csv -o people.parquet
```

## 4. Preview a load before running it

`--dry-run N` runs the **real** pipeline over N source rows with the writer switched off. It is not
a separate analyser, so it cannot disagree with the run it previews.

```bash
dtpipe -i people.csv -o duck:analytics.duckdb --table people --dry-run 3
```

It answers two questions before anything is written — how the pipeline will execute, and what the
target will have to accept:

```
╭─Pipeline Execution Plan──────────────────────╮
│  Reader  csv                  ▼ row          │
│  Sink    duck                 ▲ columnar sink│
│                                              │
│  Strategy: Columnar · 1 bridge               │
╰──────────────────────────────────────────────╯

╭─Target Schema Info──────────────────╮
│ Target: Will be created (new table) │
│ Status: ✅ Schema is compatible     │
╰─────────────────────────────────────╯
```

```
╭────────┬─────────────┬─────────────┬────────────────────╮
│ Column │ Source Type │ Target Type │ Status             │
├────────┼─────────────┼─────────────┼────────────────────┤
│ name   │ String      │ —           │ ✅ Will be created │
│ email  │ String      │ —           │ ✅ Will be created │
│ city   │ String      │ —           │ ✅ Will be created │
╰────────┴─────────────┴─────────────┴────────────────────╯
```

A dash in **Target Type** is not a problem: the table does not exist yet, so there is no type to
compare against. Against an existing target the same column reads `String -> VARCHAR`.

A dry run also prints, per column, what a row becomes at each step — most useful when the pipeline
actually changes something, which is where
[Anonymization](guides/anonymization.md#prove-it-before-you-hand-it-over) shows it. No rows are
written, no table is created or migrated, and no `--pre-exec` hook runs. One caveat worth knowing:
against a file-backed database that does not exist yet — DuckDB or SQLite — connecting creates the
database file, empty and without your table. A file target such as CSV or Parquet is not created
at all. See [Preview and checkpoints](guides/preview-and-checkpoints.md) for exactly what a dry run
guarantees.

## 5. Run it, then query the result

```bash
dtpipe -i people.csv -o duck:analytics.duckdb --table people
```

```
Schema mode: discard - incompatibilities are reported and the run continues;
columns missing from the target are not written.
Verifying target schema compatibility...
Target schema compatible.
```

Read it back — `jsonl:-` writes to standard output, so a pipeline composes with the rest of your
shell:

```bash
dtpipe -i duck:analytics.duckdb \
  --query "SELECT count(*) AS people, count(DISTINCT city) AS cities FROM people" \
  -o jsonl:- --no-stats
```

```json
{"people":1000,"cities":995}
```

## 6. Keep the pipeline

Any command line can be serialised to a YAML job and replayed:

```bash
dtpipe -i people.csv -o duck:analytics.duckdb --table people \
  --export-job load-people.yaml

dtpipe --job load-people.yaml
```

```yaml
main:
  input: people.csv
  output: duck:analytics.duckdb
  batch-size: 32768
  sampling-rate: 1
  provider-options:
    duck-writer:
      table: people
```

`--export-job` is the authority on the YAML form: whatever it writes is what the loader reads.

## Beyond copying: reshaping the data

Nothing above changes a row's shape. Two mechanisms do, and they are the subject of their own
guide — [SQL and JavaScript](guides/sql-and-javascript.md):

- **`--sql`** runs DuckDB SQL over the stream: joins, aggregates, window functions,
  deduplication, sorting. It works on **sets**, stays on the Arrow path, and is the right answer
  whenever more than one row is involved.
- **`--compute` / `--filter` / `--expand`** run JavaScript on **one row at a time** — for parsing
  a string or deriving a field, where no SQL form exists.

> [!NOTE]
> Reach for SQL first. A JavaScript step is a function call per row and pins that part of the
> pipeline to the row path, so a columnar run pays a conversion to leave Arrow and another to come
> back. A `--dry-run` names those bridges, and the guide shows how to read them.

---

## Where to go next

| | |
|:---|:---|
| How the pieces fit together | [Concepts](concepts.md) |
| Connecting to a real database | [Connection catalog](connections/README.md) |
| Anonymizing while you copy | [Anonymization](guides/anonymization.md) |
| Joining two sources | [DAG pipelines](guides/dag.md) |
| Reshaping rows with SQL or JavaScript | [SQL and JavaScript](guides/sql-and-javascript.md) |
