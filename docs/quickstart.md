# Quickstart

[← Documentation](README.md)

Five commands, no database to set up. Everything below runs against a generated dataset and two
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

Every run prints that panel: the source, each transformation in order, the target, and whether the
stream travelled row by row (`● row`) or as Arrow batches (`◈ Arrow`).

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
dtpipe -i people.csv \
  --compute "domain:row.email.split('@')[1]" \
  -o duck:analytics.duckdb --table people \
  --dry-run 3
```

It answers three questions at once — how the pipeline will execute, what the target will have to
accept, and what a row actually becomes:

```
╭─Pipeline Execution Plan──────────────────────╮
│  Reader  csv                  ▼ row          │
│  Step    Compute              ▼ row-only     │
│  Sink    duck                 ▲ columnar sink│
│                                              │
│  Strategy: Columnar · 1 bridge               │
╰──────────────────────────────────────────────╯

╭─Target Schema Info──────────────────╮
│ Target: Will be created (new table) │
│ Status: ✅ Schema is compatible     │
╰─────────────────────────────────────╯
```

No file is created, no table is migrated, and no `--pre-exec` hook runs. See
[Preview and checkpoints](guides/preview-and-checkpoints.md) for exactly what a dry run guarantees.

## 5. Run it, then query the result

```bash
dtpipe -i people.csv \
  --compute "domain:row.email.split('@')[1]" \
  -o duck:analytics.duckdb --table people
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
  --query "SELECT domain, count(*) AS n FROM people GROUP BY domain ORDER BY n DESC LIMIT 3" \
  -o jsonl:- --no-stats
```

```json
{"domain":"gmail.com","n":340}
{"domain":"yahoo.com","n":335}
{"domain":"hotmail.com","n":325}
```

## 6. Keep the pipeline

Any command line can be serialised to a YAML job and replayed:

```bash
dtpipe -i people.csv --compute "domain:row.email.split('@')[1]" \
  -o duck:analytics.duckdb --table people \
  --export-job load-people.yaml

dtpipe --job load-people.yaml
```

`--export-job` is the authority on the YAML form: whatever it writes is what the loader reads.

---

## Where to go next

| | |
|:---|:---|
| How the pieces fit together | [Concepts](concepts.md) |
| Connecting to a real database | [Connection catalog](connections/README.md) |
| Anonymizing while you copy | [Anonymization](guides/anonymization.md) |
| Joining two sources | [DAG pipelines](guides/dag.md) |
