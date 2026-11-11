# Quickstart

[← Documentation](README.md)

Six steps, no database to set up. Every output below was produced by running them.

## 1. Make a dataset

```bash
dtpipe -i generate:1000 \
  --fake "name:name.fullName" \
  --fake "email:internet.email" \
  --fake "city:address.city" \
  --drop GenerateIndex \
  -o people.csv
```

`generate:1000` produces 1 000 rows, and each `--fake` adds a column of realistic values:

```
name,email,city
Blanca Towne,Destini.Langosh82@yahoo.com,Bonniemouth
Elenor Aufderhar,Jamey.Friesen54@yahoo.com,Macejkovicbury
Mustafa Roberts,Adrian62@yahoo.com,East Ignacio
```

On a column that already exists, `--fake` replaces its values: that is
[anonymization](guides/anonymization.md).

Every run starts by printing its pipeline: the source, the steps, the target.

```
╭─ Pipeline ──────────────────╮
│   ◉  generate:1000  ◈ Arrow │
│      → fake                 │
│      → project              │
│      ──▶  people.csv        │
╰─────────────────────────────╯
```

## 2. Look at it

```bash
dtpipe inspect -i people.csv
```

```
┌───┬────────┬────────┬──────────┐
│ # │ Column │ Type   │ Nullable │
├───┼────────┼────────┼──────────┤
│ 1 │ name   │ String │ ✓        │
│ 2 │ email  │ String │ ✓        │
│ 3 │ city   │ String │ ✓        │
└───┴────────┴────────┴──────────┘
```

`inspect` works on any source, databases included.

## 3. Convert it

```bash
dtpipe -i people.csv -o people.parquet
```

The format comes from the file extension, or from a prefix such as `csv:` or `pg:`.

## 4. Preview a load

`--dry-run N` runs the pipeline on N rows and writes nothing.

```bash
dtpipe -i people.csv -o duck:analytics.duckdb --table people --dry-run 3
```

```
╭─Target Schema Info──────────────────╮
│ Target: Will be created (new table) │
│ Status: ✅ Schema is compatible     │
╰─────────────────────────────────────╯
```

A column the target cannot hold fails the preview, as it would fail the real run. See
[Preview and checkpoints](guides/preview-and-checkpoints.md).

## 5. Load it, then query it

```bash
dtpipe -i people.csv -o duck:analytics.duckdb --table people

dtpipe -i duck:analytics.duckdb \
  --query "SELECT count(*) AS people, count(DISTINCT city) AS cities FROM people" \
  -o jsonl:- --no-stats
```

```json
{"people":1000,"cities":994}
```

`jsonl:-` writes to standard output, so dtpipe fits in a shell pipe.

## 6. Save it as a job

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

Commit the file and run it from CI: see [YAML jobs](guides/yaml-jobs.md).

---

| Next | |
|:---|:---|
| How the pieces fit | [Concepts](concepts.md) |
| A real database | [Connection catalog](connections/README.md) |
| Anonymize while you copy | [Anonymization](guides/anonymization.md) |
| Join, filter, reshape | [DAG pipelines](guides/dag.md) · [SQL and JavaScript](guides/sql-and-javascript.md) |

The common flags: [REFERENCE.md](../REFERENCE.md). Every flag: `dtpipe --help`.
