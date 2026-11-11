# PostgreSQL

[← Connections](README.md) · [Documentation](../README.md)

Read and write, prefix `pg:`. Driver:
[Npgsql](https://www.npgsql.org/doc/connection-string-parameters.html).

```
pg:Host=localhost;Port=5432;Database=app;Username=app;Password=…
```

Common additions: `SSL Mode=Require`, `Command Timeout`, `Application Name=dtpipe`. The URI form
`postgresql://…` is not accepted: see the
[conversion table](../../REFERENCE.md#conversion-reference-table).

## Read and write

```bash
dtpipe -i "pg:Host=prod;Database=app;Username=app;Password=…" \
  --query "SELECT * FROM users WHERE active" \
  -o users.parquet

dtpipe -i users.parquet \
  -o "pg:Host=dwh;Database=analytics;Username=etl;Password=…" \
  --table users --strategy Upsert --key id
```

Reads and writes use `COPY`. A preview sets the source session read-only. Names fold to lower case:
`--table StockMoves` creates `stockmoves`.

## Types

| Source | Behaviour |
|:---|:---|
| Arrays (`int[]`, `text[]`…) | Kept as list columns |
| `json`, `jsonb` | Text. `--compute "sev:row.payload.severity"` extracts a field |
| `uuid` | A UUID, written as one wherever the target has the type |
| `inet`, `interval`, `timetz`, `money`, `point` | Refused when the reader opens, with the cast to use |
| `'infinity'` timestamps, `'NaN'` numerics | Refused at that row, with the cast that reads them as text |

A refusal names each column and the expression to write instead:

```
The pg reader cannot read this column, and nothing was read:
  'c': inet[]; select it as: c::text[] AS c
Write each expression in the query where the column is selected.
```

Writing, a type PostgreSQL has no equivalent for (an unsigned or 8-bit integer from DuckDB,
Parquet or MySQL) is refused before the table is touched, with the cast to apply. The full lists:
[Types a reader cannot read](../../REFERENCE.md#types-a-reader-cannot-read) ·
[Types a writer cannot write](../../REFERENCE.md#types-a-writer-cannot-write).

---

See also: [Writing to a database](../guides/write-strategies.md) ·
[REFERENCE.md](../../REFERENCE.md#providers)
