# PostgreSQL

[← Connections](README.md) · [Documentation](../README.md)

Read and write. Prefix `pg:`. Driver:
[Npgsql](https://www.npgsql.org/doc/connection-string-parameters.html).

## Connection string

```
pg:Host=localhost;Port=5432;Database=app;Username=app;Password=…
```

Common additions: `SSL Mode=Require`, `Include Error Detail=true`, `Timeout`, `Command Timeout`,
`Pooling`, `Application Name=dtpipe`.

Coming from `postgresql://user:pass@host:port/db`? That URI form is not accepted — the translation
is in [REFERENCE.md](../../REFERENCE.md#conversion-reference-table).

## Reading and writing

```bash
dtpipe -i "pg:Host=prod;Database=app;Username=app;Password=…" \
  --query "SELECT * FROM users WHERE active" \
  -o users.parquet

dtpipe -i users.parquet \
  -o "pg:Host=dwh;Database=analytics;Username=etl;Password=…" \
  --table users --strategy Upsert --key id
```

Reads stream through `COPY`, and writes use `COPY BINARY`, so a bulk load does not go through
row-by-row `INSERT`.

## What survives the trip

| Source type | Behaviour |
|:---|:---|
| `int[]`, `text[]` and other arrays | A real collection column. A NULL array, an empty array and a NULL element stay distinct end to end |
| `jsonb` / `json` | Carried as text; address a field with `--compute "sev:row.payload.severity"` to land it in its own typed column |
| `uuid` | A first-class `Guid`, written as a native UUID wherever the target has one |
| A type with no mapping (`point`) | Refused when the schema is built, rather than read as something else — cast it in the query (`col::json`) |

Identifiers fold **down**: `--table StockMoves` creates `stockmoves`. Quote the value yourself
(`--table '"StockMoves"'`) to keep the spelling, and remember it is then addressable only in
quotes.

## Preview

`--dry-run N` neutralises the writer and sets the source session `READ ONLY`, so the server refuses
a write even if the query tried one.

---

See also: [Write strategies](../guides/write-strategies.md) ·
[Incremental loading](../guides/incremental.md) · [REFERENCE.md](../../REFERENCE.md#providers)
