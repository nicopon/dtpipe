# Incremental loading

[← Documentation](../README.md)

Transfer only the rows that changed since the last successful run.

```bash
dtpipe -i "pg:Host=prod;Database=app;Username=app" \
  --query "SELECT * FROM orders
           WHERE updated_at > '${{cursor://state/orders.json|1970-01-01T00:00:00.000}}'" \
  -o "sqlite:Data Source=dw.db" --table orders --strategy Append \
  --cursor updated_at --state state/orders.json
```

## How it works

| Piece | Role |
|:---|:---|
| `--cursor COLUMN` | The column to watch. dtpipe records the highest value it wrote |
| `--state PATH` | The file that keeps that value. There is no default path |
| `${{cursor://PATH\|DEFAULT}}` | In the query, the value from the last run: **this is what filters**. `PATH` is the same file as `--state`; `DEFAULT` is used before the file exists |
| `--cursor-from VALUE` | Optional: start from this value for one run, for a backfill or a replay |

`--cursor` and `--state` go together, and the query needs the `${{cursor://…}}` filter. Without
it, every run reads the whole source; dtpipe accepts it and warns.

The value is saved only when the run succeeds, so a failed run is read again next time. With
`Append`, the rows the failed run already wrote are written again: use `Upsert` to avoid
duplicates (see [Writing to a database](write-strategies.md)).

## The state file

Plain JSON, safe to commit or to keep on a volume:

```json
{
  "version": 1,
  "cursor": { "column": "updated_at", "value": "2026-06-15T23:59:59.654321", "type": "datetime" },
  "last_run": {
    "started_at": "2026-06-16T02:00:00Z",
    "completed_at": "2026-06-16T02:03:42Z",
    "rows_transferred": 1234,
    "status": "success"
  }
}
```

## In a YAML job

```yaml
main:
  input: "pg:Host=localhost;Database=prod"
  output: "sqlite:Data Source=dw.db"
  cursor: "updated_at"
  state: "state/orders_sync.json"
  provider-options:
    pg-reader:
      query: "SELECT * FROM orders WHERE updated_at > '${{cursor://state/orders_sync.json|1970-01-01T00:00:00.000}}'"
```

## Pitfalls

- **`>` or `>=`.** With `>`, a row with the same timestamp as the last one read is never picked up.
  With `>=`, the boundary rows are read again on every run, which is safe only with `Upsert`. Use
  `>` with `Append`, or `>=` with `Upsert`.
- **Below the microsecond.** The value is kept to the microsecond. On a finer column
  (`datetime2(7)`, Oracle `TIMESTAMP(9)`, DuckDB `TIMESTAMP_NS`), use `>=` with `Upsert`.
- **Oracle needs a format mask** around the cursor literal, and the column name in upper case:
  see [Oracle](../connections/oracle.md#incremental-loading-needs-a-format-mask).
- **One state file per writing branch.** Two branches sharing one are refused.
- **Prefer an id to a timestamp** when both exist: an increasing id is immune to clock skew and to
  rows inserted with an older timestamp.

## In a distributed pipeline

The coordinator keeps the cursor instead of a state file, and the source filters with a token that
names no file: `${{cursor://|DEFAULT}}`. The rules are in
[REFERENCE.md](../../REFERENCE.md#incremental-loading-in-a-distributed-pipeline).

---

See also: [Jobs and production](production.md) ·
[REFERENCE.md](../../REFERENCE.md#incremental-loading)
