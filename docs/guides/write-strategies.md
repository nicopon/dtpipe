# Writing to a database

[← Documentation](../README.md)

```bash
dtpipe -i orders_update.csv \
  -o "mssql:Server=.;Database=app;Integrated Security=true" \
  --table Orders --strategy Upsert --key OrderId
```

## How rows land

| `--strategy` | What it does | For |
|:---|:---|:---|
| `Append` (default) | Inserts into the existing table | Daily increments |
| `Truncate` | Empties the table, then inserts | A full refresh that keeps the schema and indexes |
| `DeleteThenInsert` | Deletes all rows, then inserts | When `TRUNCATE` is not allowed |
| `Recreate` | Drops the table, recreates it from the source schema, inserts | A full refresh with schema changes |
| `Upsert` | Updates rows with a matching key, inserts the others | Syncing from a source of truth |
| `Ignore` | Inserts only the rows whose key is missing | Loading only what is new |

`--key` defaults to the target's primary key. Give it when the table does not exist yet, or when
several unique indexes could match.

A file or object storage target has no strategy: it is always replaced whole.

## When a run stops halfway

Each batch is committed on its own. A run that fails leaves the batches already written, and exits
`1`. Running it again with `Append` writes them a second time; with `Upsert`, `DeleteThenInsert`
or `Truncate` the table ends with one row per key. See also
[Jobs and production](production.md#when-a-run-fails).

## When the schemas differ

dtpipe compares the source with the target before writing:

| Flags | Mode | A column the target lacks | A type mismatch |
|:---|:---|:---|:---|
| (none) | **discard** | Reported, not written | Reported, the run goes on |
| `--strict-schema` | **freeze** | The run aborts | The run aborts |
| `--auto-migrate` | **evolve** | Added with `ALTER TABLE`, then written | Reported, the run goes on |
| `--auto-migrate --strict-schema` | **evolve, strictly** | Added | The run aborts |
| `--no-schema-validation` | **off** | Not checked | Not checked |

Every run prints the mode it used:

```
Schema mode: discard - incompatibilities are reported and the run continues;
columns missing from the target are not written.
```

## SQL around the transfer

| Hook | Runs on the target |
|:---|:---|
| `--pre-exec` | Before the transfer |
| `--post-exec` | After a successful transfer |
| `--on-error-exec` | After a failure |
| `--finally-exec` | Always, at the end |

Each takes inline SQL or a file (`@scripts/pre.sql`):

```bash
dtpipe -i daily.parquet -o "pg:…" --table stg_daily --strategy Truncate \
  --pre-exec  "@sql/drop_indexes.sql" \
  --post-exec "@sql/rebuild_indexes.sql" \
  --on-error-exec "INSERT INTO etl_errors(job, at) VALUES ('daily', now())"
```

Hooks do not run under `--dry-run`.

## Table and column names

A name is stored the way the engine stores it unquoted, so any tool can type it:

| Engine | `--table stock_moves` | `--table StockMoves` |
|:---|:---|:---|
| Oracle | `STOCK_MOVES` | `STOCKMOVES` |
| PostgreSQL | `stock_moves` | `stockmoves` |
| MySQL, SQL Server, SQLite, DuckDB | `stock_moves` | `StockMoves` |

To keep an exact spelling, quote it yourself: `--table '"StockMoves"'`. It must then be quoted
everywhere. `--prefix staging_` prefixes every table name in the run.

## Pitfalls

- **Upsert needs a unique index on exactly the key columns.** MySQL without one falls back to a
  slower path, and warns: see [MySQL](../connections/mysql.md).
- **Rows bypass the application.** Validation, audit columns or events that a service enforces in
  its own code do not run. Check with the team that owns the target.
- **`Recreate` drops the table** with its indexes, constraints and triggers, and warns when the old
  table had columns or a primary key the new one lacks.

---

See also: [Incremental loading](incremental.md) ·
[REFERENCE.md](../../REFERENCE.md#target-writer-options)
