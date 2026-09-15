# Writing to a database

[← Documentation](../README.md)

Three independent questions, three sets of flags: **how rows land** (`--strategy`), **what happens
when the schemas differ** (the schema regime), and **what SQL runs around the transfer** (hooks).

## How rows land

| `--strategy` | Behaviour | Typical use |
|:---|:---|:---|
| `Append` *(default)* | Insert into the existing table | Daily increments, log shipping |
| `Truncate` | `TRUNCATE` then insert | Full refresh, keeping schema and indexes |
| `DeleteThenInsert` | `DELETE` then insert | When `TRUNCATE` is not available to you |
| `Recreate` | Drop, create from the source schema, insert | Full refresh including schema changes |
| `Upsert` | Update matching rows by key, insert the rest | Syncing from a source of truth |
| `Ignore` | Insert only the rows that are missing | Loading only what is new |

```bash
dtpipe -i orders_update.csv \
  -o "mssql:Server=.;Database=app;Integrated Security=true" \
  --table Orders --strategy Upsert --key OrderId
```

`--key` is read from the target's primary key when you omit it. Pass it explicitly when the table
does not exist yet, or when several unique indexes could match.

> [!WARNING]
> An upsert needs a unique index covering exactly the key columns. On MySQL, without one, the
> generated clause degenerates into a plain `INSERT` — dtpipe detects that and falls back to a
> correct, slower path. See [MySQL](../connections/mysql.md).

**A file or object target has no strategies.** It is replaced wholesale, so `--strategy` is refused
rather than accepted and quietly ignored.

## What happens when the schemas differ

Before writing to a database, dtpipe compares the source schema to the target's. Three flags
describe four regimes plus an off switch:

| Flags | Regime | A column the target lacks | A type mismatch |
|:---|:---|:---|:---|
| *(none)* | **discard** | reported, skipped, the run continues | reported, the run continues |
| `--strict-schema` | **freeze** | aborts | aborts |
| `--auto-migrate` | **evolve** | `ALTER TABLE ADD COLUMN`, then written | reported, the run continues |
| `--auto-migrate --strict-schema` | **evolve, with a net** | added, then re-inspected | aborts unless migrating resolved it |
| `--no-schema-validation` | **off** | the target is never inspected | — |

Every run states the regime it validated under, whatever the outcome:

```
Schema mode: discard - incompatibilities are reported and the run continues;
columns missing from the target are not written.
```

That line matters more than it looks: on a target that happens to be compatible, **discard** and
**freeze** produce exactly the same output while offering opposite guarantees. Only the line tells
them apart.

`--no-schema-validation` cannot be combined with the other two — it returns before the inspection
they need — and the combination is refused by name.

## SQL around the transfer

| Hook | Runs |
|:---|:---|
| `--pre-exec` | On the target, before the pipeline starts |
| `--post-exec` | On the target, after a successful transfer |
| `--on-error-exec` | On the target, on failure |
| `--finally-exec` | On the target, whatever happened |

Each accepts inline SQL or a file (`@scripts/pre.sql`). They are the place for an index drop and
rebuild around a bulk load, a `MERGE` into a final table, or an audit row.

```bash
dtpipe -i daily.parquet -o "pg:…" --table stg_daily --strategy Truncate \
  --pre-exec  "@sql/drop_indexes.sql" \
  --post-exec "@sql/rebuild_indexes.sql" \
  --on-error-exec "INSERT INTO etl_errors(job, at) VALUES ('daily', now())"
```

> [!NOTE]
> None of the four runs under `--dry-run`: they are SQL on the target, and a preview does not touch
> the target. See [Preview and checkpoints](preview-and-checkpoints.md).

## Table and column names

An identifier is handed to the engine the way *that engine* reads it unquoted, so the name it
stores is the name any other tool can type:

| Engine | `--table stock_moves` | `--table StockMoves` |
|:---|:---|:---|
| Oracle | `STOCK_MOVES` | `STOCKMOVES` |
| PostgreSQL | `stock_moves` | `stockmoves` |
| MySQL, SQL Server, SQLite, DuckDB | `stock_moves` | `StockMoves` |

To keep an exact spelling, quote the value yourself — `--table '"StockMoves"'` — and remember it is
then addressable only in quotes, from every tool.

`--prefix staging_` prefixes the table name on every database writer in the run, which is the
cheap way to point a whole job at a scratch area.

---

See also: [Incremental loading](incremental.md) · [SQL Server](../connections/sql-server.md) ·
[Oracle](../connections/oracle.md) ·
[REFERENCE.md](../../REFERENCE.md#target-writer-options)
