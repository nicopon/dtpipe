# Oracle

[← Connections](README.md) · [Documentation](../README.md)

Read and write. Prefix `ora:`. Driver:
[Oracle.ManagedDataAccess](https://docs.oracle.com/en/database/oracle/oracle-database/23/odpnt/ConnectionConnectionString.html)
— **fully managed**, compiled into the binary. No Instant Client, no `LD_LIBRARY_PATH`, no
architecture-matched native package: copy the dtpipe binary onto a host and it talks to Oracle.

## Connection string

```
ora:Data Source=host:1521/SERVICE;User Id=app;Password=…
```

| `Data Source` form | When |
|:---|:---|
| `host:1521/SERVICE` | EZ-Connect — the short form, no configuration files |
| `PRODDB` | A `tnsnames.ora` alias, resolved wherever the driver finds it (`TNS_ADMIN`) |
| `(DESCRIPTION=(ADDRESS=…)(CONNECT_DATA=…))` | A full descriptor inline, for RAC or a failover list |

Everything else — `Connection Timeout`, `Pooling`, `Statement Cache Size`, proxy authentication —
is the driver's vocabulary.

## Reading

```bash
dtpipe -i "ora:Data Source=prod:1521/ORCL;User Id=app;Password=…" \
  --query "SELECT * FROM invoices WHERE updated_at >= SYSDATE - 7" \
  -o invoices.parquet
```

## Writing

```bash
dtpipe -i invoices.parquet \
  -o "ora:Data Source=dwh:1521/ORCL;User Id=etl;Password=…" \
  --table invoices --strategy Upsert --key INVOICE_ID
```

`--insert-mode` selects how rows land: `Standard` (batched), `Bulk`, or `Append` — which adds the
direct-path hint. See [Write strategies](../guides/write-strategies.md) for the strategies and the
schema regimes.

## Identifier casing: Oracle folds up

An identifier is handed to the engine the way *that engine* reads it unquoted, so the name Oracle
stores is the name any other tool can type:

| You write | Oracle stores |
|:---|:---|
| `--table stock_moves` | `STOCK_MOVES` |
| `--table StockMoves` | `STOCKMOVES` |
| `--table '"StockMoves"'` | `StockMoves`, and it is then addressable only in quotes |

This has a direct consequence on `--cursor`: name the column **as the reader returns it**, which
for an unquoted Oracle identifier is upper case — `--cursor UPDATED_AT`, not `updated_at`.

## Incremental loading needs a format mask

PostgreSQL and SQLite compare a cursor mark as a string. Oracle does not: a `TIMESTAMP` column
compared against a bare literal is parsed with the session's `NLS_TIMESTAMP_FORMAT`, which is not
the format the state file writes. Wrap it:

```bash
dtpipe -i "ora:Data Source=localhost:1521/FREEPDB1;User Id=app;Password=…" \
  -q "SELECT * FROM invoices WHERE updated_at > TO_TIMESTAMP('${{cursor://state/invoices.json|1970-01-01T00:00:00.000}}', 'YYYY-MM-DD\"T\"HH24:MI:SS.FF3')" \
  -o invoices.parquet \
  --cursor UPDATED_AT --state state/invoices.json
```

Three details decide whether it runs, and each fails with its own `ORA-` code — the escaped `"T"`,
no space in its place, and `.FF3` for the fractional digits. The full table of near-misses is in
[COOKBOOK.md](../../COOKBOOK.md#scenario-incremental-sync-from-oracle--the-cursor-needs-a-format-mask).

## Preview

`--dry-run N` neutralises the writer, and on Oracle it also sets the session read-only
(`SET TRANSACTION READ ONLY`), so the **server** refuses a write even if the query tried one.

---

See also: [dtpipe and .NET](../dotnet.md) ·
[Incremental loading](../guides/incremental.md) ·
[Write strategies](../guides/write-strategies.md) ·
[REFERENCE.md](../../REFERENCE.md#providers)
