# Oracle

[← Connections](README.md) · [Documentation](../README.md)

Read and write, prefix `ora:`. The driver,
[Oracle.ManagedDataAccess](https://docs.oracle.com/en/database/oracle/oracle-database/23/odpnt/ConnectionConnectionString.html),
is inside the binary: no Instant Client and nothing to install.

```
ora:Data Source=host:1521/SERVICE;User Id=app;Password=…
```

| `Data Source` | When |
|:---|:---|
| `host:1521/SERVICE` | The short form, with no configuration file |
| `PRODDB` | A `tnsnames.ora` alias (found through `TNS_ADMIN`) |
| `(DESCRIPTION=(ADDRESS=…)(CONNECT_DATA=…))` | A full descriptor, for RAC or failover |

## Read and write

```bash
dtpipe -i "ora:Data Source=prod:1521/ORCL;User Id=app;Password=…" \
  --query "SELECT * FROM invoices WHERE updated_at >= SYSDATE - 7" \
  -o invoices.parquet

dtpipe -i invoices.parquet \
  -o "ora:Data Source=dwh:1521/ORCL;User Id=etl;Password=…" \
  --table invoices --strategy Upsert --key INVOICE_ID
```

`--insert-mode` picks how rows land: `Standard`, `Bulk`, or `Append` (direct-path). A preview sets
the source session read-only.

## Names fold to upper case

`--table stock_moves` creates `STOCK_MOVES`. To keep another spelling, quote it:
`--table '"StockMoves"'`. For the same reason, `--cursor` takes the upper-case name:
`--cursor UPDATED_AT`.

## Incremental loading needs a format mask

Oracle reads a bare timestamp literal with the session's format, not the one the state file uses.
Wrap the cursor in `TO_TIMESTAMP`:

```bash
dtpipe -i "ora:Data Source=localhost:1521/FREEPDB1;User Id=app;Password=…" \
  -q "SELECT * FROM invoices WHERE updated_at > TO_TIMESTAMP('${{cursor://state/invoices.json|1970-01-01T00:00:00.000}}', 'YYYY-MM-DD\"T\"HH24:MI:SS.FF')" \
  -o invoices.parquet \
  --cursor UPDATED_AT --state state/invoices.json
```

| Mistake | Oracle says |
|:---|:---|
| `T` not quoted: `YYYY-MM-DDTHH24…` | `ORA-01821: date format not recognized` |
| A space instead of `"T"` | `ORA-01858: A non-numeric character was found…` |
| No `.FF`, or `.FF3` on a `TIMESTAMP(6)` | `ORA-01830: Date format picture ends before converting entire input string` |

---

See also: [Incremental loading](../guides/incremental.md) ·
[REFERENCE.md](../../REFERENCE.md#providers)
