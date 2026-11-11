# Preview and checkpoints

[← Documentation](../README.md)

## Preview a run

`--dry-run N` runs the real pipeline on the first N source rows and writes nothing.

```bash
dtpipe -i people.csv --compute "domain:row.email.split('@')[1]" \
  -o duck:analytics.duckdb --table people --dry-run 3
```

It prints the execution plan, whether the target accepts each column, and what each step does to
a row:

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

A column the target cannot hold fails the preview with the same message as the real run. A
preview cannot see what only writing finds: a constraint, a value too long for its column, a row
past the first N.

| During a preview | |
|:---|:---|
| The target | Inspected, never changed: no file created, no table created or migrated. A DuckDB or SQLite database file that does not exist yet is still created, empty, when the preview connects |
| Hooks (`--pre-exec`…) | Not run |
| Cursor and metrics file | Not written |
| A source query with `DELETE`, `UPDATE`, `DROP`, `TRUNCATE`, `ALTER`, `INSERT` or `ATTACH` | Refused |

On PostgreSQL, Oracle, MySQL and SQLite the source session is also set read-only, so the server
itself refuses a write. SQL Server has no read-only session, and the run says it had the weaker
guarantee only.

## Read less, and write it

| Flag | Effect |
|:---|:---|
| `--limit 1000` | Read at most 1 000 rows |
| `--sampling-rate 0.1` | Keep about one row in ten |
| `--sampling-seed 12345` | Make that sample the same on every run |

Unlike a preview, these write: use them to build a small real target, such as a test fixture.

## Checkpoints: run to a point, then iterate

`--checkpoint NAME` saves a branch's output, after its last step. `--from-checkpoint KEY` reads it
back in place of the source, so you can change the next steps without reading the source again.

```bash
# Once: read and mask, keep the result
dtpipe -i "ora:Data Source=prod:1521/ORCL;User Id=app;Password=…" \
  --query "SELECT * FROM big_table" --mask "email:###****" --checkpoint stage1 -o null:

# Then iterate on the next steps, with no round trip to Oracle
dtpipe --from-checkpoint <key> --compute "total:row.price*row.qty" -o csv:out.csv
```

- The key comes from what produced the rows (source, query, steps, limits), not from the name, so
  an unchanged pipeline finds its checkpoint again and a changed one does not.
- Checkpoints are always encrypted on disk.
- They belong to a session: `--session NAME`, else `DTPIPE_SESSION`, else the nearest `.dtpipe/`
  folder, else a new one. Sessions expire after 7 days (`DTPIPE_SESSION_TTL_DAYS`).
  `dtpipe session list` and `dtpipe session purge` manage them.

## Schema contracts

`--contract-save` writes down the schema a pipeline delivers to its target, after every step:

```bash
dtpipe -i people.csv --compute "domain:row.email.split('@')[1]" \
  -o duck:analytics.duckdb --table people \
  --dry-run 100 --contract-save contracts/people.json
```

```json
{
  "version": 1,
  "hash": "5617094d2289c89cca008e515e168b8baf19c559db93ff7e9d0032a0e4127140",
  "schemaSource": "batch",
  "enforcement": "VerbScanOnly",
  "producer": "1.15.2+8673ec32…",
  "schema": {
    "fields": [
      { "name": "name",   "nullable": true, "type": "utf8" },
      { "name": "email",  "nullable": true, "type": "utf8" },
      { "name": "city",   "nullable": true, "type": "utf8" },
      { "name": "domain", "nullable": true, "type": "utf8" }
    ]
  }
}
```

A preview and a full run give the same contract, so the line above works as a CI check that writes
nothing. The `hash` identifies the schema, not the data. Commit the file next to the pipeline; each
branch of a DAG needs its own path.

The team that consumes the data checks its own job against the contract:

```bash
dtpipe contract check --job consumer.yaml --contract contracts/people.json
```

It exits `0` if the consumer's target accepts that schema, `1` if not. Run it in both CIs, and a
schema change fails in the pull request that made it. `dtpipe contract show` prints a contract.

Pitfalls:

- A contract fixes columns and types only. Rules the receiving application enforces in its own
  code are not in it.
- The check runs on no rows, so a step that fails only on a value (a `--compute` reading a removed
  column) is not caught.

---

See also: [Anonymization](anonymization.md#check-before-you-hand-it-over) ·
[REFERENCE.md](../../REFERENCE.md#sample-mode-and-materialisation)
