# Preview and checkpoints

[← Documentation](../README.md)

## `--dry-run N` is the real run, with the writer switched off

It is not a separate analyser — same reader, same transformers, same mode changes — so it cannot
disagree with the run it previews. `N` bounds the **source**, not the display: a step that expands
or aggregates shows a different row count from its neighbour, and says so (`10 → 3 rows`).

```bash
dtpipe -i people.csv --compute "domain:row.email.split('@')[1]" \
  -o duck:analytics.duckdb --table people --dry-run 3
```

It answers three questions at once:

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

…plus a per-column trace of what each row becomes, stage by stage.

## What a preview guarantees

| | |
|:---|:---|
| The writer | Neutralised. Not initialised, never written to, never completed — a file is not created and a table is not migrated. The target is *inspected* for the compatibility report, never modified |
| Hooks | None of the four (`--pre-exec`, `--post-exec`, `--on-error-exec`, `--finally-exec`) runs |
| Cursor, metrics | Not advanced, not written |
| The **source** | Classified first: a query carrying `DELETE`, `UPDATE`, `DROP`, `TRUNCATE`, `ALTER`, `INSERT` or `ATTACH` is refused |

Neutralising the writer is a claim about the writer. A *reader* can mutate too — `DELETE …
RETURNING`, a procedure, an `ATTACH` inside `--sql` — and `--limit` bounds what the client reads,
never what the server already did. So where the engine supports it, the session is also set
read-only and the **server** refuses the write:

| Engine | Server-enforced read-only session |
|:---|:---|
| PostgreSQL, Oracle, MySQL | ✅ `SET TRANSACTION READ ONLY` |
| SQLite | ✅ `PRAGMA query_only` |
| SQL Server | ❌ none exists — `ApplicationIntent=ReadOnly` routes to a replica, it does not make a session read-only |

> [!IMPORTANT]
> A verb scan cannot *prove* a query is read-only: `SELECT my_function()` passes it. Each run
> states which of the two guarantees it actually had, because a guarantee that is sometimes absent
> must never be presented as though it were always there.

## Reading less, without a preview

| Flag | Effect |
|:---|:---|
| `--limit 1000` | Read at most 1 000 rows, then stop reading. Bounds the **read**, so a filter downstream yields fewer rather than topping the count back up |
| `--sampling-rate 0.1` | Keep roughly one row in ten |
| `--sampling-seed 12345` | Make that sample reproducible |

These **do** write. They are for building a small but real target — a staging table for a demo, a
fixture for a test — where a dry run writes nothing at all.

## Checkpoints: run to a point, then iterate

Reading 40 million rows out of Oracle to test the fourth transformer in a chain is a waste of a
morning. `--checkpoint` materialises a branch's output on its way to the writer;
`--from-checkpoint` replaces the reader with it.

```bash
# Once: read, mask, keep the result
dtpipe -i "ora:Data Source=prod:1521/ORCL;User Id=app;Password=…" \
  --query "SELECT * FROM big_table" --mask "email:###****" --checkpoint -o null:

# Then iterate — no round trip to Oracle
dtpipe --from-checkpoint <key> --compute "total:row.price*row.qty" -o csv:out.csv
```

The materialisation point is the **end** of the transformer chain, so a checkpoint holds what the
writer would have received.

**Checkpoints are addressed by content.** The key is a hash of what produced the rows — connection
with credentials stripped, query, the transformers up to that point, the sampling parameters — not
of the alias. Two pipelines in the same directory cannot collide, an unchanged prefix is reused,
and renaming an alias changes nothing.

**They are always encrypted** (AES-GCM), with no opt-out. What that buys is not confidentiality at
rest — the key is on the same disk — but two properties of the store as a whole: a copy of it is
inert, and a purge is made reliable by destroying the key. One cleartext session would void both
for every other session, retroactively.

Artefacts belong to a **session**: `--session NAME`, else `DTPIPE_SESSION`, else the nearest
ancestor `.dtpipe/` (as git looks for `.git`), else a new one — created **only** when something is
materialised, so an ordinary run leaves no trace. Sessions expire after 7 days
(`DTPIPE_SESSION_TTL_DAYS`) and are purged the next time the store is touched.

---

See also: [Anonymization](anonymization.md) · [SQL and JavaScript](sql-and-javascript.md) ·
[REFERENCE.md](../../REFERENCE.md#sample-mode-and-materialisation)
