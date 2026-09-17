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

## Capturing the schema a pipeline produces

A preview already knows the exact shape the writer would receive. `--contract-save` writes it down:

```bash
dtpipe -i people.csv --compute "domain:row.email.split('@')[1]" \
  -o duck:analytics.duckdb --table people \
  --dry-run 100 --contract-save contracts/people.json
```

Because a dry run *is* the real run with the writer switched off, this captures the same schema a
full run produces — the two contracts have the same hash. That is what makes the line above usable
as a CI gate: nothing is written, and the promise is still the real one.

What comes out is the schema **after** the transformers, not the source's. The `--compute` above
puts `domain` in the file. A schema you write by hand cannot know that; this one is read off the
execution path.

```json
{
  "version": 1,
  "hash": "5617094d2289c89cca008e515e168b8baf19c559db93ff7e9d0032a0e4127140",
  "schemaSource": "batch",
  "enforcement": "VerbScanOnly",
  "producer": "1.8.2+e994ee51…",
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

The values in `people.csv` are random, and that hash is still fixed: it identifies the **schema**,
not the data.

`hash` identifies the schema — an unchanged pipeline keeps it across runs. `schemaSource` is
`batch` when it came off real data and `columns` when the run produced no rows, in which case it is
flat and a nested type is missing from it. `enforcement` records what that run could guarantee
about not writing to its *source*; a real run carries none rather than claiming the weakest.

Commit the file next to the pipeline. Each branch of a DAG produces its own schema, so each needs
its own path — two branches naming one file is refused before the run starts.

### Checking the other half against it

The team that consumes the data runs the contract against their own job:

```bash
dtpipe contract check --job consumer.yaml --contract contracts/people.json
```

That is a preview whose source is the contract's schema and no rows. The consumer's transformers
are built over the shape the producer promised, its target is inspected, nothing is written, and
the exit code is the answer: `0` if the target accepts it, `1` if it does not — or if the consumer
cannot even initialise, such as a projected column the contract does not carry.

Put it in both CIs and a schema change breaks in the pull request of whoever changed it, not in
someone else's nightly run.

`dtpipe contract show contracts/people.json` prints a contract's hash and columns.

> [!IMPORTANT]
> A schema contract fixes columns and types, and nothing else. It cannot know that the receiving
> service enforces rules in its own application code, so a row can satisfy every type in it and
> still be wrong for that domain. See [Write strategies](write-strategies.md).
>
> A zero-row check also cannot see anything that only fails on a row: a `--compute` reading a
> column the producer removed throws on the first row, not at initialisation.

---

See also: [Anonymization](anonymization.md) · [SQL and JavaScript](sql-and-javascript.md) ·
[REFERENCE.md](../../REFERENCE.md#sample-mode-and-materialisation)
