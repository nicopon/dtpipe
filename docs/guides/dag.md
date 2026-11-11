# DAG pipelines: join, merge, fan out

[← Documentation](../README.md)

Repeat `-i` and one command line holds several branches. They run at the same time, in one
process, and pass rows to each other in memory.

| Flag | Meaning |
|:---|:---|
| `--alias NAME` | Name this branch so another one can read it |
| `--from A` | Read the rows of branch `A` as they stream |
| `--ref A,B` | Load branches `A` and `B` in full first, as lookup tables |
| `--sql "…"` | Run DuckDB SQL over the branches this branch reads |
| `--merge` | Concatenate every `--from` source (`UNION ALL`) |

The examples below use these three files.

`orders.csv`

```
order_id,customer_email,amount
10,alice@corp.com,120
11,bob@corp.com,80
12,alice@corp.com,45
```

`customers.csv`

```
id,email,name
1,alice@corp.com,Alice
2,bob@corp.com,Bob
```

`orders_2025.csv`

```
order_id,customer_email,amount
7,alice@corp.com,200
8,bob@corp.com,150
```

## Join two sources

```bash
dtpipe -i orders.csv --auto-column-types --alias o \
       -i customers.csv --alias c \
       --from o --ref c \
       --sql "SELECT c.name, sum(o.amount) AS total
              FROM o JOIN c ON o.customer_email = c.email
              GROUP BY c.name ORDER BY total DESC" \
       -o revenue.csv
```

```
name,total
Alice,165
Bob,80
```

Each alias is a table name in the query. The sources can be anything dtpipe reads: a CSV joined to
an Oracle table joined to a Parquet file on S3.

The run prints the DAG it built, one block per branch:

```
╭─ Pipeline ───────────────────────────────────────────────────────────────╮
│   ◉ [o]  →  [stream1]                                                    │
│       ← orders.csv  ◈ Arrow                                              │
│                                                                          │
│   ◉ [c]  →  [stream1]                                                    │
│       ← customers.csv  ◈ Arrow                                           │
│                                                                          │
│  ⚡ [stream1]  ← [o]  +ref [c]                                           │
│       SQL › SELECT c.name, sum(o.amount) AS total FROM o JOIN c ON o.... │
│       ──▶  revenue.csv                                                   │
╰──────────────────────────────────────────────────────────────────────────╯
```

`--ref` loads the lookup in memory: filter a large one upstream first. `--auto-column-types` makes
`amount` a number; without it, a CSV column is text and `sum()` fails (see
[Files](../connections/files.md#text-columns-are-text-until-you-say-otherwise)).

## Fan one source out

The source is read once and sent to every branch that reads it:

```bash
dtpipe -i orders.csv --auto-column-types --alias s \
       --from s -o orders.parquet \
       --from s --sql "SELECT customer_email, sum(amount) AS total
                       FROM s GROUP BY customer_email ORDER BY total DESC" \
       -o totals.csv
```

`totals.csv`

```
customer_email,total
alice@corp.com,165
bob@corp.com,80
```

## Merge several sources

```bash
dtpipe -i orders_2025.csv --auto-column-types --alias a \
       -i orders.csv --auto-column-types --alias b \
       --from a,b --merge -o orders_all.csv
```

```
order_id,customer_email,amount
7,alice@corp.com,200
8,bob@corp.com,150
10,alice@corp.com,120
11,bob@corp.com,80
12,alice@corp.com,45
```

The sources of a merge must have the same columns, in the same order, with the same types; a
source that differs is refused before any row moves. Their rows may come out in any order: add an
`--sql` step with an `ORDER BY` if the order matters.

## The rules that catch people out

- **Repeating `-i` or `--from` opens a new branch.** A list is therefore comma-separated:
  `--ref a,b`, never `--ref a --ref b`.
- **A branch can be read only if it has no `-o`.** To write a table and also join on it, use one
  branch that produces it, one that writes it, and one that joins.
- **`--sql` streams one `--from` source.** Read the others with `--ref`; `--from a,b --sql` is
  refused. `--merge` takes several `--from` sources.
- **A flag belongs to the stage it sits in.** Reader options go before the alias, writer options
  after `-o`. A flag given twice in one stage is refused, and so is a branch flag such as
  `--alias` given twice anywhere in the branch.

## Shapes that work

| Shape | Pattern |
|:---|:---|
| Two independent copies | `-i src1 -o dst1  -i src2 -o dst2` |
| SQL over one source | `-i src --alias a  --from a --sql "…" -o dst` |
| Join | `-i main --alias m  -i ref --alias r  --from m --ref r --sql "…" -o dst` |
| Merge | `-i a --alias a  -i b --alias b  --from a,b --merge -o dst` |
| Fan-out | `-i src --alias s  --from s -o dstA  --from s -o dstB` |
| Diamond | `-i src --alias s  --from s --filter '…' --alias hi  --from s --filter '…' --alias lo  --from hi --ref lo --sql "…" -o dst` |
| Join, then fan-out | `… --from m --ref r --sql "…" --alias j  --from j -o dstA  --from j -o dstB` |

Every shape can be saved as a YAML job with `--export-job`: see
[Jobs and production](production.md).

---

See also: [SQL and JavaScript](sql-and-javascript.md) · The grammar:
[REFERENCE.md](../../REFERENCE.md#dag-syntax)
