# Troubleshooting

[← Documentation](README.md)

## Read the error first

dtpipe refuses rather than approximates, and a refusal names the rewrite to apply. `Binder Error:
No function matches sum(VARCHAR)` and `No writer factory resolved` are both telling you exactly
what is wrong.

This page is an index by **symptom**, for when the cause is not obvious from the message — or when
there is no message at all, which is the harder case. Each row says what is happening in one line
and sends you to the page that explains it properly.

## Something failed

| Symptom | What it means | Explained in |
|:---|:---|:---|
| `No function matches sum(VARCHAR)` | A text source has no types; every column is a string | [Files](connections/files.md) |
| `No writer factory resolved` | The `-o` value matches no provider — often a prefix typo, or an extension outside the supported set | [Connection catalog](connections/README.md) |
| `--strategy` refused | The target is a file or an object: it is replaced wholesale, there is nothing to append to | [Write strategies](guides/write-strategies.md) |
| `ORA-01821` / `ORA-01858` / `ORA-01830` on an incremental run | The cursor literal is parsed with `NLS_TIMESTAMP_FORMAT` | [Incremental loading](guides/incremental.md#things-that-bite) |
| A second identical flag is refused | Repetition means "new branch" for `-i`/`--from`/`--job`, and is an error for every other scalar flag in the same stage | [Concepts](concepts.md#order-matters-and-repetition-means-something) |
| A branch cannot be read by another | It declares an `-o` of its own, so it publishes nothing | [DAG pipelines](guides/dag.md#the-rules-that-catch-people-out) |
| The run exits `130` | Cancelled — Ctrl-C, or a branch that reported a cancellation. Not an error | [Concepts](concepts.md#exit-codes) |

## Nothing failed, and the result is wrong

These have no error message to search for, which is why they are collected here.

| Symptom | What it means | Explained in |
|:---|:---|:---|
| A database source reads the whole table although you passed a filter | A file reader ignores `--query`; a database reader needs one | [Connection catalog](connections/README.md) |
| Upsert inserts duplicates instead of updating | No unique index covers exactly the key columns | [Write strategies](guides/write-strategies.md) |
| A column is missing from the target and nothing failed | Schema regime **discard**, the default — `--auto-migrate` adds it, `--strict-schema` aborts | [Write strategies](guides/write-strategies.md) |
| A flag seems to be ignored | It landed in the wrong stage or branch: reader options go before the alias, writer options after `-o` | [DAG pipelines](guides/dag.md#the-rules-that-catch-people-out) |
| An incremental run re-reads everything each time | `--cursor` and `--state` only work as a pair, and one without the other is silent | [Incremental loading](guides/incremental.md#three-flags) |
| `--cursor` never advances on Oracle | The column name is not spelled as the reader returns it — upper case for an unquoted identifier | [Incremental loading](guides/incremental.md#things-that-bite) |
| Anonymized values do not match between two tables | The generated value depends on the whole `--fake` set of the run, not only on the seed | [Anonymization](guides/anonymization.md#joins-have-to-survive--seed-the-generator-by-value) |
| A table dtpipe created is not reachable as you typed it | Identifier casing: Oracle folds up, PostgreSQL folds down | [dtpipe and .NET](dotnet.md#oracle-and-sql-server) |
| MySQL bulk load warns and slows down | `local_infile` is OFF on the server (default since MySQL 8) | [MySQL](connections/mysql.md) |

## Make the failure show itself

```bash
DEBUG=1 dtpipe …                 # verbose, branch-level logging to stderr
dtpipe … --dry-run 5             # the real pipeline, nothing written
dtpipe … --log run.log           # keep the log
dtpipe … --strict-bindings       # an undeclared flag or job-file key exits non-zero
dtpipe inspect -i "<source>"     # what does the source actually look like?
dtpipe providers                 # what does this binary actually support?
```

`--dry-run` is the single most useful of these: it runs the real pipeline over a handful of rows
with the writer neutralised, and prints the execution plan, the target compatibility report and a
per-column trace of what each step does to a row. See
[Preview and checkpoints](guides/preview-and-checkpoints.md).

## Still stuck

- [REFERENCE.md](../REFERENCE.md) — the exhaustive flag list; the answer is often a flag you have
  not met yet
- `dtpipe --help`, `dtpipe <subcommand> --help` — generated from the binary, so never out of date

---

[← Documentation](README.md)
