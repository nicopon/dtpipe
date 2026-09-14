# Troubleshooting

[← Documentation](README.md)

## Read the error first

dtpipe refuses rather than approximates, and a refusal names the rewrite to apply. `Binder Error:
No function matches sum(VARCHAR)` and `No writer factory resolved` are both telling you exactly
what is wrong — the table below is for when the *cause* is not obvious from the message.

| Symptom | Cause | Fix |
|:---|:---|:---|
| `No function matches sum(VARCHAR)` in a `--sql` branch | A text source has no types; every column is a string | `--auto-column-types` on that reader, or `--column-types "amount:decimal"` |
| `No writer factory resolved` | The `-o` value matches no provider — often a prefix typo, or an extension outside the supported set | Check `dtpipe providers`; name the prefix explicitly (`parquet:out.data`) |
| A database source reads the whole table although you passed a filter | A file reader ignores `--query`; a database reader needs one | Use `--query`, or `--table` for the whole table |
| `--strategy` refused | The target is a file or an object — it is replaced wholesale, there is nothing to append to | Drop the flag |
| Upsert silently inserts duplicates | No unique index covers exactly the key columns | Add the index (see [MySQL](connections/mysql.md)) |
| A table dtpipe created is not reachable as you typed it | Identifier casing: Oracle folds up, PostgreSQL folds down | Quote it yourself — `--table '"StockMoves"'` — or use the engine's own spelling |
| An Oracle incremental run fails with `ORA-01821` / `ORA-01858` / `ORA-01830` | The cursor literal is being parsed with `NLS_TIMESTAMP_FORMAT` | Wrap it in `TO_TIMESTAMP(…, 'YYYY-MM-DD"T"HH24:MI:SS.FF3')` — see [Oracle](connections/oracle.md) |
| `--cursor` never advances on Oracle | The column name is not spelled as the reader returns it | Upper case for an unquoted identifier: `--cursor UPDATED_AT` |
| MySQL bulk load warns and slows down | `local_infile` is OFF on the server (default since MySQL 8) | `SET GLOBAL local_infile = 1`, or accept the fallback |
| A flag seems to be ignored | It landed in the wrong stage or branch — reader options go before the alias, writer options after `-o` | Re-read the command as branches; add `--strict-bindings` to make a bad binding fail |
| A second identical flag is refused | Repetition means "new branch" for `-i`/`--from`/`--job`, and is an error for every other scalar flag in the same stage | Use a comma list (`--ref a,b`) |
| The run exits `130` | Cancelled — Ctrl-C, or a branch that reported a cancellation | Not an error; nothing is silently reported as success |
| A column is missing from the target and nothing failed | Schema regime **discard**, the default | `--auto-migrate` to add it, or `--strict-schema` to make it abort |

## Make the failure show itself

```bash
DEBUG=1 dtpipe …                 # verbose, branch-level logging to stderr
dtpipe … --dry-run 5             # the real pipeline, nothing written
dtpipe … --log run.log           # keep the log
dtpipe … --strict-bindings       # unrecognised flags become a non-zero exit
dtpipe inspect -i "<source>"     # what does the source actually look like?
dtpipe providers                 # what does this binary actually support?
```

`--dry-run` is the single most useful of these: it runs the real pipeline over a handful of rows
with the writer neutralised, and prints the execution plan, the target compatibility report and a
per-column trace. See [Preview and checkpoints](guides/preview-and-checkpoints.md).

## Still stuck

- [REFERENCE.md](../REFERENCE.md) — the exhaustive flag list; the answer is often a flag you have
  not met yet
- [COOKBOOK.md](../COOKBOOK.md) — a working recipe close to what you are doing
- `dtpipe --help`, `dtpipe <subcommand> --help` — generated from the binary, so never out of date

---

[← Documentation](README.md)
