# Troubleshooting

[← Documentation](README.md)

Read the error first: dtpipe refuses rather than guesses, and the message usually names the fix.
This page lists symptoms whose cause is not obvious from the message, or that have no message.

## Something failed

| Symptom | Cause | See |
|:---|:---|:---|
| `No function matches sum(VARCHAR)` | A text file has no types: every column is a string | [Files](connections/files.md#text-columns-are-text-until-you-say-otherwise) |
| `No writer factory resolved` | The `-o` value matches no provider: a prefix typo, or an unknown extension | [Connection catalog](connections/README.md) |
| `--strategy` refused | The target is a file or an object, which is always replaced whole | [Writing to a database](guides/write-strategies.md) |
| `takes a value, and the flag … follows it` | A flag's value was left out, so the next flag would have been taken as its value | [REFERENCE.md](../REFERENCE.md#core-options) |
| A second identical flag is refused | Repeating `-i` or `--from` opens a branch; any other flag is given once | [Concepts](concepts.md#order-matters-and-repetition-means-something) |
| A branch cannot be read by another | It has an `-o` of its own, so it publishes nothing | [DAG pipelines](guides/dag.md#the-rules-that-catch-people-out) |
| `ORA-01821`, `ORA-01858`, `ORA-01830` on an incremental run | The cursor literal needs a format mask | [Oracle](connections/oracle.md#incremental-loading-needs-a-format-mask) |
| The run exits `130` | It was cancelled. Not an error | [Concepts](concepts.md#exit-codes) |

## Nothing failed, and the result is wrong

| Symptom | Cause | See |
|:---|:---|:---|
| A column is missing from the target | The default schema mode skips it; `--auto-migrate` adds it, `--strict-schema` aborts | [Writing to a database](guides/write-strategies.md#when-the-schemas-differ) |
| Upsert inserts duplicates | No unique index covers exactly the key columns | [Writing to a database](guides/write-strategies.md) |
| A flag seems ignored | It sits in the wrong stage: reader options before the alias, writer options after `-o` | [DAG pipelines](guides/dag.md#the-rules-that-catch-people-out) |
| An incremental run reads everything every time | The query has no `${{cursor://…}}` filter. The run warns | [Incremental loading](guides/incremental.md#how-it-works) |
| `--cursor` never advances on Oracle | The column must be spelled as the reader returns it: upper case | [Incremental loading](guides/incremental.md#pitfalls) |
| Anonymized values do not match across two tables | Seed with `--fake-seed-column` on the shared key | [Anonymization](guides/anonymization.md#keep-joins-working) |
| A table dtpipe created cannot be found under the name you typed | Oracle folds unquoted names up, PostgreSQL folds them down | [Writing to a database](guides/write-strategies.md#table-and-column-names) |
| A MySQL load warns and slows down | The server's `local_infile` is off | [MySQL](connections/mysql.md) |

## Make it show itself

```bash
DEBUG=1 dtpipe …                 # verbose logging, per branch, to stderr
dtpipe … --dry-run 5             # the real pipeline on 5 rows, nothing written
dtpipe … --log run.log           # keep the log
dtpipe … --strict-bindings       # an unknown flag or job-file key fails the run
dtpipe inspect -i "<source>"     # what the source really looks like
dtpipe providers                 # what this binary supports
```

---

The common flags: [REFERENCE.md](../REFERENCE.md). Every flag: `dtpipe --help` and
`dtpipe <subcommand> --help`.
