# Jobs and production

[← Documentation](../README.md)

## Save a pipeline as a job

```bash
dtpipe -i people.csv --compute "domain:row.email.split('@')[1]" \
  -o duck:analytics.duckdb --table people \
  --export-job load-people.yaml

dtpipe --job load-people.yaml
```

`--export-job` writes the YAML file and exits without running anything. Its output is the
reference for the format: to find how a flag is written in YAML, export a pipeline that uses it.

A job has one top-level key per branch, named after the branch's alias:

```yaml
o:
  input: orders.csv
  provider-options:
    csv-reader:
      auto-column-types: true
c:
  input: customers.csv
stream1:
  from: o
  ref:
  - c
  output: revenue.csv
  provider-options:
    sql:
      query: SELECT c.name, sum(o.amount) AS total FROM o JOIN c ON o.customer_email = c.email GROUP BY c.name
```

| Key | Holds |
|:---|:---|
| `input`, `output` | The endpoints, as `-i` and `-o` |
| `from`, `ref` | The branches it reads |
| `transformers` | The steps, in order, each with `mappings` and `options` |
| `provider-options` | Options per component: `pg-reader`, `csv-writer`, `sql`… |
| `cursor`, `state` | [Incremental loading](incremental.md) |
| `batch-size`, `limit`, `sampling-rate`, `metrics-path`, `prefix` | Engine controls |

An unknown option key gets a warning that lists the valid keys. Values can come from the
environment or the keychain, so no credential is committed: see [Secrets](secrets.md).

## Override a job from the command line

Engine controls given beside `--job` override the file for every branch, so one job serves several
environments:

```bash
dtpipe --job load-people.yaml --limit 1000 --dry-run 5
dtpipe --job load-people.yaml --prefix staging_
```

A branch flag (`-i`, `-o`, `--alias`, a step, a provider option) is refused beside `--job`: to
change a branch, change the file. One command line runs one job file. The controls that can
override a job are listed in [REFERENCE.md](../../REFERENCE.md#execution--statistics).

## Run it unattended

dtpipe has no scheduler: `cron`, a CI job or an orchestrator starts it, and reads the result.

```bash
dtpipe --job nightly.yaml --metrics-path metrics.json --log run.log --strict-bindings
echo "exit=$?"     # 0 success · 1 failure · 130 cancelled
```

| Flag | Gives |
|:---|:---|
| [Exit code](../concepts.md#exit-codes) | `0`, `1` or `130`. A cancelled run never reports success |
| `--metrics-path run.json` | Rows read and written, throughput, peak memory, duration |
| `--log run.log` | The run's log in a file |
| `--strict-bindings` | Fails the run on an undeclared flag or an unknown option key, instead of a warning nobody reads |

```json
{
  "StartTime": "2026-09-14T16:59:08.834614Z",
  "EndTime": "2026-09-14T16:59:08.842365Z",
  "ReadCount": 1000,
  "WriteCount": 1000,
  "OverallThroughputRowsPerSec": 161563.9389288311,
  "PeakMemoryWorkingSetMb": 76.734375,
  "TransformerStats": {},
  "TransformerCountsByIndex": null,
  "Duration": "00:00:00.0077510"
}
```

Alert when `ReadCount` and `WriteCount` differ: a filter dropped rows, or the target refused them.

## When a run fails

dtpipe runs once. A dropped connection or a timeout ends the run with exit `1`, and the scheduler
decides whether to start it again. A new run starts from the top, while the rows the failed run
already wrote are still in the target:

- **Upsert** writes them again without duplicates; **Append** duplicates them. See
  [Writing to a database](write-strategies.md).
- **A cursor** advances only on success, so the new run reads from the last successful run, not
  from the beginning. See [Incremental loading](incremental.md).

Where neither applies, clean up the target before running again.

## Against production data

- A preview writes nothing, and on most engines the source session is read-only: see
  [Preview and checkpoints](preview-and-checkpoints.md).
- Anonymization happens in transit, with no clear copy on disk: see
  [Anonymization](anonymization.md).
- Checkpoints are encrypted on disk.

---

See also: [Secrets](secrets.md) · [DAG pipelines](dag.md) ·
[REFERENCE.md](../../REFERENCE.md#yaml-job-file-schema)
