# Running in production

[← Documentation](../README.md)

You have a pipeline that works. Now it has to run every night, unattended, against a database
someone depends on — and tell you honestly what it did.

This page is what changes between *running it* and *scheduling it*.

## There is no scheduler

A run starts, streams and exits. dtpipe has no daemon, no agent and no state between runs beyond
the files you point it at. `cron`, a CI job, an Airflow operator or a Windows scheduled task decides
*when*; dtpipe decides *what happens once started*.

That is the whole integration surface, and it is why the rest of this page is about exit codes and
files rather than about a service to operate.

## The contract a machine can read

| | |
|:---|:---|
| Exit codes | `0` success · `1` failure · `130` cancelled (Ctrl-C). **A cancellation never reports as success** |
| `--metrics-path run.json` | Structured result: rows read and written, throughput, peak memory, start and end, duration |
| `--log run.log` | The run's log to a file |
| `--strict-bindings` | Two otherwise-tolerated faults become a non-zero exit: an undeclared flag that consumes no value, and an unknown `provider-options` key |
| `--job pipeline.yaml` | The pipeline itself, in version control, reviewable in a pull request |

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

**`ReadCount` and `WriteCount` differing is the number to alert on.** A row read and not written was
dropped by a filter or refused by the target. Both cases are legitimate; neither should be a
surprise at three in the morning.

`--strict-bindings` earns its place here specifically. The case it catches most often is a job-file
key that is really the command-line flag — `throttle` where the key is `rows-per-second` — which
warns interactively and is then lost in a log nobody reads.

## When a run fails

dtpipe runs an export once. A dropped connection, a timeout or an I/O error ends it with exit `1`,
and the scheduler that started it decides whether to start it again, as it decided when to start
it at all.

**A second run starts from the top.** The source is read again and the pre-hook runs again, while
whatever the failed run already committed is still in the target. Two settings decide what that
costs:

- **A write strategy that tolerates rows written twice**: `Upsert` over `Append` lands them once.
  See [Writing to a database](write-strategies.md).
- **A cursor**: its mark advances only on success, so the second run reads again from the last run
  that succeeded, not from the beginning of the source. See [Incremental loading](incremental.md).

A cursor bounds what is read again; only the write strategy decides whether it is written twice.
On a target where neither helps, a failed run is better cleaned up than started again.

## Secrets on an agent

A build agent has environment variables rather than a desktop keychain, and `${{ENV_VAR}}` reads
them anywhere in a job file, including inside a connection string. Object-storage credentials left
unset fall back to the ambient chain — environment, shared config, instance profile.

Nothing sensitive has to sit in a job file or in shell history. The four interpolation forms and
what each one may appear in: [Secrets](secrets.md).

## Against production data

The writer is the side people worry about, and the one dtpipe can neutralise. These are the
guarantees, and their limits:

- **Anonymization happens in transit.** The source is read, never written, and no intermediate file
  holds clear values. Seeded faking keeps joins working across tables, so an anonymized extract is
  still usable. See [Anonymization](anonymization.md).
- **A preview writes nothing** — and on PostgreSQL, Oracle, MySQL and SQLite the source session is
  set read-only, so the *server* enforces it rather than a regex guessing. **On SQL Server no such
  session exists**, and the run reports the weaker guarantee instead of implying the stronger one.
  See [Preview and checkpoints](preview-and-checkpoints.md).
- **Materialised data is always encrypted** (AES-GCM, no opt-out), so a checkpoint left on a laptop
  is inert and deleting the key makes a purge reliable.

> [!IMPORTANT]
> Neutralising the writer is a claim about the writer. **A source can still mutate**: a
> `DELETE … RETURNING`, an `OUTPUT` clause, an `ATTACH` inside `--sql`. A preview classifies the
> resolved source SQL and refuses those rather than trusting that a read is a read.
>
> And masking and faking are not anonymity proofs. Choosing which columns are sensitive is yours —
> [what this is and what it is not](anonymization.md#what-this-is-and-what-it-is-not) says where
> the limits are.

## Putting it together

```bash
dtpipe --job nightly.yaml \
       --metrics-path metrics.json \
       --log run.log \
       --strict-bindings
echo "exit=$?"     # 0 success · 1 failure · 130 cancelled
```

Everything that decides *what* this run does is in `nightly.yaml`, reviewable in a pull request.
Everything on the command line decides how the machine hears about it.

---

See also: [YAML jobs](yaml-jobs.md) · [Secrets](secrets.md) ·
[Preview and checkpoints](preview-and-checkpoints.md) ·
[dtpipe and .NET](../dotnet.md)
