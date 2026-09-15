# YAML jobs

[← Documentation](../README.md)

A job file is the same pipeline, written down: reviewable, versionable, runnable from CI without a
200-character command line.

## Get one without writing one

```bash
dtpipe -i people.csv --compute "domain:row.email.split('@')[1]" \
  -o duck:analytics.duckdb --table people \
  --export-job load-people.yaml

dtpipe --job load-people.yaml
```

`--export-job` writes the file and exits without running the pipeline. It is also the **authority
on the format**: whatever it emits is what the loader accepts, so when a key in the reference is
ambiguous, export a pipeline that uses it and look.

## The shape

One top-level key per branch. The key is the branch name — and the alias other branches reference.

```yaml
main:
  input: "pg:Host=localhost;Database=prod;Username=app"
  output: "output.parquet"
  provider-options:
    pg:
      query: "SELECT * FROM users"
```

```yaml
# Reader options go under the component they configure; -reader / -writer disambiguates
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

| Section | Holds |
|:---|:---|
| `input`, `output` | The endpoints, exactly as `-i` and `-o` |
| `from`, `ref` | DAG routing — a string, or a list |
| `transformers` | The chain, in order, each with `mappings` and `options` |
| `provider-options` | Per-component options, keyed by component name (`pg`, `csv-reader`, `sql`) |
| `cursor`, `state` | Incremental loading |
| `batch-size`, `limit`, `sampling-rate`, `metrics-path`, `log-path`, `prefix` | Engine controls |

An `options:` key is the **property name**, which is not always the flag name: `--compute-types`
is `compute-types`, but `--duck-init` is `init-sql`. A key matching no property is refused **by
name**, and the error names the one that would have worked.

## Overrides

CLI flags applied alongside `--job` override the file. That is how one job serves several
environments without a template engine:

```bash
dtpipe --job load-people.yaml --limit 1000 --dry-run 5
dtpipe --job load-people.yaml --prefix staging_
```

## Values that must not be in the file

Every value in a YAML job goes through the resolver before parsing, so a credential never has to
be committed:

```yaml
main:
  input: "pg:Host=${{PGHOST}};Database=app;Username=app;Password=${{keyring://pg-pass}}"
```

See [Secrets](secrets.md).

## In CI

A job file is what you put under review; the flags that make a run legible to a machine — exit
codes, metrics, logs, `--strict-bindings`, `--retry` — are in
[Running in production](production.md).

---

See also: [DAG pipelines](dag.md) · [Incremental loading](incremental.md) ·
[Enterprise](../enterprise.md) · [REFERENCE.md](../../REFERENCE.md#yaml-job-file-schema)
