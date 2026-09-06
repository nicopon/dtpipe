# DtPipe Cookbook

Recipes and end-to-end scenarios. For the full option reference, see [REFERENCE.md](./REFERENCE.md).

> [!IMPORTANT]
> **Database Connection Strings**: All database configurations in these recipes use standard **ADO.NET connection strings** (e.g. `pg:Host=localhost;Database=mydb;...` rather than Python/SQLAlchemy connection URIs). If you are coming from Python or SQLAlchemy, refer to the [Database Connection Strings translation guide in REFERENCE.md](./REFERENCE.md#database-connection-strings-adonet-format) for formatting help.

> **Docs map:** [README.md](./README.md) — quick start · [REFERENCE.md](./REFERENCE.md) — full CLI & YAML reference · [EXTENDING.md](./EXTENDING.md) — new adapters/transformers

**Table of Contents**
- [Basic Transfers](#basic-transfers)
- [Anonymization Before Export](#anonymization-before-export)
- [Schema Transformations](#schema-transformations)
- [Database Import & Migration](#database-import--migration)
- [SQL Processors and Joins](#sql-processors-and-joins)
- [DuckDB Extensions and Cloud Storage](#duckdb-extensions-and-cloud-storage)
- [DAG Pipelines (Multi-Source)](#dag-pipelines-multi-source)
- [Standard Streams and Automation](#standard-streams-and-automation)
- [Incremental Loading](#incremental-loading)
- [AI Agent Integration](#ai-agent-integration)

---

## Basic Transfers

### Database to file

```bash
# PostgreSQL → Parquet
dtpipe -i "pg:Host=localhost;Database=prod;Username=postgres" \
       --query "SELECT * FROM users" \
       -o users.parquet

# PostgreSQL → CSV
dtpipe -i "pg:Host=localhost;Database=prod;Username=postgres" \
       --query "SELECT * FROM orders WHERE created > '2024-01-01'" \
       -o orders.csv
```

### File format conversion

```bash
# CSV → Parquet (columnar fast-path: no row conversion)
dtpipe -i data.csv -o data.parquet

# Parquet → Arrow
dtpipe -i data.parquet -o data.arrow
```

### Dry run — validate schema without writing

```bash
dtpipe -i "pg:Host=localhost;Database=prod" \
       --query "SELECT * FROM users" \
       --dry-run 20
```

---

## Anonymization Before Export

### Scenario: anonymize a production table before handing it to a third party

```bash
dtpipe \
  -i "pg:Host=localhost;Database=prod;Username=postgres" \
  --query "SELECT id, name, email, phone, birth_date, address FROM users" \
  \
  --fake "name:name.fullName" \
  --fake "email:internet.email" \
  --fake "phone:phone.phoneNumber" \
  --fake "address:address.fullAddress" \
  --fake "birth_date:date.past" \
  --null "ssn" \
  \
  -o anonymized_users.parquet
```

### Deterministic faking (preserve referential integrity across tables)

Use `--fake-seed-column` to guarantee that the same input value always produces the same anonymized output — even across separate runs or tables. You can also specify multiple columns (separated by commas) to handle composite keys.

```bash
# Users table
dtpipe -i "pg:..." --query "SELECT id, name, email FROM users" \
       --fake "name:name.fullName" --fake-seed-column id \
       -o anonymized_users.parquet

# Orders table — same seed → same anonymized names
dtpipe -i "pg:..." --query "SELECT order_id, user_id, name FROM orders" \
       --fake "name:name.fullName" --fake-seed-column user_id \
       -o anonymized_orders.parquet
```

### Composite key faking (multiple seed columns)

If your table has a composite key, pass all key columns to `--fake-seed-column` separated by commas:

```bash
dtpipe -i "pg:..." --query "SELECT region, branch, rep_name FROM sales" \
       --fake "rep_name:name.fullName" --fake-seed-column "region,branch" \
       -o anonymized_sales.parquet
```

### Row-index based seeding

Use `--fake-seed-row` to generate deterministic fake values based purely on the row index (row N always gets the same values):

```bash
dtpipe -i "pg:..." --query "SELECT name, email FROM users" \
       --fake "name:name.fullName" --fake-seed-row \
       -o anonymized_users.parquet
```

### French locale

```bash
dtpipe ... \
  --fake "name:name.fullName" \
  --fake "address:address.fullAddress" \
  --fake-locale fr
```

### Masking (partial replacement)

```bash
# "555-0199" → "555-****"
# "#" keeps the original char; any other char replaces it
dtpipe ... --mask "phone:###-****"

# IBAN: keep country code + bank code, mask the rest
dtpipe ... --mask "iban:####-####-****-****-****"
```

### Export as a reusable YAML job

```bash
# Generate once
dtpipe -i "pg:..." --query "SELECT * FROM users" \
  --fake "email:internet.email" \
  --fake "name:name.fullName" \
  --null "ssn" \
  -o anonymized.parquet \
  --export-job anonymize_users.yaml

# Run nightly
dtpipe --job anonymize_users.yaml
```

> **Round-trip invariant.** `--export-job` preserves the full pipeline semantics —
> transformers, provider options, and stream processors (`--sql` / `--merge`) — so
> running the exported YAML is equivalent to the original CLI invocation.
> This invariant is enforced end-to-end by `tests/scripts/validate_export_job.sh`
> (CLI run vs `--job` run must report identical row counts).

DtPipe uses [Bogus](https://github.com/bchavez/Bogus) for fake data generation. Syntax: `--fake "Column:Dataset.Method"` (e.g. `name.fullName`, `internet.email`, `finance.iban`, `date.past`, `random.uuid`). See the [Bogus documentation](https://github.com/bchavez/Bogus) for the full dataset/method reference.

### Scenario: build a realistic dataset from nothing

`--fake` creates a column the incoming rows do not have, so a source that carries no
columns at all is enough to produce a fully populated table. `generate:N` is that source:
it emits N rows of a single `GenerateIndex` column, and every other column comes from the
mapping.

```yaml
customers:
  input: "generate:2000"
  output: "sqlite:Data Source=shop.db"
  provider-options:
    sqlite-writer:
      table: "customers"
      strategy: "Recreate"
  transformers:
    - type: fake
      mappings:
        first_name: name.firstName
        last_name: name.lastName
        email: internet.email
        city: address.city
        signed_up: date.past
      options:
        seed: 42
```

`generate:2000` supplies nothing but a `GenerateIndex` column; every other column in the
resulting table exists because a mapping named it.

One branch per table fills a whole database in a single job: repeat the block under another
alias, with its own row count and its own writer table. `seed` makes the set reproducible,
and `compute` gives a column whose value derives from another rather than being drawn at
random.

---

## Schema Transformations

### Rename, project, drop

```bash
# Rename a column, keep only specific ones
dtpipe -i source.parquet \
  --rename "user_id:UserId" \
  --rename "created_at:CreatedAt" \
  --project "UserId,Name,Email,CreatedAt" \
  -o clean.parquet

# Remove a column (blacklist approach)
dtpipe -i source.csv \
  --drop "internal_hash" \
  --drop "legacy_field" \
  -o output.csv
```

### Compute — derived columns

```bash
# Simple expression (implicit return)
dtpipe ... --compute "IsAdult:row.age > 18"

# Multiple column derivations
dtpipe ... \
  --compute "FullName:row.first_name + ' ' + row.last_name" \
  --compute "Revenue:row.qty * row.unit_price"

# Conditional logic (explicit return needed with statements)
dtpipe ... --compute "Category:if (row.age < 18) return 'Minor'; if (row.age < 65) return 'Adult'; return 'Senior';"
```

> If the column doesn't exist, `--compute` creates it as a new virtual column.
> Use `--compute-types "Col:type"` to control its CLR type (default: `string`).

### Filter rows

```bash
dtpipe ... --filter "row.is_active && row.score >= 50"
```

### Row expansion

```bash
# If 'tags' is "a,b,c", this produces 3 output rows
dtpipe ... --expand "row.tags.split(',').map(t => ({ ...row, tag: t.trim() }))"
```

### Stateful windowing

```bash
# Compute a rolling average over 5 rows
dtpipe ... \
  --window-count 5 \
  --window-script "rows.map(r => ({ ...r, rolling_avg: rows.reduce((s, x) => s + x.val, 0) / rows.length }))"
```

### Chaining transformers

Transformers execute left-to-right. The output of each step is the input to the next.

```bash
# Anonymize first, then format using the anonymized values
dtpipe ... \
  --fake "first_name:name.firstName" \
  --fake "last_name:name.lastName" \
  --format "display_name:{first_name} {last_name}" \
  --project "id,display_name,email"
```

### Load scripts from files

For complex or multi-line logic, put the script in a `.js` file:

```bash
dtpipe ... --compute "category:@scripts/categorize.js"
```

---

## Database Import & Migration

### Write strategies

| Strategy | Behaviour | Typical use |
|:---|:---|:---|
| **Append** (default) | Insert rows into the existing table | Daily increments, log shipping |
| **Truncate** | `TRUNCATE` + insert | Full refresh, preserves schema & indexes |
| **DeleteThenInsert** | `DELETE` + insert | When TRUNCATE is unavailable |
| **Recreate** | Drop + create + insert | Full refresh including schema changes |
| **Upsert** | Update existing rows (by PK), insert new ones | Syncing from a source of truth |
| **Ignore** | Insert only missing rows (by PK) | Loading only new data |

### Examples

```bash
# Parquet → PostgreSQL (recreate table)
dtpipe \
  -i data.parquet \
  -o "pg:Host=localhost;Database=prod" \
  --table "public.imported_data" \
  --strategy Recreate

# CSV upsert with explicit key
dtpipe \
  -i orders_update.csv \
  -o "mssql:Server=.;Database=mydb" \
  --table "Orders" \
  --strategy Upsert \
  --key "OrderId"

# High-speed bulk insert (PG / Oracle / MSSQL / MySQL)
dtpipe \
  -i large_export.parquet \
  -o "pg:Host=localhost;Database=prod" \
  --table "staging" \
  --strategy Truncate \
  --insert-mode Bulk

# Auto-migrate: add missing columns without dropping the table
dtpipe \
  -i new_data.parquet \
  -o "pg:Host=localhost;Database=prod" \
  --table "users" \
  --auto-migrate
```

### MySQL upsert

The `mysql:` prefix is required — a MySQL connection string is indistinguishable from a SQL
Server one by content alone.

```bash
# PostgreSQL → MySQL, syncing on the primary key
dtpipe \
  -i "pg:Host=localhost;Database=prod;Username=postgres;Password=pass" \
  --query "SELECT * FROM orders" \
  -o "mysql:Server=localhost;Port=3306;Database=analytics;User ID=root;Password=pass" \
  --table "orders" \
  --strategy Upsert \
  --key "order_id"

# --key may be omitted: the target's PRIMARY KEY is read from information_schema
dtpipe \
  -i orders.parquet \
  -o "mysql:Server=localhost;Database=analytics;User ID=root;Password=pass" \
  --table "orders" --strategy Upsert
```

Two MySQL-specific behaviours are worth knowing before relying on this in production:

* **Upsert needs a unique index.** `ON DUPLICATE KEY UPDATE` fires on the table's own PRIMARY KEY
  or UNIQUE indexes; MySQL offers no way to name a conflict target. Without an index covering the
  key columns the clause would degrade to a plain `INSERT` and pile up duplicates, so dtpipe
  checks first and falls back to a slower `DELETE`+`INSERT` with a warning. Add the index to get
  the fast path.
* **Bulk loading needs the server's consent.** `--insert-mode Bulk` uses `MySqlBulkCopy`, i.e.
  `LOAD DATA LOCAL INFILE`, which requires `local_infile=ON` — **off by default since MySQL 8**.
  dtpipe probes it and falls back to batched `INSERT` with a warning rather than failing
  mid-transfer. Enable it with `SET GLOBAL local_infile = 1;` (or in `my.cnf` to survive a
  restart).

See [REFERENCE.md#mysql](./REFERENCE.md#mysql) for the full type mapping.

### Pre/post execution hooks

```bash
# Run SQL before and after the pipeline
dtpipe -i data.parquet \
  -o "sqlite:app.db" --table "users" \
  --pre-exec "DELETE FROM users WHERE is_temp = 1" \
  --post-exec "UPDATE users SET synced_at = CURRENT_TIMESTAMP"

# Load SQL from a file
dtpipe ... --pre-exec "@scripts/pre_migration.sql"
```

---

## SQL Processors and Joins

### In-memory SQL join (DuckDB)

DuckDB is the default SQL engine. The `--from` source streams; `--ref` sources are preloaded
into memory before query execution (required for cost-based join planning).

```bash
dtpipe \
  -i "pg:..." --query "SELECT * FROM orders" --alias orders \
  -i "metadata.csv" --alias meta \
  --from orders --ref meta \
  --sql "SELECT o.*, m.category FROM orders o JOIN meta m ON o.product_id = m.id" \
  -o "enriched.parquet"
```

### Multi-ref JOIN

```bash
dtpipe \
  -i events.parquet --alias ev \
  -i users.csv --alias users \
  -i products.csv --alias products \
  --from ev --ref users,products \
  --sql "
    SELECT e.ts, u.name, p.title
    FROM ev e
    JOIN users u ON e.user_id = u.id
    JOIN products p ON e.product_id = p.id
  " \
  -o enriched_events.parquet
```

### DuckDB SQL features

DuckDB supports standard SQL plus window functions, CTEs, JSON accessors, and more.

```bash
# Window function
dtpipe -i sales.parquet --alias s \
  --from s \
  --sql "SELECT *, SUM(amount) OVER (PARTITION BY region ORDER BY date) AS running_total FROM s" \
  -o enriched.parquet

# JSON field access (from JSONL source)
dtpipe -i data.jsonl --alias m \
  --from m \
  --sql "SELECT m.user.id, m.meta.details.code FROM m"

# Identifiers that are SQL keywords must be quoted
dtpipe -i orders.parquet --alias o \
  --from o \
  --sql 'SELECT "order".id, "order".amount FROM o AS "order"'
```

> **Tip:** DuckDB queries can be developed and tested externally with the DuckDB CLI before use in DtPipe.

### UNION ALL (merge processor)

```bash
dtpipe \
  -i archive_2023.parquet --alias a \
  -i archive_2024.parquet --alias b \
  --from a,b --merge \
  -o combined.parquet
```

### YAML job (merge)

```yaml
a:
  input: "archive_2023.parquet"
b:
  input: "archive_2024.parquet"
combined:
  from: "a,b"
  provider-options:
    merge: {}
  output: "combined.parquet"
```

---

## DuckDB Extensions and Cloud Storage

DtPipe's native provider list is intentionally focused. Rather than shipping adapters
for every cloud store or SaaS format, DtPipe delegates to DuckDB's extension ecosystem —
making DuckDB an on-demand connector for sources and destinations it can't reach natively.
Load an extension with `--duck-init` on any DuckDB branch (reader, writer, or `--sql` processor)
to read remote files directly in a query, write to a DuckDB-supported target, or join local
data with remote sources. The examples below cover S3 and env-var patterns (Azure, inline and
DuckDB-file variants use the same mechanism).
`--duck-init` value forms: `keyring://alias`, `${{keyring://alias}}`, `${{ENV_VAR}}`,
`@/path/file.sql` — composable, full syntax in [REFERENCE.md#value-resolution](./REFERENCE.md#value-resolution) and [REFERENCE.md#provider-specific-options](./REFERENCE.md#provider-specific-options).

### Object storage: S3 and Azure Blob

Object-storage locations are ordinary inputs and outputs, handled by the DuckDB engine in-process.
Reads stream straight from the object — nothing is downloaded to a temp file first.

```bash
# Read a Parquet object from S3, write a local CSV
dtpipe -i "s3://analytics/events/2026-08-26.parquet" --s3-region eu-west-1 -o events.csv

# Write to a MinIO bucket (an explicit http:// endpoint selects path-style, no TLS)
dtpipe -i sales.csv -o "s3://warehouse/sales/2026-08.parquet" \
  --s3-endpoint "http://127.0.0.1:9000" \
  --s3-access-key "${{keyring://minio-key}}" \
  --s3-secret-key "${{keyring://minio-secret}}"

# Azure Blob round-trip
dtpipe -i "azure://reports/daily.parquet" --azure-connection-string "${{keyring://azure-conn}}" -o daily.csv
dtpipe -i daily.csv -o "azure://reports/daily-copy.parquet" --azure-connection-string "${{keyring://azure-conn}}"

# Glob across many objects — partitioned layouts read as one stream
dtpipe -i "s3://analytics/events/dt=*/part-*.parquet" --s3-region eu-west-1 -o all_events.parquet

# Object storage to object storage, with different credentials on each side
dtpipe -i "s3://source-bucket/in.parquet" --s3-access-key "${{keyring://src-key}}" --s3-secret-key "${{keyring://src-secret}}" \
       -o "s3://target-bucket/out.parquet" --s3-access-key "${{keyring://dst-key}}" --s3-secret-key "${{keyring://dst-secret}}"
```

With no `--s3-access-key` / `--s3-secret-key`, the ambient credential chain is used (`AWS_*`
environment variables, shared config, instance profile) — the usual CI and EC2 setup needs no flags.

In a YAML job:

```yaml
main:
  input: "s3://analytics/events/2026-08-*.parquet"
  provider-options:
    s3:
      s3-region: "eu-west-1"
      s3-secret-key: "${{keyring://aws-secret}}"
  output: "events.parquet"
```

Notes:
- Format comes from the extension (`.parquet`, `.csv`, `.tsv`, `.json`, `.jsonl`, `.ndjson`).
  Anything else is refused with the supported list — use `--duck-init` + `--query` for other formats.
- A write replaces the target key and is issued only once the pipeline completes, so a failed run
  leaves the existing object intact. `--strategy` does not apply to objects.
- The `httpfs` / `azure` DuckDB extensions are installed on first use, so the host needs access to
  DuckDB's extension repository once.

### DuckDB Hub connections (`duck+{provider}:`) — retired

The `duck+{provider}:` prefix meant `ATTACH`: integrating another database as a SQL catalog inside
the in-process DuckDB. **It now accepts nothing.** The prefix is kept only so that typing one gives
an actionable error instead of an obscure DuckDB parse failure.

The rule was always *no hub route where a native provider exists* — `ATTACH` reaches a catalog, but
not `COPY`, bulk load, or upsert, so the native route is strictly more capable. PostgreSQL and
SQLite were excluded on that basis from the start. `duck+mysql:` was the last one left, and only
because MySQL had no native provider; [`mysql:`](./REFERENCE.md#mysql) closed that gap.

```bash
# Was: -i "duck+mysql:Host=localhost;Database=mydb;User=root;"
# Now: the native provider, which also brings bulk load and upsert
dtpipe \
  -i "mysql:Server=localhost;Database=mydb;User ID=root;Password=pass" \
  --query "SELECT * FROM users" \
  -o users.parquet
```

Object storage (`s3://`, `azure://`, `https://`…) was never a hub target either — it holds files,
not catalogs. Reach those locations through the DuckDB engine with `--duck-init` (see
[DuckDB Extensions and Cloud Storage](#duckdb-extensions-and-cloud-storage) above), or through the
`s3://` / `azure://` providers. Other DuckDB extensions (`excel`, `ducklake`, …) are reached the
same way, and none of that is affected by the hub prefix's retirement.

### Recommended: credentials in the OS keyring

```bash
# Store once — never appears in shell history again
dtpipe secret set s3-init "INSTALL httpfs; LOAD httpfs; SET s3_region='eu-west-1'; SET s3_access_key_id='AKIA...'; SET s3_secret_access_key='...';"

# Use by alias
dtpipe \
  -i events.parquet --alias ev \
  --from ev \
  --duck-init "keyring://s3-init" \
  --sql "SELECT * FROM ev JOIN read_parquet('s3://bucket/meta.parquet') m ON ev.id = m.id" \
  -o result.parquet
```

### Credentials from environment variables (CI/CD)

```bash
# Typically set by your CI/CD system (GitHub Actions, GitLab CI, etc.)
export AWS_REGION="eu-west-1"
export AWS_ACCESS_KEY_ID="AKIA..."
export AWS_SECRET_ACCESS_KEY="..."

dtpipe \
  -i data.parquet --alias src \
  --from src \
  --duck-init 'INSTALL httpfs; LOAD httpfs; SET s3_region="${{AWS_REGION}}"; SET s3_access_key_id="${{AWS_ACCESS_KEY_ID}}"; SET s3_secret_access_key="${{AWS_SECRET_ACCESS_KEY}}";' \
  --sql "SELECT * FROM src" \
  -o result.parquet
```

> **Other `duck-init` patterns** — same mechanism, different secret shape. See [REFERENCE.md#provider-specific-options](./REFERENCE.md#provider-specific-options) and [REFERENCE.md#value-resolution](./REFERENCE.md#value-resolution) for the full `keyring://` / `${{keyring://…}}` / `${{ENV}}` / `@file` composable syntax.
> - Database credentials (main use — inline in the connection string, not `duck-init`): `dtpipe -i "pg:Host=prod;Database=app;Username=${{keyring://pg-user}};Password=${{keyring://pg-pass}}" --query "SELECT * FROM orders" -o out.parquet`
> - Azure: `INSTALL azure; LOAD azure; SET azure_storage_connection_string='…'` (same `keyring://azure-init` pattern)
> - DuckDB file I/O: `--duck-init "LOAD spatial"` on a `duck:` reader/writer, or pre-load cloud creds on a `duck:` writer (`-o duck:output.duckdb --duck-init "keyring://azure-init"`).

### YAML job with duck-init

```yaml
ev:
  input: "events.parquet"
enrich:
  from: "ev"
  provider-options:
    sql:                          # applies to the --sql stream processor
      query: "SELECT * FROM ev JOIN read_parquet('s3://bucket/ref.parquet') r ON ev.id = r.id"
      duck-init: "keyring://s3-init"
  output: "result.parquet"
```

> `--duck-init` is scoped to a single DuckDB connection. In a DAG with both a DuckDB reader/writer and a `--sql` branch, each uses its own connection — specify `--duck-init` on each branch that needs it.

---

## DAG Pipelines (Multi-Source)

### Fan-out (tee): write one source to multiple destinations

```bash
dtpipe \
  -i "pg:..." --query "SELECT * FROM events" --alias src \
  --from src -o archive.parquet \
  --from src --fake "user_id:random.uuid" -o anonymized.parquet
```

### Diamond: split → transform → rejoin

```bash
dtpipe \
  -i transactions.parquet --alias all \
  --from all --filter "row.amount > 1000" --alias high \
  --from all --filter "row.amount <= 1000" --alias low \
  --from high --ref low \
  --sql "SELECT h.*, l.count AS low_count FROM high h JOIN low l ON h.category = l.category" \
  -o enriched.parquet
```

### SQL output fed to multiple consumers

```bash
dtpipe \
  -i "pg:..." --query "SELECT * FROM orders" --alias raw \
  -i "pg:..." --query "SELECT * FROM customers" --alias cust \
  --from raw --ref cust \
  --sql "SELECT o.*, c.segment FROM raw o JOIN cust c ON o.cid = c.id" --alias joined \
  --from joined -o joined.parquet \
  --from joined --fake "email:internet.email" -o anonymized.parquet
```

---

## Standard Streams and Automation

### Standard input/output

```bash
# Read from stdin, write to stdout
cat data.csv | dtpipe -i csv --fake "name:name.fullName" -o parquet | gzip > out.parquet.gz

# Compose with other tools
duckdb -csv -c "SELECT * FROM 'source.csv' WHERE active = true" | \
  dtpipe -i csv --fake "name:name.fullName" -o parquet:clean.parquet
```

### Large XML files (streaming)

```bash
# Auto-discover schema, then export
cat catalog.xml | \
  dtpipe -i xml \
  --path "//Product" \
  --auto-column-types \
  -o "pg:Host=localhost;Database=prod" \
  --table "products" --strategy Upsert
```

XML and JSONL sources preserve nested objects as Arrow `StructType` columns. To flatten for SQL
or CSV, apply a `--sql` step:

```bash
dtpipe -i data.xml --path "//User" --alias u \
  --from u \
  --sql "SELECT u.id, u.profile.email AS email FROM u" \
  -o flat_users.csv
```

### Production YAML automation

For repeated tasks, define your pipeline in a YAML job file.

```bash
# 1. Generate from CLI
dtpipe -i "pg:..." --query "SELECT * FROM users" \
  --fake "email:internet.email" -o clean_users.parquet \
  --export-job nightly.yaml

# 2. Run (with optional runtime overrides)
dtpipe --job nightly.yaml
dtpipe --job nightly.yaml --limit 1000 --dry-run
```

#### Multi-branch YAML (DAG)

```yaml
# nightly_pipeline.yaml
users:
  input: "pg:Host=prod;Database=app;Username=postgres"
  output: "clean_users.parquet"
  provider-options:
    pg:
      query: "SELECT * FROM users"
  transformers:
    - fake:
        mappings:
          email: internet.email
          name: name.fullName
        options:
          locale: fr
          seed-column: id

orders:
  input: "pg:Host=prod;Database=app;Username=postgres"
  output: "orders.parquet"
  provider-options:
    pg:
      query: "SELECT * FROM orders"
```

```bash
dtpipe --job nightly_pipeline.yaml
```

#### Provider-specific options in YAML

```yaml
main:
  input: input_data.csv
  output: export_data.csv
  provider-options:
    csv:           # applied to the reader
      separator: ","
      has-header: true
    csv-writer:    # applied to the writer
      separator: ";"
      quote: "'"
```

### Security & secrets

```bash
# Store once
dtpipe secret set oracle-prod "ora:Data Source=PROD;User Id=scott;Password=tiger"

# Use by alias — password never in shell history or ps output
dtpipe -i keyring://oracle-prod --query "SELECT * FROM users" -o users.parquet
```

### Sampling for testing or CI

```bash
# 10% random sample
dtpipe -i "pg:..." --query "SELECT * FROM large_table" \
       --sampling-rate 0.1 -o sample.parquet

# Reproducible sample (same subset every run)
dtpipe -i "pg:..." --query "SELECT * FROM large_table" \
       --sampling-rate 0.1 --sampling-seed 12345 -o sample.parquet
```

---

## Incremental Loading

Incremental loading enables transfer of only changed/new rows. DtPipe handles this dynamically using cursor tracking and query interpolation. **Canonical flag table, state file format and `${{cursor://…}}` resolution rules: [REFERENCE.md#incremental-loading](./REFERENCE.md#incremental-loading) and [REFERENCE.md#value-resolution](./REFERENCE.md#value-resolution).**

### Scenario: incremental sync of a postgres table to sqlite

In this recipe, we sync user records from PostgreSQL into SQLite, keeping track of the last processed `updated_at` timestamp.

#### First execution (Full Load)
On the very first run, no state file exists yet. We provide a default timestamp value (e.g. `'1970-01-01'`) using the fallback syntax:

```bash
dtpipe \
  -i "pg:Host=localhost;Database=prod" \
  --query "SELECT * FROM users WHERE updated_at >= '${{cursor://state/users_sync.json|1970-01-01}}'" \
  -o "sqlite:Data Source=dw.db" \
  --table "users" \
  --strategy Recreate \
  --key id \
  --cursor "updated_at" \
  --state "state/users_sync.json"
```

After this runs successfully, DtPipe automatically generates the state file `state/users_sync.json` containing the maximum `updated_at` value processed.

#### Subsequent executions (Incremental Sync)
Subsequent runs will load the cursor from the state file and substitute it into the query. We change the query condition to `>` and the strategy to `Upsert` (to merge updates):

```bash
dtpipe \
  -i "pg:Host=localhost;Database=prod" \
  --query "SELECT * FROM users WHERE updated_at > '${{cursor://state/users_sync.json}}'" \
  -o "sqlite:Data Source=dw.db" \
  --table "users" \
  --strategy Upsert \
  --key id \
  --cursor "updated_at" \
  --state "state/users_sync.json"
```

### Scenario: YAML job file for incremental loading

You can configure incremental loading directly in a YAML job file. Here is a configuration that does an incremental sync of an orders table:

```yaml
main:
  input: "pg:Host=localhost;Database=prod"
  output: "sqlite:Data Source=dw.db"
  cursor: "updated_at"
  state: "state/orders_sync.json"
  provider-options:
    pg:
      query: "SELECT * FROM orders WHERE updated_at > '${{cursor://state/orders_sync.json|2026-01-01}}'"
    sqlite-writer:
      table: "orders"
      strategy: "Upsert"
      key: "id"
```

To run this job:
```bash
dtpipe --job sync_orders.yaml
```

---

## AI Agent Integration

### Scenario: Interactive Pipeline Generation using `dtpipe agent`

Instead of writing YAML jobs or long CLI commands by hand, you can describe your data integration goal in natural language using the `dtpipe agent` subcommand:

```bash
# Launch interactive mode (auto-discovers local Ollama models)
dtpipe agent
```

When prompted, enter your data integration mission:
> *"Inspect csv:invoices.csv, compute gross_total = subtotal * (1 + tax), filter gross_total > 100, and save to jsonl:high_invoices.jsonl"*

The agent will:
1. Inspect the source schema.
 2. Validate the YAML topology.
 3. Execute the pipeline.
 4. Render the DAG topology box and allow exporting a standalone `high_invoices.yaml` file for production automation.

### Guardrails: keep the agent safe and deterministic

By default (no flags) the agent is **fail-closed** — it is deterministic, only *plans*, and writes
nothing. Every unlock is explicit:

```bash
# Safest: plan only, deterministic, dry-run. The execute-yaml-job tool is never offered to the LLM.
dtpipe agent "inspect csv:sales.csv and summarize totals"

# Replicate a plan to prove determinism (variance must be 0 for a stable mission)
dtpipe agent "..." --temperature 0 --seed 42 --repeat 3

# Allow a real, guarded write (destructive SQL still denied unless allowed)
dtpipe agent "load csv:orders.csv into pg" --mode execute --apply

# Permitted only when you know the SQL is destructive/reaches the network
dtpipe agent "..." --apply --allow-destructive --allow-network
```

The guardrails (`ISqlSafetyPolicy` / `IApprovalGate`):
- **Dry-run by default** — `execute-yaml-job` writes nothing unless `--apply`.
- **Destructive verbs** (`DROP`/`DELETE`/`TRUNCATE`/`UPDATE`/`ALTER`/`INSERT`/`ATTACH`) and
  **network access** (`LOAD httpfs`/`azure`, remote `read_parquet`/`read_csv`) are denied unless
  `--allow-destructive` / `--allow-network` are set.
- **Planner mode** hides `execute-yaml-job` from the model; execution is a deterministic engine step.
- **Non-destructive context** — inspected schemas/samples/errors survive compaction.
- **Parallel tools** — every `ToolCall` is executed (independent ones in parallel; `--sequential` forces one at a time).

See `REFERENCE.md` → *Agent Guardrails* for the full policy.




---

## Iterating on a transformer without re-reading the source

The slow part of tuning a pipeline is usually the read. Materialise once, then iterate against
the checkpoint.

```bash
# 1. Read Oracle once, mask, and keep the result
dtpipe -i oracle:"User Id=app;Password=…;Data Source=//db:1521/ORCL" \
       --query "SELECT * FROM customers" \
       --mask email --checkpoint -o null:
# → dtpipe names the checkpoint by content and prints the store location once

# 2. See what you have
dtpipe session list
dtpipe session show default

# 3. Iterate — no Oracle round-trip
dtpipe --from-checkpoint <key> --compute "domain=email.split('@')[1]" -o csv:out.csv
dtpipe --from-checkpoint <key> --compute "domain=email.split('@').pop()" -o csv:out.csv
```

The key is a hash of the *definition* — connection, query, transformers, sampling — so re-running
step 1 unchanged reuses the same checkpoint instead of re-reading. Change the query or a
transformer and you get a different key, because it is a different prefix.

Artefacts are encrypted and expire (7 days by default). To get rows out deliberately, use a real
destination: `dtpipe --from-checkpoint <key> -o parquet:extract.parquet`.

## Seeing what a pipeline will actually do

`--dry-run N` runs the pipeline over N source rows with the writer neutralised. Because it is the
real execution path, a step that changes the row count shows it:

```bash
dtpipe -i csv:orders.csv \
       --window-key customer_id \
       --window-script "return [{customer_id: rows[0].customer_id, total: rows.reduce((a,r)=>a+Number(r.amount),0)}]" \
       -o pg:"Host=db;Database=app" --table order_totals --dry-run 10
```

The trace marks the window step `10 → 3 rows` and shows the aggregated values — the same rows a
real run would write. The target is inspected for schema compatibility but never created or
migrated, and no hook runs.

Against PostgreSQL, Oracle, MySQL or SQLite the session is also set read-only, so the server
itself refuses a write. Against SQL Server there is no such mechanism, and the run says so rather
than implying a guarantee it does not have.

## Giving an agent a fast, safe loop

```bash
export DTPIPE_SESSION=mission-7      # one mission, one session
dtpipe agent --mode plan
```

In plan mode the model can call `dry-run` freely: every call executes the real pipeline over a
few rows, writes nothing to any target, and returns the rows leaving each stage. `list-checkpoints`
and `read-checkpoint` let it look at what it materialised. When the mission ends:

```bash
dtpipe session purge mission-7
```
