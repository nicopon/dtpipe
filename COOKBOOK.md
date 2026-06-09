# DtPipe Cookbook

Recipes and end-to-end scenarios. For the full option reference, see [REFERENCE.md](./REFERENCE.md).

**Table of Contents**
- [Basic Transfers](#basic-transfers)
- [Anonymization Before Export](#anonymization-before-export)
- [Schema Transformations](#schema-transformations)
- [Database Import & Migration](#database-import--migration)
- [SQL Processors and Joins](#sql-processors-and-joins)
- [DuckDB Extensions and Cloud Storage](#duckdb-extensions-and-cloud-storage)
- [DAG Pipelines (Multi-Source)](#dag-pipelines-multi-source)
- [Standard Streams and Automation](#standard-streams-and-automation)

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

Use `--fake-seed-column` to guarantee that the same input value always produces the same
anonymized output — even across separate runs or tables.

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

DtPipe uses [Bogus](https://github.com/bchavez/Bogus) for fake data generation. Syntax: `--fake "Column:Dataset.Method"` (e.g. `name.fullName`, `internet.email`, `finance.iban`, `date.past`, `random.uuid`). See the [Bogus documentation](https://github.com/bchavez/Bogus) for the full dataset/method reference.

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

# High-speed bulk insert (PG / Oracle / MSSQL)
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
  --from ev --ref users --ref products \
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

---

## DuckDB Extensions and Cloud Storage

DtPipe's native provider list is intentionally focused. Rather than shipping adapters
for every cloud store or SaaS format, DtPipe delegates to DuckDB's extension ecosystem —
making DuckDB an on-demand connector for sources and destinations it can't reach natively.
Load an extension with `--duck-init` on any DuckDB branch (reader, writer, or `--sql` processor)
to read remote files directly in a query, write to a DuckDB-supported target, or join local
data with remote sources. The examples below cover S3, Azure Blob, and local DuckDB files.
`--duck-init` value forms: `keyring://alias`, `${{keyring://alias}}`, `${{ENV_VAR}}`,
`@/path/file.sql` — composable, full syntax in [REFERENCE.md](./REFERENCE.md#duckdb-options).

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

### Inline keyring secrets (mix multiple credentials)

```bash
# Store individual values
dtpipe secret set s3-region "eu-west-1"
dtpipe secret set s3-key "AKIAIOSFODNN7EXAMPLE"
dtpipe secret set s3-secret "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"

# Reference them inline
dtpipe \
  -i orders.parquet --alias local \
  --from local \
  --duck-init "INSTALL httpfs; LOAD httpfs; SET s3_region='${{keyring://s3-region}}'; SET s3_access_key_id='${{keyring://s3-key}}'; SET s3_secret_access_key='${{keyring://s3-secret}}';" \
  --sql "SELECT l.*, r.category FROM local l JOIN read_parquet('s3://bucket/reference.parquet') r ON l.product_id = r.id" \
  -o enriched.parquet
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

### Azure Blob Storage

```bash
dtpipe secret set azure-init "INSTALL azure; LOAD azure; SET azure_storage_connection_string='DefaultEndpointsProtocol=https;...';"

dtpipe \
  -i data.parquet --alias src \
  --from src \
  --duck-init "keyring://azure-init" \
  --sql "SELECT * FROM src JOIN read_parquet('azure://container/ref.parquet') r ON src.id = r.id" \
  -o result.parquet
```

### Write a DuckDB file with cloud credentials pre-loaded

```bash
dtpipe \
  -i "pg:Host=prod;Database=app" --query "SELECT * FROM orders" \
  -o "duck:output.duckdb" --table orders \
  --duck-init "keyring://azure-init"
```

### Read from a DuckDB file with an extension

```bash
dtpipe \
  -i "duck:warehouse.duckdb" --query "SELECT * FROM spatial_data" \
  --duck-init "LOAD spatial" \
  -o result.parquet
```

### YAML job with duck-init

```yaml
enrich:
  input: "events.parquet"
  from: "ev"
  sql: "SELECT * FROM ev JOIN read_parquet('s3://bucket/ref.parquet') r ON ev.id = r.id"
  output: "result.parquet"
  provider-options:
    duck:                         # applies to --sql processor
      duck-init: "keyring://s3-init"
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
