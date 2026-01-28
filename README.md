# QueryDump

[![Open in GitHub Codespaces](https://github.com/codespaces/badge.svg)](https://github.com/codespaces/new?repo=nicopon/QueryDump)

CLI tool to export database data to Parquet, CSV, or another database. Supports anonymization, light data transformation, and YAML job files. Designed for low memory footprint via streaming.

## Quick Start

```bash
./build.sh
./dist/release/querydump --input "duckdb:source.db" --query "SELECT * FROM users" --output users.parquet
```

> 💡 Yes, DuckDB can do this on its own. This is just to illustrate the basic syntax—keep reading for the *actually useful* stuff. 😉

## Features

- **Multi-Database**: Oracle, SQL Server, PostgreSQL, DuckDB, SQLite, CSV, Parquet
- **Zero Dependencies**: Self-contained binary. Uses embedded engines (DuckDB, SQLite) and fully managed drivers (Oracle, SQL Server, PostgreSQL)—no external client installation required.
- **Streaming**: `IDataReader`-based processing for large datasets with minimal memory
- **Anonymization**: Replace sensitive data with realistic fake values thanks to [Bogus](https://github.com/bchavez/Bogus)
- **Transformations**: Null, Overwrite, Format templates with .NET format specifiers, Mask patterns
- **YAML Job Files**: Define reusable export configurations
- **Output Formats**: Parquet (Snappy), CSV, or direct database insert

---

## Installation

```bash
./build.sh
```
Executable: `./dist/release/querydump` (standalone, no runtime dependencies)

---

## Input & Output

### Input Sources (`--input`)

| Prefix | Provider | Example |
|--------|----------|---------|
| `duckdb:` | DuckDB | `duckdb:mydata.db` |
| `sqlite:` | SQLite | `sqlite:local.sqlite` |
| `postgresql:` | PostgreSQL | `postgresql:Host=...;Database=...` |
| `oracle:` | Oracle | `oracle:Data Source=...;User Id=...` |
| `mssql:` | SQL Server | `mssql:Server=...;Database=...` |
| `csv:` | CSV File | `csv:data.csv` or `data.csv` |
| `parquet:` | Parquet File | `parquet:data.parquet` or `data.parquet` |

Provider is auto-detected from prefix or file extension where possible.

### Output Destinations (`--output`)

**File** (extension determines format):
- `.parquet` → Parquet with Snappy compression
- `.csv` → RFC 4180 CSV

**Database** (prefixed connection string):
```bash
--output "duckdb:target.db"
--output "sqlite:target.sqlite"
--output "oracle:User/Pass@TargetDB"
--output "mssql:Server=.;Database=TargetDB;Trusted_Connection=True;"
--output "postgresql:Host=localhost;Database=TargetDB;Username=postgres;"
```

---

## Job Lifecycle (YAML & CLI)

QueryDump is designed for an iterative workflow: **Experiment** in CLI, **Export** to YAML, and **Automate** in production.

### 1. The Iterative Workflow
```bash
# A. Experiment interactively (Dry Run + Sampling)
./querydump --input "oracle:..." --query "SELECT * FROM Users" --output users.csv --sample-rate 0.1 --dry-run

# B. Export stable configuration to YAML
./querydump --input "oracle:..." --query "SELECT * FROM Users" --output users.parquet --fake "NAME:name.fullName" --export-job job.yaml

# C. Run in production using the job file
./querydump --job job.yaml
```

### 2. Job File Example
```yaml
input: oracle:Data Source=PROD_DB;User Id=scott;Password=tiger;
query: SELECT id, name, email, phone FROM subscribers
output: subscribers_anonymized.parquet

# Data Processing Pipeline (Transformers)
transformers:
  - null:
      mappings:
        phone: ~
  - fake: 
      mappings:
        name: name.fullName
        email: internet.email
      options:
        locale: fr
        seed-column: id

# Provider-specific Settings
provider-options:
  parquet:
    compression: snappy
```

### 3. CLI Overrides
You can override job file settings at runtime:
```bash
# Run job but limit output for a quick check
./querydump --job job.yaml --limit 100 --dry-run
```

---

## Data Processing Pipeline

QueryDump processes data in a streaming pipeline. Transformations are applied in **CLI argument order**. 

> 💡 **Grouping Rule**: Consecutive arguments of the same type (e.g., three `--fake`) form a **single pipeline step** to optimize performance.

### 1. Transformer Types
| Feature | Flag | Description |
|---------|------|-------------|
| **Nullify**| `--null` | Sets specific columns to `null` |
| **Mask** | `--mask` | Replaces characters using pattern (e.g., `###-**`) |
| **Fake** | `--fake` | Inserts realistic fake data (Bogus) |
| **Format** | `--format` | .NET string templates (e.g. `{NAME}: {DATE:d}`) |
| **Script** | `--script` | Custom Javascript logic (Jint) |
| **Static** | `--overwrite`| Forces a fixed value |
| **Filter** | `--project` / `--drop` | Whitelist or blacklist columns |

### 2. Common Configurations
#### Basics: Null, Overwrite, Format
```bash
--null "INTERNAL_ID" \
--overwrite "STATUS:anonymized" \
--format "DISPLAY_NAME:{FIRSTNAME} {LASTNAME}"
```

#### Advanced: Masking & Scripting
```bash
--mask "EMAIL:###****" \
--script "FULL_NAME:return row.FIRSTNAME + ' ' + row.LASTNAME;"
```

#### Anonymization (Fake Data)
Use [Bogus API](https://github.com/bchavez/Bogus) selectors for realistic data.
```bash
--fake "NAME:name.fullName" --fake "EMAIL:internet.email" --fake-locale fr
```

### 3. Pipeline Control
- **Determinism**: Use `--fake-seed-column ID` for stable fake values across runs.
- **Skip Nulls**: Use `--fake-skip-null`, `--mask-skip-null`, or `--format-skip-null` to preserve existing null values.
- **Filtering**: `--project "ID,NAME,EMAIL"` (keep only these) or `--drop "TEMP_COL"` (remove this).

---

### Full CLI Reference

#### 1. Core Options
| Option | Alias | Description | Default |
|--------|-------|-------------|---------|
| `--input` | `-i` | Input connection string (prefixed) | Required* |
| `--query` | `-q` | SQL query (SELECT only) | Required* |
| `--output` | `-o` | Output file or connection (prefixed) | Required* |
| `--job` | | Path to YAML job file | - |
| `--export-job` | | Export config to YAML file and exit | - |
| `--batch-size` | `-b` | Rows per batch | `50000` |
| `--limit` | | Max rows to export | `0` (unlimited) |
| `--dry-run` | | Preview schemas & sample data | `false` |
| `--log [FILE]` | | Log file path (incl. memory stats) | - |

#### 2. Debugging & Performance
- **Enable Trace**: Set env var `DEBUG=1` to see full stack traces.
- **Memory Stats**: Use `--log` to see Managed vs WorkingSet memory usage after each batch.

#### 2. Pipeline Tools (Flags)
Detailed lists for fine-tuning your data processing.

**Transformers:**
- `--null` / `--overwrite` / `--format` / `--mask` / `--script`
- `--project` (whitelist) / `--drop` (blacklist)

**Pipeline Modifiers:**
- `--fake-locale` (en, fr, etc.)
- `--fake-seed-column [COL]` (Deterministic)
- `--fake-deterministic` (Row-index based)
- `--{transformer}-skip-null` (Preserve original nulls)

#### 3. Database Writer Options
Customize target behavior per provider.

### Oracle (`oracle`)
- `--ora-table`: Target table name (default: `EXPORT_DATA`).
- `--ora-strategy`: `Append` (default), `Truncate`, or `DeleteThenInsert`.
- `--ora-insert-mode`: Insertion method (default: `Standard`).
  - `Standard`: Uses efficient Array Binding.
  - `Append`: Same as Standard but adds `/*+APPEND*/` hint for direct-path insertion.
  - `Bulk`: Uses `OracleBulkCopy`.

### SQL Server (`mssql`)
- `--mssql-table`: Target table name (default: `EXPORT_DATA`).
- `--mssql-strategy`: `Append` (default), `Truncate`, or `DeleteThenInsert`.
- `--mssql-insert-mode`: `Standard` (default) or `Bulk`.

### PostgreSQL (`postgres`)
- `--pg-table`: Target table name (default: `Export`).
- `--pg-strategy`: `Append` (default), `Truncate`, or `DeleteThenInsert`.
- `--pg-insert-mode`: `Standard` (default) or `Bulk` (Binary Copy).

---

## Common Fakers

| Dataset | Method | Description |
|---------|--------|-------------|
| **Name** | `name.firstName`, `name.lastName`, `name.fullName` | Names |
| **Internet** | `internet.email`, `internet.userName` | Email, usernames |
| **Address** | `address.streetAddress`, `address.city`, `address.zipCode` | Addresses |
| **Phone** | `phone.phoneNumber` | Phone numbers |
| **Company** | `company.companyName` | Company names |
| **Date** | `date.past`, `date.future` | Dates |
| **Finance** | `finance.iban`, `finance.bic` | Banking |

> Use `--fake-list` to see all 100+ available generators.

---

## Testing

For information on how to run and extend the integration tests, see [Integration Tests](tests/scripts/README.md).

---

## License

MIT
