# QueryDump

CLI tool to export database data to Parquet, CSV, or another database. Supports anonymization, light data transformation, and YAML job files. Designed for low memory footprint via streaming.

## Quick Start

```bash
./build.sh
./dist/release/querydump --input "duckdb:source.db" --query "SELECT * FROM users" --output users.parquet
```

> 💡 Yes, DuckDB can do this on its own. This is just to illustrate the basic syntax—keep reading for the *actually useful* stuff. 😉

## Features

- **Multi-Database**: Oracle, SQL Server, PostgreSQL, DuckDB, SQLite, CSV, Parquet
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
```

---

## YAML Job Files

Define reusable export configurations in YAML. Use `--job` to load, `--export-job` to export current CLI config.

### Example Job File

```yaml
# Source: DuckDB database with customers table
input: duckdb:customers.db
query: SELECT id, name, email, phone FROM customers
output: customers_anon.parquet

# Transformers applied in order (same as CLI order)
transformers:
  # 1. Set phone column to NULL
  - null:
      mappings:
        phone: ~  # Value is ignored, only key matters if no column to list

  # 2. Replace name and email with fake data
  - fake:
      mappings:
        name: name.fullName
        email: internet.email
        # Note: Values are based on Bogus capabilities
      options:
        locale: fr
        seed-column: id  # Same id = same fake values
```

### Usage

```bash
# Run from job file
./querydump --job export_config.yaml

# Run for test (overrides limit/dry-run only)
./querydump --job export_config.yaml --limit 100 --dry-run

# Export current CLI config to YAML
./querydump --input "..." --query "..." --output "..." --fake "NAME:name.fullName" --export-job config.yaml
```

---

## Data Transformation

Transformers are applied in **CLI argument order**. Each `--null`, `--overwrite`, `--fake`, or `--format` creates a pipeline step.

### Transform Order Example

```bash
--null "PHONE" --fake "NAME:name.fullName" --format "DISPLAY:{NAME}"
```
Pipeline: `Null → Fake → Format`

```bash
--fake "NAME:name.fullName" --null "PHONE" --format "DISPLAY:{NAME}"
```
Pipeline: `Fake → Null → Format`

### Setting Columns to Null

```bash
--null "SENSITIVE_DATA" --null "INTERNAL_ID"
```

### Static Value Overwrite

```bash
--overwrite "STATUS:anonymized"
--overwrite "COMMENT:redacted"
```

### Format Templates

Use `{COLUMN}` placeholders with optional [.NET format specifiers](https://learn.microsoft.com/en-us/dotnet/standard/base-types/formatting-types):

```bash
--format "DISPLAY_NAME:{FIRSTNAME} {LASTNAME}"
--format "DATE_FR:{DATE:dd/MM/yyyy}"
--format "AMOUNT:{PRICE:0.00}€"
```

### Mask Patterns

Mask data using patterns where `#` preserves the original character and any other character replaces it:

```bash
--mask "EMAIL:###****"        # "test@example.com" → "tes****ample.com"
--mask "PHONE:##-##-****"     # "0612345678" → "06-12-****5678"
--mask "SSN:***-**-####"      # "123-45-6789" → "***-**-6789"
```

> 💡 If the pattern is shorter than the data, remaining characters are preserved.

---

## Anonymization (Fake Data)

Replace real data with fake values using [Bogus](https://github.com/bchavez/Bogus?tab=readme-ov-file#bogus-api-support).

### Basic Usage

```bash
./querydump --input "duckdb:customers.db" \
  --query "SELECT NAME, EMAIL FROM customers" \
  --output customers_anon.csv \
  --fake "NAME:name.fullName" \
  --fake "EMAIL:internet.email" \
  --fake-locale fr
```

### Deterministic Mode

**Column-based seeding** (same input value → same fake output):
```bash
--fake "USERNAME:name.fullName" --fake-seed-column USER_ID
```

**Row-index seeding** (reproducible order-based):
```bash
--fake "USERNAME:name.fullName" --fake-deterministic
```

> ⚠️ Row-index seeding depends on query order. If rows are added, removed, or reordered, fake values will shift. Prefer `--fake-seed-column` for stable determinism.

### Variant Suffix

Get different values from the same faker:
```bash
--fake "EMAIL_PERSO:internet.email"
--fake "EMAIL_PRO:internet.email#work"
```

### Virtual Columns

Create fake columns not in the query, then use them in `--format`:
```bash
--query "SELECT USER_ID FROM users" \
--fake "IBAN:finance.iban" \
--fake "BIC:finance.bic" \
--format "BANK_REF:{IBAN}-{BIC}"
```

### List Available Fakers

```bash
./querydump --fake-list
```

---

## CLI Reference

### Core Options

| Option | Alias | Description | Default |
|--------|-------|-------------|---------|
| `--input` | `-i` | Input connection string (prefixed) | Required* |
| `--query` | `-q` | SQL query (SELECT only) | Required* |
| `--output` | `-o` | Output file or connection (prefixed) | Required* |
| `--job` | | Path to YAML job file | - |
| `--export-job` | | Export config to YAML file and exit | - |
| `--batch-size` | `-b` | Rows per batch | `50000` |
| `--limit` | | Maximum rows to export (overrides YAML) | `0` |
| `--dry-run` | | Display source/target schemas and sample row (overrides YAML) | `false` |
| `--connection-timeout` | | Connection timeout (seconds) | `10` |
| `--query-timeout` | | Query timeout (seconds, 0=none) | `0` |
| `--unsafe-query` | | Bypass SQL validation (allow DDL) | `false` |

> \* Required unless `--job` is specified. When using `--job`, these options are ignored.

### Transformation Options

| Option | Description |
|--------|-------------|
| `--null` | Set column(s) to null (repeatable) |
| `--overwrite` | `COLUMN:value` static replacement (repeatable) |
| `--format` | `TARGET:{SOURCE}` or `{SOURCE:fmt}` template (repeatable) |

### Skip-Null Options

Prevent transformers from modifying null values:

| Option | Description |
|--------|-------------|
| `--overwrite-skip-null` | Don't overwrite null values |
| `--fake-skip-null` | Don't generate fake for null values |
| `--format-skip-null` | Skip if all source data is null |

### Anonymization Options

| Option | Description | Default |
|--------|-------------|---------|
| `--fake` | `COLUMN:faker.method` mapping (repeatable) | - |
| `--mask` | `COLUMN:pattern` masking (`#` = keep) (repeatable) | - |
| `--mask-skip-null` | Don't mask null values | `false` |
| `--fake-locale` | Locale (en, fr, de, ja...) | `en` |
| `--fake-seed` | Global seed for reproducibility | - |
| `--fake-seed-column` | Column for deterministic seeding | - |
| `--fake-deterministic` | Row-index based determinism | `false` |
| `--fake-list` | List fakers and exit | - |

### Database Writer Options

| Option | Description | Default |
|--------|-------------|---------|
| `--duckdb-writer-table` | DuckDB target table | `Export` |
| `--duckdb-writer-strategy` | `Append`/`Truncate`/`Recreate` | `Append` |
| `--sqlite-table` | SQLite target table | `Export` |
| `--sqlite-strategy` | `Append`/`Truncate`/`Recreate` | `Append` |
| `--oracle-writer-table` | Oracle target table | `EXPORT_DATA` |
| `--oracle-writer-strategy` | `Append`/`Truncate`/`Recreate` | `Append` |
| `--oracle-writer-bulk-size` | Oracle bulk batch size | `5000` |

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

## License

MIT
