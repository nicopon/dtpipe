# QueryDump

Command-line tool to export database data (Oracle, SQL Server, DuckDB) to Parquet, CSV, or another database. Designed for low memory footprint and seamless DuckDB ingestion.

## Features

- **Streaming**: Stream-based reading (`IDataReader`) to handle large datasets with minimal memory.
- **Formats**: Parquet (Snappy compression), CSV (RFC 4180), and Database Export (DuckDB, Oracle).
- **Multi-Database**: Supports Oracle, SQL Server, DuckDB (Read & Write).
- **Anonymization**: Mask sensitive columns with fake data during export.

## Quick Start

**Build**
```bash
./build.sh
```
The executable is generated at `./dist/release/querydump` (standalone, no runtime dependencies).

**Basic Usage**
```bash
./dist/release/querydump \
  --input "duckdb:source.db" \
  --query "SELECT * FROM users" \
  --output users.parquet
```

> 💡 Yes, DuckDB can do this on its own. This is just to illustrate the basic syntax—keep reading for the *actually useful* stuff. 😉

---

## Usage

### Input Sources

Use `--input` (or `-i`) with a prefixed connection string:

| Prefix | Provider | Example |
|--------|----------|---------|
| `duckdb:` | DuckDB | `duckdb:mydata.db` |
| `oracle:` | Oracle | `oracle:Data Source=...;User Id=...` |
| `mssql:` | SQL Server | `mssql:Server=...;Database=...` |

The provider is auto-detected from the prefix. File extensions `.db` and `.duckdb` are also recognized for DuckDB.

### Output Destinations

Use `--output` (or `-o`) with either:

**File path** (extension determines format):
- `.parquet` → Parquet with Snappy compression
- `.csv` → RFC 4180 CSV

**Database connection** (prefixed):
```bash
--output "duckdb:target.db"
--output "oracle:User/Pass@TargetDB"
```

#### Database Writer Options

| Option | Description | Default |
|--------|-------------|---------|
| `--duckdb-writer-table` | Target table name | `Export` |
| `--duckdb-writer-strategy` | `Append`, `Truncate`, `Recreate` | `Append` |
| `--oracle-writer-table` | Target table name | `EXPORT_DATA` |
| `--oracle-writer-strategy` | `Append`, `Truncate`, `Recreate` | `Append` |
| `--oracle-writer-bulk-size` | Bulk copy batch size | `5000` |

**Example: DuckDB to DuckDB with table recreation**
```bash
./dist/release/querydump \
  --input "duckdb:source.db" \
  --query "SELECT * FROM Users" \
  --output "duckdb:target.db" \
  --duckdb-writer-table "AnonymizedUsers" \
  --duckdb-writer-strategy Recreate
```

---

### Data Transformation

Transform column values during export using the following options (applied in order: Null → Overwrite → Format).

#### Setting Columns to Null
```bash
--null "SENSITIVE_DATA"
--null "INTERNAL_ID"
```

#### Static Value Overwrite
```bash
--overwrite "STATUS:anonymized"
--overwrite "COMMENT:redacted"
```

#### Format Templates
Use `{COLUMN}` placeholders to create derived columns, with optional format specifiers.

**Simple substitution:**
```bash
--format "DISPLAY_NAME:{FIRSTNAME} {LASTNAME}"
--format "FULL_ADDRESS:{STREET}, {CITY} {ZIP}"
```

**With .NET format specifiers** (see [numeric](https://learn.microsoft.com/en-us/dotnet/standard/base-types/standard-numeric-format-strings) and [date/time](https://learn.microsoft.com/en-us/dotnet/standard/base-types/custom-date-and-time-format-strings) docs):
```bash
--format "DATE_FR:{DATE:dd/MM/yyyy}"    # → 15/01/2024
--format "AMOUNT:{PRICE:0.00}€"          # → 123.46€
--format "ID:{CODE:D6}"                  # → 000042
```

**Combined:**
```bash
--format "LABEL:{PRICE:0.00}€ - {NAME}"  # → 99.50€ - Product
```

---

### Anonymization (Fake Data)

Replace real data with realistic fake values using [Bogus](https://github.com/bchavez/Bogus).

#### Basic Usage
```bash
./dist/release/querydump \
  --input "duckdb:customers.db" \
  --query "SELECT NAME, EMAIL FROM customers" \
  --output customers_anon.csv \
  --fake "NAME:name.fullName" \
  --fake "EMAIL:internet.email" \
  --fake-locale fr
```

#### Deterministic Mode

**Column-based seeding** (same input value = same fake output):
```bash
--fake "USERNAME:name.fullName" \
--fake-seed-column USER_ID
```

**Row-index seeding** (reproducible order-based):
```bash
--fake "USERNAME:name.fullName" \
--fake-deterministic
```

#### Variant Suffix
Get different values from the same faker using `#variant`:
```bash
--fake "EMAIL_PERSO:internet.email"       # Value A
--fake "EMAIL_PRO:internet.email#work"    # Different value B
```

#### Virtual Columns
Create fake columns not in the query, then use them in `--format`:
```bash
--query "SELECT USER_ID FROM users" \
--fake "IBAN:finance.iban" \
--fake "BIC:finance.bic" \
--format "BANK_REF:{IBAN}-{BIC}"
```

#### List Available Fakers
```bash
./dist/release/querydump --fake-list
```

---

## CLI Reference

| Option | Alias | Description | Default |
|--------|-------|-------------|---------|
| `--input` | `-i` | Input connection string (prefixed) | **Required** |
| `--query` | `-q` | SQL query (SELECT only) | **Required** |
| `--output` | `-o` | Output file or connection (prefixed) | **Required** |
| `--batch-size` | `-b` | Rows per batch | `50000` |
| `--connection-timeout` | | Connection timeout (seconds) | `10` |
| `--query-timeout` | | Query timeout (seconds, 0=none) | `0` |
| **Transformation** |
| `--null` | | Set column(s) to null | - |
| `--overwrite` | | `COLUMN:value` static replacement | - |
| `--format` | | `TARGET:{SOURCE}` or `{SOURCE:fmt}` | - |
| **Anonymization** |
| `--fake` | | `COLUMN:faker.method` mapping | - |
| `--fake-locale` | | Locale (en, fr, de, ja...) | `en` |
| `--fake-seed` | | Global seed for reproducibility | - |
| `--fake-seed-column` | | Column for deterministic seeding | - |
| `--fake-deterministic` | | Row-index based determinism | `false` |
| `--fake-list` | | List fakers and exit | - |
| **Reader** |
| `--ora-fetch-size` | | Oracle fetch buffer (bytes) | `1048576` |
| **Writer** |
| `--duckdb-writer-table` | | DuckDB target table | `Export` |
| `--duckdb-writer-strategy` | | `Append`/`Truncate`/`Recreate` | `Append` |
| `--oracle-writer-table` | | Oracle target table | `EXPORT_DATA` |
| `--oracle-writer-strategy` | | `Append`/`Truncate`/`Recreate` | `Append` |
| `--oracle-writer-bulk-size` | | Oracle bulk batch size | `5000` |

---

## Common Fakers

| Dataset | Method | Description | Example (fr) |
|---------|--------|-------------|--------------|
| **Name** | `name.firstName` | First name | *Jean* |
| | `name.lastName` | Last name | *Dupont* |
| | `name.fullName` | Full name | *Jean Dupont* |
| **Internet** | `internet.email` | Email address | *jean.dupont@gmail.com* |
| | `internet.userName` | Username | *jdupont* |
| **Address** | `address.streetAddress` | Street | *12 rue de la Paix* |
| | `address.city` | City | *Paris* |
| | `address.zipCode` | Zip code | *75001* |
| **Phone** | `phone.phoneNumber` | Phone | *01 02 03 04 05* |
| **Company** | `company.companyName` | Company | *Corp Inc.* |
| **Date** | `date.past` | Past date | *2023-12-01* |
| | `date.future` | Future date | *2030-01-01* |
| **Finance** | `finance.iban` | IBAN | *FR76...* |
| | `finance.bic` | BIC/SWIFT | *BNPAFRPP* |

> Use `--fake-list` to see all 100+ available generators.

---

## License

MIT
