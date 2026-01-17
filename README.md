# QueryDump

Command-line tool to export database data (Oracle, SQL Server, DuckDB...) to Parquet or CSV files, designed for low memory footprint and seamless DuckDB ingestion.

## Features

- **Streaming**: Stream-based reading (`IDataReader`) to handle large datasets.
- **Formats**: Parquet (Snappy compression) and CSV (RFC 4180 compatible).
- **Multi-Database**: Supports Oracle, SQL Server, DuckDB.
- **Anonymization**: Capability to mask sensitive columns with fake data during export.

## Build

Prerequisites: .NET 8 or higher.

```bash
./build.sh
```
The executable will be generated in `./dist/release/querydump` (standalone, no runtime dependencies required).

## Usage Examples

The tool attempts to detect the database provider via the connection string if the `--provider` parameter is not specified.

### 1. Standard Export (Oracle)
Uses environment variable for the connection string.

```bash
export ORACLE_CONNECTION_STRING="Data Source=..."
./dist/release/querydump -q "SELECT * FROM clients" -o ./clients.parquet
```

### 2. SQL Server to CSV Export
Explicitly specifies provider and connection string.

```bash
./dist/release/querydump \
  -p sqlserver \
  -c "Server=myServer;Database=myDB;Trusted_Connection=True;" \
  -q "SELECT * FROM Users" \
  -o users.csv
```

### 3. Export with Anonymization
Replaces real values with fake data (names, emails, cities...).

```bash
./dist/release/querydump \
  -q "SELECT CUSTNAME, EMAIL FROM CLI" \
  -o export_gdpr.csv \
  --fake "CUSTNAME:name.lastname" \
  --fake "EMAIL:internet.email" \
  --fake-locale fr
```

### 4. Column References for Consistent Data
Use `{{COLUMN}}` to reference other columns and create consistent data:

```bash
./dist/release/querydump \
  -q "SELECT FIRSTNAME, LASTNAME, FULLNAME, EMAIL FROM USERS" \
  -o users_anon.csv \
  --fake "FIRSTNAME:name.firstname" \
  --fake "LASTNAME:name.lastname" \
  --fake "FULLNAME:{{FIRSTNAME}} {{LASTNAME}}" \
  --fake "EMAIL:{{FIRSTNAME}}.{{LASTNAME}}@company.com"
```

This ensures that the generated `FULLNAME` and `EMAIL` use the same first and last names.

### 5. Hardcoded Values
If the value doesn't match a known faker, it's used as a literal string:

```bash
--fake "STATUS:anonymized"
--fake "CODUSER:anonymous"
```

### 6. Setting Columns to Null
Use `--null` to explicitly set columns to null:

```bash
--null "SENSITIVE_DATA"
--null "INTERNAL_ID"
```

### 7. Static Value Overwrite
Use `--overwrite` to replace column values with a static string:

```bash
--overwrite "STATUS:anonymized"
--overwrite "COMMENT:redacted"
```

### 8. Clone Columns with Templates
Use `--clone` to copy values from other columns using templates:

```bash
--clone "DISPLAY_NAME:{{FIRSTNAME}} {{LASTNAME}}"
--clone "FULL_ADDRESS:{{STREET}}, {{CITY}} {{ZIP}}"
```

### 9. Deterministic Fake Data
Use `--fake-seed-column` to generate reproducible fake data based on a source column value:

```bash
./dist/release/querydump \
  -q "SELECT USER_ID, USERNAME, EMAIL FROM USERS" \
  -o users_anon.csv \
  --fake "USERNAME:name.fullname" \
  --fake "EMAIL:internet.email" \
  --fake-seed-column USER_ID
```

This ensures that the same `USER_ID` always produces the same fake `USERNAME` and `EMAIL`, even across different runs.

### 10. Row-Index Deterministic Mode
Use `--fake-deterministic` without a seed column for reproducible fakes based on row position:

```bash
./dist/release/querydump \
  -q "SELECT * FROM USERS ORDER BY ID" \
  -o users_anon.csv \
  --fake "USERNAME:name.fullname" \
  --fake-deterministic
```

### 11. Variant Suffix for Different Values
Use `#variant` suffix to get different values from the same faker:

```bash
--fake "EMAIL_PERSO:internet.email"      # Value A
--fake "EMAIL_PRO:internet.email#work"   # Different value B
--fake "EMAIL_BKP:internet.email#work"   # Same as EMAIL_PRO (same variant)
```

### 12. Virtual Columns for Composition
Create fake columns not in query, then compose them with `--clone`:

```bash
# Query: SELECT USER_ID, BANK_REF FROM users
./dist/release/querydump \
  -q "SELECT USER_ID, BANK_REF FROM users" \
  -o users.csv \
  --fake "IBAN:finance.iban" \
  --fake "BIC:finance.bic" \
  --clone "BANK_REF:{{IBAN}}-{{BIC}}"
```

Virtual columns (IBAN, BIC) are automatically detected (not in query) and available for `--clone` templates.

To list available data generators:
```bash
./dist/release/querydump --fake-list
```

## Options

| Option | Alias | Description | Default |
|---|---|---|---|
| `--query` | `-q` | SQL query to execute (SELECT only) | **Required** |
| `--output` | `-o` | Output file (.parquet or .csv) | **Required** |
| `--connection` | `-c` | Connection string | Auto (ENV) |
| `--provider` | `-p` | `auto`, `oracle`, `sqlserver`, `duckdb`... | `auto` |
| `--batch-size` | `-b` | Rows per output batch | 5000 |
| `--connection-timeout` | | Connection timeout (seconds) | 10 |
| `--query-timeout` | | Query timeout (seconds, 0=none) | 0 |
| **Transformers** |
| `--null` | | Column(s) to set to null (repeatable) | - |
| `--overwrite` | | `COLUMN:value` static overwrite (repeatable) | - |
| `--clone` | | `TARGET:{{SOURCE}}` template clone (repeatable) | - |
| `--fake` | | `COLUMN:dataset.method` mapping (repeatable) | - |
| `--fake-locale` | | Bogus locale for fake data | `en` |
| `--fake-seed` | | Global seed for reproducible fakes | - |
| `--fake-seed-column` | | Column for deterministic seeding | - |
| `--fake-deterministic` | | Row-index based deterministic mode | false |
| `--fake-table-size` | | Precomputed table size (power of 2) | 65536 |
| **Reader Options** |
| `--ora-fetch-size` | | Oracle fetch buffer size (bytes) | 1MB |

## Common Fakers

Here are some of the most useful fakers you can use with `--fake`. Use `--fake-list` to see the full list of 100+ available generators.

| Dataset | Method | Description | Example (fr) |
|---|---|---|---|
| **Name** | `name.firstName` | First name | *Jean* |
| | `name.lastName` | Last name | *Dupont* |
| | `name.fullName` | Full name | *Jean Dupont* |
| **Internet** | `internet.email` | Email address | *jean.dupont@gmail.com* |
| | `internet.userName` | Username | *jdupont* |
| **Address** | `address.streetAddress` | Street address | *12 rue de la Paix* |
| | `address.city` | City name | *Paris* |
| | `address.zipCode` | Zip code | *75001* |
| | `address.country` | Country | *France* |
| **Phone** | `phone.phoneNumber` | Phone number | *01 02 03 04 05* |
| **Company** | `company.companyName` | Company name | *Corp Inc.* |
| **Date** | `date.past` | Date in the past | *2023-12-01* |
| | `date.future` | Date in the future | *2030-01-01* |

> **Note**: The anonymization feature is powered by [Bogus](https://github.com/bchavez/Bogus). For a complete list of all available datasets and methods, please refer to their official documentation.


## License

MIT
