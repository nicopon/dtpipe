# QueryDump

Command-line tool to export database data (Oracle, SQL Server, DuckDB...) to Parquet or CSV files, designed for low memory footprint and seamless DuckDB ingestion.

## Features

- **Streaming**: Stream-based reading (`IDataReader`) to handle large datasets.
- **Formats**: Parquet (Snappy compression) and CSV (RFC 4180 compatible).
- **Multi-Database**: Supports Oracle, SQL Server, DuckDB, Postgres, MySQL, SQLite.
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
| `--fake` | | Mapping `COLUMN:dataset.method` | - |
| `--fake-locale` | | Locale for fake data | `en` |
| `--oracle-fetch-size` | `-f` | Oracle read buffer size (bytes) | 1MB |
| `--batch-size` | `-b` | Output batch size (rows per Parquet group / CSV flush) | 50k |
| `--connection-timeout` | | Connection timeout (seconds) | 10 |

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
