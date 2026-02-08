# DtPipe

[![Open in GitHub Codespaces](https://github.com/codespaces/badge.svg)](https://github.com/codespaces/new?repo=nicopon/DtPipe)

**A simple, self-contained CLI for performance-focused data streaming & anonymization.**

DtPipe streams data from any source (SQL, CSV, Parquet) to any destination, applying intelligent transformations on the fly. It is designed for CI/CD pipelines, test data generation, and large dataset migration.

---

### 🚀 [**See the COOKBOOK for Recipes & Examples**](./COOKBOOK.md) 🍳
*Go here for Anonymization guides, Pipeline examples, and detailed tutorials.*

---

## Capabilities

- **Streaming Architecture**: Handles millions of rows with constant, low memory usage.
- **Multi-Provider**: Native support for **Oracle**, **SQL Server**, **PostgreSQL**, **DuckDB**, **SQLite**, **Parquet**, and **CSV**.
- **Zero Dependencies**: Single static binary. No drivers to install.
- **Anonymization Engine**: Built-in **Bogus** integration to fake Names, Emails, IBANs, and more.
- **Pipeline Transformation**: Mask, Nullify, Format, or Script (JS) data during export.
- **Production Ready**: YAML job configuration, Environment variable support, and robust logging.

## Installation

### .NET Global Tool (Recommended)
You can install DtPipe as a global tool if you have the .NET SDK installed.

```bash
dotnet tool install -g dtpipe --prerelease
dtpipe --help
```

### Build from Source
**Prerequisite:** [.NET 10 SDK](https://dotnet.microsoft.com/download/dotnet/10.0) is required to compile.

```bash
# Bash (Mac/Linux/Windows Git Bash)
./build.sh

# PowerShell (Windows/Cross-platform)
./build.ps1
```

Binary created at: `./dist/release/dtpipe`

> **Note:** The pre-compiled binaries in [GitHub Releases](https://github.com/nicopon/DtPipe/releases) are **self-contained**. You do NOT need to install .NET to run them.

## Quick Reference

### CLI Usage

```bash
dtpipe --input [SOURCE] --query [SQL] --output [DEST] [OPTIONS]
```


### 1. Connection Strings (Input & Output)

DtPipe auto-detects providers from file extensions (`.csv`, `.parquet`, `.db`, `.sqlite`) or explicit prefixes.

| Provider | Prefix / Format | Example |
|:---|:---|:---|
| **DuckDB** | `duck:` | `duck:my.db` |
| **SQLite** | `sqlite:` | `sqlite:data.sqlite` |
| **PostgreSQL**| `pg:` | `pg:Host=localhost;Database=mydb` |
| **Oracle** | `ora:` | `ora:Data Source=PROD;User Id=scott` |
| **SQL Server**| `mssql:` | `mssql:Server=.;Database=mydb` |
| **CSV** | `csv:` / `.csv` | `data.csv` |
| **Parquet** | `parquet:` / `.parquet`| `data.parquet` |
| **Sample Gen** | `sample:` | `sample:1000000` (generate 1M rows) |
| **Keyring** | `keyring://` | `keyring://my-prod-db` |
| **STDIN/OUT** | `csv` or `parquet` | `csv` (no file path) |

### 2. Anonymization & Fakers

Use `--fake "Col:Generator"` to replace sensitive data.
*See [COOKBOOK.md](./COOKBOOK.md#anonymization-the-fakers) for more examples.*

| Category | Key Generators |
|:---|:---|
| **Identity** | `name.fullName`, `name.firstName`, `internet.email` |
| **Address** | `address.fullAddress`, `address.city`, `address.zipCode` |
| **Finance** | `finance.iban`, `finance.creditCardNumber` |
| **Phone** | `phone.phoneNumber` |
| **Dates** | `date.past`, `date.future`, `date.recent` |
| **System** | `random.uuid`, `random.number`, `random.boolean` |

> Use `--fake-list` to print all available generators.

### 3. CLI Options Reference

#### Core
| Flag | Description |
|:---|:---|
| `-i`, `--input` | **Required**. Source connection string or file path. |
| `-q`, `--query` | **Required** (for queryable sources). SQL statement. |
| `-o`, `--output`| **Required**. Target connection string or file path. |
| `--limit` | Stop after N rows. |
| `--batch-size` | Rows per buffer (default: 50,000). |
| `--dry-run` | Preview data, **validate constraints**, and check schema compatibility. |
| `--key` | Comma-separated Primary Keys for Upsert/Ignore. Auto-detected from target if omitted. |

#### Automation
| Flag | Description |
|:---|:---|
| `--job [FILE]` | Execute a YAML job file. |
| `--export-job` | Save current CLI args as a YAML job. |
| `--log [FILE]` | Write execution statistics to file (Optional). |

#### Transformation Pipeline
| Flag | Description |
|:---|:---|
| `--fake "[Col]:[Method]"` | Generate fake data using Bogus. |
| `--mask "[Col]:[Pattern]"` | Mask chars (`#` keeps char, others replace). |
| `--null "[Col]"` | Force column to NULL. |
| `--overwrite "[Col]:[Val]"`| Set column to fixed value. |
| `--format "[Col]:[Fmt]"` | Apply .NET format string. |
| `--compute "[Col]:[JS]"` | Apply Javascript logic. Supports inline code or file paths (`@file.js`). |
| `--filter "[JS]"` | Drop rows based on JS logic (must return true/false). |
| `--expand "[JS]"` | Multi-row expansion. JS expression returning an array. |
| `--window-count [N]` | Accumulate rows in a window of size N. |
| `--window-script "[JS]"` | Script to execute on window `rows` (must return array). |
| `--project`, `--drop` | Whitelist or Blacklist columns. |

#### Pipeline Modifiers
| Flag | Description |
|:---|:---|
| `--fake-locale [LOC]` | Locale for fakers (e.g. `fr`, `en_US`). |
| `--fake-seed-column [COL]`| Make faking deterministic based on a column value. |
| `--[type]-skip-null` | Skip transformation if value is NULL. |

#### Database Writer Options
| Flag | Description |
|:---|:---|
| `--ora-strategy` | `Append`, `Truncate`, `DeleteThenInsert`, `Recreate`, `Upsert`, `Ignore`. |
| `--ora-insert-mode` | `Standard`, `Append` (Direct-Path), `Bulk`. |
| `--pg-strategy` | `Append`, `Truncate`, `DeleteThenInsert`, `Recreate`, `Upsert`, `Ignore`. |
| `--pg-insert-mode` | `Standard`, `Bulk` (Binary Copy). |
| `--mssql-strategy` | `Append`, `Truncate`, `DeleteThenInsert`, `Recreate`, `Upsert`, `Ignore`. |
| `--duck-strategy` | `Append`, `Truncate`, `DeleteThenInsert`, `Recreate`, `Upsert`, `Ignore`. |
| `--sqlite-strategy` | `Append`, `DeleteThenInsert`, `Recreate`, `Upsert`, `Ignore`. |
| `--unsafe-query` | Allow non-SELECT queries (use with caution). |

---

## 🔒 Secret Management

DtPipe includes a built-in secret manager that uses your **Operating System's Keyring** (Windows Credential Manager, macOS Keychain, or Linux Secret Service) to store connection strings securely.

### 1. Store a Secret
```bash
dtpipe secret set prod-db "ora:Data Source=PROD;User Id=scott;Password=tiger"
```

### 2. Use it in a Transfer
Use the `keyring://` prefix followed by your alias.
```bash
dtpipe -i keyring://prod-db -q "SELECT * FROM users" -o users.parquet
```

### 3. Manage Secrets
| Command | Description |
|:---|:---|
| `dtpipe secret list` | List all stored aliases. |
| `dtpipe secret get <alias>` | Print the secret value (useful for verification). |
| `dtpipe secret delete <alias>`| Delete a specific secret. |
| `dtpipe secret nuke` | Delete ALL secrets. |

---


## Contributing
Want to add a new database adapter or a custom transformer? Check out the [Developer Guide](./EXTENDING.md).

## License
MIT
