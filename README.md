# DtPipe

**A simple, self-contained CLI for performance-focused data streaming & anonymization.**

DtPipe streams data from any source (SQL, CSV, Parquet) to any destination, applying intelligent transformations on the fly. It is designed for CI/CD pipelines, test data generation, and large dataset migration.

---

### 🚀 [**See the COOKBOOK for Recipes & Examples**](./COOKBOOK.md) 🍳
*Go here for Anonymization guides, Pipeline examples, and detailed tutorials.*

---

- **Modular Architecture**: Clean separation between `Core` engine, `Adapters`, and `XStreamers`.
- **Zero-Copy Streaming**: Direct memory mapping (Zero-Copy) for columnar formats (Arrow, Parquet, DuckDB, DataFusion) via the **DuckXStreamer** and **DataFusionXStreamer** bridges.
- **Micro-Performance**: Handles millions of rows with optimized batching and constant, low memory usage.
- **Multi-Provider**: Native support for **Oracle**, **SQL Server**, **PostgreSQL**, **DuckDB**, **SQLite**, **Parquet**, **CSV**, and **JsonL**.
- **Anonymization Engine**: Built-in **Bogus** integration to fake Names, Emails, IBANs, and more.
- **Production Ready**: YAML job configuration, execution hooks, and robust instrumentation.

## Installation

### .NET Global Tool (Recommended)
You can install DtPipe as a global tool if you have the .NET SDK installed.

```bash
dotnet tool install -g dtpipe
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
>
> **Performance Tip:** For high-speed SQL lookups and joins, check out the **[Zero-Copy XStreamers](./COOKBOOK.md#high-performance-joins-with-duckxstreamer)** in the cookbook.

## Quick Reference

### CLI Usage

```bash
dtpipe --input [SOURCE] --query [SQL] --output [DEST] [OPTIONS]
```


### 1. Connection Strings (Input & Output)

DtPipe auto-detects providers from file extensions (`.csv`, `.parquet`, `.duckdb`, `.sqlite`) or explicit prefixes. **Using an explicit prefix is recommended** to avoid ambiguity and improve performance.

| Provider | Input | Output | Prefix / Format | Example |
|:---|:---:|:---:|:---|:---|
| **DuckDB** | ✅ | ✅ | `duck:` | `duck:my.duckdb` |
| **SQLite** | ✅ | ✅ | `sqlite:` | `sqlite:data.sqlite` |
| **PostgreSQL**| ✅ | ✅ | `pg:` | `pg:Host=localhost;Database=mydb` |
| **Oracle** | ✅ | ✅ | `ora:` | `ora:Data Source=PROD;User Id=scott` |
| **SQL Server**| ✅ | ✅ | `mssql:` | `mssql:Server=.;Database=mydb` |
| **CSV** | ✅ | ✅ | `csv:` / `.csv` | `data.csv` |
| **JsonL** | ✅ | ✅ | `jsonl:` / `.jsonl`| `data.jsonl` |
| **Apache Arrow** | ✅ | ✅ | `arrow:` / `.arrow`| `data.arrow` |
| **Parquet** | ✅ | ✅ | `parquet:` / `.parquet`| `data.parquet` |
| **Data Gen** | ✅ | — | `generate:` | `generate:1000k` |
| **Null** | — | ✅ | `null:` | `null:` |
| **STDIN/OUT** | ✅ | ✅ | `-` / `{CP}:-` | `csv` (for `csv:-`) |

> [!TIP]
> **Secure your Connection Strings:** Instead of typing passwords in plain text, use **[Secret Management](#-secret-management)**. 
> Use the prefix `keyring://my-alias` anywhere a connection string is required. DtPipe will automatically resolve it from your OS keychain.

> [!IMPORTANT]
> **Explicit use of `-` is required for standard input/output.**
> Using a shorthand like `csv` is equivalent to `csv:-`. If a provider does not support pipes (like `pg:`), using its name without a connection string will throw an error to prevent accidental data swallowing.

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

### 3. Positional CLI Option Scoping (Reader vs Writer)

DtPipe resolves options logically based on their position relative to the **output flag (`-o`)**.

* **Global / Reader Scope:** Options placed *before* `-o` apply universally to the pipeline, acting as Reader properties or global pipeline properties.
* **Writer Scope:** Options placed *after* `-o` specifically target the Writer, overriding global defaults.

```bash
# Example: Use a comma separator for the Reader, but a semicolon separator for the Writer
dtpipe -i input.csv --csv-separator "," -o output.csv --csv-separator ";"
```

### 4. CLI Options Reference

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
| `--sampling-rate` | Probability 0.0-1.0 to include a row (default: 1.0). |
| `--sampling-seed` | Seed for sampling (ensures reproducibility). |

#### Automation
| Flag | Description |
|:---|:---|
| `--job [FILE]` | Execute a YAML job file. |
| `--export-job` | Save current CLI args as a YAML job. |
| `--log [FILE]` | Write execution statistics to file (Optional). |
| `--metrics-path`| Path to structured metrics JSON output. |

#### Transformation Pipeline
| Flag | Description |
|:---|:---|
| `--fake "[Col]:[Method]"` | Generate fake data using Bogus. |
| `--mask "[Col]:[Pattern]"` | Mask chars (`#` keeps char, others replace). |
| `--null "[Col]"` | Force column to NULL. |
| `--overwrite "[Col]:[Val]"`| Set column to fixed value. |
| `--format "[Col]:[Fmt]"` | Apply .NET format string. |
| `--compute "[Col]:[JS]"` | Apply Javascript logic on the `row` object. If `[Col]` doesn't exist, it is created as a **new virtual column**. Supports inline code or file paths (`@file.js`). Example: `TITLE:row.TITLE.substring(0,5)` |
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
| `--strategy` | `Append`, `Truncate`, `DeleteThenInsert`, `Recreate`, `Upsert`, `Ignore`. Works for all providers. |
| `--insert-mode` | `Standard`, `Bulk`. Works for supported providers (SqlSever, Oracle, PostgreSQL). |
| `--table` | Target table name. Overrides default 'export'. |
| `--auto-migrate` | Automatically add missing columns to target table. |
| `--strict-schema`| Abort if schema errors are found. |
| `--unsafe-query` | Allow non-SELECT queries (use with caution). |

---
 
## 🔒 Secret Management
 
DtPipe includes a built-in secret manager that uses your **Operating System's Keyring** (Windows Credential Manager, macOS Keychain, or Linux Secret Service) to store connection strings securely.

This allows you to share scripts and YAML jobs without exposing production credentials. A secret can store a complete connection string, including its provider prefix (e.g., `pg:Host=...`).

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


## 🔀 Multi-Stream Pipelines (DAG)

DtPipe supports chaining multiple data sources and XStreamers in a single command to build complex, high-performance pipelines.

### How it works

A pipeline with multiple branches is automatically detected when you provide:
- Multiple `--input` flags (sequential branches)
- One or more `--xstreamer` / `-x` flags (SQL joins on in-memory streams)

Each branch can be given an `--alias` name so that XStreamers can reference it.

### Example: In-Memory Join

```bash
# 1. Load customers into memory as "customers"
# 2. Load orders into memory as "orders"
# 3. Join them via DuckDB XStreamer and write to Parquet
dtpipe \
  -i customers.parquet --alias customers \
  -i orders.csv --alias orders \
  -x duck --main orders --ref customers \
  -q "SELECT o.*, c.name FROM orders o JOIN customers c ON o.customer_id = c.id" \
  -o result.parquet
```

| Option | Description |
|:---|:---|
| `--alias [NAME]` | Name this branch for use in joins |
| `-x`, `--xstreamer` | Start an XStreamer branch (e.g. `duck`) |
| `--main [ALIAS]` | Primary stream the XStreamer consumes |
| `--ref [ALIAS]` | Reference stream(s) for lookup/join |

---

## Contributing
Want to add a new database adapter or a custom transformer? Check out the [Developer Guide](./EXTENDING.md).

## License
MIT
