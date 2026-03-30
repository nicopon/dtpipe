# DtPipe

A self-contained CLI for streaming and anonymizing data across databases and file formats.

DtPipe reads from a source, applies transformations row by row (or in columnar batches when possible), and writes to a destination — with no intermediate staging required. It targets automation and CI/CD scenarios where you need repeatable, low-overhead data movement.

---

### 🚀 [See the COOKBOOK for Recipes & Examples](./COOKBOOK.md)
*Anonymization guides, pipeline examples, and detailed tutorials.*

---

## Installation

### .NET Global Tool (Recommended)

```bash
dotnet tool install -g dtpipe
dtpipe --help
```

### Build from Source

**Prerequisite:** [.NET 10 SDK](https://dotnet.microsoft.com/download/dotnet/10.0)

```bash
# Bash (Mac/Linux/Windows Git Bash)
./build.sh

# PowerShell (Windows/Cross-platform)
./build.ps1
```

Binary created at: `./dist/release/dtpipe`

> **Note:** Pre-compiled binaries in [GitHub Releases](https://github.com/nicopon/DtPipe/releases) are self-contained — no .NET runtime required.

---

### Shell Autocompletion

DtPipe supports smart suggestions for `bash`, `zsh`, and `powershell`:
- strategies (`Append`, `Truncate`, `Upsert`…), providers (`pg:`, `ora:`, `csv:`…), and keyring aliases (`keyring://…`)

```bash
dtpipe completion --install
```

Restart your terminal (or `source ~/.zshrc`) to activate.

---

## Quick Reference

### CLI Usage

```bash
dtpipe --input [SOURCE] --query [SQL] --output [DEST] [OPTIONS]
```

### 1. Connection Strings (Input & Output)

DtPipe detects providers from file extensions (`.csv`, `.parquet`, `.duckdb`, `.sqlite`) or explicit prefixes. Explicit prefixes are recommended to avoid ambiguity.

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
| **Data Gen** | ✅ | — | `generate:` | `generate:1M` |
| **Null** | — | ✅ | `null:` | `null:` |
| **STDIN/OUT** | ✅ | ✅ | `-` / `{CP}:-` | `csv` (for `csv:-`) |

> [!TIP]
> **Secure your connection strings:** use the `keyring://my-alias` prefix anywhere a connection string is expected. DtPipe resolves it from the OS keychain at runtime. See [Secret Management](#-secret-management).

> [!IMPORTANT]
> `-` is required explicitly for standard input/output.
> `csv` is shorthand for `csv:-`. Providers that don't support pipes (like `pg:`) will raise an error if given a bare name without a connection string.

### 2. Anonymization & Fakers

Use `--fake "Col:Generator"` to replace sensitive data.
*See [COOKBOOK.md](./COOKBOOK.md#anonymization-the-fakers) for the full reference.*

| Category | Key Generators |
|:---|:---|
| **Identity** | `name.fullName`, `name.firstName`, `internet.email` |
| **Address** | `address.fullAddress`, `address.city`, `address.zipCode` |
| **Finance** | `finance.iban`, `finance.creditCardNumber` |
| **Phone** | `phone.phoneNumber` |
| **Dates** | `date.past`, `date.future`, `date.recent` |
| **System** | `random.uuid`, `random.number`, `random.boolean` |

### 3. Positional CLI Option Scoping (Reader vs Writer)

Options are scoped based on their position relative to the output flag (`-o`):

- **Before `-o`**: applied to the reader / pipeline globally.
- **After `-o`**: applied to the writer, overriding reader defaults.

```bash
# Comma separator for input, semicolon for output
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
| `--dry-run` | Preview data, validate constraints, and check schema compatibility. |
| `--key` | Comma-separated primary keys for Upsert/Ignore. Auto-detected from target if omitted. |
| `--sampling-rate` | Probability 0.0–1.0 to include a row (default: 1.0). |
| `--sampling-seed` | Seed for sampling (ensures reproducibility). |
| `--connection-timeout` | Connection timeout in seconds. |
| `--query-timeout` | Query timeout in seconds (0 = no timeout). |
| `--no-stats` | Disable progress bars and statistics output. |

#### Automation
| Flag | Description |
|:---|:---|
| `--job [FILE]` | Execute a YAML job file. |
| `--export-job` | Save current CLI args as a YAML job. |
| `--log [FILE]` | Write execution statistics to file (optional). |
| `--metrics-path`| Path to structured metrics JSON output. |

#### Transformation Pipeline
| Flag | Description |
|:---|:---|
| `--fake "[Col]:[Method]"` | Generate fake data using Bogus. |
| `--mask "[Col]:[Pattern]"` | Mask chars (`#` keeps char, others replace). |
| `--null "[Col]"` | Force column to NULL. |
| `--overwrite "[Col]:[Val]"`| Set column to fixed value. |
| `--format "[Col]:[Fmt]"` | Apply .NET format string. |
| `--compute "[Col]:[JS]"` | Apply JavaScript logic on the `row` object. Creates a new virtual column if `[Col]` doesn't exist. Supports inline code or file paths (`@file.js`). Example: `TITLE:row.TITLE.substring(0,5)` |
| `--filter "[JS]"` | Drop rows based on JS logic (must return true/false). |
| `--expand "[JS]"` | Multi-row expansion. JS expression returning an array. |
| `--window-count [N]` | Accumulate rows in a window of size N. |
| `--window-script "[JS]"` | Script to execute on window `rows` (must return array). |
| `--rename "[Old]:[New]"` | Rename a column. |
| `--project`, `--drop` | Whitelist or blacklist columns. |

#### Pipeline Modifiers
| Flag | Description |
|:---|:---|
| `--fake-locale [LOC]` | Locale for fakers (e.g. `fr`, `en_US`). |
| `--fake-seed-column [COL]`| Make faking deterministic based on a column value. |
| `--[type]-skip-null` | Skip transformation if value is NULL. |
| `--throttle [N]` | Limit throughput to N rows/sec. |

#### Database Writer Options
| Flag | Description |
|:---|:---|
| `--strategy` | `Append`, `Truncate`, `DeleteThenInsert`, `Recreate`, `Upsert`, `Ignore`. Works for all providers. |
| `--insert-mode` | `Standard`, `Bulk`. Supported for SQL Server, Oracle, PostgreSQL. |
| `--table` | Target table name. Overrides default `export`. |
| `--auto-migrate` | Automatically add missing columns to target table. |
| `--strict-schema`| Abort if schema errors are found. |
| `--no-schema-validation` | Disable schema check entirely. |
| `--pre-exec` | SQL or command to run before the transfer. |
| `--post-exec` | SQL or command to run after a successful transfer. |
| `--on-error-exec` | SQL or command to run on error. |
| `--finally-exec` | SQL or command to always run (success or failure). |
| `--unsafe-query` | Allow non-SELECT queries (use with caution). |

---
 
## 🔒 Secret Management
 
DtPipe integrates with the OS keyring (Windows Credential Manager, macOS Keychain, Linux Secret Service) to store connection strings without exposing them in scripts, command history, or `ps` output.

A secret can store a full connection string including its provider prefix (e.g. `pg:Host=...`).

### Store a Secret
```bash
dtpipe secret set prod-db "ora:Data Source=PROD;User Id=scott;Password=tiger"
```

### Use it in a Transfer
```bash
dtpipe -i keyring://prod-db -q "SELECT * FROM users" -o users.parquet
```

### Manage Secrets
| Command | Description |
|:---|:---|
| `dtpipe secret list` | List all stored aliases. |
| `dtpipe secret get <alias>` | Print the secret value. |
| `dtpipe secret delete <alias>`| Delete a specific secret. |
| `dtpipe secret nuke` | Delete ALL secrets. |

---

## 🔀 Multi-Stream Pipelines (DAG)

You can run multiple branches in a single command using `--input` flags and `--sql` processors. Each branch can be named with `--alias` so that downstream steps can reference it.

A pipeline with multiple inputs or processors is assembled as a DAG and executed concurrently.

### Example: In-Memory Join

```bash
dtpipe \
  -i customers.parquet --alias customers \
  -i orders.csv --alias orders \
  --from orders --ref customers \
  --sql "SELECT o.*, c.name FROM orders o JOIN customers c ON o.customer_id = c.id" \
  -o result.parquet
```

| Option | Description |
|:---|:---|
| `--alias [NAME]` | Name this branch for downstream reference |
| `--from [ALIAS[,ALIAS...]]` | Start a processor or fan-out branch. Accepts one or more comma-separated streaming aliases. Fan-out consumers use a single alias; multi-stream processors use multiple. |
| `--ref [ALIAS[,ALIAS...]]` | Materialized reference sources for lookup/join (preloaded into memory, comma-separated). Used with `--sql` for JOIN queries. |
| `--sql "[QUERY]"` | SQL query to execute on the upstream sources via DataFusion |
| `--merge` | UNION ALL processor: concatenates all `--from` streams in order. Requires at least 2 streaming sources. |

---

## Contributing
Adding a new database adapter or transformer? See the [Developer Guide](./EXTENDING.md).

## License
MIT
