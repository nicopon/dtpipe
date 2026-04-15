# DtPipe

A self-contained CLI for streaming and anonymizing data across databases and file formats.

**DtPipe** is an **Arrow-native, Zero-Copy** data pipeline. It reads from a source, applies transformations in columnar batches, and writes to a destination — with no intermediate staging and minimal memory overhead. It targets automation and CI/CD scenarios where you need repeatable, high-performance data movement.

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
| **XML** | ✅ | — | `xml:` / `.xml` | `data.xml` |
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

#### 🔵 Core Essentials
The mandatory flags to define your pipeline.

| Flag | Example / Syntax | Description |
|:---|:---|:---|
| `-i`, `--input` | `-i data.csv` | **Required**. Source connection string or file path. |
| `-q`, `--query` | `-q "SELECT *..."` | **Required** (for SQL sources). The query to execute. |
| `-o`, `--output` | `-o target.parquet`| **Required**. Target connection string or file path. |
| `--dry-run` | `--dry-run 10` | Preview N rows in the terminal without writing. |

> **Example**: `dtpipe -i orders.csv -o orders.parquet`

#### 📥 Source (Reader) Configuration
Adjust how DtPipe pulls data from the source.

| Flag | Example / Syntax | Description |
|:---|:---|:---|
| `--connection-timeout`| `30` | Connection timeout in seconds. |
| `--query-timeout` | `0` | Timeout in seconds (0 = no timeout). |
| `--unsafe-query` | | Allow non-SELECT queries (e.g. EXEC stored procs). |
| **CSV** | `--csv-separator ","` | Set separator, header usage, or [explicit typing](#1-connection-strings). |
| **XML** | `--xml-path "//Item"`| Set record selector (XPath-like) and [namespaces/types](./COOKBOOK.md#parsing-large-xml-files-streaming). |
| **JSONL**| `--jsonl-indented` | Handle structured JSON lines encoding or formatting. |

#### 🧪 Data Transformation (Mutations)
Modify the content of your rows.

| Flag | Example / Syntax | Description |
|:---|:---|:---|
| `--fake` | `"Email:internet.email"`| Generate fake data. Syntax: `COL:Dataset.Method`. [See Full Dataset List](./COOKBOOK.md#anonymization-the-fakers). |
| `--mask` | `"Phone:###-****"` | Partial masking (`#` keeps, other replaces). |
| `--null` | `--null "Secret"` | Force a column value to NULL. |
| `--overwrite` | `"Status:Active"` | Set a static value for every row in a column. |
| `--format` | `"{First} {Last}"` | [.NET Composite Format](https://learn.microsoft.com/en-us/dotnet/standard/base-types/composite-formatting) using source column names. |
| `--compute` | `"row.Age > 18"` | Arbitrary JS logic. Returns the last expression or explicit `return`. |
| `--filter` | `"row.Val > 100"` | Drop rows where the JS expression returns `false`. |
| `--expand` | `"row.Items.map..."`| Multi-row expansion via JS (must return an array). |
| `--window-script` | | Stateful batch processing on `rows` array. See [Windowing Aggregations](./COOKBOOK.md#window-aggregations-stateful). |
| `--ignore-nulls` | | Global flag to skip transformations if input is NULL. |

#### 📐 Schema & Projection
Modify the structure of the output table.

| Flag | Example / Syntax | Description |
|:---|:---|:---|
| `--rename` | `"Old:New"` | Rename a column before writing. |
| `--project` | `"Id,Name"` | Keep ONLY these columns (Whitelist). |
| `--drop` | `"OldId"` | Remove specific columns (Blacklist). |

#### 📤 Target (Writer) Configuration
Control how data is persisted to the destination.

| Flag | Example / Syntax | Description |
|:---|:---|:---|
| `--strategy` | `Upsert` | `Append`, `Truncate`, `DeleteThenInsert`, `Recreate`, `Upsert`, `Ignore`. |
| `--table` | `--table "users"` | Override target table name (default is `export`). |
| `--key` | `"Id,Code"` | PKs for Upsert/Ignore. Auto-detected from DB if omitted. |
| `--insert-mode` | `Bulk` | `Standard` or `Bulk` (high-speed insert for PG/Ora/MSSQL). |
| `--auto-migrate` | | Automatically `ALTER TABLE` to add missing columns. |
| `--pre-exec` | `"TRUNCATE..."` | SQL script to run **before** the pipe starts (Database writers only). |
| `--post-exec` | `"ANALYZE..."` | SQL script to run **after** a successful transfer (Database writers only). |

#### ⚙️ Execution & Statistics
Performance tuning and automation.

| Flag | Example / Syntax | Description |
|:---|:---|:---|
| `--limit` | `--limit 1000` | Stop once N rows have been processed. |
| `--throttle` | `--throttle 500` | Limit throughput to N rows/sec (e.g. for API safety). |
| `--sampling-rate` | `0.1` | Probability (0.0 to 1.0) to include a row. |
| `--batch-size` | `10000` | Buffer size for columnar batching (default: 50,000). |
| `--no-stats` | | Hide progress bars and transfer statistics. |
| `--job` | `--job my.yaml` | Run a pipeline from a YAML configuration file. |
| `--metrics-path` | `metrics.json` | Write structured execution results to a JSON file. |

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
| `--sql "[QUERY]"` | SQL query to execute on the upstream sources. Default engine: DuckDB. |
| `--sql-engine [ENGINE]` | Choose engine for `--sql`: `duckdb` (default — standard SQL, no build step) or `datafusion` (experimental — zero-copy Arrow output, requires `./build_experimental.sh`). |
| `--merge` | UNION ALL processor: concatenates all `--from` streams in order. Requires at least 2 streaming sources. |

---

## Contributing
Adding a new database adapter or transformer? See the [Developer Guide](./EXTENDING.md).

## License
MIT
