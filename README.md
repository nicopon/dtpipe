# DtPipe

A self-contained CLI for streaming, transforming, and anonymizing data across databases and file formats.

DtPipe reads from a source, applies row and columnar transformations in batches, and writes to a destination with no intermediate staging. It is designed for automation and CI/CD workflows where repeatable, observable data pipelines matter.

---

### 📖 [Recipes & Examples → COOKBOOK.md](./COOKBOOK.md)  ·  [Full CLI Reference → REFERENCE.md](./REFERENCE.md)

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

---

## Quick Start

### Export a database table

```bash
dtpipe \
  -i "pg:Host=localhost;Database=prod;Username=postgres" \
  --query "SELECT * FROM users" \
  -o users.parquet
```

### Anonymize before export

```bash
dtpipe \
  -i "pg:Host=localhost;Database=prod;Username=postgres" \
  --query "SELECT * FROM users" \
  --fake "email:internet.email" \
  --fake "name:name.fullName" \
  --mask "phone:###-****" \
  --null "ssn" \
  -o anonymized_users.parquet
```

### In-memory SQL join

```bash
dtpipe \
  -i orders.parquet --alias orders \
  -i customers.csv --alias customers \
  --from orders --ref customers \
  --sql "SELECT o.*, c.name FROM orders o JOIN customers c ON o.customer_id = c.id" \
  -o result.parquet
```

### Run from a YAML job file

```bash
# Generate a reusable job file from any CLI command
dtpipe -i "pg:..." --query "SELECT * FROM users" --fake "email:internet.email" \
       -o users.parquet --export-job nightly.yaml

# Run it (with optional overrides)
dtpipe --job nightly.yaml --limit 1000
```

### Incremental loading (cursor-driven)

```bash
# First run: Full load, initializes the state file with the max updated_at cursor value
dtpipe \
  -i "pg:Host=localhost;Database=prod" \
  --query "SELECT * FROM users WHERE updated_at >= '${{cursor://state.json|1970-01-01}}'" \
  -o "sqlite:Data Source=dw.db" \
  --table "users" \
  --strategy Recreate \
  --key id \
  --cursor "updated_at" \
  --state "state.json"

# Subsequent runs: Incremental load, only retrieves newer records
dtpipe \
  -i "pg:Host=localhost;Database=prod" \
  --query "SELECT * FROM users WHERE updated_at > '${{cursor://state.json}}'" \
  -o "sqlite:Data Source=dw.db" \
  --table "users" \
  --strategy Upsert \
  --key id \
  --cursor "updated_at" \
  --state "state.json"
```

---


## Providers

DtPipe detects providers from file extensions (`.csv`, `.parquet`…) or explicit prefixes.
Explicit prefixes are recommended to avoid ambiguity.

| Provider | Input | Output | Prefix |
|:---|:---:|:---:|:---|
| **DuckDB** | ✅ | ✅ | `duck:` |
| **SQLite** | ✅ | ✅ | `sqlite:` |
| **PostgreSQL** | ✅ | ✅ | `pg:` |
| **Oracle** | ✅ | ✅ | `ora:` |
| **SQL Server** | ✅ | ✅ | `mssql:` |
| **CSV** | ✅ | ✅ | `csv:` / `.csv` |
| **JsonL** | ✅ | ✅ | `jsonl:` / `.jsonl` |
| **XML** | ✅ | — | `xml:` / `.xml` |
| **Apache Arrow** | ✅ | ✅ | `arrow:` / `.arrow` |
| **Parquet** | ✅ | ✅ | `parquet:` / `.parquet` |
| **Data Gen** | ✅ | — | `generate:N` |
| **Null** | — | ✅ | `null:` |
| **Checksum** | — | ✅ | `checksum:` |

> Use `keyring://alias` anywhere a connection string is expected. DtPipe resolves it from the OS keychain at runtime. Run `dtpipe secret set prod-db "pg:..."` to store a secret.

> DtPipe's native providers cover common sources and destinations. For everything
> else — object storage (S3, GCS, Azure Blob), Iceberg, MySQL/MariaDB, HTTP APIs,
> spatial formats — DuckDB's extension ecosystem serves as a connector multiplier.
> Load an extension with `--duck-init` on a DuckDB reader, writer, or `--sql` branch
> to reach any source or destination DuckDB supports natively. No additional adapters required.

---

## Key Concepts

Transformers (`--fake`, `--mask`, `--compute`, `--filter`, …) chain left-to-right. When source and destination are both columnar (Parquet, DuckDB, Arrow), data flows through without row conversion. Multiple `--input` sources with `--from`, `--sql`, or `--merge` form a DAG executed concurrently. Any CLI command can be saved to a YAML job file with `--export-job` and replayed with `--job`.

**DuckDB is a remarkable engine** — fast, self-contained, with a rich SQL dialect and a thriving extension ecosystem. DtPipe uses it as a first-class component precisely because of that quality. When DuckDB alone covers your use case, use it directly. DtPipe adds value in the scenarios it wasn't designed for: anonymizing or masking data in transit, routing one source to multiple destinations concurrently, writing to target databases with strategies like upsert, auto-migrate, or bulk insert, reading from Oracle, SQL Server, or XML streams, and packaging pipelines as repeatable YAML jobs with integrated secret management. DtPipe contributes the pipeline layer; DuckDB contributes the SQL engine.

---

## Documentation

| Document | Contents |
|:---|:---|
| [REFERENCE.md](./REFERENCE.md) | Full CLI option tables, YAML job schema, DAG topology reference, secret management |
| [COOKBOOK.md](./COOKBOOK.md) | End-to-end scenarios: anonymization, schema transforms, SQL joins, DAG pipelines, YAML automation |
| [EXTENDING.md](./EXTENDING.md) | Adding adapters (readers/writers) and transformers |

---

### Shell Autocompletion (experimental)

```bash
dtpipe completion --install
```

Restart your terminal (or `source ~/.zshrc`) to activate.

---

## Contributing

See [EXTENDING.md](./EXTENDING.md) for the adapter and transformer patterns.

## License
MIT
