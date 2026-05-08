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

> Use `keyring://alias` anywhere a connection string is expected. DtPipe resolves it from the OS keychain at runtime. Run `dtpipe secret set prod-db "pg:..."` to store a secret.

---

## Key Concepts

- **Arrow-native pipeline** — transformations run on columnar Arrow batches; when source and destination are both columnar (Parquet, DuckDB, Arrow), data passes through without row conversion.
- **Transformer pipeline** — `--fake`, `--mask`, `--compute`, `--filter`, and others chain in left-to-right order. Each flag group forms one pipeline step.
- **DAG execution** — multiple `--input` sources with `--from`, `--sql`, or `--merge` assemble an in-memory directed acyclic graph, executed concurrently.
- **YAML jobs** — any CLI command can be exported to a YAML job file with `--export-job` and replayed with `--job`. CLI flags override YAML values at runtime.
- **Secret management** — connection strings can be stored in the OS keyring and referenced as `keyring://alias`, keeping credentials out of scripts and command history.

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
