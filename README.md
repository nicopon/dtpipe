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
# Full load on first run (state file does not exist → uses default 1970-01-01)
dtpipe \
  -i "pg:Host=localhost;Database=prod" \
  --query "SELECT * FROM users WHERE updated_at >= '${{cursor://state.json|1970-01-01}}'" \
  -o "sqlite:Data Source=dw.db" --table "users" --strategy Recreate --key id \
  --cursor "updated_at" --state "state.json"

# Subsequent runs: cursor is resolved from state.json, only newer rows are fetched (switch to Upsert)
# See REFERENCE.md#incremental-loading for full flag table and state file format,
# and COOKBOOK.md#incremental-loading for the complete recipe.
```

### See what a pipeline will do, before it does it

```bash
# Runs the pipeline over 10 source rows with the writer neutralised: same reader, same
# transformers, same bridges. A step that expands or aggregates shows its new row count.
dtpipe -i "pg:Host=localhost;Database=prod" --query "SELECT * FROM orders" \
  --mask email -o "parquet:out.parquet" --dry-run 10

# Materialise a point and iterate on it without reading the source again
dtpipe -i "oracle:..." --query "SELECT * FROM big_table" --checkpoint -o null:
dtpipe --from-checkpoint <key> --compute "total=price*qty" -o csv:out.csv
```

Checkpoints live in `.dtpipe/`, are encrypted, and expire. See
[REFERENCE.md#sample-mode-and-materialisation](./REFERENCE.md#sample-mode-and-materialisation).

### Database Resilience (Retry Policy)

```bash
# Automatically retry transient database connection and timeout errors with Polly exponential backoff
dtpipe \
  -i "pg:Host=localhost;Database=prod" \
  --query "SELECT * FROM sales" \
  -o "mssql:Server=remote_db;Database=analytics" \
  --retry
```

### Start AI Agent MCP Server (Model Context Protocol)

```bash
# Launch native MCP server over STDIO for AI assistants (Cursor, Claude Desktop, Antigravity)
dtpipe mcp
```
Includes tools: `dry-run`, `suggest-pipeline`, `list-cursors`, `execute-yaml-job`, and schema discovery.

### Interactive AI Agent Mode

```bash
# Launch the interactive AI agent (supports Ollama & OpenAI backends)
dtpipe agent --provider openai --api-key "sk-..."

# Or run a one-shot mission
dtpipe agent "Inspect csv:invoices.csv, anonymize email, and output to jsonl:users.jsonl"
```
> Features local Ollama auto-discovery, official OpenAI SDK integration, a full-screen session surface on a real terminal (steps, detail, plan/DAG, transcript, an input line between turns — `--no-tui` for scrollback instead), Spectre DAG topology rendering, and 1-click YAML pipeline export.

---


## Providers

DtPipe detects providers from file extensions (`.csv`, `.parquet`…) or explicit prefixes — explicit prefixes are recommended to avoid ambiguity. **Full table with capabilities, query requirements and Stdin/Stdout support: [REFERENCE.md#providers](./REFERENCE.md#providers).**

| Provider family | Examples | Prefix |
|:---|:---|:---|
| **Databases** | PostgreSQL, MySQL, SQLite, DuckDB, SQL Server, Oracle | `pg:`, `mysql:`, `sqlite:`, `duck:`, `mssql:`, `ora:` |
| **Files** | CSV, JsonL, Parquet, Arrow, XML | `csv:`, `jsonl:`, `parquet:`, `arrow:`, `xml:` |
| **Object storage** | S3-compatible, Azure Blob | `s3://bucket/key.parquet`, `azure://container/blob.csv` |
| **Special** | Data Gen (source), Null/Checksum (sink) | `generate:N`, `null:`, `checksum:` |

> Use `keyring://alias` anywhere a connection string is expected. DtPipe resolves it from the OS keychain at runtime. Run `dtpipe secret set prod-db "pg:..."` to store a secret.

> For object storage (S3, GCS, Azure Blob), Iceberg, MySQL/MariaDB, HTTP APIs, spatial formats — use DuckDB's extension ecosystem as a connector multiplier. Load an extension with `--duck-init` on any DuckDB reader, writer, or `--sql` branch — no additional adapter required. See [REFERENCE.md#provider-specific-options](./REFERENCE.md#provider-specific-options) and [COOKBOOK.md#duckdb-extensions-and-cloud-storage](./COOKBOOK.md#duckdb-extensions-and-cloud-storage).

---

## Key Concepts

*   **Providers — where data comes from and goes to.** DtPipe reads from databases (`pg:`, `mysql:`, `mssql:`, `ora:`, `sqlite:`, `duck:`) and files (`csv:`, `parquet:`, `jsonl:`…), and writes to the same set. The provider is inferred from the file extension or an explicit prefix. See [Providers](./REFERENCE.md#providers) for the full list.
*   **Transformers — what happens in between.** Flags like `--fake`, `--mask`, `--compute`, `--filter`, `--rename` are chained left-to-right on every row. Example: anonymize, then derive a column, then filter: `--fake "email:internet.email" --compute "fullName:row.first+' '+row.last" --filter "row.age>=18"`. See [COOKBOOK.md](./COOKBOOK.md#schema-transformations).
*   **DAG pipelines — combine or split streams.** Use `--alias` to name a source, then `--from` / `--ref` / `--sql` / `--merge` to join, union or fan-out without temp files. Typical uses: enrich a stream with a lookup table, or write one source to two sinks at once. See [DAG Syntax](./REFERENCE.md#dag-syntax) and [DAG recipes](./COOKBOOK.md#dag-pipelines-multi-source).
*   **YAML jobs — make it repeatable.** Any CLI pipeline can be saved with `--export-job pipeline.yaml` and replayed with `dtpipe --job pipeline.yaml` (CI/CD, cron, overrides via CLI). See [YAML Job Schema](./REFERENCE.md#yaml-job-file-schema).
*   **Secrets — keep credentials out of shell history.** `dtpipe secret set prod-db "pg:..."` stores in the OS keychain; reference as `keyring://prod-db` or `${{keyring://alias}}` anywhere a connection string is expected. See [Secret Management](./REFERENCE.md#secret-management).

> **Why DuckDB?** DuckDB is fast, self-contained and speaks rich SQL — DtPipe embeds it as its SQL engine (`--sql`, `duck:`). When DuckDB alone covers your use case, use it directly. DtPipe adds value where DuckDB stops: anonymization/masking in transit, concurrent fan-out, database write strategies (upsert, auto-migrate, bulk), Oracle/SQL Server/XML sources, and repeatable YAML jobs with secret management.

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
