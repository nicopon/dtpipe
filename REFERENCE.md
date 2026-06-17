# DtPipe CLI Reference

## Synopsis

```
dtpipe -i SOURCE [OPTIONS] -o DESTINATION
dtpipe --job FILE [OVERRIDES]
```

---

## Core Options

| Flag | Description |
|:---|:---|
| `-i`, `--input SOURCE` | Source connection string or file path |
| `-o`, `--output DEST` | Target connection string or file path |
| `-q`, `--query SQL` | SQL query (required for database sources) |
| `--dry-run [N]` | Preview N rows without writing (default: 10) |
| `--job FILE`, `-j FILE` | Load a pipeline from a YAML job file |
| `--export-job FILE` | Serialize the current CLI pipeline to YAML and exit |
| `--alias NAME` | Name the current branch for DAG references |
| `--cursor COLUMN` | Cursor column for incremental loading (writer-side tracking) |
| `--state PATH` | State file path for cursor persistence (writer-side) |
| `--cursor-from VALUE` | Global override cursor value for this run |
| `--version` | Print version and exit |

---

## Secret Management

DtPipe stores secrets in the OS credential store (macOS Keychain, Windows Credential Manager, Linux Secret Service).

```bash
# Store a connection string
dtpipe secret set prod-db "pg:Host=...;Password=secret"

# Use it as a connection string
dtpipe -i keyring://prod-db --query "SELECT * FROM users" -o users.parquet

# Inline substitution within a larger string
dtpipe -i duck:memory --duck-init "LOAD httpfs; SET s3_access_key_id='${{keyring://aws-key}}';" ...
```

| Command | Description |
|:---|:---|
| `dtpipe secret set <alias> <value>` | Store or update a secret |
| `dtpipe secret list` | List all stored aliases |
| `dtpipe secret get <alias>` | Print a secret value |
| `dtpipe secret delete <alias>` | Delete a specific secret |
| `dtpipe secret nuke` | Delete all stored secrets |

Secrets can be referenced in two ways:
- **`keyring://alias`** — replaces the entire value (connection strings, `--duck-init`)
- **`${{keyring://alias}}`** — inline substitution within a string

> See [Value Resolution](#value-resolution) for the full resolution pipeline, supported contexts, and CLI/YAML differences.

---

## Value Resolution

DtPipe resolves string values through a sequential pipeline before use. The available mechanisms depend on the context.

### Resolution pipeline

1. **Full-value replacement** (mutually exclusive — first match wins):
   - `@/path/to/file` — load entire file content
   - `keyring://alias` — load full value from OS keyring

2. **Inline substitution** (applied to the result of step 1):
   - `${{ENV_VAR}}` — substitute an environment variable
   - `${{keyring://alias}}` — substitute an inline keyring secret
   - `${{cursor://path|default}}` — substitute a cursor value from a state file (with optional default value if the state file does not exist)

Steps are composable: a keyring block can itself contain `${{ENV_VAR}}` placeholders that are resolved afterwards.

### Compatibility matrix

Not all mechanisms are available in every context:

| Context | `@file` | `keyring://` | `${{ENV_VAR}}` | `${{keyring://…}}` | `${{cursor://…}}` |
|:---|:---:|:---:|:---:|:---:|:---:|
| Connection strings (`-i`, `-o`) | ✅ | ✅ | ✅ | ✅ | ✅ |
| `--duck-init` | ✅ | ✅ | ✅ | ✅ | ✅ |
| `--query` | ✅ | ✅ | ✅ | ✅ | ✅ |
| `--pre-exec`, `--post-exec`, etc. | ✅ | ✅ | ✅ | ✅ | ✅ |
| `--compute`, `--expand`, `--filter` scripts| ✅ | ✅ | ✅ | ✅ | ✅ |
| YAML job files (all values) | — | — | ✅ | ✅ | ✅ |

> [!IMPORTANT]
> **YAML Interpolation**: In YAML job files, `${{ENV_VAR}}` and `${{keyring://...}}` interpolations are applied to the raw YAML text *before* parsing, meaning they work on **all** values (including configuration properties that aren't normally resolved, like `batch-size` or `separator`).
> Full-value replacement (`@file` and `keyring://alias` without braces) only works for specific string fields that pass through the CLI resolver (connection strings, queries, hooks, and transformer scripts).

### Examples

```bash
# Connection string from keyring (full replacement)
dtpipe -i keyring://prod-db -q "SELECT * FROM users" -o users.parquet

# Inline keyring secrets in duck-init
dtpipe -i duck:memory \
  --duck-init "LOAD httpfs; SET s3_access_key_id='${{keyring://aws-key}}';" \
  -q "SELECT * FROM read_parquet('s3://bucket/data.parquet')" \
  -o result.csv

# Load SQL from a file
dtpipe -i pg:... -q @queries/export.sql -o export.parquet

# Environment variables
dtpipe -i "pg:Host=${{DB_HOST}};Database=${{DB_NAME}}" -q "SELECT 1" -o out.csv

# Composable: keyring value containing env var placeholders
dtpipe secret set s3-init "LOAD httpfs; SET s3_region='${{AWS_REGION}}';"
dtpipe -i duck:memory --duck-init "keyring://s3-init" ...
# → loads the block, then substitutes ${{AWS_REGION}}
```

---

## Providers

| Provider | Input | Output | Prefix | Requires query | Stdin/Stdout | Notes |
|:---|:---:|:---:|:---|:---:|:---:|:---|
| **DuckDB** | ✅ | ✅ | `duck:` | ✅ | — | `--duck-init` supported |
| **SQLite** | ✅ | ✅ | `sqlite:` | ✅ | — |
| **PostgreSQL** | ✅ | ✅ | `pg:` | ✅ | — |
| **Oracle** | ✅ | ✅ | `ora:` | ✅ | — |
| **SQL Server** | ✅ | ✅ | `mssql:` | ✅ | — |
| **CSV** | ✅ | ✅ | `csv:` / `.csv` | — | ✅ |
| **JsonL** | ✅ | ✅ | `jsonl:` / `.jsonl` | — | ✅ |
| **XML** | ✅ | — | `xml:` / `.xml` | — | ✅ |
| **Apache Arrow** | ✅ | ✅ | `arrow:` / `.arrow` | — | ✅ |
| **Parquet** | ✅ | ✅ | `parquet:` / `.parquet` | — | ✅ |
| **Data Gen** | ✅ | — | `generate:N` | — | — |
| **Null** | — | ✅ | `null:` | — | — |
| **Checksum** | — | ✅ | `checksum:` | — | — |

> For Stdin/Stdout: use `-` as the connection string (`csv:-`) or the bare provider name (`csv` = `csv:-`).

> **DuckDB dual role**: Beyond being a regular read/write provider, DuckDB also serves as the **internal SQL engine** for `--sql` branches in DAG pipelines (joins, unions, CTEs). See [DAG Syntax](#dag-syntax) and [Provider-Specific Options](#provider-specific-options) for details on `--duck-init`.

---

## Source (Reader) Options

| Flag | Example | Description |
|:---|:---|:---|
| `--connection-timeout` | `30` | Connection timeout in seconds |
| `--query-timeout` | `0` | Query timeout in seconds (0 = no timeout) |
| `--unsafe-query` | | Allow non-SELECT queries (stored procs, etc.) |
| `--csv-separator` | `","` | CSV field separator |
| `--csv-has-header` | | CSV has a header row (default: true) |
| `--encoding` | `ISO-8859-1` | Text file encoding |
| `--column-types` | `"Id:uuid,Qty:int32"` | Explicit column type declarations for text readers |
| `--auto-column-types` | | Infer column types from the first 100 rows |
| `--path` | `"//Product"` | XPath / JSON path for record selection (XML, JsonL) |
| `--duck-init` | `"LOAD httpfs"` | **(DuckDB only)** SQL executed after connection open. See [Value Resolution](#value-resolution) |

---

## Data Transformations

Transformers execute in left-to-right order. Consecutive flags of the same type are grouped
into one step; a different flag type starts a new step.

```
--fake A --fake B --format C --fake D
→  FakeTransformer(A, B) → FormatTransformer(C) → FakeTransformer(D)
```

| Flag | Syntax | Description |
|:---|:---|:---|
| `--fake` | `"Col:dataset.method"` | Generate fake data via [Bogus](https://github.com/bchavez/Bogus) |
| `--fake-locale` | `fr` | Locale for fake data generation |
| `--fake-seed` | `12345` | Global seed for reproducible random fakes (also acts as a base offset for deterministic row/column faking) |
| `--fake-seed-column` | `"UserId"` or `"Region,Branch"` | Column(s) used as a deterministic seed (same input -> same output). Supports comma-separated columns for composite seeds. |
| `--fake-seed-row` | | Row-index based deterministic mode (row N always gets the same values). Formerly `--fake-deterministic` (deprecated, throws error). |
| `--skip-null` | | Skip fake generation when the source value is null |
| `--mask` | `"Phone:###-****"` | Partial masking (`#` keeps original char, any other replaces) |
| `--null` | `"ColName"` | Force a column to NULL |
| `--overwrite` | `"Status:Active"` | Set a static value for every row in a column |
| `--format` | `"Display:{First} {Last}"` | [.NET Composite Format](https://learn.microsoft.com/en-us/dotnet/standard/base-types/composite-formatting) using column names as placeholders |
| `--compute` | `"Col:row.A * 2"` | JS expression. Implicit return for single expressions; use `return` with statements |
| `--compute-types` | `"Col:int32"` | Declare the CLR type of a computed or new column |
| `--filter` | `"row.Val > 100"` | Drop rows where the JS expression returns falsy |
| `--expand` | `"row.Tags.split(',')"` | Expand one row into multiple (must return an array of objects) |
| `--window-count` | `5` | Window size for stateful batch processing |
| `--window-script` | `"rows.map(...)"` | JS logic executed over a sliding window of rows |
| `--ignore-nulls` | | Skip transformations when the input cell is NULL |

---

## Schema & Projection

| Flag | Syntax | Description |
|:---|:---|:---|
| `--rename` | `"OldName:NewName"` | Rename a column |
| `--project` | `"Id,Name,Email"` | Keep only these columns (whitelist) |
| `--drop` | `"InternalId"` | Remove a column (blacklist) |

---

## Target (Writer) Options

| Flag | Syntax | Description |
|:---|:---|:---|
| `--strategy` | `Append` | Write strategy. One of: `Append`, `Truncate`, `DeleteThenInsert`, `Recreate`, `Upsert`, `Ignore` |
| `--table` | `"users"` | Override target table name (default: `export`) |
| `--key` | `"Id,Code"` | Primary key column(s) for `Upsert`/`Ignore`. Auto-detected from DB if omitted |
| `--insert-mode` | `Bulk` | `Standard` or `Bulk` (high-speed batch insert for PG, Oracle, MSSQL) |
| `--auto-migrate` | | `ALTER TABLE` to add missing columns automatically |
| `--strict-schema` | | Reject rows that don't match the target schema |
| `--no-schema-validation` | | Disable schema validation entirely |
| `--pre-exec` | `"TRUNCATE ..."` | SQL script to run **before** the pipeline starts |
| `--post-exec` | `"ANALYZE ..."` | SQL script to run **after** a successful transfer |
| `--on-error-exec` | `"..."` | SQL script to run on pipeline error |
| `--finally-exec` | `"..."` | SQL script to run regardless of outcome |
| `--prefix` | `"staging_"` | Table name prefix applied to all DB writers |
| `--duck-init` | `"LOAD azure"` | **(DuckDB only)** SQL executed after connection open. See [Value Resolution](#value-resolution) |

> `--pre-exec`, `--post-exec` etc. accept inline SQL or a file path (`@scripts/pre.sql` or a `.sql` file path).
> `--duck-init` runs on the DuckDB connection before reads or writes (unlike `--pre-exec` which runs on the target DB after connection).

---

## Execution & Statistics

| Flag | Syntax | Description |
|:---|:---|:---|
| `--limit` | `1000` | Stop after N rows have been processed |
| `--sampling-rate` | `0.1` | Row inclusion probability (0.0–1.0) |
| `--sampling-seed` | `12345` | Fixed seed for deterministic, reproducible sampling |
| `--batch-size` | `10000` | Rows per columnar batch (default: 50,000) |
| `--no-stats` | | Suppress progress bars and transfer statistics |
| `--metrics-path` | `metrics.json` | Write structured execution results to a JSON file |
| `--log` | `pipeline.log` | Write log output to a file |

---

## DAG Syntax

### Options

| Option | Description |
|:---|:---|
| `--alias NAME` | Name the current branch for downstream reference |
| `--from ALIAS[,ALIAS...]` | Streaming source(s). Fan-out uses a single alias; multi-stream processors use comma-separated aliases |
| `--ref ALIAS[,ALIAS...]` | Materialized reference source(s) — fully preloaded before query execution. Use for JOIN lookups |
| `--sql "QUERY"` | Inline SQL executed by the internal DuckDB engine (standard SQL, window functions, CTEs, JSON) |
| `--duck-init "SQL"` | SQL to run on the DuckDB SQL processor connection after open (e.g. `LOAD httpfs`). See [Value Resolution](#value-resolution) |
| `--merge` | UNION ALL of all `--from` sources. Requires at least 2 streaming sources |

> **`--ref` is intentionally materialized.** Secondary sources declared via `--ref` are read fully
> into memory so the query engine can build a cost-based plan. Only the `--from` source streams.
> Pre-filter large lookup tables upstream before using them as `--ref`.

> **SQL engine**: The `--sql` processor uses DuckDB internally — the same engine available as a read/write provider (`duck:`). This means all DuckDB SQL extensions and functions are available in `--sql` branches. Use `--duck-init` to load extensions before query execution. See [Provider-Specific Options](#provider-specific-options) for details.

### Canonical topologies

| Topology | Pattern |
|:---|:---|
| **Linear** | `-i {src} -o {dst}` |
| **Two independent sources** | `-i {src1} -o {dst1}  -i {src2} -o {dst2}` |
| **SQL (single source)** | `-i {src} --alias a  --from a --sql "SELECT * FROM a" -o {dst}` |
| **SQL JOIN (main + ref)** | `-i {main} --alias m  -i {ref} --alias r  --from m --ref r --sql "SELECT * FROM m JOIN r ON ..."` |
| **Merge (UNION ALL)** | `-i {srcA} --alias a  -i {srcB} --alias b  --from a,b --merge -o {dst}` |
| **Fan-out (tee)** | `-i {src} --alias s  --from s -o {dstA}  --from s -o {dstB}` |
| **Fan-out + SQL** | `-i {src} --alias s  --from s -o {dstA}  --from s --sql "SELECT ..."` |
| **Diamond** | `-i {src} --alias s  --from s --filter '...' --alias hi  --from s --filter '...' --alias lo  --from hi --ref lo --sql "..."` |
| **Join → fan-out** | `... --from m --ref r --sql "..." --alias j  --from j -o {dstA}  --from j -o {dstB}` |

---

## YAML Job File Schema

### Minimal example

```yaml
main:
  input: "pg:Host=localhost;Database=prod;Username=postgres"
  output: "output.parquet"
  provider-options:
    pg:
      query: "SELECT * FROM users"
```

### Full structure

```yaml
branch-name:
  # I/O
  input: "..."
  output: "..."

  # Engine controls (all optional — defaults shown)
  batch-size: 50000
  limit: 0
  sampling-rate: 1.0
  sampling-seed: null
  dry-run-count: 0
  metrics-path: null
  log-path: null
  prefix: null

  # DAG routing (optional)
  from: "upstream-alias"
  ref:
    - "ref-alias"

  # Transformer pipeline (optional)
  transformers:
    - fake:
        mappings:
          name: name.fullName
          email: internet.email
        options:
          locale: fr
          seed: 12345
          seed-column: id
          deterministic: true
          skip-null: true
    - null:
        mappings:
          phone: ~
    - compute:
        compute:
          - "FullName:row.Name + ' ' + row.Surname"
    - filter:
        filter: "row.Active"
    - project:
        mappings:
          id: ~
          name: ~
          email: ~

  # Provider-specific options (keyed by component name)
  provider-options:
    pg:                      # applies to pg reader
      query: "SELECT * FROM users"
    csv-writer:              # applies to csv writer only (suffix -writer or -reader)
      separator: ";"
      quote: "'"
```

### Transformer YAML reference

| Transformer | YAML key structure | Notes |
|:---|:---|:---|
| `fake` | `mappings: {col: dataset.method}` + `options: {locale, seed, seed-column, deterministic, skip-null}` | |
| `null` | `mappings: {col: ~}` | Value is ignored |
| `overwrite` | `mappings: {col: value}` | |
| `mask` | `mappings: {col: pattern}` | `#` keeps, any other char replaces |
| `format` | `mappings: {col: "{A} {B}"}` | .NET composite format |
| `compute` | `compute: ["col:expression", ...]` | JS expressions list |
| `filter` | `filter: "expression"` | JS boolean expression |
| `expand` | `expand: "expression"` | Must return an array of objects |
| `window` | `mappings: {script: "..."}` + `options: {count: N}` | |
| `project` | `mappings: {col: ~}` | Listed columns are kept |
| `drop` | `mappings: {col: ~}` | Listed columns are removed |
| `rename` | `mappings: {OldName: NewName}` | |

### Environment variable and secret interpolation

Environment variables and secrets use the `${{...}}` syntax. See [Value Resolution](#value-resolution) for the full compatibility matrix and CLI/YAML differences.

---

## Provider-Specific Options

### DuckDB

DuckDB serves a dual role in dtpipe: it is both a standard read/write **provider** (`duck:`) and the **internal SQL engine** powering `--sql` branches. The `--duck-init` flag applies to all three integration points:

| Component | Flag | When it runs |
|:---|:---|:---|
| Reader (`duck:`) | `--duck-init` | After connection open, before query |
| Writer (`duck:`) | `--duck-init` | After connection open, before schema initialization |
| SQL processor (`--sql`) | `--duck-init` | After connection open and built-in `SET` statements, before Arrow stream registration |

> `--pre-exec` / `--post-exec` run SQL **on the target database after writes**; `--duck-init` runs **on the DuckDB connection before reads or queries**. They serve different purposes and can be combined.

In YAML job files, use the `provider-options` block keyed by component name:

```yaml
provider-options:
  duck:           # reader
    duck-init: "LOAD httpfs; SET s3_region='eu-west-1';"
  duck-writer:    # writer
    duck-init: "keyring://azure-init"
```

For a `--sql` branch, pass `--duck-init` alongside `--from` and `--sql` on the same branch:

```bash
dtpipe -i events.parquet --alias ev \
  --from ev \
  --duck-init "LOAD httpfs; SET s3_region='${{keyring://s3-region}}';" \
  --sql "SELECT * FROM ev JOIN read_parquet('s3://bucket/ref.parquet') r ON ev.id = r.id" \
  -o result.parquet
```

## Incremental Loading

DtPipe supports cursor-driven incremental loading to transfer only new or updated records since the last successful run.

### Overview

Incremental loading uses two key mechanisms:
1. **State Persistence**: The CLI tracks the maximum value observed in a designated cursor column and writes it to a JSON state file after a successful execution.
2. **Query Interpolation**: The SQL query uses the `${{cursor://path|default}}` resolver to filter for records greater than (or equal to) the last saved value.

### CLI Flags

- `--cursor COLUMN` — Specifies the column to observe for tracking the maximum value (e.g. `updated_at` or `id`).
- `--state PATH` — Specifies the path to the state file where the cursor metadata will be saved.
- `--cursor-from VALUE` — Global override to temporarily force a starting cursor value for the current run, ignoring the state file.

### State File Format

The state file is stored as a simple, human-readable JSON file:
```json
{
  "version": 1,
  "cursor": {
    "column": "updated_at",
    "value": "2026-06-15T23:59:59.000",
    "type": "datetime"
  },
  "last_run": {
    "started_at": "2026-06-16T02:00:00Z",
    "completed_at": "2026-06-16T02:03:42Z",
    "rows_transferred": 1234,
    "status": "success"
  }
}
```

### DAG Validation

To prevent concurrent writes or corrupted cursor states, DtPipe enforces that **no two writers may share the same state file**. If the DAG validator detects duplicate state files across branches, pipeline execution will fail immediately.

---

## Shell Completion

```bash
dtpipe completion --install   # installs for bash, zsh, or PowerShell
```

Restart your terminal (or `source ~/.zshrc`) to activate. Completion suggests providers
(`pg:`, `csv:`…), strategies (`Append`, `Upsert`…), and flag names based on cursor position.
