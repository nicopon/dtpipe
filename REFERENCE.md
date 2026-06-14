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
| `--version` | Print version and exit |

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
| `--duck-init` | `"LOAD httpfs"` | **(DuckDB only)** SQL executed after connection open. Supports `@file`, `keyring://alias`, `${{ENV_VAR}}`, `${{keyring://alias}}` |

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
| `--duck-init` | `"LOAD azure"` | **(DuckDB only)** SQL executed after connection open. Supports `@file`, `keyring://alias`, `${{ENV_VAR}}`, `${{keyring://alias}}` |

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

---

## DuckDB Options

`--duck-init` runs SQL immediately after the DuckDB connection opens, before any query execution. It applies to all three DuckDB integration points:

| Component | Flag | When it runs |
|:---|:---|:---|
| Reader (`duck:`) | `--duck-init` | After connection open, before query |
| Writer (`duck:`) | `--duck-init` | After connection open, before schema initialization |
| SQL processor (`--sql`) | `--duck-init` | After connection open and built-in `SET` statements, before Arrow stream registration |

### Value resolution

The `--duck-init` value is resolved through a sequential pipeline before execution:

| Syntax | Resolution |
|:---|:---|
| `@/path/to/init.sql` | Load file content (replaces the full value) |
| `keyring://alias` | Load full block from OS keyring (replaces the full value) |
| `${{ENV_VAR}}` | Substitute environment variable inline |
| `${{keyring://alias}}` | Substitute OS keyring secret inline |

`@file` and `keyring://` (standalone) are mutually exclusive — the first match wins. Environment variable and inline keyring substitutions are applied to the result afterwards, so a keyring value can itself contain `${{VAR}}` placeholders.

```bash
# Inline SQL
--duck-init "INSTALL httpfs; LOAD httpfs; SET s3_region='eu-west-1';"

# From a file
--duck-init "@/path/to/init.sql"

# Full block from the OS keyring (credentials never appear in shell history)
dtpipe secret set s3-init "LOAD httpfs; SET s3_region='eu-west-1'; SET s3_access_key_id='AKIA...';"
--duck-init "keyring://s3-init"

# Inline keyring secrets (mix multiple secrets in one string)
--duck-init "LOAD httpfs; SET s3_region='${{keyring://s3-region}}'; SET s3_access_key_id='${{keyring://s3-key}}';"

# Environment variables
--duck-init "LOAD httpfs; SET s3_region='${{AWS_REGION}}'; SET s3_access_key_id='${{AWS_ACCESS_KEY_ID}}';"

# Composable: keyring value that itself contains env var placeholders
dtpipe secret set s3-init "LOAD httpfs; SET s3_region='${{AWS_REGION}}';"
--duck-init "keyring://s3-init"   # → loads block, then substitutes ${{AWS_REGION}}
```

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

---

## DAG Syntax

### Options

| Option | Description |
|:---|:---|
| `--alias NAME` | Name the current branch for downstream reference |
| `--from ALIAS[,ALIAS...]` | Streaming source(s). Fan-out uses a single alias; multi-stream processors use comma-separated aliases |
| `--ref ALIAS[,ALIAS...]` | Materialized reference source(s) — fully preloaded before query execution. Use for JOIN lookups |
| `--sql "QUERY"` | Inline SQL (DuckDB dialect: standard SQL, window functions, CTEs, JSON) |
| `--duck-init "SQL"` | SQL to run on the DuckDB SQL processor connection after open (e.g. `LOAD httpfs`). `@path` reads from a file |
| `--merge` | UNION ALL of all `--from` sources. Requires at least 2 streaming sources |

> **`--ref` is intentionally materialized.** Secondary sources declared via `--ref` are read fully
> into memory so the query engine can build a cost-based plan. Only the `--from` source streams.
> Pre-filter large lookup tables upstream before using them as `--ref`.

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

### Environment variable interpolation

Use `${{VAR_NAME}}` (double braces) to inject environment variables at runtime:

```yaml
main:
  input: "${{DB_CONN}}"
  provider-options:
    pg:
      query: "SELECT * FROM ${{TARGET_TABLE}}"
```

---

## Secret Management

```bash
# Store a connection string in the OS keyring (macOS Keychain, Windows Credential Manager, Linux Secret Service)
dtpipe secret set prod-db "pg:Host=...;Password=secret"

# Reference it anywhere a connection string is expected
dtpipe -i keyring://prod-db --query "SELECT * FROM users" -o users.parquet
```

| Command | Description |
|:---|:---|
| `dtpipe secret set <alias> <value>` | Store or update a secret |
| `dtpipe secret list` | List all stored aliases |
| `dtpipe secret get <alias>` | Print a secret value |
| `dtpipe secret delete <alias>` | Delete a specific secret |
| `dtpipe secret nuke` | Delete all stored secrets |

---

## Shell Completion

```bash
dtpipe completion --install   # installs for bash, zsh, or PowerShell
```

Restart your terminal (or `source ~/.zshrc`) to activate. Completion suggests providers
(`pg:`, `csv:`…), strategies (`Append`, `Upsert`…), and flag names based on cursor position.
