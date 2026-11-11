# DtPipe CLI Reference

> **Docs map:** [README.md](./README.md) — quick start · [docs/](./docs/README.md) — guides ·
> [EXTENDING.md](./EXTENDING.md) — new adapters/transformers · [CLAUDE.md](./CLAUDE.md) —
> contributor internals.

## Table of Contents

- [Synopsis](#synopsis)
- [Core Options](#core-options)
- [Secret Management](#secret-management)
- [Value Resolution](#value-resolution)
- [Providers](#providers)
- [Source (Reader) Options](#source-reader-options)
- [Data Transformations](#data-transformations)
- [Schema & Projection](#schema--projection)
- [Target (Writer) Options](#target-writer-options)
- [Execution & Statistics](#execution--statistics)
- [DAG Syntax](#dag-syntax)
- [YAML Job File Schema](#yaml-job-file-schema)
- [Provider-Specific Options](#provider-specific-options)
- [Incremental Loading](#incremental-loading)
- [Sample Mode and Materialisation](#sample-mode-and-materialisation)
- [Shell Completion](#shell-completion)
- [Studio](#studio-dtpipe-studio)
- [Model Context Protocol (MCP) Server](#model-context-protocol-mcp-server)
- [AI Agent Subcommand](#ai-agent-subcommand-dtpipe-agent)
- [Agent Guardrails](#agent-guardrails-isqlsafetypolicy--iapprovalgate)

## Synopsis

```
dtpipe -i SOURCE [OPTIONS] -o DESTINATION
dtpipe --job FILE [OVERRIDES]
```

This page lists the flags in common use. `dtpipe --help` lists every flag the binary carries,
grouped by provider and transformer; `dtpipe providers` lists the providers.

---

## Core Options

| Flag | Description |
|:---|:---|
| `-i`, `--input SOURCE` | Source ADO.NET connection string or file path |
| `-o`, `--output DEST` | Target ADO.NET connection string or file path |
| `-q`, `--query SQL` | SQL query (required for database sources) |
| `--dry-run N` | Run the pipeline over N source rows without writing |
| `--job FILE`, `-j FILE` | Load a pipeline from a YAML job file; given once per command line. The file is the source of the branches: [only engine controls](#execution--statistics) may be given beside it |
| `--export-job FILE` | Serialize the current CLI pipeline to YAML and exit |
| `--alias NAME` | Name the current branch for DAG references |
| `--cursor COLUMN` | Cursor column for incremental loading (writer-side tracking) |
| `--state PATH` | State file path for cursor persistence (writer-side) |
| `--cursor-from VALUE` | Global override cursor value for this run |
| `--version` | Print version and exit |

---

## Secret Management

DtPipe stores secrets in the OS credential store (macOS Keychain, Windows Credential Manager, Linux
Secret Service).

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

> See [Value Resolution](#value-resolution) for the full resolution pipeline, supported contexts,
> and CLI/YAML differences.

---

## Value Resolution

DtPipe resolves string values through a sequential pipeline before use. The available mechanisms
depend on the context.

### Resolution pipeline

1. **Full-value replacement** (mutually exclusive — first match wins):
   - `@/path/to/file` — load entire file content
   - `keyring://alias` — load full value from OS keyring

2. **Inline substitution** (applied to the result of step 1):
   - `${{ENV_VAR}}` — substitute an environment variable
   - `${{keyring://alias}}` — substitute an inline keyring secret
   - `${{cursor://path|default}}` — substitute a cursor value from a state file (with optional
     default value if the state file does not exist)

Steps are composable: a keyring block can itself contain `${{ENV_VAR}}` placeholders that are
resolved afterwards.

### Compatibility matrix

Not all mechanisms are available in every context:

| Context | `@file` | `keyring://` | `${{ENV_VAR}}` | `${{keyring://…}}` | `${{cursor://…}}` |
|:---|:---:|:---:|:---:|:---:|:---:|
| Connection strings (`-i`, `-o`) | ✅ | ✅ | ✅ | ✅ | ✅ |
| `--duck-init` | ✅ | ✅ | ✅ | ✅ | ✅ |
| `--query` | ✅ | ✅ | ✅ | ✅ | ✅ |
| `--pre-exec`, `--post-exec`, etc. | ✅ | ✅ | ✅ | ✅ | ✅ |
| `--compute`, `--expand`, `--filter` scripts| ✅ | ✅ | ✅ | ✅ | ✅ |
| YAML job files (text values) | — | — | ✅ | ✅ | ✅ |

> [!IMPORTANT]
> **YAML Interpolation**: In YAML job files, `${{ENV_VAR}}`, `${{keyring://...}}` and
> `${{cursor://...}}` interpolations run through the same resolver engine as the CLI (env → keyring
> → cursor, in that order), applied to each scalar as it is read. They reach every **text** value —
> `input`, `output`, `query`, and every `provider-options` entry, `separator` included. They do not
> reach a key the loader converts to another type: `batch-size: ${{SIZE}}` fails with the token
> quoted back, because the token is what `int` is asked to parse. Unresolved variables are left
> verbatim. Full-value replacement (`@file` and `keyring://alias` without braces) only works for
> specific string fields that pass through the CLI resolver (connection strings, queries, hooks,
> and transformer scripts).
>
> `--export-job` writes the reference, never the value it resolves to — on both entry points.

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
| **MySQL / MariaDB** | ✅ | ✅ | `mysql:` | ✅ | — | Bulk needs `local_infile=ON`; see [MySQL](#mysql) |
| **Oracle** | ✅ | ✅ | `ora:` | ✅ | — |
| **SQL Server** | ✅ | ✅ | `mssql:` | ✅ | — |
| **CSV** | ✅ | ✅ | `csv:` / `.csv` | — | ✅ |
| **JsonL** | ✅ | ✅ | `jsonl:` / `.jsonl` | — | ✅ |
| **XML** | ✅ | — | `xml:` / `.xml` | — | ✅ |
| **Apache Arrow** | ✅ | ✅ | `arrow:` / `.arrow` | — | ✅ |
| **Parquet** | ✅ | ✅ | `parquet:` / `.parquet` | — | ✅ |
| **S3 object storage** | ✅ | ✅ | `s3://` / `s3a://` | — | — | See [Object storage](#object-storage-s3-azure) |
| **Azure Blob** | ✅ | ✅ | `azure://` / `az://` | — | — | See [Object storage](#object-storage-s3-azure) |
| **Data Gen** | ✅ | — | `generate:N` | — | — |
| **Null** | — | ✅ | `null:` | — | — |
| **Checksum** | — | ✅ | `checksum:` | — | — |

> `dtpipe providers` lists what the binary carries, as a table on stderr. `dtpipe providers --format
> json` writes the same inventory to stdout for a script: one object per component with `name`,
> `kind` (`reader`, `writer` or `stream-processor`), `stdio` and `category`.

> For Stdin/Stdout: use `-` as the connection string (`csv:-`) or the bare provider name (`csv` =
> `csv:-`).

> **DuckDB dual role**: Beyond being a regular read/write provider, DuckDB also serves as the
> **internal SQL engine** for `--sql` branches in DAG pipelines (joins, unions, CTEs). See
> [DAG Syntax](#dag-syntax) and [Provider-Specific Options](#provider-specific-options) for details
> on `--duck-init`.

### Database Connection Strings (ADO.NET format)

DtPipe is powered by .NET database providers, which expect standard **ADO.NET connection strings**
rather than the connection URIs typically used in the Python/data ecosystem (e.g. by SQLAlchemy or
psycopg2).

* **ADO.NET format**: A list of semicolon-separated `Key=Value;` pairs (e.g.
  `Host=localhost;Database=mydb;`).
* **Python URIs** (e.g. `postgresql://user:pass@host:port/db`) are **not natively supported** by the
  underlying database drivers and must be translated.

For a comprehensive catalog of all connection string options, parameters, and database drivers,
visit **[connectionstrings.com](https://www.connectionstrings.com/)**.

#### Conversion Reference Table

If you are coming from Python or SQLAlchemy, use this translation guide to build your `-i` / `-o`
strings:

| Database | Prefix | Python URI Format | ADO.NET Format (DtPipe) |
|:---|:---|:---|:---|
| **PostgreSQL** | `pg:` | `postgresql://user:pass@host:port/db` | `pg:Host=host;Port=port;Database=db;Username=user;Password=pass` |
| **MySQL** | `mysql:` | `mysql+pymysql://user:pass@host:port/db` | `mysql:Server=host;Port=3306;Database=db;User ID=user;Password=pass` |
| **SQL Server** | `mssql:` | `mssql+pyodbc://user:pass@host/db` | `mssql:Server=host;Database=db;User Id=user;Password=pass;TrustServerCertificate=True` |
| **SQLite** | `sqlite:` | `sqlite:///path/to/file.db` | `sqlite:Data Source=path/to/file.db` |
| **Oracle** | `ora:` | `oracle+oracledb://user:pass@host:port/?service_name=service` | `ora:Data Source=host:port/service;User Id=user;Password=pass` |

---

## Source (Reader) Options

| Flag | Example | Description |
|:---|:---|:---|
| `--connection-timeout` | `30` | Connection timeout in seconds |
| `--query-timeout` | `0` | Query timeout in seconds (0 = no timeout) |
| `--unsafe-query` | | Allow non-SELECT queries (stored procs, etc.) |
| `--csv-separator` | `","` | CSV field separator |
| `--csv-has-header` | `false` | Whether the CSV has a header row (default: `true`). Takes `true` or `false`: see [Boolean flags](#boolean-flags) |
| `--encoding` | `ISO-8859-1` | Text file encoding |
| `--column-types` | `"Id:uuid,Qty:int32"` | Explicit column type declarations for text readers. A hint naming no type is refused — see [Type hints](#type-hints) |
| `--auto-column-types` | | Infer column types from a sample of rows (size is `--max-sample`, default varies by reader) |
| `--max-sample` | `500` | Rows sampled to infer schema or column types for a text reader (CSV, JSONL, XML); `0` disables inference. Default: 100 for CSV, 1000 for JSONL and XML |
| `--schema-save` | `people.dtschema` | Save the schema a text reader discovered, to reuse it with `--schema-load` |
| `--schema-load` | `people.dtschema` | Take the column types from a saved schema instead of inferring them |
| `--path` | `"//Product"` | XPath / JSON path for record selection (XML, JsonL) |
| `--xml-namespaces` | `"p=urn:orders"` | **(XML only)** Namespace prefixes used in `--path`, as `prefix=uri`, comma-separated |
| `--ora-fetch-size` | `1048576` | **(Oracle only)** Fetch size in bytes |
| `--throttle` | `100` | **(`generate:` only)** Rows per second, to stand in for a slow source |
| `--query` / `-q` | `"SELECT ..."` | SQL query executed by database readers (or a path to a `.sql` file) |
| `--table` / `-t` | `"users"` | Source table name — auto-builds `SELECT * FROM <table>` when no `--query` is given (database readers). A qualified name is split on the dot and each part quoted the way the source engine spells it: `eshop.customers` reads `"eshop"."customers"` on PostgreSQL, `` `eshop`.`customers` `` on MySQL, `[eshop].[customers]` on SQL Server. Spell the value yourself — `'"an.awkward.name"'` — to keep a dot inside a single identifier |
| `--duck-init` | `"LOAD httpfs"` | **(DuckDB only)** SQL executed after connection open. See [Value Resolution](#value-resolution) |

---

## Data Transformations

Transformers execute in left-to-right order. Consecutive flags of the same type are grouped
into one step; a different flag type starts a new step. An option applies to the whole step it
sits in, wherever it falls among the step's flags.

```
--fake A --fake B --fake-locale fr --format C --fake D
→  FakeTransformer(A, B, locale fr) → FormatTransformer(C) → FakeTransformer(D)
```

- **One step holds one value per option.** Giving the same option twice in a step is accepted when
  both values agree and refused when they differ. To run two `--fake` groups with different
  locales or seeds, put another transformer's flag between them, or use a separate branch.
- **A step's mappings read the columns the step receives.** `--compute "b:1" --compute
  "c:row.b + 1"` fails with `Column 'b' not found`: both are one step, and `b` does not exist yet.
  Put the dependent mapping in a later step — another transformer's flag between the two, or a
  new branch (`--from`). `--fake`, `--format` and `--expand` mappings may use what an earlier
  mapping of the same step produced.
- **`--skip-null` belongs to `--fake` and to `--compute`.** It binds to the step it follows and is
  refused anywhere else, since both claim it.

| Flag | Syntax | Description |
|:---|:---|:---|
| `--fake` | `"Col:dataset.method"` | Generate fake data via [Bogus](https://github.com/bchavez/Bogus). **A column that does not exist in the incoming rows is created**, so `--fake` both anonymizes what is there and synthesizes what is not. **A value that is not a known faker path is refused**, naming what the dataset has — `Col:firstName` is an error, `Col:name.firstName` is the path |
| `--fake-locale` | `fr` | Locale for fake data generation |
| `--fake-seed` | `12345` | Global seed for reproducible random fakes (also acts as a base offset for deterministic row/column faking) |
| `--fake-seed-column` | `"UserId"` or `"Region,Branch"` | Column(s) used as a deterministic seed (same input -> same output). Supports comma-separated columns for composite seeds. |
| `--fake-seed-row` | | Row-index based deterministic mode (row N always gets the same values) |
| `--skip-null` | | Skip fake generation when the source value is null. Also a `--compute` option: it binds to the step it follows |
| `--mask-skip-null`, `--overwrite-skip-null` | | Leave a null value null instead of masking or overwriting it |
| `--format-skip-null` | | Write null instead of the template when every column it reads is null |
| `--mask` | `"Phone:###-****"` | Partial masking (`#` keeps original char, any other replaces) |
| `--null` | `"ColName"` | Force a column to NULL |
| `--overwrite` | `"Status:Active"` | Set a static value for every row in a column. The value is text and the column becomes a string; for a typed constant use `--compute "Col:12345" --compute-types "Col:int32"` |
| `--format` | `"Display:{First} {Last}"` | [.NET Composite Format](https://learn.microsoft.com/en-us/dotnet/standard/base-types/composite-formatting) using column names as placeholders |
| `--compute` | `"Col:row.A * 2"` | JS expression, returned as is; a body of statements needs `return`. A result of `undefined` is an error, `null` is an empty value |
| `--compute-types` | `"Col:int32"` | Declare the CLR type of a computed or new column. Same vocabulary as `--column-types` — see [Type hints](#type-hints) |
| `--filter` | `"row.Val > 100"` | Drop rows where the JS expression returns falsy. Columns are read off `row`; a name the schema does not carry is an error, not an empty result |
| `--expand` | `"row.Tags.split(',').map(t => ({ ...row, Tag: t }))"` | Expand one row into multiple. Must return an array of **row objects**: an array of plain values is refused |
| `--expand-types` | `"Tag:string"` | Declare a column `--expand` creates. Undeclared keys are not in the output schema and are dropped |
| `--window-count` | `5` | Accumulate N rows, then run `--window-script` over them |
| `--window-key` | `"CustomerId"` | Run `--window-script` each time this column's value changes |
| `--window-script` | `"rows.map(...)"` | JavaScript over the accumulated rows (`rows`); must return an array of rows |

#### Type hints

`--column-types`, `--compute-types` and `--expand-types` share one vocabulary. A hint that names no
type is **refused**, rather than leaving the column at whatever type it already had.

A `datetime` column is a wall clock and a `datetimeoffset` column an instant. On every machine, a
text with an offset under `datetime` reads as its UTC value, and a text with no offset under
`datetimeoffset` reads as UTC. Every reader gives a `datetimeoffset` in UTC, since Arrow keeps no
offset per value: `03:04:05+02:00` reads as `01:04:05+00:00`, the same instant. A `string` column
keeps the text as written.

A `decimal`, `double` or `float` column reads a text with `.` as its decimal point and no group
separator: `1,5` and `1,000` are refused, so a file written with `--csv-decimal-separator ,` reads
back only as `string`. A number past the range of a `double` or a `float` is refused too; the
spelled-out `NaN` and `Infinity` read as what they say.

A decimal reads at the scale its source declares, and with no trailing zeros when it declares none,
whichever way the pipeline goes: `4.2` in a `DECIMAL(10,2)` reads as `4.20`, and `10.50` under
`--column-types v:decimal`, which declares no width, as `10.5`. A column with no declared width
holds 18 decimals: a value with more is refused, and one with only zeros past them reads without
them. A `string` column keeps the text as written.

Inference applies one rule to a number on every text reader, over the whole sample. A column is a
`double` when a `double` writes each value back with the same digits. Otherwise it is a `decimal`
when each value is written as one, and a `string` when not, so a number is never rounded to fit: a
40-digit integer stays text. Declare `decimal` for a column that needs exact arithmetic, since
`4.29` is inferred as a `double`.

| Type | Accepted spellings |
|:---|:---|
| `string` | `string`, `str`, `text` |
| `int` | `int`, `int32`, `integer` |
| `long` | `long`, `int64` |
| `float` | `float`, `float32`, `single` |
| `double` | `double`, `float64`, `number` |
| `decimal` | `decimal`, `numeric`, `money` |
| `bool` | `bool`, `boolean` |
| `DateTime` | `datetime`, `date` |
| `DateTimeOffset` | `datetimeoffset`, `timestamp`, `datetimetz` |
| `Guid` | `guid`, `uuid` |

---

## Schema & Projection

| Flag | Syntax | Description |
|:---|:---|:---|
| `--rename` | `"OldName:NewName"` | Rename a column |
| `--project` | `"Id,Name,Email"` | Keep only these columns (whitelist) |
| `--drop` | `"InternalId"` | Remove a column (blacklist) |

### Collection columns

A PostgreSQL array (`int[]`, `text[]`) and a DuckDB `LIST` move like any other column. A NULL
list, an empty list and a NULL element inside a list stay distinct end to end.

| Target | Result |
|:---|:---|
| Parquet | a native Parquet `LIST` |
| Arrow | an Arrow `ListType` column |
| CSV, and any text target | the list rendered as JSON — `[10,20,30]` |
| A database column that is not itself a collection | the same JSON text |

A nested object — a JSONL record with an object in one of its fields, an Arrow struct — follows the
same rule wherever the target has no composite type of its own: it is written as JSON text
(`{"severity":"info"}`), in the target's own text column. The declared schema decides, and the
rendering never changes it: Parquet still writes a real `LIST` for a column typed as one, and only
falls back to JSON text for a shape it was given no element type for.

To land a nested object as something other than text — a PostgreSQL `jsonb`, a typed column per
field — name it in the pipeline rather than relying on the target:

```bash
dtpipe -i events.jsonl \
  --compute "severity:row.payload.severity" --compute-types "severity:string" --drop payload \
  -o "pg:Host=…" --table events
```

**A list of text into Parquet is refused rather than approximated.** Writing it would need
definition levels, which the Parquet library exposes for value types only; without them a NULL list
and an empty one cannot be told apart. Project the column instead: `array_to_json(col)::text`.

A **source** column whose type the reader cannot map at all — a PostgreSQL `point` — is refused
when the schema is built, rather than read as something else. Cast it in your query: `col::json`,
`array_to_json(col)::text`.

A DuckDB `STRUCT`, `MAP` or `LIST` read through `duck:` arrives as a real composite, like any other
source: the reader takes DuckDB's own Arrow columns. So a field is addressable in the query
(`SELECT payload.severity AS sev`), and the JSON rendering above applies only where it applies to
everything else — at a target with no composite type of its own. `duck:` and `parquet:` have one
that dtpipe does not write, so they refuse the column instead of writing it as something else: see
[Types a writer cannot write](#types-a-writer-cannot-write). A `LIST` of scalars stays a `LIST` in
DuckDB.

---

## Target (Writer) Options

| Flag | Syntax | Description |
|:---|:---|:---|
| `--strategy` | `Append` | Write strategy. One of: `Append`, `Truncate`, `DeleteThenInsert`, `Recreate`, `Upsert`, `Ignore`. **Database targets only** — a file or object writer replaces its target and refuses the flag rather than accepting one it cannot honour |
| `--table` | `"users"` | Override target table name (default: `export`) |
| `--csv-header` | `false` | **(CSV only)** Whether to write the header row (default: `true`). Takes `true` or `false`: see [Boolean flags](#boolean-flags) |
| `--csv-quote` | `"'"` | **(CSV only)** Quote character (default: `"`) |
| `--csv-null-value` | `"NULL"` | **(CSV only)** Text written for a null value (default: empty) |
| `--csv-date-format` | `"dd/MM/yyyy"` | **(CSV only)** Format of a date (default: `yyyy-MM-dd`) |
| `--csv-timestamp-format` | `"yyyy-MM-dd HH:mm:ss"` | **(CSV only)** Format of a timestamp (default: `yyyy-MM-dd HH:mm:ss.ffffff`) |
| `--arrow-compression` | `zstd` | **(Arrow only)** Buffer compression: `none` (default), `lz4` or `zstd` |
| `--key` | `"Id,Code"` | Primary key column(s) for `Upsert`/`Ignore`. Auto-detected from DB if omitted |
| `--insert-mode` | `Bulk` | `Standard` or `Bulk` (high-speed batch insert for PG, Oracle, MSSQL) |
| `--auto-migrate` | | `ALTER TABLE` to add missing columns automatically. See [Schema modes](#schema-modes) |
| `--strict-schema` | | Abort the run on any schema incompatibility. See [Schema modes](#schema-modes) |
| `--no-schema-validation` | | Skip target inspection entirely. See [Schema modes](#schema-modes) |
| `--pre-exec` | `"TRUNCATE ..."` | SQL script to run **before** the pipeline starts |
| `--post-exec` | `"ANALYZE ..."` | SQL script to run **after** a successful transfer |
| `--on-error-exec` | `"..."` | SQL script to run on pipeline error |
| `--finally-exec` | `"..."` | SQL script to run regardless of outcome |
| `--prefix` | `"staging_"` | Table name prefix applied to all DB writers |
| `--duck-init` | `"LOAD azure"` | **(DuckDB only)** SQL executed after connection open. See [Value Resolution](#value-resolution) |

> `--pre-exec`, `--post-exec` etc. accept inline SQL or a file path (`@scripts/pre.sql` or a `.sql`
> file path). `--duck-init` runs on the DuckDB connection before reads or writes (unlike
> `--pre-exec` which runs on the target DB after connection).

### Identifier casing

An identifier — the target table, and every column — is handed to the engine the way that engine
reads it **unquoted**, so the name it stores is the name any other tool can type. Quoting is
reserved for what actually needs it: a reserved word, or a character the parser would not accept.

| Engine | `--table stock_moves` creates | `--table StockMoves` creates |
|:---|:---|:---|
| Oracle | `STOCK_MOVES` | `STOCKMOVES` |
| PostgreSQL | `stock_moves` | `stockmoves` |
| MySQL, SQL Server, SQLite, DuckDB | `stock_moves` | `StockMoves` |

The rule is one rule; the outcomes differ because the engines do. Oracle folds up, PostgreSQL
folds down, the rest store what they are given and compare case-insensitively.

**To keep an exact spelling, quote the value yourself** — `--table '"StockMoves"'` creates
`StockMoves` on every engine, and it is then addressable only in quotes, from dtpipe and from
anything else.

> Quoting a name to preserve the case as typed is what dtpipe used to do for the table, and only
> the table — columns already folded. On Oracle it produced `"stock_moves"`, which
> `SELECT * FROM stock_moves` cannot reach (`ORA-00942`): a table dtpipe alone could address.

### Types a writer cannot write

A writer writes each column under the type its target format has for it — a DuckDB `DATE` stays a
`DATE` in DuckDB, Parquet and PostgreSQL, an unsigned integer stays unsigned where the format has
one. A type the format does not carry is **refused, never converted** and never written as text:
PostgreSQL has no unsigned integer, MySQL no type for an instant, Oracle no time of day. The
refusal comes before the target is touched — no file is created, no table emptied — names every
column at once, and gives the step that casts each one to a type the writer accepts:

```unknown
The pg writer cannot write these 2 columns as they arrive, and nothing was written:
  'qty': uint16; accepted instead: int32.
  'total': uint64; accepted instead: decimal(20, 0).
Cast each column to an accepted type in a SQL step before the writer: give this branch --alias src, and write to the target from the step:
  --from src --sql "SELECT * REPLACE (CAST(qty AS INTEGER) AS qty, CAST(total AS DECIMAL(20,0)) AS total) FROM src" -o <target>
```

The cast is yours to choose: a wider type changes what the target holds. `--dry-run` refuses in
the same words.

Every writer that reads values answers from one list, so a type it does not name is refused before
the target, never failed at its first value: an interval, a union, and the large, view and run-end
encodings of Arrow, along with a half-precision float. Only `arrow:` writes them, since it reads no
value. Beyond that list, `duck:` refuses a `STRUCT`, a `MAP`, a duration and a list of anything but
a scalar, which its driver cannot append. `parquet:` refuses a column of nulls, a struct, a map, a
fixed-size list, a duration and a list of anything but a number or a boolean. Each gives the cast
that brings the column to a type the writer accepts.

A decimal column created for a source that declared no width holds what such a column carries:

| Writer | Column |
|:---|:---|
| `pg:` | `NUMERIC` |
| `ora:` | `NUMBER` |
| `duck:`, `mysql:`, `mssql:` | `DECIMAL(38,18)`: 20 integer digits, 18 decimals. Past them, a value is refused |
| `parquet:` | `DECIMAL(38,18)`, like a declared one |
| `sqlite:` | `REAL`, which refuses a value a double would round: see [Values a type cannot hold](#values-a-type-cannot-hold) |

A table that already exists keeps its columns, and a value with more decimals than its column keeps
is refused rather than rounded: see [Values a type cannot hold](#values-a-type-cannot-hold).
`--no-schema-validation` does not inspect the target, so nothing checks the value there.

Read back, a column created `DECIMAL(38,18)` declares that width: `1.23456` written to it reads as
`1.234560000000000000`. A `--sql` step is the same, since DuckDB declares what it returns.

A DuckDB 128-bit integer (`HUGEINT`) is written as one only by the `duck:` writer, over its whole
range, `-2^127` to `2^127-1`. That writer does not write the unsigned one (`UHUGEINT`), and refuses
it with `CAST(col AS VARCHAR)` as the step. Every other writer refuses both, with the same step,
because no number type it has keeps 39 digits. `csv:` and `jsonl:` write the digits as a number.

### Types a reader cannot read

A reader reads natively each type it knows and **refuses the rest when it opens**, before the first
row, naming every such column at once with its type and the expression, in the source's own SQL,
that reads it. It never decodes a type it does not know as text. The PostgreSQL reader refuses
`timetz`, `interval`, the network, geometric, range and text-search types, `money`, `oid` and the
`reg*` types, composites, and arrays of any of them:

```
The pg reader cannot read these 2 columns, and nothing was read:
  'ip': inet; select it as: ip::text AS ip
  'price': money; select it as: price::numeric AS price
Write each expression in the query where the column is selected.
```

A reader that produces Arrow itself (`duck:`, `--sql`, `arrow:`) hands over a dictionary-encoded
column decoded: a DuckDB `ENUM` is text from there on, and a table recreated from one has a
`VARCHAR`. It refuses no other type when it opens, since `arrow: → arrow:` carries any type
untouched. A step that has to read a value of a type with no reading stops there, naming the column.

`--dry-run` refuses in the same words.

### Values a type cannot hold

A column's type can be readable and one of its values not. That value **stops the run with code 1**,
naming where it stands, the column, its type and the way out. Nothing replaces it, and the refusal
never quotes it:

```
Record 12: column 'valid_to' (timestamp) holds infinity, which no date or time type here can hold. Select it as: valid_to::text AS valid_to
```

| Value | Where | Way out |
|:---|:---|:---|
| `infinity` or `-infinity` under `date`, `timestamp`, `timestamptz`, in an array too; a year outside 1 to 9999 | `pg:` reader | `col::text` |
| `NaN` or more digits than a `decimal` holds under `numeric`; a `text[]` of two dimensions | `pg:` reader | `col::text` |
| A decimal past the width it reads under | The database readers | A cast in the source query |
| A value past the width of its Arrow type | Any reader | A cast in the source query, or a `string` column |
| A text that does not read as the declared or inferred type: `1,5` under `decimal`, more digits than a `decimal` holds, or more decimals than its column's width | `csv:`, `jsonl:`, `xml:` | Correct the cell, or a `string` column |
| A value of another kind than its `list` or `struct` column | `jsonl:` | A `string` column, which keeps its JSON text |
| `NaN`, `Infinity`, `-Infinity`: JSON has no number for them | `jsonl:` writer | Filter it out, or read the column as text |
| `NaN`: SQLite stores it as NULL | `sqlite:` writer | Filter it out, or read the column as text |
| A decimal a double would round, under a `REAL` column | `sqlite:` writer | A `string` column |
| A decimal with more decimals than the existing column it goes to keeps: `1.23456` into a `DECIMAL(18,3)` | The database writers, under a strategy that keeps the table | Round it in a SQL step, or widen the column |

In a sample that holds a scalar beside an object, the column is a `struct` with a `_value` field,
and the scalar is that field's value: it is read, not refused.

### Schema modes

Before writing to a database target, dtpipe compares the source schema with the target's and acts
on the difference. Three independent flags decide how, and they describe **four regimes plus an
off switch** — there is no `--schema-evolution` flag, and these are the names for what the
combinations already do.

| Flags | Regime | A column the target does not have | A type mismatch |
|:---|:---|:---|:---|
| *(none)* | **discard** | reported, then skipped — the run continues | reported, the run continues |
| `--strict-schema` | **freeze** | aborts the run | aborts the run |
| `--auto-migrate` | **evolve** | `ALTER TABLE ADD COLUMN`, then written | reported, the run continues |
| `--auto-migrate --strict-schema` | **evolve, with a net** | added, then the target is re-inspected | aborts, unless migrating resolved it |
| `--no-schema-validation` | **off** | the target is never inspected — neither question is asked | |

`--no-schema-validation` **cannot be combined** with either of the other two: it returns before the
inspection both of them need, so the run would silently do the opposite of what half the line asked
for. The combination is refused with an error naming the two flags.

Each run states the regime it validated under, in one line, whatever the outcome:

```
Schema mode: freeze (--strict-schema) - any incompatibility aborts the run.
```

That line is not decoration. On a target that happens to be compatible, **discard** and **freeze**
produce exactly the same output while offering opposite guarantees, and only the regime tells them
apart.

**The regime applies only where the target is inspected.** A file or object target is replaced
wholesale, and `--strategy Recreate` drops and rebuilds the table from the source schema, so there
is nothing to compare and no mode line is printed. `Recreate` still looks at the table it replaces,
once, to warn when that table has columns the source lacks or a primary key the run does not
recreate. The key of the new table is the one `--key` names, never the old table's; indexes,
defaults and storage settings of the old table are not carried over.

### File globs

A local input whose **file name** carries `*` or `?` is read as one stream over every file it
matches, in ordinal path order:

```bash
dtpipe -i "daily/*.csv" -o events.parquet
dtpipe -i "exports/part-?.jsonl" -o "pg:Host=…" --table events
```

- **The schema is the first file's.** A later file with different columns stops the run and names
  it, rather than dropping or misaligning them.
- **Matching nothing is an error**, not an empty read.
- **The wildcard stays in the file name.** A pattern spanning directories is not supported; object
  storage, which globs through DuckDB, does support one (`s3://bucket/dt=*/part-*.parquet`).
- Every file adapter gets this — CSV, JSONL, Parquet, XML — and the reader keeps its own options
  and its columnar path where it has one.

### Object storage (`s3://`, `azure://`)

Object-storage locations are first-class inputs and outputs. They go through the DuckDB engine
already in the process, so globs, range requests and multipart uploads work without any extra
dependency. Reads stream: no object is downloaded to a temp file first. Writes stage the rows
before uploading (a Parquet footer is only known at the end), spilling to the temp directory if
the output exceeds memory.

```bash
dtpipe -i s3://bucket/events/2026-08-*.parquet --s3-region eu-west-1 -o events.csv
dtpipe -i sales.csv -o azure://reports/sales.parquet --azure-connection-string "${{keyring://azure-conn}}"
```

| Provider | Schemes | Options |
|:---|:---|:---|
| `s3` | `s3://`, `s3a://` | `--s3-endpoint`, `--s3-region`, `--s3-access-key`, `--s3-secret-key`, `--s3-session-token`, `--s3-url-style` |
| `azure` | `azure://`, `az://` | `--azure-connection-string`, `--azure-account-name`, `--azure-account-key`, `--azure-sas`, `--azure-endpoint` |

- **Format** comes from the extension, through a closed map: `.parquet`, `.csv`, `.tsv`, `.json`,
  `.jsonl`, `.ndjson`. Anything else is an error naming the supported set — no content sniffing.
  For a format outside that map, use `--duck-init` + `--query` with the matching DuckDB function.
- **Credentials** accept the usual value forms (`${{keyring://alias}}`, `${{ENV_VAR}}`, `@file`).
  Leave the key pair unset to use the ambient credential chain (env, shared config, instance
  profile). Secrets are scoped to their bucket/container, so a read and a write in the same
  pipeline can use different credentials.
- **Writes replace the target key.** Object storage has no append or upsert, so `--strategy` is
  refused rather than ignored. The upload is issued once the pipeline completes: a failed run
  leaves the existing object untouched rather than replacing it with a partial one.
- **Reads glob natively**: `s3://bucket/dt=*/part-*.parquet` reads every match. A local path globs
  too, over one directory — see [File globs](#file-globs).
- `https://`, `gs://` and other schemes are **not** claimed by any provider. Reach them through
  the DuckDB engine (`--duck-init "INSTALL httpfs; …"` + `read_parquet(…)` / `COPY … TO …`).
- Object-storage connection strings count as network access for the agent guardrails: a job using
  one needs `--allow-network` (see
  [Agent Guardrails](#agent-guardrails-isqlsafetypolicy--iapprovalgate)).
- The `httpfs` / `azure` extensions are installed from DuckDB's extension repository on first use,
  so the host needs access to it once (or an extension directory already holding them).

---

## Execution & Statistics

| Flag | Syntax | Description |
|:---|:---|:---|
| `--limit` | `1000` | Read at most N rows from the source, then stop reading. Bounds the **read**, so a filter downstream yields fewer than N rather than topping the count back up |
| `--sampling-rate`, `--sample-rate` | `0.1` | Row inclusion probability (0.0–1.0) |
| `--sampling-seed`, `--sample-seed` | `12345` | Fixed seed for deterministic, reproducible sampling |
| `--batch-size` | `10000` | Rows per batch (default: 32,768) |
| `--max-batch-bytes` | `268435456` | Soft byte cap per read batch: a batch flushes when either `--batch-size` rows **or** this many bytes accumulate, whichever comes first. `0` (default) disables the byte cap. Bounds memory when rows carry large text/blob columns. Estimate is approximate — the row that crosses the bound is kept. Applies to the wire-decode readers (PostgreSQL `COPY`, and the ADO readers: MySQL, Oracle, SQL Server, SQLite) and the row→columnar bridge. |
| `--no-stats` | | Suppress progress bars and transfer statistics |
| `--metrics-path` | `metrics.json` | Write structured execution results to a JSON file |
| `--log` | `pipeline.log` | Write log output to a file. The log belongs to the process, so a job file has no key for it |
| `--strict-bindings` | | Non-zero exit on the two binding faults that are otherwise tolerated: a flag nothing declares that consumes no value (one followed by a value is refused by the parser either way), and a job file's unrecognized `provider-options` key (a warning otherwise). A flag that exists but is not an option of the branch's component is refused with or without it |
| `--dry-run` | `10` | Run the pipeline over N source rows with the writer neutralised — see [Sample Mode](#sample-mode-and-materialisation) |
| `--ignore-nulls` | | Skip transformations when the input cell is NULL |
| `--session` | `mission-7` | Name the session materialised artefacts belong to |
| `--checkpoint` | `stage1` | Materialise this branch's output in the session store |
| `--from-checkpoint` | `<key>` | Resume this branch from a stored checkpoint instead of its input |
| `--contract-save` | `orders.json` | Write the schema this branch produces to a contract file — see [Contracts](#the-schema-a-pipeline-produces-is-a-contract) |
| `--from-contract` | `orders.json` | Take this branch's source schema from a contract, with no rows — see [Contracts](#the-schema-a-pipeline-produces-is-a-contract) |

**Beside `--job`**, a command line gives no branch: `-i`, `-o`, `--from`, `--alias`, a transformer
or a provider option is refused, naming the flag, since the file holds the branches and the flag
would change nothing. The flags that apply to every branch of the loaded job are `--limit`,
`--batch-size`, `--max-batch-bytes`, `--sampling-rate`, `--sampling-seed`, `--prefix`, `--cursor`,
`--state`, `--metrics-path`, `--dry-run`, `--session`, `--bind-compression`,
`--bind-compression-level` and `--edges-only`; `--checkpoint`, `--from-checkpoint`,
`--contract-save` and `--from-contract` are keys of the file, not overrides. The flags of the whole
command line (`--no-stats`, `--log`, `--strict-bindings`, `--export-job`, `--cursor-from`,
`--bind-input`, `--bind-output`) are given beside it as anywhere.

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
| `--merge` | UNION ALL of all `--from` sources. Requires at least 2 streaming sources that share one schema: same columns, names and types, else the run is refused before any row moves |

> **`--ref` is intentionally materialized.** Secondary sources declared via `--ref` are read fully
> into memory so the query engine can build a cost-based plan. Only the `--from` source streams.

> **An alias list is comma-separated; repeating a flag never adds to it.** Repetition has one
> meaning in this grammar — `-i` and `--from` open a new branch — so `--from a --from b`
> declares two branches, not one branch reading two sources. Write `--ref a,b`; `--ref a --ref b`
> is refused.

> **How many aliases `--from` accepts depends on the processor.** `--merge` takes several;
> `--sql` streams exactly one and materializes the rest through `--ref`, so `--from a,b --sql`
> is refused with the rewrite to apply.

> **A branch is readable only while it has no output of its own.** A branch publishes its stream to
> the other branches exactly when it declares no `output:`; one that writes to a target publishes
> nothing, and naming it in `from`/`ref` is refused at validation. So a table that must be both
> written *and* joined on takes two branches — one that produces it, one that writes it — plus the
> branch that joins. See [DAG pipelines](./docs/guides/dag.md).

#### Binding a fragment's boundary

| Option | Description |
|:---|:---|
| `--bind-input ALIAS=LOCATION[,ALIAS=LOCATION...]` | Wire a real location into a branch's `arrow:-` input |
| `--bind-output ALIAS=LOCATION[,ALIAS=LOCATION...]` | Wire a real location into a branch's `arrow:-` output |
| `--bind-compression none\|lz4\|zstd` | Compress the Arrow IPC stream of every `arrow:-` output |
| `--bind-compression-level N` | Level of that codec; needs `--bind-compression`. Default: the codec's own |
| `--edges-only` | Refuse every reader and writer that is not an `arrow:-` edge, and run `--sql` without external access |

A fragment written by `dtpipe split` keeps the boundary its text names — a branch whose input or
output is exactly `arrow:-` — and a single shell pipe wires a two-process chain without either
flag. Beyond one edge per direction (a fragment with two links, or two producers feeding a join),
name where each `arrow:-` actually is for this run:

```
dtpipe --job anonymiser.yaml --bind-input anonymiser=/run/42/in --bind-output anonymiser=/run/42/out
dtpipe --job join.yaml --bind-input orders=/run/42/orders,customers=/run/42/customers
```

Each value is a comma-separated `alias=location` list — a path, or `pipe://<name>` for a
named pipe (`arrow:` connects to it as a client only; something else, such as a distributed
pipeline node, must already be serving it) — never an adapter prefix: the boundary stays an Arrow
IPC stream. A location ending in `.arrow` or `.arrowfile` is refused, since the `arrow:` reader and
writer would read that extension as a request for file format instead. Binding a branch whose input
or output is not exactly `arrow:-`, an unknown alias, or the same `(branch, direction)` pair twice
are refused by name; combining either flag with `--export-job` is refused, since a binding names a
location specific to this run.

`--bind-compression` and `--bind-compression-level` apply to the outputs written `arrow:-`, whether
`--bind-output` binds them or they stay on stdout, and to no other output; a branch's own
`compression` option, when its text sets one, wins. A reader detects compression by itself, so
nothing is given on the reading side. The two flags never enter a job's text. An unknown codec or
a level that is not an integer is refused before the run starts. The `arrow` writer's `compression`
option takes the same three values and is refused at build time when it holds anything else.

`--edges-only` is what a pipeline node gives a runner's fragment, whose host the fragments of every
run share. The job's text may name `arrow:-` as an input or an output and nothing else; a location
bound by `--bind-input` or `--bind-output` must be `-`, `pipe://<name>` or a file of the working
directory named bare, which may also hold a branch's `contract-save`. A branch that sets
`checkpoint`, `from-checkpoint`, `from-contract`, `metrics-path`, `cursor` or `state` is refused
too, each by its branch, before any row moves. Transformers and processors stay: `--sql` reads the
branches it names in `--from` and `--ref` and nothing else, with DuckDB's `enable_external_access`
off and its configuration locked, so a file, `ATTACH`, an extension or a `SET` is refused, and
`--duck-init` is refused outright. An `@file` value (a `--compute`, `--filter` or `--expand` script
read from a file) is refused as well. DuckDB still spills to its temporary directory under the
working directory.

#### Implicit branch-split rules

Branches are separated implicitly while walking the arguments. One pure function
(`BranchSplitDecision`) decides; the triggers are:

| Token | Splits when |
|:---|:---|
| `-i` / `--input` | an input was already seen in the current branch |
| `--from` | a `--from` or `--input` was already seen — the first `--from` in a fresh branch stays in the current branch |
| *positional SQL text* | a bare (non-flag) token starts a new branch that becomes the `--sql` processor branch |

Neither `--sql` nor boolean processor flags (e.g. `--merge`) trigger a split, and neither does
`--job`: it is given once per command line, `-j` being the same flag, and a second one is refused
with both files named. A job file holds as many branches as it declares.
> Pre-filter large lookup tables upstream before using them as `--ref`.

#### Duplicate flags are an error

A flag of the branch itself — the DAG flags (`--alias`, `--ref`, `-o`) and the engine controls
(`--limit`, `--batch-size`, `--cursor`, `--checkpoint`, …) — may appear **at most once per
branch**, whichever stage it is written in: `-o` opens the writer stage, not a new branch. Any
other non-repeatable flag may appear **at most once per stage** — stages being reader (before the
first transformer), pipeline (transformer scope) and writer (after `-o`) — so a flag both the
reader and the writer take binds each side. A repeated flag is a hard error that names both
values, not a silent last-wins:

```
# ERROR: --alias given twice in one branch: 'orders' and 'report'
dtpipe -i src.csv --alias orders -o out.csv --alias report

# ERROR: --sql provided twice in the same branch
dtpipe -i src.csv --alias s --from s --sql "SELECT * FROM s" --sql "SELECT count(*) FROM s" -o out.csv

# OK: a reader flag and a writer flag of the same name = two independent bindings
dtpipe -i in.csv --csv-separator ";" -o out.csv --csv-separator "|"
```

#### Boolean flags

An option that is **off** by default is a switch: write the flag alone to turn it on (`--no-stats`,
`--merge`). An option that is **on** by default takes a value, because the flag alone would change
nothing: `--csv-header false`, `--csv-has-header false`. The value is `true` or `false`, after a
space. `--csv-header=false` is refused, a value other than `true`/`false` is refused at the flag,
and so is a `true` or `false` written after a switch. The help marks the first kind `<true|false>`.

Global scalar flags (`--log`, `--metrics-path`, …) may appear only once per command line.
The SQL query of a branch must come from exactly one source: an explicit `--sql "<query>"`
**or** one positional query — combining both is an error.

Transformer options are the exception: consecutive flags of one transformer form one step, so
an option may repeat inside it (`--fake A --fake-seed-row --fake B --fake-seed-row`) as long as
every occurrence carries the same value. Different values are refused — see
[Data Transformations](#data-transformations).

> **SQL engine**: The `--sql` processor uses DuckDB internally — the same engine available as a
> read/write provider (`duck:`). This means all DuckDB SQL extensions and functions are available in
> `--sql` branches. Use `--duck-init` to load extensions before query execution. See
> [Provider-Specific Options](#provider-specific-options) for details.

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
    pg-reader:
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
  prefix: null

  # DAG routing (optional)
  from: "upstream-alias"
  ref:
    - "ref-alias"

  # Transformer pipeline (optional)
  transformers:
    - type: fake
      mappings:
        name: name.fullName
        email: internet.email
      options:
        locale: fr
        seed: 12345
        seed-column: id
        skip-null: true
    - type: "null"
      mappings:
        phone: ~
    - type: compute
      mappings:
        FullName: "row.Name + ' ' + row.Surname"
    - type: filter
      mappings:
        "row.Active": ""
    - type: project
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
| `fake` | `mappings: {col: dataset.method}` + `options: {locale, seed, seed-column, seed-row, skip-null}` | A mapped column that the incoming rows do not have is **created**; a value that is not a known faker path is **refused** |
| `null` | `mappings: {col: ~}` | Value is ignored |
| `overwrite` | `mappings: {col: value}` | A mapped column the incoming rows do not carry is **created** (string, appended); `skip-null` has nothing to skip on one |
| `mask` | `mappings: {col: pattern}` | `#` keeps, any other char replaces |
| `format` | `mappings: {col: "{A} {B}"}` | .NET composite format |
| `compute` | `mappings: {col: expression}` | JS expression per column |
| `filter` | `mappings: {expression: ~}` | JS boolean expression, carried by the key |
| `expand` | `mappings: {expression: ~}` | Carried by the key; must return an array of objects |
| `window` | `options: {count: N, script: "..."}` | Mapping values are taken as script lines when no `script` option is set |
| `project` | `options: {project: "a,b", drop: "c", rename: "Old:New"}` | One transformer covers all three; `--export-job` emits this shape and its round-trip is tested |

An `options:` key is the **property name**, which is not always the command-line flag: the compute
output type is `--compute-types` on the command line and `compute-types` in YAML, but DuckDB's init
SQL is `--duck-init` and `init-sql`. A key that matches no property is refused by name, and names
the one that would have worked. `--export-job` emits the binding form and its round-trip is tested,
so it is the authority on any shape this table leaves open.

### Environment variable and secret interpolation

Environment variables and secrets use the `${{...}}` syntax. See
[Value Resolution](#value-resolution) for the full compatibility matrix and CLI/YAML differences.

What stands between the braces is a variable name (letters, digits and underscores, not starting
with a digit) or a reference (`scheme://…`). Any other expression is refused, so a name the
environment holds under another spelling is never silently left as written.

---

## Provider-Specific Options

### DuckDB

DuckDB serves a dual role in dtpipe: it is both a standard read/write **provider** (`duck:`) and the
**internal SQL engine** powering `--sql` branches. The `--duck-init` flag applies to all three
integration points:

| Component | Flag | When it runs |
|:---|:---|:---|
| Reader (`duck:`) | `--duck-init` | After connection open, before query |
| Writer (`duck:`) | `--duck-init` | After connection open, before schema initialization |
| SQL processor (`--sql`) | `--duck-init` | After connection open and built-in `SET` statements, before Arrow stream registration |

> `--pre-exec` / `--post-exec` run SQL **on the target database after writes**; `--duck-init` runs
> **on the DuckDB connection before reads or queries**. They serve different purposes and can be
> combined.

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

In a YAML job, the same branch nests both under `provider-options.sql` (not `provider-options.duck`
— that key belongs to the reader/writer):

```yaml
ev:
  input: "events.parquet"
enrich:
  from: "ev"
  provider-options:
    sql:
      query: "SELECT * FROM ev JOIN read_parquet('s3://bucket/ref.parquet') r ON ev.id = r.id"
      duck-init: "LOAD httpfs; SET s3_region='${{keyring://s3-region}}';"
  output: "result.parquet"
```

### MySQL

The `mysql:` prefix is **required**. Unlike file providers, MySQL is never inferred from the
connection string's content: `Server=host;Database=db` is character-for-character what a SQL
Server connection string looks like, so any content heuristic would claim connections belonging
to the other provider. Same deliberate choice as `pg:`, `mssql:`, and `ora:`.

```bash
dtpipe -i "pg:Host=localhost;Database=prod;Username=postgres;Password=pass" \
       --query "SELECT * FROM orders" \
       -o "mysql:Server=localhost;Port=3306;Database=analytics;User ID=root;Password=pass" \
       --table orders --strategy Upsert --key order_id
```

#### Insert modes

| `--insert-mode` | Mechanism | Requirement |
|:---|:---|:---|
| `Bulk` (default) | `MySqlBulkCopy` | `local_infile=ON` on the server |
| `Standard` | Batched multi-row `INSERT` | none |

`MySqlBulkCopy` is built on `LOAD DATA LOCAL INFILE`, which needs two independent switches: the
client flag (dtpipe sets it for you) **and** the server's `local_infile`, which is **OFF by
default since MySQL 8** and requires `SUPER` to change. dtpipe probes `@@GLOBAL.local_infile`
once per run; when it is off, it warns and falls back to batched `INSERT` rather than failing
mid-stream. To enable the fast path:

```sql
SET GLOBAL local_infile = 1;   -- or local_infile=ON in my.cnf, to survive a restart
```

#### Upsert and the unique-index requirement

`--strategy Upsert` and `--strategy Ignore` generate `INSERT … ON DUPLICATE KEY UPDATE`. MySQL
fires that clause on **the table's own PRIMARY KEY or UNIQUE indexes** — there is no way to name
a conflict target as `ON CONFLICT (…)` does in PostgreSQL. Two consequences worth knowing:

* **No matching index, no upsert.** If no unique index covers exactly the key columns, the clause
  degenerates into a plain `INSERT` and duplicates accumulate silently. dtpipe checks for such an
  index and, when it is missing, warns and falls back to an explicit `DELETE`+`INSERT` that
  produces the correct result more slowly. Add the index to get the fast path.
* **Other unique indexes also fire.** A row conflicting on an unrelated `UNIQUE` column is updated
  too. That is MySQL's semantics, not dtpipe's; it has no equivalent in the other providers.

`--key` may be omitted: the writer reads the target's primary key from `information_schema` and
uses it. Pass `--key` explicitly when the target has several unique indexes and you need a
specific one, or when the table does not exist yet and dtpipe must generate the `PRIMARY KEY`.

#### Type mapping notes

| Source CLR type | MySQL type | Note |
|:---|:---|:---|
| `Guid` | `CHAR(36)` | MySQL has no UUID type; `CHAR(36)` is what round-trips back to `Guid` |
| `bool` | `TINYINT(1)` | a bare `TINYINT` would come back as a number |
| `decimal` | `DECIMAL(p,s)` as the source declares it, else `DECIMAL(38,18)` | MySQL's ceiling is `DECIMAL(65,30)` |
| `DateTime` | `DATETIME(6)` | |
| `DateTimeOffset` | `DATETIME(6)` | **the zone offset is not preserved** — MySQL has no offset-carrying type |
| `string` | `LONGTEXT`, or `VARCHAR(255)` when part of the key | `LONGTEXT` cannot be indexed without a prefix length |

## Incremental Loading

DtPipe supports cursor-driven incremental loading to transfer only new or updated records since the
last successful run.

### Overview

Incremental loading uses two key mechanisms:
1. **State Persistence**: The CLI tracks the maximum value observed in a designated cursor column
   and writes it to a JSON state file after a successful execution.
2. **Query Interpolation**: The SQL query uses the `${{cursor://path|default}}` resolver to filter
   for records greater than (or equal to) the last saved value.

### CLI Flags

- `--cursor COLUMN` — Specifies the column to observe for tracking the maximum value (e.g.
  `updated_at` or `id`).
- `--state PATH` — Specifies the path to the state file where the cursor metadata will be saved.
- `--cursor-from VALUE` — Global override to temporarily force a starting cursor value for the
  current run, ignoring the state file.

In a distributed pipeline the coordinator keeps the cursor instead of a state file:
[Incremental loading in a distributed pipeline](#incremental-loading-in-a-distributed-pipeline).

### State File Format

The state file is stored as a simple, human-readable JSON file:
```json
{
  "version": 1,
  "cursor": {
    "column": "updated_at",
    "value": "2026-06-15T23:59:59.654321",
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

To prevent concurrent writes or corrupted cursor states, DtPipe enforces that **no two writers may
share the same state file**. If the DAG validator detects duplicate state files across branches,
pipeline execution will fail immediately.

---

## Sample Mode and Materialisation

### `--dry-run N` runs the pipeline

`--dry-run N` runs the real pipeline over the first N **source** rows with the writer neutralised:
same reader, same steps, same row/Arrow bridges. A step that expands or aggregates shows a
different row count from its neighbour (`10 → 3 rows`). A column type the writer refuses fails the
preview with the same message as the run; what only writing finds (a constraint, a value too wide,
a row past N) is not seen.

```bash
dtpipe -i pg:"Host=db;Database=app" --query "SELECT * FROM orders" \
       --window-key customer_id --window-script "…" \
       -o parquet:out.parquet --dry-run 10
```

| | During a sample run |
|:---|:---|
| The writer | Never initialised, written to or completed: no file is created, no table created or migrated. The target is inspected for the compatibility report |
| Hooks | None of `--pre-exec`, `--post-exec`, `--on-error-exec`, `--finally-exec` runs |
| Column types | Asked of the writer as the run asks; a refusal names every column and the cast that fixes it |
| Cursor, metrics | Not advanced, not written |
| The source | A query carrying `DELETE`, `UPDATE`, `DROP`, `TRUNCATE`, `ALTER`, `INSERT` or `ATTACH` is refused. PostgreSQL, Oracle, MySQL (`SET TRANSACTION READ ONLY`) and SQLite (`PRAGMA query_only`) also get a read-only session. SQL Server has none, so there the verb scan is the only guarantee |

The run states which of the two guarantees it had: a verb scan cannot prove a query read-only
(`SELECT my_function()` passes it).

### Materialising a point in the pipeline

| Flag | |
|:---|:---|
| `--checkpoint <name>` | Store this branch's output, after its last step, in the session store |
| `--from-checkpoint <key>` | Read a stored checkpoint in place of the branch's source |

```bash
dtpipe -i oracle:"…" --query "SELECT * FROM big_table" --mask email --checkpoint stage1 -o null:
dtpipe --from-checkpoint <key> --compute "total=price*qty" -o csv:out.csv
```

- The key is a hash of what produces the rows: the source and its connection (credentials
  stripped), the query, every step and its configuration in order, a processor's options, the row
  limit, the sampling parameters, the definition of a branch it reads, and the content of a file a
  step reads with `@file`. The alias, the checkpoint name and the writer's options are not part of
  it. A sample never shares a key with the full run.
- A checkpoint is Arrow: a row-mode pipeline is bridged to Arrow when it is stored, which can
  normalise a type the row path carried loosely.
- A checkpoint is a snapshot: nothing detects that its source changed.

### The schema a pipeline produces is a contract

`--contract-save <path>` writes the schema a branch delivers to its writer, after every step, with
a hash. A sample run and a real run produce the same contract.

```bash
dtpipe -i pg:"Host=db;Database=app" --query "SELECT id, email, total FROM orders" \
       --mask email -o parquet:orders.parquet \
       --dry-run 100 --contract-save contracts/orders.json
```

| Field | Meaning |
|:---|:---|
| `hash` | SHA-256 of the canonical schema. An unchanged pipeline keeps it |
| `schemaSource` | `batch` when taken from real rows; `columns` when the run produced none, in which case nested types are missing |
| `enforcement` | What a sample run could guarantee about not writing to its source; empty for a real run |
| `producer`, `branchAlias` | The build that captured it, and the branch it describes |

Commit the file next to the pipeline, not under `.dtpipe/`. Each branch needs its own path; two
branches naming one file are refused. A contract fixes columns and types only, not the rules a
receiving application enforces in its own code.

#### Checking a consumer against a contract

```bash
dtpipe contract check --job consumer.yaml --contract contracts/orders.json
```

A sample run whose source is the contract's schema and no rows. Exit `0` when the consumer's target
accepts it, `1` when it does not or when the consumer cannot start (a projected column the contract
lacks). A target the consumer overwrites is judged as one that does not exist yet. `--branch
<alias>` names the branch the contract feeds when the job has several sources; `dtpipe contract
show <path>` prints a contract. A step that fails only on a value is not caught.

#### Comparing two versions of a contract

```bash
dtpipe contract diff contracts/orders.json contracts/orders.new.json
dtpipe contract diff --json old.json new.json     # for a CI step
```

Exit `0` when the new contract still satisfies a consumer the old one satisfied, `1` when not.

| Change | Verdict |
|:---|:---|
| Column added | compatible |
| Column removed, or renamed | **breaks** |
| Number widened (`int32` → `int64`) | compatible |
| Number narrowed, or family changed | **breaks** |
| Decimal precision or scale reduced | **breaks** |
| `nullable: false` → `true` | **breaks** |
| `nullable: true` → `false` | compatible |
| A timestamp's time zone changed or dropped | **breaks** |
| Extension metadata dropped (`arrow.uuid` → raw binary) | **breaks** |
| Column order | indifferent |

A decimal with no declared width is written as `decimal128:38:18` with the field metadata
`Apache.Arrow.Serialization:decimal-width = undeclared`.

#### Offering the points a job could be cut at

```bash
dtpipe split producer.yaml
dtpipe split producer.yaml --branch orders --rows 100
```

Lists one candidate per stage (`0` is the reader, `1..n` the steps in order), with the schema that
would cross and the cost: `free` for a columnar stage, `two bridges` for a row stage. The schemas
come from a sample run. A job with several branches reading a source needs `--branch`.

#### Making the cut

```bash
dtpipe split producer.yaml --at 1 --out orders
dtpipe split producer.yaml --at 1 --out orders --acknowledge
dtpipe --job orders-producer.yaml | dtpipe --job orders-consumer.yaml
```

| Flag | |
|:---|:---|
| `--at <k>` | Cut after stage `k`: the producer keeps stages `1..k`, the consumer the rest |
| `--out <prefix>` | Writes `<prefix>-producer.yaml` and `<prefix>-consumer.yaml`, meeting on `arrow:-` |
| `--acknowledge` | Write the files. Without it, the placement is printed and the exit code is `1` |

The source, cursor, `--limit` and sampling go to the producer; the target, `--prefix` and
`--checkpoint` to the consumer; a `provider-options` block follows the component it configures,
and a block neither end owns stops the cut. `--metrics-path` goes to neither. A secret written in
full stops the cut, naming where it is: replace it with `${{ENV_VAR}}` or `${{keyring://alias}}`.
The halves keep every `${{…}}` as written.

### Sessions

| A checkpoint belongs to | |
|:---|:---|
| `--session <name>` | An explicit session |
| `DTPIPE_SESSION` | The session of the shell |
| the nearest `.dtpipe/` above the working directory | As git finds `.git` |
| a new `.dtpipe/` | Created only when something is materialised |
| user state | When the working directory cannot be written |

`.dtpipe/` ignores itself in git. Sessions expire after 7 days (`DTPIPE_SESSION_TTL_DAYS`) and are
purged the next time the store is used.

```bash
dtpipe session list              # sessions, checkpoint counts, size, time left
dtpipe session show <name>       # the checkpoints in one session
dtpipe session purge [<name>]    # remove one, or every expired one
```

### Encryption

Checkpoints are always encrypted (AES-GCM). The key is kept apart, in user state, mode `0600`:
`DTPIPE_STATE_HOME`, else `$XDG_STATE_HOME/dtpipe` on Linux, `~/Library/Application Support/dtpipe`
on macOS, `%LOCALAPPDATA%\dtpipe` on Windows. If that folder cannot be written, materialising is
refused. A purge destroys the key before the files.

`--sampling-seed` gives the same sample only for the same query and the same row order; without an
`ORDER BY`, the order is not guaranteed.

---

## Shell Completion

```bash
dtpipe completion --install   # installs for bash, zsh, or PowerShell
```

Restart your terminal (or `source ~/.zshrc`) to activate. Completion suggests providers
(`pg:`, `csv:`…) strategies (`Append`, `Upsert`…), and flag names based on cursor position.

---

## Studio (`dtpipe studio`)

`dtpipe studio [folder]` opens a local page over the YAML jobs of a folder (default: the current
directory). Each job shows as cards, one per source, step and sink, which you edit as cards or as
YAML and preview on the real engine without writing a target.

```bash
dtpipe studio ./jobs              # prints the address to open, then serves until stopped
dtpipe studio ./jobs --port 8765  # a fixed port instead of any free one
dtpipe studio ./jobs --coordinator https://coordinator.example  # signed in to a coordinator
```

| Option | Description |
|---|---|
| `--port` | Port on 127.0.0.1. Default: any free port |
| `--coordinator` | A distributed pipeline's coordinator, by its address: its bricks and its library. Default: none |
| `--key` | Sign in to the coordinator with a key it made for you, as a reference (`${{NAME}}`, `keyring://alias`, `@file`), never the key itself. Default: through the browser |

| Action | What it does |
|:---|:---|
| **▶ Preview** | Runs the job as `--dry-run` does, over the rows you ask for (up to 100 000 per source; a card shows up to 1 000). Each card then shows its rows, columns and types, and a badge with its state |
| **Save** | Writes the job composed from the cards. A job written by hand loses its inner comments and key order, and the page says so. Refuses a file changed on disk since it was opened, and never overwrites another file |
| **Variables** | Lists every `${{…}}` the job reads. For a preview, give each a keyring alias or a value; a preview with an unresolved variable is refused |
| **Import** | Turns a dtpipe command line into a new job, as `--export-job` would. `--bind-input` and `--bind-output` are refused |
| **Write targets…** | Runs the saved job for real, as `dtpipe --job`, after you confirm each target. **Stop** ends it with code 130 |
| **Review before writing** | An option of **Write targets…**: the run stops before every writing branch and holds the rows; **Write** writes them all, **Abandon** forgets them |
| **▶** on a card | Runs the job up to that card |
| **📌** on a card | Keeps the card's rows as a checkpoint, so later runs start from there. **Forget pins** clears them all |

Editing a card clears the preview and the checkpoints of every card that depends on it. A preview
through pins is not exactly the job's run: every cut passes through Arrow and batches differ.

The studio keeps its own files under `.dtpipe/studio/` in the folder: the cards' layout
(`<job>-layout.json`), the page's state (`view.json`), the launch values (`<job>-launch.json`,
aliases and typed values, never a keyring value), and the pins (`<job>-debug.json`). Checkpoints
live in the session `.dtpipe/sessions/studio-debug/`.

### With a coordinator

`--coordinator` names the one coordinator the studio talks to. **Sign in** goes through the
coordinator's identity provider; with `--key`, the studio exchanges the key instead. Tokens stay in
the studio's memory. The coordinator's administrator must list you, and
`Coordinator:Studio:ClientId` names the studio's client.

| Action | What it does |
|:---|:---|
| Palette | Offers the bricks of the coordinator's nodes, an offline node greyed out, with each node's dtpipe version |
| **Publish** | Sends the saved job to the library under an id you choose. It replaces only the version you last read; if someone else saved since, it asks. A deployed pipeline keeps running its armed version until deployed again |
| **Open from library** | Writes a library pipeline as a new `<id>.yaml` in the folder, never over a file |

A preview with bricks reads at most 100 rows of each source brick through the coordinator; a sink
brick receives nothing, its rows going to a throwaway file under `.dtpipe/studio/samples/`. A run
with a source brick is refused unless every output is a sink brick.

The page listens on 127.0.0.1 only, answers only the address it printed (which carries a session
token), refuses requests from other sites, and reads and writes only the `.yaml` and `.yml` files
of the folder and its own files. Quitting from the page exits 0; Ctrl+C exits 130.

---

## Distributed pipeline (experimental)

> **Experimental.** May change or disappear without notice; see
> [docs/experimentation.md](./docs/experimentation.md). A walkthrough from nothing to a first run is
> [docs/guides/distributed-first-pipeline.md](./docs/guides/distributed-first-pipeline.md); running
> the two hosts for real is [deploy/README.md](./deploy/README.md).

A **coordinator** cuts a pipeline into fragments and runs each on a **node**, a host that carries
data; a **runner** is a node with no data that runs every step between the bricks. The coordinator
and the runner are published as container images, `ghcr.io/nicopon/dtpipe-coordinator` and
`ghcr.io/nicopon/dtpipe-runner`, by each release. A node that serves real data is `dtpipe node`, or
your own .NET host that calls `AddDtPipeNode`, and declares its **bricks**. Every setting below is
read from the environment first (`Coordinator__PublicUrl`), then from command-line arguments
(`--Coordinator:PublicUrl`); a secret belongs in the environment only, or in a file named by
`@path` where a setting says so.

### Coordinator settings (`Coordinator__*`)

| Setting | Description |
|---|---|
| `Authority` | An OpenID Connect provider the coordinator trusts, on an origin of its own; `https` in production. Unset: every identity is verified by a key |
| `Audience` | The audience a service's token must name. Default: `dtpipe-coordinator` |
| `Ui__ClientId` / `Ui__ClientSecret` | The provider's confidential client the pages sign people in with (the secret is required with `Authority`, refused without). Default id: `coordinator-ui` |
| `Studio__ClientId` | The public client `dtpipe studio` signs in with, and the client a person's key token names. Unset: no person's token reaches `/api` |
| `Roles__Admin` / `__Designer` / `__Reader` | With `Authority`: lists of people, matched on the claim `Roles__Claim` (default `email`), beside the roles the rights give. Nobody unlisted and absent from the rights may do anything |
| `PublicUrl` | The address people reach the coordinator at, behind a TLS proxy: scheme, host, port, no path. The issuer of the tokens a key is exchanged for; required without `Authority` |
| `Auth__AllowHttpKeys` | `true` lets a key be exchanged while `PublicUrl` is `http` (the key then crosses the network in clear, which the log and every page say). Default: refused |
| `Auth__PersonKeys` | `false` lets people sign in through the provider only. Default: `true`; refused without `Authority` |
| `Auth__ServiceTokenLifetime` / `__PersonTokenLifetime` | How long a token exchanged for a service's or a person's key lasts, five minutes at least. Default: 10 minutes / 1 hour |
| `Bootstrap__AdminKey` | The first administrator's key, `dtp_<id>_<secret>`, as `@path` to a file or a value of the environment. Required without `Authority`; added while the rights hold no administrator with a live key |
| `Bootstrap__Identities__<n>__Id` / `__Key` / `__Kind` / `__Name` / `__Groups__<n>` / `__Pipelines__<n>` / `__Role` | An identity the deployment gives with its key (`@path` or the environment), added at start when its id is absent, never changed after. Default kind: `node` |
| `Store` | `sqlite` (default; a file in the state directory), `postgresql` or `sqlserver`: one coordinator holds a database |
| `Sqlite__Directory` | Where the SQLite file lives, if not the state directory |
| `ConnectionString` | The database of `Store=postgresql` or `sqlserver` (a secret: the environment or `@path`); refused with `sqlite` |
| `StoreLockWait` | How long a start waits for another coordinator to leave the database before it refuses. Default: 30 seconds |
| `Standby` | `passive`: a start that finds the database held waits for it without bound, serving `/livez` 200 and `/healthz` 503 `passive`, then takes over. `postgresql` or `sqlserver` with `Library__Remote__Url` only; refused beside `StoreLockWait` |
| `Initialize` | `true` lets a store that holds nothing begin: the first start, or after data was lost |
| `StateDirectory` | Where the library, the rights and scratch files live. Default: `./state` |
| `Library__Directory` | Where the library's git repository lives. Default: `library` under `StateDirectory` |
| `Library__Remote__Url` / `__Token` / `__User` / `__Timeout` | A git remote every library save is pushed to before it is accepted (token: a secret) |
| `RightsSeed` | A rights document a new store starts from. Default: no node, no edge |
| `LibrarySeed` | A folder of `.yaml` pipelines (with optional `.bindings.json`) a new library starts with |
| `Identity__LabelClaims` | Claims shown beside a host waiting for approval; a label only |
| `Health__MaxAge` | The longest a health read is kept. Default: five minutes |

The state directory is the coordinator's whole state when the store is SQLite: back it up stopped,
as described in [deploy/README.md](./deploy/README.md#backing-up-a-sqlite-coordinator). With
PostgreSQL or SQL Server the state is in the database, [one database for each
coordinator](./deploy/README.md#keeping-the-state-in-a-database).
`GET /healthz` answers 200, anonymously, once the server serves.

An identity in **Access** is verified by the provider or by a key the coordinator makes,
`dtp_<id>_<43 characters>`: shown once when it is made, then kept only as its hash. A node presents
its key as its client secret at the coordinator's `/connect/token`; a person types it into the
sign-in form, or hands it to `dtpipe studio --key`. Two keys may be live at once, for a rotation,
and revoking one ends what it opened at the next request. The server never makes a key on its
own: a deployment gives the first ones through `Bootstrap__*`.

### Node host settings (`DtPipeNode__*`)

| Setting | Description |
|---|---|
| `Name` / `Description` | The name the host proposes. The coordinator names a node from its rights, not from this |
| `CoordinatorUrl` | The coordinator's base address |
| `Oidc__Authority` / `__ClientId` / `__ClientSecret` / `__Scope` | The client-credentials client the host authenticates as: `ClientId` is the node's `Name` and `Authority` the first issuer the coordinator names (its own, without a provider), unless given; a node verified by a key beside a provider sets it to the coordinator's address. `ClientSecret` is the key or the provider's secret, `@path` to read it from a file. The client id is the node's identity |
| `DtPipe` | The `dtpipe` binary every fragment runs as a child process. Default: `dtpipe` on the path; the binary itself under `dtpipe node` |
| `StateDirectory` | Where fragment jobs are written and run from. Default: a directory under the temp directory |
| `Policy` | `Bricks` (default: a data node runs only its own bricks) or `AnyFragment` (a runner) |
| `UnwiredRunTimeout` | How long a launched run may wait for its edges to be wired. Default: five minutes |
| `FaultInjection` | `true` lets the host end a run's child when the coordinator asks, for a test bench. Default: off |
| `ArrowCompression` | Arrow IPC buffer compression codec (`none`, `lz4` or `zstd`; refused at start otherwise), passed to each fragment as `--bind-compression`. Default: `lz4` |
| `ArrowCompressionLevel` | Optional compression level for the selected codec, passed as `--bind-compression-level` |
| `BatchBytes` | TransportR batch size in bytes for the data plane client. Default: `262144` (256 KB). For high-throughput WAN, 256 KB–512 KB is recommended |
| `MaxInFlightBatches` | Maximum in-flight TransportR batches before sender pause. Default: `16` (4 MB window). On high-latency WAN links, `32` (8 MB window) is recommended |
| `ReadChunkBytes` | Buffer size in bytes for reading/writing child fragment stdio pipes. Default: `262144` (256 KB) |
| `Variables__<NAME>` | A value every fragment reads as `${{NAME}}`; it never leaves the node |

A **brick** is a read or a write the node's owner declared on the node's own data. A pipeline names
it by reference: `input: brick:<node>/<brick>` reads a source, `output: brick:<node>/<brick>` with
`from` writes a sink. `<id>.bindings.json`, beside its YAML in the library, keeps for each reference
a mode (`strict`, `contract`, `compatible` or `tolerant`) that says how far the brick may change
after it was checked, and the pin a check wrote. Arming a pipeline whose reference no check has
pinned runs the check and writes the pin.

#### A node from the CLI (`dtpipe node`)

`dtpipe node` is a data node whose bricks are the branches of the [pipeline grammar](#dag-syntax): a
branch that only reads (`-i`) is a source, one that only writes (`-o`) is a sink, and its `--alias`
is the brick's id. `--job FILE` gives the same branches in a job file instead of on the line (a flag
of a branch beside `--job` is refused), which is how a sink sits beside sources: a command line
holds as many sources as it likes, or one sink. A branch that reads another (`--from`), transforms,
or both reads and writes is refused by name, since everything between a read and a write belongs on
a runner. A `--job` file is read as written, like a command line: a `${{NAME}}`, a `keyring://`
alias and a `${{cursor://|default}}` stay in the brick's text, in its version and in the job of the
fragment, and are resolved only when the child runs. The node's settings are the `DtPipeNode__*` of
the table above, from the environment; four options override them. `dtpipe node --runner` is a
runner instead: it hosts no data, runs any fragment the coordinator sends, and refuses a brick given
beside it (`-i`, `-o`, `--alias`, `--job`).

| Option | Description |
|---|---|
| `--runner` | Host no data and run any fragment the coordinator sends. Takes no value and no brick; with no `--description`, the node describes itself as a runner |
| `--job FILE`, `-j FILE` | YAML job file declaring the bricks (sources and sinks) offered by this node |
| `--name` / `--coordinator` / `--description` | `DtPipeNode__Name`, `__CoordinatorUrl` and `__Description` |
| `--client-secret REF` | `DtPipeNode__Oidc__ClientSecret`, as a reference: `keyring://alias`, `${{NAME}}` or `@file`. A value is refused; the environment variable takes a reference too |

```bash
DtPipeNode__Oidc__ClientSecret=keyring://node-secret \
  dtpipe node --name crm --coordinator https://coordinator.example \
    -i 'pg:Host=db;Database=crm;Password=${{CRM_PASSWORD}}' --alias customers
```

A brick carries no title or description: the studio shows its id. A brick's version is the hash of
its text, so a credential written into it would let anyone who sees the version test guesses
offline: the node refuses a brick that holds one (a `${{NAME}}` or `keyring://` reference passes).
That check reads the keys it knows as credentials, in a connection string and in a query alike: a
secret under another key is not seen, and a harmless `WHERE token_count = 5` is refused as one. A
file a brick reads by `@file` (a query) stays a reference in its text: give an absolute path, since
fragments run in the node's own directory, and know that the file's content is not part of the
brick's version. The `dtpipe` every fragment runs is the binary that started the node.

#### A key from the CLI (`dtpipe keygen`)

`dtpipe keygen ID` prints a new key for the identity `ID`, in the shape a coordinator reads:
`dtp_<id>_<43 characters of base64url>`, the secret 256 random bits. `ID` is the identity's client
id: lowercase letters, digits and `-`, a letter or digit first. The coordinator finds the identity
by the id the key names and judges the key by its hash alone, so a text of another shape is
refused, and it never checks how random the secret is: make keys with this command.

The command reaches no coordinator and keeps nothing; the key is the only output. Give it to the
coordinator (`Coordinator__Bootstrap__AdminKey`, `Coordinator__Bootstrap__Identities__<n>__Key`,
each as `@file`) and to the host it opens (`--client-secret @file`).

| Option | Description |
|---|---|
| `--out FILE` | Write the key to `FILE`, created readable by its owner only, instead of standard output. An existing file is never replaced |

```bash
dtpipe keygen admin --out admin.key
dtpipe keygen runner-1 --out runner-1.key
```

#### Incremental loading in a distributed pipeline

The coordinator keeps the cursor of each branch that writes, since replicas share no disk and a run
succeeds by the coordinator's verdict, not a fragment's. Two bricks make it: a source that filters
with a token naming no file, and a sink that names the column to follow, each declared on the node
that owns its database, with absolute paths:

```bash
dtpipe node --name shop --coordinator http://coordinator.localtest.me:8080 \
  --client-secret '${{SHOP_KEY}}' --alias orders-new \
  -i sqlite:/srv/shop/shop.db \
  --query 'SELECT * FROM orders WHERE OrderId > ${{cursor://|-1}} ORDER BY OrderId'
dtpipe node --name warehouse --coordinator http://coordinator.localtest.me:8080 \
  --client-secret '${{WAREHOUSE_KEY}}' --alias orders-copy \
  -o sqlite:/srv/warehouse/warehouse.db --table orders_copy --cursor OrderId
```

A pipeline that reads `brick:shop/orders-new` and writes `brick:warehouse/orders-copy` then loads
only the new orders on each run.

- **Key**: the library pipeline and the alias of the branch that writes. The column is the sink
  brick's `cursor`; a brick never sets `state` (the node gives the child a file of its own and reads
  the value once the child exits with 0). A source brick filters with `${{cursor://|default}}`, a
  token that names no file: a brick whose token names one is not offered. A cursor brick is declared
  the same way on a command line or in a `--job` file.
- **At launch** the coordinator hands the kept value to the fragment that holds the one source brick
  upstream of the sink, as `DTPIPE_CURSOR_OVERRIDE` in the child's environment. A cursor that does
  not exist yet is no error: the token's default is used.
- **Not served**: a `cursor` on a source brick. A sink fed by no source brick (an input written on
  the runner) has no cursor to start from: the run succeeds and keeps none, and its page says so.
- **At the verdict** of a run that succeeded as a whole, it keeps the value each sink reported. A
  failed or cancelled run, or one that lost a replica, advances nothing; so does a value out of the
  rule below or a store that refuses the write, which the run's page says by name.
- **Refused by name**: a launch while a run of the pipeline is queued or in flight, once a cursor
  exists (409, not queued); a brick whose version changed since the value was kept; a cursor whose
  sink or source is no longer in the plan; two cursors under one source; a sink fed by several
  sources.
- **A value** is an integer (`-?[0-9]+`), an ISO 8601 date or instant, or a text of letters, digits,
  spaces and `_ . : + / @ -`, at most 200 characters, of the type it was kept with.

| Route | Caller | Description |
|---|---|---|
| `GET /api/library/{id}/cursors` | reader | The pipeline's cursors: alias, column, type, value, the run that kept it or who set it |
| `PUT /api/library/{id}/cursors/{alias}` | the right to launch (designer) | Sets a value, `{"value": "…", "type": "integer"}`; the type may be left out for a cursor that exists and must be its own: another type is refused until a reset. Refused while a run of the pipeline is in flight |
| `DELETE /api/library/{id}/cursors/{alias}` | the right to launch (designer) | Resets: the next run starts from the token's default |

`/service` has no cursor route, and a launcher's token never opens `/api`. `--cursor-from` is not
served: it is a launch value for one run of a command line. The contract number of `dtpipe studio`'s
API is 9.

### Rights and identities

The identity provider says who a caller is; the coordinator's **rights** say what it may do. Each
identity (the `client_id` of a client-credentials client) has a name and belongs to groups, and a
**matrix** lists which group may send rows to which (`*` stands for any group, such as `*` →
`runner` and `runner` → `*`, while `*` → `*` is refused). An edge checks that every group of the
sender may send to every group of the receiver. When a pipeline flows through intermediate nodes or
runners, origin groups propagate transitively along dependencies: an edge leaving a runner requires
authorization from each upstream source group to the destination groups, so an intermediate hop
cannot mask upstream origins. A branch carries the origins of the branches it reads (`from`, `ref`)
and no other: a runner's step reads and writes its edges only, so two flows that cross one runner
need no grant between their groups. Branches that read each other in a cycle are refused. An
identity has a kind: `node` (the default, hosts fragments), `monitor` (reads `/health/pipelines` and
nothing else) or `launcher` (starts the runs of the pipelines it lists on `/service`, and reads
them). A host whose identity the rights do not know waits in the **Access** view until an
administrator accepts it.

### Routes for services

| Route | Caller | Description |
|---|---|---|
| `GET /healthz` | anyone | The server serves |
| `GET /health/pipelines` | monitor | Whether each armed pipeline can be launched, and why not |
| `POST /service/runs` | launcher | Start a run: `{"pipelineId": "…"}`; an optional `Idempotency-Key` header makes a repeat answer the same run. 409 if the pipeline is not armed |
| `GET /service/runs/{id}` | launcher | The run's `state` and whether its sinks may be partial |
| `POST /service/runs/{id}/cancel` | launcher | Cancel a run the same launcher started |

A service sends a bearer token from the provider's client-credentials flow; a person's token never
reaches `/service`, and a service's never reaches `/api`.

---

## Model Context Protocol (MCP) Server

`dtpipe mcp` starts an MCP server over STDIO, so an AI assistant (Cursor, Claude Desktop, VS Code…)
can use dtpipe's tools.

```bash
dtpipe mcp
```

### Exposed MCP Tools

| Tool Name | Parameters | Description |
|:---|:---|:---|
| `list-providers` | *(none)* | List all registered data readers, transformers, and writers |
| `inspect` | `input`, `query?` | Inspect data source schema or auto-discover database tables if query is omitted |
| `preview-data` | `input`, `limit?`, `query?` | Preview sample data rows (default: 5 rows) |
| `validate-yaml-job` | `yamlContent` | Validate a YAML job without running it: syntax, topology, provider-option keys, and every transformer built as the engine builds it |
| `execute-yaml-job` | `yamlContent`, `apply?`, `allowDestructive?`, `allowNetwork?` | Execute a YAML job in-memory. **Dry-run by default** (no write); `apply=true` performs a write, subject to the guardrails below |
| `dry-run` | `yamlContent`, `rows?` | Run the pipeline over a small sample with the writer neutralised, and return the rows leaving each stage, plus any column one transformer produced and a later one wrote over. See [Sample Mode](#sample-mode-and-materialisation) |
| `get-dag-topology` | `yamlContent` | Structured branch topology: alias, input, output, the ordered `transformers[]`, stream processor, and the `from[]` / `ref[]` aliases per branch. Runs nothing |
| `list-cursors` | *(none)* | List the incremental cursors stored in the current workspace: column, last value, last run time, status and row count |
| `list-checkpoints` | `session?` | List the materialised checkpoints of a session, each named by the hash of what produces it |
| `read-checkpoint` | `checkpointKey`, `rows?`, `session?` | Read rows back from a materialised checkpoint without touching the original source |
| `help` | *(none)* | General usage guidelines, YAML job structures, and DAG topology rules |
| `get-adapter-help` | `adapterName` | Inspect connection string format, reader/writer options, and YAML examples for an adapter |
| `get-transformer-help` | `transformerName` | Inspect options, mapping syntax, and YAML examples for a transformer |
| `get-anonymization-help` | *(none)* | Every Bogus faker dataset and method, each with one sample value, plus the anonymization options |

---

### AI Agent Subcommand (`dtpipe agent`)

```bash
dtpipe agent [<prompt>] [options]
```

A loop in which a model (local Ollama, or OpenAI with `--provider`) uses the tools above to design
and validate a pipeline. Guide: [The agent](./docs/guides/agent.md).

On an interactive terminal the session runs full screen: the steps, the plan under construction
and a transcript, with an input line between turns. `Esc` during a turn cancels the model call and
keeps the session; `Ctrl-C` ends it with code 130. Elsewhere, or with `--no-tui`, the output
scrolls, streaming the model's answer token by token on an ANSI terminal.

| Command | Effect |
|:---|:---|
| `/exec` | Runs the validated plan through `execute-yaml-job` (a preview unless `--apply`, which asks once more) |
| `/mode` | Cycles the operating mode |
| `/save [path]` | Writes the plan's YAML |
| `/note <text>` | Adds a note to the session trace |
| `/review` | Moves to the steps list |
| `/quit` | Leaves |

After a turn, a menu offers a review of the session and, when a validated YAML exists, **Execute
this plan now**, which runs it without the model. A turn that ends says why: an answer, the
iteration cap, a failed or silent call, an empty answer, or an interruption.

| Option | Alias | Description | Default |
|:---|:---|:---|:---|
| `<prompt>` | `-p`, `--prompt` | Task description for the AI agent | *(Interactive prompt if omitted)* |
| `--provider` | | LLM provider: `ollama` (local) or `openai` | `ollama` |
| `--api-key` | | API key for the `openai` provider. Falls back to `DTPIPE_LLM_API_KEY` | *(unset)* |
| `--model` | `-m` | Model name (e.g. `qwen2.5-coder:7b` for Ollama, `gpt-4o` for OpenAI) | *(Auto-discovered from Ollama)* |
| `--url` | `-u` | API endpoint URL | `http://localhost:11434` (ollama) · `https://api.openai.com` (openai) |
| `--max-iterations` | | Maximum loop iterations per turn | `25` |
| `--llm-timeout` | | Seconds with no token before the turn ends with an error. One call also stops after three times this value | `300` |
| `--max-output-tokens` | | Tokens one response may generate before it is cut | `4096` |
| `--no-stream` | | One blocking call per step, no streaming | `false` |
| `--no-tui` | | Scrolling output instead of the full-screen surface | `false` |
| `--detail` | | What scrolls per step: `compact`, `peek` (+ a preview of the reasoning), `full` (+ all of it) | `compact` |
| `--show-thinking` | | Alias for `--detail full` (implied by `DEBUG=1`) | `false` |
| `--num-ctx` | | Context window to request (Ollama `num_ctx`) | `16384` |
| `--interactive` | `-i` | Force interactive model selection and prompt entry | `false` |
| `--mode` | | `plan` designs and validates only; `execute` and `autonomous` may run, through the guardrails | `plan` |
| `--temperature` | | Sampling temperature. `0` is greedy, and weaker models loop on it | `1` |
| `--seed` | | Fixed seed for reproducible sampling | `0` |
| `--repeat` | | Replicate the validated plan N times and report the variance | `1` |
| `--sequential` | | One tool call at a time instead of independent calls in parallel | `false` |
| `--trace` | | Record the session as JSON lines (a directory gets a timestamped file). Defaults to `$DTPIPE_AGENT_TRACE` | off |
| `--mcp-server` | | Drive another MCP server's tools instead of dtpipe's: the command that starts it over stdio | *(unset)* |
| `--apply` | | Perform a real write | `false` (dry-run) |
| `--allow-destructive` | | Allow destructive SQL verbs | `false` |
| `--allow-network` | | Allow network access in SQL | `false` |

#### Driving another MCP server (`--mcp-server`)

```bash
dtpipe agent -p "list the configured connections" \
  --mcp-server "sometool mcp" --trace ./traces/ --no-tui
```

The command is split on spaces and started as a child process; its tools are offered to the model
in place of dtpipe's, and its handshake instructions become the system message. No dtpipe pipeline
can run (`/exec` refuses), `--mode` does not filter that catalogue, and dtpipe's `--apply` and
`--allow-*` flags do not apply to its tools. Use it to compare a server's surface under the same
loop and trace.

---

### Agent Guardrails (`ISqlSafetyPolicy` / `IApprovalGate`)

With no flags, the agent can write nothing: mode `plan`, previews only, destructive SQL and network
access refused.

- **A write** needs `apply=true` (`--apply`), an approval, and a clean SQL check. A context with
  nobody to approve, such as the MCP server, denies writes.
- **The SQL check** refuses `DROP`, `DELETE`, `TRUNCATE`, `UPDATE`, `ALTER`, `INSERT` and `ATTACH`
  unless `--allow-destructive`, and `LOAD httpfs` / `LOAD azure` or `read_parquet` / `read_csv` /
  `read_json` over `http(s)://`, `s3://`, `ftp://` or `gs://` unless `--allow-network`. It reads
  every query, hook and SQL step of the job (`@file` contents included) and each connection
  string; it never reads a key, a comment or a transformer option as SQL.
- **In plan mode** the tool that writes is not offered to the model at all.
- **The trace** (`--trace`) records the launch, the role prompt, the tool catalogue as offered,
  every step, each verdict and your notes, flushed line by line. Connection-string credentials are
  blanked, but treat a trace as sensitive. It is a diagnostic, never a gate.
- **Determinism**: `--temperature 0 --seed N --repeat 3` reports the number of distinct plans
  minus one; `0` means reproducible.

