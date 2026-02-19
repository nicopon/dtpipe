# DtPipe Cookbook 🍳

This document contains recipes and examples for using DtPipe to solve common data export and transformation problems.

**Table of Contents**
- [Basic Usage](#basic-usage)
- [Anonymization (The "Fakers")](#anonymization-the-fakers)
- [Common Transformations](#common-transformations)
- [Advanced Pipelines](#advanced-pipelines)
- [Standard Streams & Linux Pipes](#standard-streams--linux-pipes)
- [Database Import & Migration](#database-import--migration)
- [Production Automation (YAML)](#production-automation-yaml)
- [Security & Secrets](#security--secrets)

---

## Basic Usage

### Simple Database Export
Export a table from a database (detects `duck`, `sqlite`, `pg`, `ora`, `mssql`) to a Parquet file.

```bash
# Export from PostgreSQL to Parquet
dtpipe -i "pg:Host=localhost;Database=prod;Username=postgres" -q "SELECT * FROM users" -o users.parquet
```

### Export to CSV
Simply change the output extension to `.csv`.

```bash
dtpipe -i "pg:Host=localhost;Database=prod;Username=postgres" -q "SELECT * FROM users" -o users.csv
```

### Dry Run (Preview)
Use `--dry-run [LIMIT]` to preview data without writing a full file.

```bash
dtpipe -i "pg:Host=localhost;Database=prod;Username=postgres" -q "SELECT * FROM users" --dry-run 100
```

---

## Anonymization (The "Fakers")
 
DtPipe maps your configuration directly to [Bogus Datasets](https://github.com/bchavez/Bogus?tab=readme-ov-file#locales).
 
**Resources:**
- [Available Locales](https://github.com/bchavez/Bogus?tab=readme-ov-file#locales)
- [API Inventory (Datasets)](https://github.com/bchavez/Bogus?tab=readme-ov-file#locales)
 
### 1. General Usage
The syntax is `--fake "{Column}:{Dataset}.{Method}"`, where `Dataset.Method` corresponds exactly to the Bogus API.

#### Personal Information
Replace names and emails with culturally appropriate fake data.
 
```bash
dtpipe ... \
  --fake "FirstName:name.firstName" \
  --fake "LastName:name.lastName" \
  --fake "Email:internet.email" \
  --fake-locale fr
```
 
#### Dates and Numbers
Generate other types of data using the same mechanism.
 
```bash
dtpipe ... \
  --fake "BirthDate:date.past" \
  --fake "Score:random.number"
```
 
### 2. Deterministic Faking
DtPipe provides a special **Deterministic Mode** that guarantees referential integrity across tables.
 
Unlike standard seeding (which restarts the sequence), this mode uses a **stable hash** of a specific column (e.g., `UserId`) to generate the fake value.
 
- If `UserId=123` becomes "Alice", it will **always** become "Alice", even if the row order changes or if you run the export again next month.
- This allows you to anonymize `Users` and `Orders` tables separately while maintaining the foreign key relationships (provided they both use the same fake seed).
 
```bash
dtpipe ... \
  --fake "Name:name.fullName" \
  --fake-seed-column "UserId"
```

---

## Common Transformations

Simple, high-performance transformers for cleaning and shaping data.

### 1. Masking Sensitive Strings
Partially hide data instead of fully replacing it.

```bash
# Turns "555-0199" into "555-****"
./dist/release/dtpipe ... --mask "Phone:###-****"

# Masking patterns:
# # - Keep original character
# * - Replace with literal '*' (or any other char)
```

### 2. Overwriting & Nullifying
Hardcode values or erase sensitive columns.

```bash
# Force "Status" to "Archived" and "Notes" to NULL
dtpipe ... \
  --overwrite "Status:Archived" \
  --null "Notes"
```

### 3. Formatting Strings
Combine columns using [.NET Composite Formatting](https://learn.microsoft.com/en-us/dotnet/standard/base-types/composite-formatting) syntax.

```bash
# Create "DisplayName" from First and Last names
dtpipe ... --format "DisplayName:{FirstName} {LastName}"
```

---

## Advanced Pipelines

Chain multiple transformers to clean and shape your data.

### Pipeline Construction
DtPipe builds the pipeline by scanning your CLI arguments from left to right.
Crucially, **consecutive** arguments of the same type (e.g., multiple `--fake` flags) are grouped into a single transformation step.

```mermaid
graph LR
    Args[CLI Arguments] -->|Parsing| List[Instruction List]
    List -->|Grouping| Steps[Pipeline Steps]
```

**Example:**
`--fake A --fake B --format C --fake D`

**Resulting Pipeline:**

```mermaid
graph TB
    S1["1. FakeTransformer<br/>(A, B)"]
    S2["2. FormatTransformer<br/>(C)"]
    S3["3. FakeTransformer<br/>(D)"]
    
    S1 --> S2 --> S3
```

This means:
- `--fake "A" --fake "B"` creates **one** Faker step (efficient).
- `--fake "A" --format "C" --fake "B"` creates **three** steps (A -> Format -> B).

### Execution Order
**Crucial:** The pipeline is executed in the **exact order** of these groups.

```bash
# 1. First, anonymize the Name
# 2. Then, use the NEW (anonymized) name to format the greeting
dtpipe ... \
  --fake "Name:name.fullName" \
  --format "Greeting:Hello, {Name}!"
```

If you swap the order, the `Greeting` would contain the *original* name, because formatting would happen before faking.

### Javascript Scripting
Use `--script` for complex logic.

**Syntax Rules:**
1.  **Implicit Return**: If your script is a single expression without a semicolon, it is automatically returned.
    *   `row.Age > 18` -> Becomes `return row.Age > 18;`
2.  **Explicit Return**: If you use statements or semicolons, you **MUST** use the `return` keyword.
    *   `row.Age > 18;` -> Returns `undefined` (Use `return`!)
    *   `if (row.Age > 18) return 'Yes';` -> Works.

```bash
# Simple Expression (Implicit Return)
./dtpipe ... --compute "IsAdult:row.Age > 18"

# Complex Logic (Explicit Return)
./dtpipe ... \
   --compute "Category:if (row.Age < 18) return 'Minor'; else return 'Adult';"

### 4. Generating Test Data
Use the `generate:<count>` provider to generate rows on-the-fly. By default, it only generates a `SampleIndex` column (useful for seeding). Combine it with `--fake` for rich datasets.

```bash
# Generate 1M rows of fake users
./dtpipe -i "generate:1000000" \
  --fake "Id:random.number" \
  --fake "Name:name.fullName" \
  --fake "Email:internet.email" \
  --drop "SampleIndex" \
  -o users.csv
```
> **Tip:** Use `--drop "SampleIndex"` if you don't want the sequence index in your final output.

### 5. Random Sampling
Use `--sampling-rate [0-1]` to export only a subset of your data. This works with any provider.

```bash
# Export only 10% of a large database table
dtpipe -i "ora:..." -q "SELECT * FROM LargeTable" --sampling-rate 0.1 -o subset.parquet
```

#### Deterministic Sampling (Seed)
By default, sampling is random. Use `--sampling-seed [N]` to initialize the random generator with a specific value.
This ensures that the **same subset of rows** is selected for the same input data, which is essential for **reproducibility** in tests or debugging.

```bash
# Always get the same 10% subset
dtpipe ... --sampling-rate 0.1 --sampling-seed 12345 ...

### Filtering Data
Use `--filter` to drop rows that don't match a JavaScript condition.

```bash
# Only keep active users over 18
./dtpipe ... --filter "row.IsActive && row.Age >= 18"
```

### Row Expansion
Use `--expand` to turn a single input row into multiple output rows. The expression must return an array.

```bash
# If 'Tags' is "A,B,C", this creates 3 rows, one for each tag
./dtpipe ... --expand "row.Tags.split(',').map(t => ({ ...row, Tag: t }))"
```

### Window Aggregations (Stateful)
Accumulate rows and process them as a batch using `--window-count` and `--window-script`.

```bash
# Calculate a rolling average for 5 rows
./dtpipe ... \
  --window-count 5 \
  --window-script "rows.map(r => ({ ...r, Avg: rows.reduce((s, x) => s + x.Val, 0) / rows.length }))"
```

### External Script Files
Keep your CLI clean by moving complex logic into `.js` files.

```bash
# Explicit file loading (recommended)
./dtpipe ... --compute "Category:@scripts/categorize_age.js"
```

---

## Standard Streams & Linux Pipes

Integrate DtPipe with standard Unix tools using `stdin`/`stdout`.

### Zipped Parquet Output
Read a CSV natively and compress the output on-the-fly using `gzip`.

```bash
dtpipe -i "csv:large_data.csv" -o parquet | gzip > large_data.parquet.gz
```

### Filter and Anonymize in a Pipe
Use another tool (like DuckDB or `jq`) to filter, then DtPipe to anonymize.

```bash
duckdb -csv -c "SELECT * FROM 'source.csv' WHERE active=true" | \
  dtpipe -i csv \
  --fake "Name:name.fullName" \
  -o parquet:clean_data.parquet
```

> **Note**: When using pipes, you MUST explicitly specify the format (e.g. `-i csv` or `-o csv`) because there is no file extension to detect.

---

---
 
 ## Database Import & Migration
 
 DtPipe can write to DuckDB, SQLite, PostgreSQL, Oracle, and SQL Server using 6 standardized strategies.
 
 ### Write Strategies
 
 Control how DtPipe handles existing tables using the `--strategy` flag.
 
 | Strategy | Behavior | Use Case |
 |:--- |:--- |:--- |
 | **Append** (Default) | Inserts rows into the existing table. | Log shipping, daily increments. |
 | **Truncate** | Empties the table via native `TRUNCATE TABLE`. *(Not available for SQLite)* | Prudent refresh (preserves schema & indexes). |
 | **DeleteThenInsert** | Deletes rows (via `DELETE FROM`) then inserts. | Use when TRUNCATE is unavailable/restricted. |
 | **Recreate** | Drops the table (`DROP IF EXISTS`) and recreates it. | Full refresh including schema updates. |
 | **Upsert** | Updates existing rows (by PK), inserts new ones. | Syncing data where source is source-of-truth. |
 | **Ignore** | Inserts new rows, ignores existing ones (by PK). | Loading "missing" data only. |

 > **Note for Upsert/Ignore**: These strategies require a Primary Key. DtPipe attempts to auto-detect it, but you can force it via `--key "Col1,Col2"`.
 
 ### Examples
 
 #### 1. Load Parquet into PostgreSQL (Recreate)
 Good for full reloads where the schema might have changed.
 
 ```bash
 dtpipe \
   -i data.parquet \
   -o "pg:Host=localhost;Database=prod" \
   --table "public.imported_data" \
   --strategy Recreate
 ```
 
 #### 2. Append to Oracle Table
 Efficiently adds new rows to an existing table.
 
 ```bash
 dtpipe \
   -i "new_sales.csv" \
   -o "ora:Data Source=PROD;..." \
   --table "SALES_DATA" \
   --strategy Append
 ```
 
 #### 3. Upsert with Explicit Key
 Syncs data from CSV to SQL Server, updating existing records based on `OrderId`.
 
 ```bash
 dtpipe \
   -i "orders_update.csv" \
   -o "mssql:Server=.;Database=mydb" \
   --table "Orders" \
   --strategy Upsert \
   --key "OrderId"
 ```
 
 ---
 
 ## Production Automation (YAML)

For repeated tasks, define your job in a YAML file.

### 1. Generate a Job File
Configure your export in the CLI once, then save it.

```bash
dtpipe -i "ora:..." -q "SELECT..." --fake "..." --export-job nightly_export.yaml
```

### 2. Run the Job
```bash
dtpipe --job nightly_export.yaml
```

### 3. Override at Runtime
You can override specific settings from the YAML file via CLI flags (e.g., for ad-hoc limits).

```bash
dtpipe --job nightly_export.yaml --limit 50
```

### Example YAML

```yaml
input: ora:Data Source=PROD;User Id=...
query: SELECT * FROM sensitive_table
output: clean_data.parquet

transformers:
  - fake:
      mappings:
        name: name.fullName
        email: internet.email
      options:
        locale: fr
        seed-column: id
```

### Example 2: File-to-File (No Query)
When transforming from CSV or Parquet, the `query` field is optional.

```yaml
input: raw_data.csv
output: clean_data.parquet

transformers:
  - script:
      # Use @ to load script from file
      category: "@scripts/categorize.js"
```

---

## Security & Secrets

Never hardcode passwords in scripts or YAML files. DtPipe provides multiple ways to handle credentials safely.

### 1. Using Environment Variables
The most common approach for CI/CD. The shell expands variables before passing them to DtPipe.

```bash
# Set your connection string
export MY_CONN="ora:Data Source=PROD;User Id=scott;Password=tiger"

# Use it in the CLI
dtpipe -i "$MY_CONN" -q "SELECT * FROM users" -o users.parquet
```

### 2. Using the OS Keyring (Zero-Exposure)
For local development or secure servers, store secrets in the system keyring (macOS Keychain, Windows Credential Manager, Linux Secret Service).

**Step 1: Store the secret once**
```bash
dtpipe secret set oracle-prod "ora:Data Source=PROD;User Id=scott;Password=tiger"
```

**Step 2: Reference it by alias**
The password never appears in your shell history or `ps` output.
```bash
dtpipe -i keyring://oracle-prod -q "SELECT * FROM users" -o users.parquet
```

### 3. Loading Queries from Files
Avoid exposing complex or sensitive SQL queries in your command line or job files.

```bash
# Store your SQL in a file
dtpipe -i keyring://prod-db -q "@queries/extract_users.sql" -o users.parquet
```
DtPipe automatically detects if `-q` points to a file and loads its content. Using the `@` prefix is recommended to be explicit.
