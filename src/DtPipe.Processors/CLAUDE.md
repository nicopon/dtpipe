# DtPipe.Processors — SQL stream processors

Each processor lives in its own subdirectory (`DuckDB/`, `Merge/`…) with a matching sub-namespace.

`CompositeSqlTransformerFactory` is the DI entry point for `--sql` branches. The only engine is
DuckDB — `DuckDBSqlTransformerFactory` / `DuckDBSqlProcessor`: zero-copy Arrow C Data Interface on
read (`--from`), lazy streaming fetch on write, schema inferred from the prepared statement before
execution. The fetch goes through the shared DuckDB read path; never reimplement it here
(`src/DtPipe.Adapters.Shared/CLAUDE.md`).

- `DuckHubConnectionParser` parses `duck+{provider}:` strings and issues `INSTALL` / `LOAD` /
  `ATTACH`.
- `--retry` uses Polly v8 (`DatabaseRetryPolicy`).
- `--duck-init` / `--compute` / `--expand` values resolve through `IStringContentResolver`
  (`CliStringContentResolver` for the CLI, `DefaultStringContentResolver` headless).
- `DuckInitSqlRunner` (Core) is the single init-SQL runner for reader, writer and processor.
- Each factory validates its own `--from` arity: `--merge` takes several aliases, `--sql` exactly one
  and materialises the rest through `--ref` (cost-based planning, see `REFERENCE.md#dag-syntax`).

User-facing syntax: `REFERENCE.md#provider-specific-options`, `docs/guides/sql-and-javascript.md`.
