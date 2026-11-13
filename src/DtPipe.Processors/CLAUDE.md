# DtPipe.Processors — SQL stream processors

Each processor lives in its own subdirectory (`DuckDB/`, `Merge/`…) with a matching sub-namespace.

`CompositeSqlTransformerFactory` is the DI entry point for `--sql` branches. The only engine is
DuckDB — `DuckDBSqlTransformerFactory` / `DuckDBSqlProcessor`: zero-copy Arrow C Data Interface on
read (`--from`), lazy streaming fetch on write, schema inferred from the prepared statement before
execution. The fetch goes through the shared DuckDB read path; never reimplement it here
(`src/DtPipe.Adapters.Shared/CLAUDE.md`).

**`ChannelArrowStream` waits on the branch's token, never on its argument.** DuckDB reads it through the
C Data exporter, which passes none: a wait with no token holds the run forever once its source stops,
since `DagOrchestrator` returns only when every branch has ended. A cancellation stays a failure of the
stream, never its end, or the query finishes on part of its input.
`[CI: DagOrchestratorTests, validate_cancellation.sh]`

- `DuckHubConnectionParser` parses `duck+{provider}:` strings and issues `INSTALL` / `LOAD` /
  `ATTACH`.
- `--duck-init` / `--compute` / `--expand` values resolve through `IStringContentResolver`
  (`CompositeStringContentResolver` for the CLI, `DefaultStringContentResolver` headless).
- `DuckInitSqlRunner` (Core) is the single init-SQL runner for reader, writer and processor.
- `MergeTransformer.OpenAsync` refuses channels that do not share a schema (count, names, structural
  types; a zone, nullability or field metadata is not compared, since a published channel schema
  carries none of them reliably). It never converts. `[CI: MergeTransformerSchemaTests]`
- Each factory validates its own `--from` arity: `--merge` takes several aliases, `--sql` exactly one
  and materialises the rest through `--ref` (cost-based planning, see `REFERENCE.md#dag-syntax`).

User-facing syntax: `REFERENCE.md#provider-specific-options`, `docs/guides/sql-and-javascript.md`.
