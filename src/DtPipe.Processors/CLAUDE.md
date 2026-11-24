# DtPipe.Processors — SQL stream processors

Each processor lives in its own subdirectory (`DuckDB/`, `Merge/`…) with a matching sub-namespace.

`CompositeSqlTransformerFactory` is the DI entry point for `--sql` branches. The only engine is
DuckDB — `DuckDBSqlTransformerFactory` / `DuckDBSqlProcessor`: Arrow C Data table functions on read
(`--from` and `--ref`), lazy streaming fetch on write, schema inferred from the prepared statement before
execution. The fetch goes through the shared DuckDB read path; never reimplement it here
(`src/DtPipe.Adapters.Shared/CLAUDE.md`).

**`DuckDbArrowTableSource` waits on the branch's token, never on DuckDB's callbacks.** An empty or
blocked `--from` channel stops on cancellation and sets a DuckDB function error so the run returns 130
instead of hanging in `DagOrchestrator`, which waits until every branch ends.
`[CI: DagOrchestratorTests, validate_cancellation.sh]`

**Each source registers a 0-arg table function (`__dtp_scan_{n}()`) exposed under a view of its alias,
and converts one column at a time.** `duckdb_table_function_supports_projection_pushdown` gives `Init`
the exact projected column indices, so views stay `SELECT * FROM __dtp_scan_{n}()` without `EXPLAIN` or
`disabled_optimizers='filter_pushdown'`. Converting per column with `duckdb_schema_from_arrow` /
`duckdb_data_chunk_from_arrow` and transferring via `duckdb_vector_reference_vector` is mandatory:
DuckDB 1.5.6 clears `arrow_array->release` on column 0 while allocating `owned_data` inside the column
loop, so a multi-column call releases the array before later columns are read.
`[CI: DuckDBSqlProcessorTests]`

**A projected column identifier is one of the source's columns or one DuckDB reserves; any other is an
error naming the alias.** The scan skips only `COLUMN_IDENTIFIER_ROW_ID` and `COLUMN_IDENTIFIER_EMPTY`
(`DuckDbArrowTableSource.IsReservedColumnId`), and `Init` refuses, once per scan, any other identifier
past the source's columns: skipped, it would leave its output vector unfilled. DuckDB passes neither
reserved identifier to a function that declares no virtual column: a scan that reads no column
(`COUNT(*)`) is given column 0. `[CI: DuckDbArrowTableFunctionTests; DuckDBSqlProcessorTests —
ScanReadingNoColumn]`

**Batches are sliced at `STANDARD_VECTOR_SIZE` (2048) and deterministically released.** Each 1-column
slice retains its buffers (`ArrowOwnership.RetainArray`), and `CArrowArrayReleaseHook` chains a release
onto its exported `CArrowArray` so the slice disposes as soon as DuckDB releases its vector: the
exporter's release runs once, the slice is disposed once, and an array DuckDB did not take is released
on the error path. The hook reaches Apache.Arrow's internal `release` field at the address the C Data
Interface gives it, immediately before `private_data`. `[CI: DuckDBSqlProcessorTests;
CArrowArrayReleaseHookTests — the chain without DuckDB, and the struct's layout]`

**`--ref` sources are re-scannable; `--from` allows a single scan.** Each `Init` on a `--ref` opens an
independent cursor over its in-memory batches (informed by `duckdb_bind_set_cardinality(..., isExact:
true)`). A second active `Bind` during `Prepare` or second `Init` on `--from` fails naming the alias,
catching both duplicate query references and recursive CTE rescans without rejecting DuckDB's
post-prepare rebinds (e.g. `read_parquet`).
`[CI: DuckDBSqlProcessorTests — RefScannedTwice, RefJoinedInARecursivePart, MainScannedTwice,
MainScannedInRecursiveCte, ScanReadingNoColumn]`

**The `--from` stream declares a cardinality far above any `--ref`, never none.** A `--ref` declares its
exact row count; a stream declared without one is estimated at one row, and DuckDB builds every hash join
on it: the whole stream held in memory and no row out before its end.
`[CI: DuckDBSqlProcessorTests — MainJoinedWithRef_StreamsBeforeMainEnds]`

**The `--ref` sources are drained together, then registered one after the other.** The drains
overlap, since the wait on the sources is the cost; registration mutates the connection and `_sources`,
so two registrations at once race on both. `[CI: DuckDBSqlProcessorTests — RefsEndingTogether]`

**The processor closes the result reader, then DuckDB, then `_sources`.** Closing `_conn` destroys all
DuckDB scan states and drops every vector reference so the releases `CArrowArrayReleaseHook` chained
run while `_sources` and its `GCHandle` / converted schemas / `--ref` batches are still valid.
`[CI: DuckDBSqlProcessorTests — RefBatchesNoScanRead]`

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
