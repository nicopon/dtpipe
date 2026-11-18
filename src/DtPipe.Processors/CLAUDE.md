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

**A registered scan exports its columns behind a per-stream prefix and the user's table is a view that
names them back.** DuckDB hands an Arrow scan only the columns its plan uses, in the plan's order, and
EXPLAIN lists them per `ARROW_SCAN` without naming the table: the prefix is what ties a scan's
projection to exactly one stream. Never merge the projections of two scans: two sources that carry a
column of the same name hand each other's column to DuckDB, which reads it as another type and faults
natively. `[CI: DuckDBSqlProcessorTests — SourcesSharingAColumnName]`

**A stream is read by one scan.** Each scan ends on the stream's end, so a source scanned twice
hands the second scan nothing, in code 0. A `--ref` the plan scans more than once (a scan in the
recursive part of a recursive CTE counts twice: it runs at each iteration) is read once into a
`MATERIALIZED` CTE put in front of the query under its alias (first in the query's own `WITH` list),
off the raw scan, and the plan is checked again; the main (`--from`) is refused. Never a table in
place of the CTE: its known size moves a join's build side onto the stream, which DuckDB estimates
at one row. Never a CTE inside the view: a view is copied again at each reference. A scan that reads
no column names no source in EXPLAIN, so `ProjectedArrowStream` fails the query on a second end.
`[CI: DuckDBSqlProcessorTests — RefScannedTwice, RefJoinedInARecursivePart, MainScannedTwice,
ScanReadingNoColumn]`

**The `--ref` sources are drained together, then registered one after the other.** The drains
overlap, since the wait on the sources is the cost; a registration writes the processor's maps of the
sources and uses the one connection, so two registrations at once lose an entry and the query is
refused or faults. `[CI: DuckDBSqlProcessorTests — RefsEndingTogether]`

**The processor closes DuckDB, then releases each stream it exported, then frees its struct.** DuckDB
never calls an exported stream's release (a scan reads a copy whose release does nothing), so the
processor's dispose imports the struct and disposes the import: the exporter's handle is freed and the
stream disposed, which disposes the `--ref` batches no scan read. A stream's dispose runs once, whoever
calls it. Released while the connection is open, a stream is a freed handle a running scan still calls
into, a native fault; never released, it keeps the stream and all its batches reachable.
`[CI: DuckDBSqlProcessorTests — RefBatchesNoScanRead]` `[unchecked: the release itself and its order]`

**A batch DuckDB has read is not disposed by its stream; its finalizer takes it back**, for a `--ref`
as for the `--from`. The export reaches a buffer with no handle only by pinning it, so a stream that
disposed a batch it returned would free memory a scan still reads. `[unchecked]`

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
