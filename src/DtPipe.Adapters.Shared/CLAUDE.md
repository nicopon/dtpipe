# DtPipe.Adapters.Shared — the DuckDB read path

Mechanics behind the root rule "One way to read DuckDB".

`DuckDbArrowNative` + `DuckDbArrowResultReader` (`Infrastructure/DuckDb/`) own the whole
prepare-to-batch half: `duckdb_prepare` → schema off the prepared statement →
`duckdb_execute_prepared_streaming` → `duckdb_fetch_chunk` → `duckdb_data_chunk_to_arrow`.
**Both `DuckDataSourceReader` (`duck:`) and `DuckDBSqlProcessor` (`--sql`) read through it.**
Adapters and Processors are siblings that cannot reference each other, which is why this code lives
here: a copy in either is a second P/Invoke surface onto one native library, and type handling
(`ENUM`, `ARRAY`, `BOOLEAN`, `HUGEINT`) drifts between the copies.

`[unchecked — validate_core_boundary.sh does not look here]`

## Ownership of imported batches

A `RecordBatch` imported by `CArrowArrayImporter` follows the ordinary rule — the consumer disposes
what it receives — but the C Data release callback frees its memory, which never passes through a
`MemoryAllocator`. `TrackingMemoryPool` (and so `ArrowOwnershipTests`) cannot observe it.
`CDataReleaseProbe` counts the release callback instead, and `CDataOwnershipTests` runs the engine's
consumers — segment runner, writer boundary and its `--limit` slice, fan-out, row bridge — over
imported batches.

**Never use peak memory to check ownership.** A finalizer releases a batch a consumer forgot to
dispose, so a broken dispose leaves peak memory unchanged. `validate_duck_streaming.sh` reads the
peak only to show that the result streams chunk by chunk rather than being held whole.

## Memory ceiling and spill directory

`DuckDbResourceSettings` is applied by the `duck:` reader, the `duck:` writer and the `--sql`
processor right after their connection opens, before `--duck-init` so a user's own `SET` wins. It
sets `memory_limit` to a fraction of what the runtime reports as available and gives the instance
its own `temp_directory`. Never hard-code either at a call site: the sites would disagree.

- **One spill directory per instance.** Instances sharing one delete each other's live spill files
  when one closes, and the survivor's query fails with an I/O error.
- **Apply it before any `enable_external_access=false`:** the spill directory is locked after that.
- **A `ref` table is the processor's own table, not a scan.** `duckdb_arrow_scan` hands its single
  stream to the first scan and leaves nothing for the next, so only a table can be read twice.

`[CI: DuckDbResourceSettingsTests, DuckDBSqlProcessorTests]` `[unchecked: a new DuckDB connection
site that skips the helper]`
