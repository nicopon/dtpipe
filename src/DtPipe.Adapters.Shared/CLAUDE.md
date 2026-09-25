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
