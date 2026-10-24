# DtPipe.Core — DAG orchestrator, Arrow ownership, redaction

Mechanics for editing Core. The rules themselves are in the root `CLAUDE.md`.

## DAG engine

`DagOrchestrator` spawns one concurrent `Task` per branch; a linear run goes through it once too.
Each branch runs in the engine, which lives in the CLI's assembly (`src/DtPipe/CLAUDE.md`, "The
engine and its host"); its kernel is `PipelineExecutor.ExecuteSegmentedPipelineAsync`. What the
orchestrator reports is a `DagLogEvent` of plain text, never markup. Branches communicate through
`IMemoryChannelRegistry` (Arrow `Channel<RecordBatch>`).
Fan-out (broadcast/tee) resolves through `BranchChannelContext.AliasMap` — logical alias → physical
channel, including `s__fan_0` sub-channels — which `DagOrchestrator` populates and factories consume
directly (e.g. `DuckDBSqlTransformerFactory`).

`DagOrchestrator.ExecuteAsync` waits at its single exit (`finally`) for every task it started,
branches and broadcast multiplexers: it cancels what still runs, awaits each task, and logs an
exception that arrives after the verdict without changing the verdict.

### Cancellation (F16)

- `LinearPipelineService` tells the dedicated user token apart from internal cancellation sources:
  it returns 130 on user shutdown and lets internal cancellation propagate.
- A DAG branch reporting 130 makes `DagOrchestrator` cancel the rest and return 130.
- The only site allowed to swallow cancellation is `DagOrchestrator.ExecuteBranchAsync`'s
  orphaned-producer path: returning 0 there is normal fan-out once the consumers complete.

`[local: validate_cancellation.sh — drives real interrupts]`

## Transformer pipeline

`IDataTransformer`: `InitializeAsync` (schema), `Transform` (per row), `Flush` (end of stream).
`PipelineSegmenter` groups consecutive columnar-capable transformers into segments, bridged to row
mode through Arrow zero-copy.

## RecordBatch ownership

Arrow buffers are off-heap (`NativeMemoryAllocator`) and reference-counted: an undisposed
`RecordBatch` is native memory the GC cannot see, reclaimed only when its finalizer runs.

- A **reader** or **row→columnar bridge** produces batches and hands each one to its consumer.
- `PipelineExecutor.ApplyColumnarSegmentAsync` owns every batch it pulls and disposes that input
  after the transformer chain — **unless the transformer returned the same reference**
  (`ReferenceEquals`: pass-through, the one object is still live). It never disposes what it yields;
  the next segment or the writer owns that.
- An `IColumnarTransformer` returning a **new** `RecordBatch` that reuses an input column buffer
  **must** wrap that column in `ArrowOwnership.RetainArray(...)`, or the input's dispose frees
  buffers the output still points at. `git grep RetainArray` shows the idiom.
- The **writer** takes ownership in `WriteRecordBatchAsync` and disposes.
- `BridgeColumnarToRowsAsync` is a terminal consumer: `using (batch)`.
- A reader handing over **Arrow IPC** batches re-homes them through `ArrowOwnership.TakeOwnership`.
  IPC column buffers carry no shared handle — the message body is one allocation owned by the
  `RecordBatch` — so `RetainArray` bumps nothing there. Apache.Arrow keeps its sharing machinery
  `internal`, so the copy is the only fix available from outside.
- **Fan-out**: the broadcaster owns the upstream batch, gives each of N consumers its own batch via
  `ArrowOwnership.RetainAll` (a refcount bump, not a deep `Clone`), then disposes its reference.
  Each consumer disposes what it received.

`[CI: ArrowOwnershipTests — TrackingMemoryPool returns to zero after a linear chain and after a
fan-out with a row branch; CDataOwnershipTests for C Data imports, see
src/DtPipe.Adapters.Shared/CLAUDE.md]` `[unchecked: a transformer that aliases a column without
retaining it]`

## Infrastructure/Arrow boundary

`Infrastructure/Arrow/` is generic Arrow code inside Core (type-map facade, row↔columnar bridges,
`ArrowOwnership`), **not** standalone. Its surface onto the rest of DtPipe is exactly three types:
`PipeColumnInfo`, `IRowToColumnarBridge`, `IColumnarToRowBridge`. Keep engine concerns out of it:
this is the hottest path in the product.

`[CI: validate_core_boundary.sh — the allowlist is checked against every type Core/Models and
Core/Abstractions declare]`

## Redaction (`Security/ConnectionStringSanitizer`)

- **`Redact`** parses a connection string and prints only keys on the safe list; every other
  `key=value` is masked, including a key nobody anticipated. Use it wherever the value *is* a
  connection: a message, the DAG panel, an MCP plan, the agent's approval dialog.
- **`Sanitize`** scans text with no grammar (a driver exception, a YAML excerpt, a tool argument)
  for keys that look sensitive. Best-effort by construction.

Both consult the same safe list, so they never disagree about one key.

A value can be a secret by its shape: a coordinator's identity key (`dtp_{client id}_{43 base64url}`)
is masked by both wherever it stands, under any key or none, and makes `CarriesLiteralSecret` true.
The coordinator makes it and the studio presents it, both reading its shape from
`CoordinatorApi.ClientIdOfKey` (`DtPipe.Jobs`), which references no Core, so the shape is written
twice: change both together.

**Never anchor the sensitive-key pattern on `\b`**: `\b` breaks on neither `_` nor a capital, so it
finds no `secret` in `s3_secret_access_key`, `SecretAccessKey`, `AccountKey` or
`SharedAccessSignature` — forms this product builds itself.

**No site sits on the other side of that line**, not even `CheckpointKey`, which hashes rather than
prints: one exception with no check is how unredacted sites go unnoticed.

To step over a prefix before the first key, use `ComponentSelector.SkipSelector`; never hand-roll the
`(?!//)` — hand-rolled copies of the grammar drift.

`[CI: ConnectionStringSanitizerTests — its form table is the specification, add a new
secret-bearing form there first; LinearPipelineServiceTests over both routing failures;
validate_secret_redaction.sh]` `[unchecked: a connection reaching a message under a name that does
not look like one; a credential a driver quotes back in its own exception]`
