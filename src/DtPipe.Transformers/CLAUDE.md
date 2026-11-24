# DtPipe.Transformers

Each transformer lives in its own subdirectory (`Row/Expand/`, `Arrow/Filter/`…) with a matching
sub-namespace.

- **A columnar transformer that returns a new `RecordBatch` reusing any input column must wrap that
  column in `ArrowOwnership.RetainArray(...)`.** The segment runner disposes the input after the
  chain; without the retain the output points at freed buffers. Returning the *same* reference is
  pass-through and needs nothing. `git grep RetainArray` shows the idiom; full ownership rules in
  `src/DtPipe.Core/CLAUDE.md`. `[unchecked]`
- A transformer that aggregates or expands emits through `TransformMany` + `Flush`, not `Transform`
  (`WindowDataTransformer.Transform` returns `null`). Sample mode observes the real run, so this is
  what `--dry-run` reports.
- **A JavaScript row is a value.** The four JavaScript transformers hand a script the view they reuse
  from row to row (a cursor, a window's pool and array) only when `JsRowRetention.CanShareView` proves
  the script keeps nothing of it; any other script gets a view of its own per row, its values copied
  when it is made (`JsRowObjectInstance.Own`), so a kept row never reads a batch its owner disposed.
  One predicate decides for the four, at initialisation. `[CI: JsRowRetentionTests, JsKeptRowTests]`
