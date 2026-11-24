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
- **A JavaScript script keeps nothing from one call to the next.** `JsEngineProvider` freezes each
  engine's realm when it makes it (`JsRealmFreeze`): a write to a global, a built-in or a prototype
  throws, naming the write. The four JavaScript transformers hand every script the view they move from
  row to row; a view refuses to lose its extensibility and takes `Object.prototype` back at each move,
  and each window gets a new array over the views. Never give a script an object that outlives its
  call without closing it the same way: a script that kept a row would read the row the view moved to,
  in a batch its owner may have disposed. `[CI: JsRealmFreezeTests, JsKeptRowTests]`
