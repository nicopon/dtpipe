# DtPipe.Adapters — selectors, new adapters, adapter help

Mechanics for editing adapters. Full patterns for a new adapter: `EXTENDING.md`. Readers and writers
live under `Adapters/<Name>/`.

## Connection selectors are invisible to providers

`ComponentSelector` (`DtPipe.Core.Abstractions`) is the single authority on the
`{component}[+{variant}]:` grammar, and the only place that knows prefixes exist.

- **Never test for your own prefix.** `CanHandle` receives the RAW string and judges by content only:
  file extension (`.duckdb`, `.csv`) or connection-string keywords (`Host=`, `Data Source=`). A
  prefix test there hands the provider a string the router never stripped. See the warning on
  `IDataFactory.CanHandle`.
- **Route every site through `ComponentSelector`** (`git grep ComponentSelector` lists them). Never
  hand-roll `StartsWith(ComponentName + ":")`: hand-rolled copies of the grammar drift.
- **A remote URI is never a selector.** The grammar ends in `(?!//)`, so `s3://bucket/key.parquet`
  reaches the provider intact — a property of the grammar, not a guard each caller repeats.
- **Variants reach the provider as data, not text to re-parse.** `ComponentSelector` splits
  `duck+mysql:Host=…` into variant `mysql` + details `Host=…`; the router puts the variant on
  `ConnectionRoute.InputVariant` / `OutputVariant`, and `CliProviderFactory` pushes it onto options
  implementing `IVariantAwareOptions`. The selector owns the syntax; the provider decides which
  variants are valid (`DuckHubConnectionParser`). Retiring a variant is a provider change, never a
  grammar change.

`[CI: ComponentSelectorTests, RemoteUriClaimTests.No_Component_Selector_Strips_A_Remote_Uri —
catalogue-wide]` `[unchecked: a routing site that bypasses ComponentSelector entirely]`

## Writing adapter help (`[Description]` / `[ComponentHelp]`)

`get-adapter-help` is the only view a model gets of an adapter, so these attributes are a contract.

- **Say what reflection cannot.** Option names, types and descriptions are already emitted. Use
  `usageNotes` for prerequisites (MySQL bulk needs `local_infile=ON` server-side), silent fallbacks,
  and semantics that make an option dangerous (`--strategy Upsert` needs a PRIMARY KEY or UNIQUE
  index over the key columns, or MySQL appends duplicates). An option a model can set without
  knowing its failure mode is worse than one it cannot see.
- **Give reader and writer their own attributes**; both are emitted. The writer's carries the write
  semantics, not a copy of the reader's.
- **Keep the component's own side of an example concrete and the counterpart a placeholder**
  (`<adapter-prefix>:<target>` / `<source>`). A real counterpart anchors the model on an unrelated
  component, and a verbatim copy writes a file nobody asked for; a placeholder fails closed with "No
  writer factory resolved". Only exception: `generate:` ↔ `null:`, where the pairing is the lesson.
- **Name the driver and say the key list is open.** ADO.NET fixes the `Key=Value` form, not the
  vocabulary: the driver (Npgsql, MySqlConnector, …) owns the option set. Naming it lets a model reach
  past the keys shown and away from another driver's options.

`[CI: McpAdapterHelpTests — both roles present, counterpart placeholders, driver named]`
`[unchecked: the first bullet]`
