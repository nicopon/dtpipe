---
paths:
  - "src/DtPipe.Adapters/**"
  - "src/DtPipe.Adapters.Shared/**"
  - "tests/DtPipe.Tests/**"
---

# Adapters

Rules of readers, writers and dialects. The root `CLAUDE.md` holds the rules every task needs.

## Validators CI never runs — run them before you push

`build.yml` runs every `tests/scripts/validate_*.sh` **except** the scripts that source
`lib/test_connections.sh` (they need a database), `validate_vitals` and `validate_xml` (2.7 GB of
scratch). Run that set with the `dtpipe-prepush` skill, which derives it the same way. A new
validator declares a database need by sourcing that file; never add an exclusion list.

**Run them before pushing any change to an adapter, a dialect, a type mapping or a cursor.** A green
CI says nothing about them. `[unchecked]`

## Connection selectors are invisible to providers

`ComponentSelector` is the **single authority** on `{component}[+{variant}]:`.
- `CanHandle` judges the raw string by **content** (extension, connection-string keywords), never by
  its own prefix.
- Route every site through `ComponentSelector`; never hand-roll `StartsWith(name + ":")`.
- A remote URI (`s3://…`) is never a selector.
- An internal channel is never a selector: a transport (`IInternalChannelCapable`) is registered
  apart from the reader and writer factories (`InternalChannelTransports`) and picked by capability
  for a typed endpoint, so no routing site sees its name.
- Variants reach providers as data (`IVariantAwareOptions`), never as text to re-parse.

`[CI: ComponentSelectorTests, RemoteUriClaimTests, LinearPipelineServiceTests — an internal channel
prefix]` `[unchecked: a site that bypasses the
selector]` Detail: `src/DtPipe.Adapters/CLAUDE.md`.

## One way to read DuckDB

`duck:` and `--sql` both read through `DuckDbArrowResultReader`. **Never reimplement the fetch loop
in one consumer**: a copy is a second P/Invoke surface onto the same library, and it drifts. Init
SQL follows the same rule: `DuckInitSqlRunner` (Core) is the single runner for reader, writer and
processor. `[unchecked]` Detail: `src/DtPipe.Adapters.Shared/CLAUDE.md`.

**A NULL from `duckdb_fetch_chunk` is not always the end of the rows.** It also answers a stream that
failed, so `DuckDbArrowResultReader` asks the result before ending: taken for the end, a query failing
mid-stream exits 0 with part of its rows. `[CI: ReadConformanceTests, DuckDBSqlProcessorTests,
validate_cancellation.sh]`

## The conformance table

**A reader reads a cell into the same value on both paths or refuses it naming the column; a writer
writes a column so that a third party reads it back, or its gate refuses it before the target exists.**
`tests/DtPipe.Tests/Unit/Conformance/` holds both faces for the adapters in the process, and answers
every server writer's gate; `Integration/Conformance/` holds the cells that need a server, played by
`validate_type_conformance.sh`. A cell known false names the lot that corrects it and turns red the
day it reads right, so a correction enters the gate with its lot. A new `ArrowTypeId` without a case
fails the table. A cell replaces a unit test of the same case, it does not add to it.
`[CI: ReadConformanceTests, WriteConformanceTests]` `[local: validate_type_conformance.sh]`

**A temporal value leaving a reader is never `DateTimeKind.Local`.** It would let the host's zone into
the data path, which the cell's spelling shows even on a host in UTC. A driver's kind is the adapter's
to correct (`ParquetStreamReader.WallClock`), and a text is read through
`TemporalNormalization.TryParseWallClock` or `TryParseInstant`.
`[CI: ReadConformanceTests]` `[local: validate_temporal.sh]`

**A gate is the list of what its writer accepts.** `ColumnTypeRefusals.Collect` refuses every type
`ArrowTypeMap.HasValueReading` does not name, which is the switch of `GetValue`, so an Arrow type
nobody named is refused before the target rather than failed at its first value. Only a writer that
reads no value has no gate. `CreateEnriched` hands the gate the field a column arrived in when the CLR
type cannot describe it: a struct and a type with no reading would otherwise answer as something else,
or throw. `[CI: WriteConformanceTests]`

**A reader that produces Arrow itself decodes a dictionary** through `ArrowDictionaries`: the sampler
and a columnar writer have no form for one. `[CI: WriteConformanceTests]`

**A decimal with no declared width creates what holds it**, `DecimalSpelling.Undeclared` of each
dialect: the keyword where it is exact and unbounded, else the width it travels at. Never fall back
on an engine's own default: a scale narrower than the value's rounds it, in code 0.
`[CI: WriteConformanceTests]` `[local: validate_type_conformance.sh, validate_declared_decimals.sh]`

**A decimal reads at the scale its column declares, and bare when it declares none, on both paths.**
`DecimalWidth` (`Apache.Arrow.Serialization`, `Mapping/`) holds the rule and the column's limits: Arrow
stores one scale per column, so a path that kept each value's own scale would disagree with the one
that went through a batch. `ArrowTypeMap.GetValue` reads the undeclared mark; a reader that hands a value
over without a batch calls `TryRead`, which also refuses what a batch would. **A `DateTimeOffset` leaving
a reader is in UTC**, for the same reason. `[CI: ReadConformanceTests]`
`[local: validate_type_conformance.sh, validate_declared_decimals.sh]`

## A value a type cannot hold

**A value a reader or a writer cannot carry stops the run with `ColumnValueRefusedException`: where it
stands, the column, its type as the source spells it and the way out, never the value.** It never
reaches a person as a driver's or a builder's message, and no reader replaces it with another value.
The standalone Arrow libraries raise `ArrowValueException`, which the reader that called them turns
into that refusal. A text reads as a fractional number through `TextNumber`, for every text reader and
for the writers' converter: `NumberStyles.Number` alone reads `1,5` as 15. Inference chooses between a
`double` and a `decimal` there too, one rule for every text reader, judged off the text: a decimal has
already rounded a number past its own digits.

**Never set `Npgsql.DisableDateTimeInfinityConversions`**: it is an `AppContext` value of the process,
and these adapters are libraries. `PostgresTemporal` reads a temporal value off its binary form, which
is what tells `-infinity` from a real `0001-01-01`.
`[CI: ReadConformanceTests, WriteConformanceTests, PostgresTemporalTests]`
`[local: validate_type_conformance.sh]` `[unchecked: a new read path that lets a driver's fault through]`

## Adding an adapter

Full patterns: `EXTENDING.md`.
- Row writers build a `ColumnConverterFactory` converter once per column at init, never per-cell
  `ValueConverter.ConvertValue()`.
- Columnar writers implement `IColumnarDataWriter` and read with `GetValueForField`.
- Text readers implement `IColumnTypeInferenceCapable` for `--auto-column-types`.

`[unchecked]` Help attributes are a contract for models: `src/DtPipe.Adapters/CLAUDE.md`.
