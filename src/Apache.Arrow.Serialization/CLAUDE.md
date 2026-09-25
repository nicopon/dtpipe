# Apache.Arrow.Serialization

Standalone library: no DtPipe dependency, nothing beyond `Apache.Arrow`.

```
Apache.Arrow.Serialization ← standalone
Apache.Arrow.Ado           ← uses ArrowTypeResult
DtPipe.Core                ← ArrowTypeMapper is a facade over ArrowTypeMap
```

`ArrowTypeMap` (`Mapping/ArrowTypeMap.cs`) is the canonical CLR↔Arrow map.
`FixedSizeBinaryArrayBuilder` exists only in `Reflection/FixedSizeBinaryArrayBuilder.cs`; Core
consumes it by project reference. `ArrowSerializer` / `ArrowDeserializer` usage: `EXTENDING.md`.

## Representation rules are named for themselves, not for Arrow

A database `BINARY(16)` column needs RFC 4122 byte order and a row-mode DB parameter needs the same
temporal rule, so each rule lives under its own name in `Mapping/` and the Arrow-facing spellings
delegate to it:

| Rule | Owner | Arrow-facing spelling |
|---|---|---|
| RFC 4122 big-endian byte order | `Rfc4122Guid.ToBigEndianBytes` / `FromBigEndianBytes` | `ArrowTypeMap.ToArrowUuidBytes` / `FromArrowUuidBytes` |
| Zone-less `DateTime` handling | `TemporalNormalization.ToOffset` / `ToWallClock` | called directly by `ArrowTypeMap.GetValue` and the readers |

**`TemporalNormalization` owns both directions; change them together.** A `DateTime` with
`Kind=Unspecified` is a wall clock with no zone. `new DateTimeOffset(dt)` and
`TimestampArray.Builder.Append(DateTime)` both resolve it against `TimeZoneInfo.Local`, which makes
the output depend on the machine's time zone.

`[CI: validate_core_boundary.sh — no new DateTimeOffset( outside the rule, no using DtPipe. here]`
`[local: validate_temporal.sh — the binary under two TZ values must produce identical output]`
