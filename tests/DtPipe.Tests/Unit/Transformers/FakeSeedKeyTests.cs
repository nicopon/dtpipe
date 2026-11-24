using System.Globalization;
using System.Text;
using Apache.Arrow;
using Apache.Arrow.Arrays;
using Apache.Arrow.Serialization.Mapping;
using Apache.Arrow.Types;
using AwesomeAssertions;
using DtPipe.Transformers.Arrow.Fake;
using Xunit;

namespace DtPipe.Tests.Unit.Transformers;

/// <summary>
/// The key a seed column gives a row is a function of the value the column holds as
/// <see cref="ArrowTypeMap.CompileValueReader"/> reads it, never of how the column stores it.
/// </summary>
public class FakeSeedKeyTests
{
    private static readonly DateTimeOffset Instant = new(2024, 3, 5, 10, 11, 12, TimeSpan.Zero);
    private static readonly DateTimeOffset EarlierInstant = new(1965, 7, 8, 1, 2, 3, TimeSpan.Zero);
    private static readonly long InstantSeconds = Instant.ToUnixTimeSeconds();
    private static readonly long EarlierSeconds = EarlierInstant.ToUnixTimeSeconds();

    private static (ulong[] Keys, bool[] HasKey) KeysOf(IArrowArray column)
    {
        var schema = new Schema.Builder().Field(new Field("seed", column.Data.DataType, true)).Build();
        // Not disposed: the caller keeps reading the column, and a batch disposes the arrays it holds.
        var batch = new RecordBatch(schema, new[] { column }, column.Length);
        var keys = new ulong[column.Length];
        var hasKey = new bool[column.Length];
        ArrowSeedKeyExtractor.FillRowKeys(batch, new[] { 0 }, keys, hasKey);
        return (keys, hasKey);
    }

    private static long PerSecond(TimeUnit unit) => unit switch
    {
        TimeUnit.Second => 1L,
        TimeUnit.Millisecond => 1_000L,
        TimeUnit.Microsecond => 1_000_000L,
        _ => 1_000_000_000L
    };

    /// <summary>A timestamp column from raw stored values, null for a null cell.</summary>
    private static TimestampArray Timestamps(TimeUnit unit, string? zone, params long?[] stored)
    {
        var values = new ArrowBuffer.Builder<long>();
        var validity = new ArrowBuffer.BitmapBuilder();
        var nulls = 0;
        foreach (var v in stored)
        {
            values.Append(v ?? 0);
            validity.Append(v.HasValue);
            if (!v.HasValue) nulls++;
        }
        var type = new TimestampType(unit, zone);
        return new TimestampArray(new ArrayData(type, stored.Length, nulls, 0, new[] { validity.Build(), values.Build() }));
    }

    private static Date32Array Dates32(params int?[] daysSinceEpoch)
    {
        var builder = new Date32Array.Builder();
        foreach (var d in daysSinceEpoch)
        {
            if (d is { } days) builder.Append(DateOnly.FromDayNumber(DateOnly.FromDateTime(DateTime.UnixEpoch).DayNumber + days));
            else builder.AppendNull();
        }
        return builder.Build();
    }

    private static Date64Array Dates64(params long?[] millisSinceEpoch)
    {
        var values = new ArrowBuffer.Builder<long>();
        var validity = new ArrowBuffer.BitmapBuilder();
        var nulls = 0;
        foreach (var v in millisSinceEpoch)
        {
            values.Append(v ?? 0);
            validity.Append(v.HasValue);
            if (!v.HasValue) nulls++;
        }
        return new Date64Array(new ArrayData(Date64Type.Default, millisSinceEpoch.Length, nulls, 0, new[] { validity.Build(), values.Build() }));
    }

    private static StringArray Text(params string?[] values)
    {
        var builder = new StringArray.Builder();
        foreach (var v in values)
        {
            if (v is null) builder.AppendNull();
            else builder.Append(v);
        }
        return builder.Build();
    }

    private static DictionaryArray DictionaryOfText(IArrowType indexType, string?[] entries, int?[] positions)
        => new(new DictionaryType(indexType, StringType.Default, false), Integers(indexType, positions), Text(entries));

    /// <summary>An integer column of the given type, from values that fit the narrowest type.</summary>
    private static IArrowArray Integers(IArrowType type, int?[] values)
    {
        var width = type switch
        {
            Int8Type or UInt8Type => 1,
            Int16Type or UInt16Type => 2,
            Int32Type or UInt32Type => 4,
            _ => 8
        };
        var bytes = new byte[width * values.Length];
        var validity = new ArrowBuffer.BitmapBuilder();
        var nulls = 0;
        for (var i = 0; i < values.Length; i++)
        {
            BitConverter.GetBytes((long)(values[i] ?? 0)).AsSpan(0, width).CopyTo(bytes.AsSpan(i * width));
            validity.Append(values[i].HasValue);
            if (!values[i].HasValue) nulls++;
        }
        return ArrowArrayFactory.BuildArray(new ArrayData(type, values.Length, nulls, 0, new[] { validity.Build(), new ArrowBuffer(bytes) }));
    }

    /// <summary>
    /// Each group holds one value in several representations: every array of a group must give the
    /// same keys, row by row.
    /// </summary>
    public static TheoryData<string> GroupNames => new()
    {
        "timestamp-whole-seconds",
        "timestamp-sub-second",
        "timestamp-nanosecond-digits-the-reader-drops",
        "date",
        "date-and-the-midnight-instant",
        "dictionary-of-text",
        "text-and-dictionary-of-text",
        "integers"
    };

    private static IArrowArray[] Group(string name)
    {
        var units = new[] { TimeUnit.Second, TimeUnit.Millisecond, TimeUnit.Microsecond, TimeUnit.Nanosecond };
        var zones = new string?[] { null, "UTC", "Europe/Paris", "+02:00" };
        long Days(DateTimeOffset d) => d.ToUnixTimeSeconds() / 86_400;

        switch (name)
        {
            case "timestamp-whole-seconds":
                return (from unit in units
                        from zone in zones
                        select (IArrowArray)Timestamps(unit, zone,
                            InstantSeconds * PerSecond(unit), null, EarlierSeconds * PerSecond(unit))).ToArray();

            case "timestamp-sub-second":
                return (from unit in new[] { TimeUnit.Millisecond, TimeUnit.Microsecond, TimeUnit.Nanosecond }
                        from zone in zones
                        select (IArrowArray)Timestamps(unit, zone,
                            InstantSeconds * PerSecond(unit) + 345 * PerSecond(unit) / 1000,
                            null,
                            EarlierSeconds * PerSecond(unit) + 5 * PerSecond(unit) / 1000)).ToArray();

            case "timestamp-nanosecond-digits-the-reader-drops":
                return new IArrowArray[]
                {
                    Timestamps(TimeUnit.Microsecond, "UTC", InstantSeconds * 1_000_000 + 345_678),
                    Timestamps(TimeUnit.Nanosecond, "UTC", InstantSeconds * 1_000_000_000 + 345_678_000),
                    Timestamps(TimeUnit.Nanosecond, "UTC", InstantSeconds * 1_000_000_000 + 345_678_099),
                    Timestamps(TimeUnit.Nanosecond, null, InstantSeconds * 1_000_000_000 + 345_678_042)
                };

            case "date":
                {
                    var days = new[] { (int)Days(Instant), (int)Days(EarlierInstant) };
                    return new IArrowArray[]
                    {
                    Dates32(days[0], null, days[1]),
                    Dates64(days[0] * 86_400_000L, null, days[1] * 86_400_000L)
                    };
                }

            case "date-and-the-midnight-instant":
                {
                    var days = new[] { (int)Days(Instant), (int)Days(EarlierInstant) };
                    return new IArrowArray[]
                    {
                    Dates32(days[0], null, days[1]),
                    Dates64(days[0] * 86_400_000L, null, days[1] * 86_400_000L),
                    Timestamps(TimeUnit.Millisecond, null, days[0] * 86_400_000L, null, days[1] * 86_400_000L),
                    Timestamps(TimeUnit.Second, "Europe/Paris", days[0] * 86_400L, null, days[1] * 86_400L)
                    };
                }

            case "dictionary-of-text":
                {
                    var entries = new string?[] { "alice", "é€", "zed" };
                    var positions = new int?[] { 1, null, 0, 1, 0 };
                    return new IArrowArray[]
                    {
                    DictionaryOfText(Int8Type.Default, entries, positions),
                    DictionaryOfText(Int16Type.Default, entries, positions),
                    DictionaryOfText(Int32Type.Default, entries, positions),
                    DictionaryOfText(Int32Type.Default, new string?[] { "zed", "alice", "é€" }, new int?[] { 2, null, 1, 2, 1 })
                    };
                }

            case "text-and-dictionary-of-text":
                return new IArrowArray[]
                {
                    Text("é€", null, "alice", "é€", "alice"),
                    DictionaryOfText(Int8Type.Default, new string?[] { "alice", "é€" }, new int?[] { 1, null, 0, 1, 0 }),
                    DictionaryOfText(Int32Type.Default, new string?[] { "alice", "é€" }, new int?[] { 1, null, 0, 1, 0 })
                };

            case "integers":
                return new IArrowType[]
                {
                    Int8Type.Default, Int16Type.Default, Int32Type.Default, Int64Type.Default,
                    UInt8Type.Default, UInt16Type.Default, UInt32Type.Default, UInt64Type.Default
                }.Select(t => Integers(t, new int?[] { 42, null, 100 })).ToArray();

            default:
                throw new ArgumentOutOfRangeException(nameof(name), name, null);
        }
    }

    [Theory]
    [MemberData(nameof(GroupNames))]
    public void SameValueInAnotherRepresentation_GivesTheSameKey(string group)
    {
        var arrays = Group(group);
        var (expectedKeys, expectedHasKey) = KeysOf(arrays[0]);

        foreach (var array in arrays.Skip(1))
        {
            var (keys, hasKey) = KeysOf(array);
            keys.Should().Equal(expectedKeys, $"{array.Data.DataType} holds the value {arrays[0].Data.DataType} holds");
            hasKey.Should().Equal(expectedHasKey);
        }
    }

    [Theory]
    [MemberData(nameof(GroupNames))]
    public void Key_IsThatOfTheValueTheCanonicalReaderReturns(string group)
    {
        foreach (var array in Group(group))
        {
            var field = new Field("seed", array.Data.DataType, true);
            var reader = ArrowTypeMap.CompileValueReader(array, field);
            var (keys, hasKey) = KeysOf(array);

            for (var i = 0; i < array.Length; i++)
            {
                var value = reader(i);
                if (value is null)
                {
                    keys[i].Should().Be(ExpectedKey(null), $"row {i} of {array.Data.DataType} is null");
                    hasKey[i].Should().BeFalse();
                }
                else
                {
                    keys[i].Should().Be(ExpectedKey(value), $"row {i} of {array.Data.DataType} reads as {value} ({value.GetType().Name})");
                    hasKey[i].Should().BeTrue();
                }
            }
        }
    }

    /// <summary>What a key is, said from the value the reader returns and not from any storage.</summary>
    private static ulong ExpectedKey(object? value)
    {
        ulong valueHash;
        switch (value)
        {
            case null:
                return StableHash.Combine64(ArrowSeedKeyExtractor.InitialAccumulator, 0, ArrowSeedKeyExtractor.NullLaneSentinel);
            case string s:
                valueHash = StableHash.Compute64(Encoding.UTF8.GetBytes(s));
                break;
            case DateOnly d:
                valueHash = StableHash.Compute64((long)d.DayNumber * TimeSpan.TicksPerDay);
                break;
            case DateTimeOffset t:
                valueHash = StableHash.Compute64(t.UtcTicks);
                break;
            case DateTime dt:
                dt.Kind.Should().NotBe(DateTimeKind.Local, "the host's zone is no part of a value");
                valueHash = StableHash.Compute64(dt.Ticks);
                break;
            case ulong u:
                valueHash = StableHash.Compute64(u);
                break;
            default:
                valueHash = StableHash.Compute64(Convert.ToInt64(value, CultureInfo.InvariantCulture));
                break;
        }
        return StableHash.Combine64(ArrowSeedKeyExtractor.InitialAccumulator, 0, valueHash);
    }

    [Fact]
    public void ADateAndAnInstantAtMidnight_GiveTheSameKey_BecauseTheReaderGivesTheSameMoment()
    {
        var day = (int)(new DateTimeOffset(2024, 3, 5, 0, 0, 0, TimeSpan.Zero).ToUnixTimeSeconds() / 86_400);
        var date = KeysOf(Dates32(day)).Keys;
        var wallClock = KeysOf(Timestamps(TimeUnit.Microsecond, null, day * 86_400_000_000L)).Keys;
        var instant = KeysOf(Timestamps(TimeUnit.Microsecond, "Europe/Paris", day * 86_400_000_000L)).Keys;

        wallClock.Should().Equal(date);
        instant.Should().Equal(date);
    }

    [Fact]
    public void ANanosecondTimestamp_KeepsTheTicksTheReaderKeeps()
    {
        var micro = KeysOf(Timestamps(TimeUnit.Microsecond, "UTC", InstantSeconds * 1_000_000 + 345_678)).Keys;
        var nano = KeysOf(Timestamps(TimeUnit.Nanosecond, "UTC", InstantSeconds * 1_000_000_000 + 345_678_900)).Keys;
        var anotherTick = KeysOf(Timestamps(TimeUnit.Nanosecond, "UTC", InstantSeconds * 1_000_000_000 + 345_678_100)).Keys;

        nano.Should().NotEqual(micro, "a tick is 100 ns: 345 678 900 ns is the tick after 345 678 000 ns");
        anotherTick.Should().NotEqual(micro);
        anotherTick.Should().NotEqual(nano);
    }

    /// <summary>
    /// Keys of the kinds of column whose key does not change when a key is the value as read, frozen
    /// from the engine as it was, so tables masked before and after still join on such a column.
    /// </summary>
    public static TheoryData<string> UnchangedKeyCases => new()
    {
        "text", "int64", "decimal", "double", "float", "boolean"
    };

    private static (IArrowArray Column, ulong[] Keys) UnchangedKeyCase(string name)
    {
        switch (name)
        {
            case "text":
                return (Text("alice", "", "é€"), new[] { 13092267151035995877UL, 9886184608339236366UL, 2535225055826895363UL });
            case "int64":
                return (Integers(Int64Type.Default, new int?[] { 42, 7, 0 }), new[] { 10946711343035318437UL, 13225839796362995591UL, 0UL });
            case "decimal":
                {
                    var decimals = new Decimal128Array.Builder(new Decimal128Type(12, 2));
                    decimals.Append(12.50m).Append(0m).Append(-3.1m);
                    return (decimals.Build(), new[] { 2146094541429152032UL, 4725836489796705975UL, 1872993749876966964UL });
                }
            case "double":
                {
                    var doubles = new DoubleArray.Builder();
                    doubles.Append(1.5).Append(-0.0).Append(3.14);
                    return (doubles.Build(), new[] { 17396341484489785116UL, 0UL, 728351177676754453UL });
                }
            case "float":
                {
                    var floats = new FloatArray.Builder();
                    floats.Append(1.5f).Append(-0.0f).Append(3.14f);
                    return (floats.Build(), new[] { 97362038816800587UL, 0UL, 12021506241369032493UL });
                }
            case "boolean":
                {
                    var booleans = new BooleanArray.Builder();
                    booleans.Append(true).Append(false).AppendNull();
                    return (booleans.Build(), new[] { 8841707400507832957UL, 0UL, 13067915187859978867UL });
                }
            default:
                throw new ArgumentOutOfRangeException(nameof(name), name, null);
        }
    }

    [Theory]
    [MemberData(nameof(UnchangedKeyCases))]
    public void KeysOfTextIntegersDecimalsDoublesAndBooleans_AreUnchanged(string kind)
    {
        var (column, expected) = UnchangedKeyCase(kind);
        KeysOf(column).Keys.Should().Equal(expected, kind);
    }

    [Theory]
    [InlineData("tr-TR")]
    [InlineData("fr-FR")]
    [InlineData("de-DE")]
    public void Key_DoesNotDependOnTheHostCulture(string culture)
    {
        IArrowArray[] columns = BuildColumnsOfEveryKeyedType();
        var reference = WithCulture("en-US", () => columns.Select(c => KeysOf(c).Keys).ToArray());
        var underTest = WithCulture(culture, () => columns.Select(c => KeysOf(c).Keys).ToArray());

        for (var c = 0; c < columns.Length; c++)
            underTest[c].Should().Equal(reference[c], $"{columns[c].Data.DataType} under {culture}");
    }

    private static IArrowArray[] BuildColumnsOfEveryKeyedType()
    {
        var decimals = new Decimal128Array.Builder(new Decimal128Type(12, 4));
        decimals.Append(1234.5m).Append(-0.0001m).AppendNull();
        var doubles = new DoubleArray.Builder();
        doubles.Append(1234.5).Append(-0.0001).AppendNull();
        var floats = new FloatArray.Builder();
        floats.Append(1234.5f).Append(-0.0001f).AppendNull();
        var booleans = new BooleanArray.Builder();
        booleans.Append(true).Append(false).AppendNull();
        return new IArrowArray[]
        {
            decimals.Build(), doubles.Build(), floats.Build(), booleans.Build(),
            Text("alice", null, "é€"),
            Integers(Int32Type.Default, new int?[] { 1234, null, -5 }),
            Timestamps(TimeUnit.Nanosecond, "Europe/Paris", InstantSeconds * 1_000_000_000 + 123_456_700, null, 0),
            Timestamps(TimeUnit.Millisecond, null, InstantSeconds * 1000 + 5, null, 0),
            Dates32(19_000, null, 0),
            Dates64(19_000L * 86_400_000L, null, 0),
            DictionaryOfText(Int16Type.Default, new string?[] { "alice", "é€" }, new int?[] { 1, null, 0 })
        };
    }

    private static T WithCulture<T>(string name, Func<T> action)
    {
        var previous = CultureInfo.CurrentCulture;
        var previousUi = CultureInfo.CurrentUICulture;
        try
        {
            CultureInfo.CurrentCulture = CultureInfo.GetCultureInfo(name);
            CultureInfo.CurrentUICulture = CultureInfo.GetCultureInfo(name);
            return action();
        }
        finally
        {
            CultureInfo.CurrentCulture = previous;
            CultureInfo.CurrentUICulture = previousUi;
        }
    }
}
