using Apache.Arrow;
using Apache.Arrow.Ipc;
using Apache.Arrow.Scalars;
using Apache.Arrow.Serialization.Mapping;
using Apache.Arrow.Types;
using TimeUnit = Apache.Arrow.Types.TimeUnit;

namespace DtPipe.Tests.Unit.Conformance;

/// <summary>
/// One column of one Arrow type, as the write face feeds it to a writer: a value, then a null where
/// the type has one. Reached through an IPC file, so a case holds any type a reader could hand over.
/// </summary>
internal sealed record ArrowTypeCase(string Name, Field Field, Func<IArrowArray> Build)
{
    public ArrowTypeId TypeId => Field.DataType.TypeId;

    public void WriteIpc(string path)
    {
        var column = Build();
        using var batch = new RecordBatch(new Schema([Field], null), [column], column.Length);
        using var stream = File.Create(path);
        using var writer = new ArrowFileWriter(stream, batch.Schema);
        writer.WriteRecordBatch(batch);
        writer.WriteEnd();
    }
}

internal static class ArrowTypeCases
{
    private const string Column = "v";
    private static readonly Guid Uuid = Guid.Parse("6f9619ff-8b86-d011-b42d-00c04fc964ff");
    private static readonly DateTimeOffset Instant = new(2024, 1, 2, 3, 4, 5, 123, 456, TimeSpan.Zero);
    private static readonly DateOnly Date = new(2024, 1, 2);

    /// <summary>More digits than a double keeps, and more decimals than DuckDB's default DECIMAL.</summary>
    private const decimal WideDecimal = 12345678901234567.891m;

    private static readonly TimeOnly Time = new(10, 11, 12, 123, 456);
    private static readonly StructType StructOfInt = new([new Field("item", new StructType([new Field("a", Int32Type.Default, true)]), true)]);
    private static readonly StructType StructType = new([new Field("a", Int32Type.Default, true), new Field("b", StringType.Default, true)]);
    private static readonly MapType MapOfString = new(StringType.Default, Int32Type.Default, nullable: true);

    /// <summary>
    /// Identifiers with no case, and why. Anything else without one fails the write face, so a type a
    /// new Apache.Arrow adds is answered for rather than met at run time.
    /// </summary>
    public static readonly IReadOnlyDictionary<ArrowTypeId, string> NoCase = new Dictionary<ArrowTypeId, string>
    {
        [ArrowTypeId.RecordBatch] = "the identifier of a batch, never of a column",
        [ArrowTypeId.Extension] = "Apache.Arrow reads a registered extension type by its storage: the cases of a field's metadata stand for it",
    };

    private static Field F(IArrowType type, params (string Key, string Value)[] metadata)
        => new(Column, type, true, metadata.Length == 0 ? null : metadata.Select(m => KeyValuePair.Create(m.Key, m.Value)));

    private static Field Logical(ArrowTypeResult logical) => ArrowTypeMap.GetField(Column, logical);

    public static readonly ArrowTypeCase[] All =
    [
        new("null", F(NullType.Default), () => new NullArray(2)),
        new("bool", F(BooleanType.Default), () => new BooleanArray.Builder().Append(true).AppendNull().Build()),
        new("bool8", F(Int8Type.Default, ("ARROW:extension:name", "arrow.bool8")), () => new Int8Array.Builder().Append(1).AppendNull().Build()),
        new("int8", F(Int8Type.Default), () => new Int8Array.Builder().Append(-5).AppendNull().Build()),
        new("int16", F(Int16Type.Default), () => new Int16Array.Builder().Append(-300).AppendNull().Build()),
        new("int32", F(Int32Type.Default), () => new Int32Array.Builder().Append(-70000).AppendNull().Build()),
        new("int64", F(Int64Type.Default), () => new Int64Array.Builder().Append(-5000000000).AppendNull().Build()),
        new("uint8", F(UInt8Type.Default), () => new UInt8Array.Builder().Append(200).AppendNull().Build()),
        new("uint16", F(UInt16Type.Default), () => new UInt16Array.Builder().Append(60000).AppendNull().Build()),
        new("uint32", F(UInt32Type.Default), () => new UInt32Array.Builder().Append(4000000000).AppendNull().Build()),
        new("uint64", F(UInt64Type.Default), () => new UInt64Array.Builder().Append(18000000000000000000).AppendNull().Build()),
        new("int128", Logical(ArrowTypeMap.GetLogicalType(typeof(Int128))), () => Fixed(16, ArrowOpaqueCodec.Encode(Int128.MaxValue))),
        new("int128-min", Logical(ArrowTypeMap.GetLogicalType(typeof(Int128))), () => Fixed(16, ArrowOpaqueCodec.Encode(Int128.MinValue))),
        new("uint128", Logical(ArrowTypeMap.GetLogicalType(typeof(UInt128))), () => Fixed(16, ArrowOpaqueCodec.Encode(UInt128.MaxValue))),
        new("halffloat", F(HalfFloatType.Default), () => new HalfFloatArray.Builder().Append((Half)1.5).AppendNull().Build()),
        new("float", F(FloatType.Default), () => new FloatArray.Builder().Append(1.5f).AppendNull().Build()),
        new("double", F(DoubleType.Default), () => new DoubleArray.Builder().Append(1.5).AppendNull().Build()),
        new("double-nan", F(DoubleType.Default), () => new DoubleArray.Builder().Append(double.NaN).AppendNull().Build()),
        new("double-inf", F(DoubleType.Default), () => new DoubleArray.Builder().Append(double.PositiveInfinity).AppendNull().Build()),
        new("double-shortest", F(DoubleType.Default), () => new DoubleArray.Builder().Append(0.1).AppendNull().Build()),
        new("float-shortest", F(FloatType.Default), () => new FloatArray.Builder().Append(0.1f).AppendNull().Build()),
        new("decimal32", F(new Decimal32Type(9, 2)), () => new Decimal32Array.Builder(new Decimal32Type(9, 2)).Append(4.29m).AppendNull().Build()),
        new("decimal64", F(new Decimal64Type(18, 2)), () => new Decimal64Array.Builder(new Decimal64Type(18, 2)).Append(4.29m).AppendNull().Build()),
        new("decimal128(10,2)", F(new Decimal128Type(10, 2)), () => new Decimal128Array.Builder(new Decimal128Type(10, 2)).Append(4.29m).AppendNull().Build()),
        new("decimal128(38,18)", F(new Decimal128Type(38, 18)), () => new Decimal128Array.Builder(new Decimal128Type(38, 18)).Append(4.29m).AppendNull().Build()),
        new("decimal-undeclared", Logical(ArrowTypeMap.DefaultDecimal), () => new Decimal128Array.Builder(ArrowTypeMap.DefaultDecimalType).Append(4.29m).AppendNull().Build()),
        new("decimal128-wide", F(new Decimal128Type(38, 18)), () => new Decimal128Array.Builder(new Decimal128Type(38, 18)).Append(WideDecimal).AppendNull().Build()),
        new("decimal-undeclared-wide", Logical(ArrowTypeMap.DefaultDecimal), () => new Decimal128Array.Builder(ArrowTypeMap.DefaultDecimalType).Append(WideDecimal).AppendNull().Build()),
        new("decimal-undeclared-fraction", Logical(ArrowTypeMap.DefaultDecimal), () => new Decimal128Array.Builder(ArrowTypeMap.DefaultDecimalType).Append(1.23456m).AppendNull().Build()),
        new("decimal256", F(new Decimal256Type(50, 10)), () => new Decimal256Array.Builder(new Decimal256Type(50, 10)).Append(4.29m).AppendNull().Build()),
        new("string", F(StringType.Default), () => new StringArray.Builder().Append("héllo").AppendNull().Build()),
        new("largestring", F(LargeStringType.Default), () => new LargeStringArray.Builder().Append("héllo").AppendNull().Build()),
        new("stringview", F(StringViewType.Default), () => new StringViewArray.Builder().Append("héllo").AppendNull().Build()),
        new("binary", F(BinaryType.Default), () => new BinaryArray.Builder().Append("abc"u8).AppendNull().Build()),
        new("largebinary", F(LargeBinaryType.Default), () => new LargeBinaryArray.Builder().Append("abc"u8).AppendNull().Build()),
        new("binaryview", F(BinaryViewType.Default), () => new BinaryViewArray.Builder().Append("abc"u8).AppendNull().Build()),
        new("fixedbinary(8)", F(new FixedSizeBinaryType(8)), () => Fixed(8, "abcdefgh"u8.ToArray())),
        new("uuid", Logical(ArrowTypeMap.GetLogicalType(typeof(Guid))), () => Fixed(16, ArrowTypeMap.ToArrowUuidBytes(Uuid))),
        new("date32", F(Date32Type.Default), () => new Date32Array.Builder().Append(Date).AppendNull().Build()),
        new("date64", F(Date64Type.Default), () => new Date64Array.Builder().Append(Date).AppendNull().Build()),
        new("time32(ms)", F(new Time32Type(TimeUnit.Millisecond)), () => new Time32Array.Builder(new Time32Type(TimeUnit.Millisecond)).Append(new TimeOnly(10, 11, 12, 123)).AppendNull().Build()),
        new("time64(us)", F(new Time64Type(TimeUnit.Microsecond)), () => new Time64Array.Builder(new Time64Type(TimeUnit.Microsecond)).Append(Time).AppendNull().Build()),
        new("timestamp(s)", F(new TimestampType(TimeUnit.Second, (string?)null)), () => Timestamp(new TimestampType(TimeUnit.Second, (string?)null))),
        new("timestamp(us)", F(new TimestampType(TimeUnit.Microsecond, (string?)null)), () => Timestamp(new TimestampType(TimeUnit.Microsecond, (string?)null))),
        new("timestamp(ns)", F(new TimestampType(TimeUnit.Nanosecond, (string?)null)), () => Timestamp(new TimestampType(TimeUnit.Nanosecond, (string?)null))),
        new("timestamp(us,UTC)", F(new TimestampType(TimeUnit.Microsecond, "UTC")), () => Timestamp(new TimestampType(TimeUnit.Microsecond, "UTC"))),
        new("timestamp(us,Asia/Tokyo)", F(new TimestampType(TimeUnit.Microsecond, "Asia/Tokyo")), () => Timestamp(new TimestampType(TimeUnit.Microsecond, "Asia/Tokyo"))),
        new("duration(us)", F(DurationType.Microsecond), () => new DurationArray.Builder(DurationType.Microsecond).Append(TimeSpan.FromSeconds(90)).AppendNull().Build()),
        new("interval(ym)", F(IntervalType.YearMonth), () => new YearMonthIntervalArray.Builder().Append(new YearMonthInterval(14)).AppendNull().Build()),
        new("interval(dt)", F(IntervalType.DayTime), () => new DayTimeIntervalArray.Builder().Append(new DayTimeInterval(1, 500)).AppendNull().Build()),
        new("interval(mdn)", F(IntervalType.MonthDayNanosecond), () => new MonthDayNanosecondIntervalArray.Builder().Append(new MonthDayNanosecondInterval(1, 2, 3)).AppendNull().Build()),
        new("list<int32>", F(new ListType(Int32Type.Default)), Int32List),
        new("list<decimal(10,2)>", F(new ListType(new Decimal128Type(10, 2))), DecimalList),
        new("list<timestamp>", F(new ListType(new TimestampType(TimeUnit.Microsecond, (string?)null))), TimestampList),
        new("list<uuid>", F(new ListType(Logical(ArrowTypeMap.GetLogicalType(typeof(Guid))))), UuidList),
        new("list<struct>", F(new ListType(StructOfInt.Fields[0])), StructList),
        new("largelist", F(new LargeListType(Int32Type.Default)), () => BuildLargeList()),
        new("fixedlist", F(new FixedSizeListType(Int32Type.Default, 2)), () => BuildFixedList()),
        new("listview", F(new ListViewType(Int32Type.Default)), () => BuildListView()),
        new("largelistview", F(new LargeListViewType(Int32Type.Default)), () => BuildLargeListView()),
        new("struct", F(StructType), BuildStruct),
        new("map", F(MapOfString), BuildMap),
        new("union(sparse)", F(Union(UnionMode.Sparse)), BuildSparseUnion),
        new("union(dense)", F(Union(UnionMode.Dense)), BuildDenseUnion),
        new("dictionary", F(new DictionaryType(Int32Type.Default, StringType.Default, false)), BuildDictionary),
        new("runend", F(new RunEndEncodedType(Int32Type.Default, Int32Type.Default)), BuildRunEnd),
    ];

    public static ArrowTypeCase Named(string name) => All.Single(c => c.Name == name);

    private static IArrowArray Fixed(int width, byte[] value)
    {
        var builder = new Apache.Arrow.Serialization.Reflection.FixedSizeBinaryArrayBuilder(width);
        builder.Append(value);
        builder.AppendNull();
        return builder.Build();
    }

    private static IArrowArray Timestamp(TimestampType type)
    {
        var whole = type.Unit == TimeUnit.Second ? new DateTimeOffset(2024, 1, 2, 3, 4, 5, TimeSpan.Zero) : Instant;
        return new TimestampArray.Builder(type).Append(whole).AppendNull().Build();
    }

    private static IArrowArray Int32List()
    {
        var builder = new ListArray.Builder(Int32Type.Default);
        var values = (Int32Array.Builder)builder.ValueBuilder;
        builder.Append();
        values.Append(1);
        values.Append(2);
        builder.AppendNull();
        return builder.Build();
    }

    private static IArrowArray DecimalList()
    {
        var builder = new ListArray.Builder(new Decimal128Type(10, 2));
        var values = (Decimal128Array.Builder)builder.ValueBuilder;
        builder.Append();
        values.Append(4.29m);
        builder.AppendNull();
        return builder.Build();
    }

    private static IArrowArray TimestampList()
    {
        var type = new TimestampType(TimeUnit.Microsecond, (string?)null);
        var offsets = new ArrowBuffer.Builder<int>().Append(0).Append(1).Append(1).Build();
        var valid = new ArrowBuffer.BitmapBuilder().Append(true).Append(false).Build();
        return new ListArray(new ListType(type), 2, offsets, new TimestampArray.Builder(type).Append(Instant).Build(), valid, 1, 0);
    }

    private static IArrowArray UuidList()
    {
        var elements = new Apache.Arrow.Serialization.Reflection.FixedSizeBinaryArrayBuilder(16);
        elements.Append(ArrowTypeMap.ToArrowUuidBytes(Uuid));
        var offsets = new ArrowBuffer.Builder<int>().Append(0).Append(1).Append(1).Build();
        var valid = new ArrowBuffer.BitmapBuilder().Append(true).Append(false).Build();
        return new ListArray(new ListType(Logical(ArrowTypeMap.GetLogicalType(typeof(Guid)))), 2, offsets, elements.Build(), valid, 1, 0);
    }

    private static IArrowArray StructList()
    {
        var element = new StructType([new Field("a", Int32Type.Default, true)]);
        var children = new StructArray(element, 1, [new Int32Array.Builder().Append(1).Build()], ArrowBuffer.Empty);
        var offsets = new ArrowBuffer.Builder<int>().Append(0).Append(1).Append(1).Build();
        var valid = new ArrowBuffer.BitmapBuilder().Append(true).Append(false).Build();
        return new ListArray(new ListType(new Field("item", element, true)), 2, offsets, children, valid, 1, 0);
    }

    private static IArrowArray BuildLargeList()
    {
        var offsets = new ArrowBuffer.Builder<long>().Append(0).Append(2).Append(2).Build();
        var valid = new ArrowBuffer.BitmapBuilder().Append(true).Append(false).Build();
        return new LargeListArray(new LargeListType(Int32Type.Default), 2, offsets, new Int32Array.Builder().Append(1).Append(2).Build(), valid, 1, 0);
    }

    private static IArrowArray BuildFixedList()
    {
        var valid = new ArrowBuffer.BitmapBuilder().Append(true).Append(false).Build();
        return new FixedSizeListArray(new FixedSizeListType(Int32Type.Default, 2), 2,
            new Int32Array.Builder().Append(1).Append(2).Append(0).Append(0).Build(), valid, 1, 0);
    }

    private static IArrowArray BuildListView()
    {
        var offsets = new ArrowBuffer.Builder<int>().Append(0).Append(2).Build();
        var sizes = new ArrowBuffer.Builder<int>().Append(2).Append(0).Build();
        var valid = new ArrowBuffer.BitmapBuilder().Append(true).Append(false).Build();
        return new ListViewArray(new ListViewType(Int32Type.Default), 2, offsets, sizes, new Int32Array.Builder().Append(1).Append(2).Build(), valid, 1, 0);
    }

    private static IArrowArray BuildLargeListView()
    {
        var offsets = new ArrowBuffer.Builder<long>().Append(0).Append(2).Build();
        var sizes = new ArrowBuffer.Builder<long>().Append(2).Append(0).Build();
        var valid = new ArrowBuffer.BitmapBuilder().Append(true).Append(false).Build();
        return new LargeListViewArray(new LargeListViewType(Int32Type.Default), 2, offsets, sizes, new Int32Array.Builder().Append(1).Append(2).Build(), valid, 1, 0);
    }

    private static IArrowArray BuildStruct()
    {
        var valid = new ArrowBuffer.BitmapBuilder().Append(true).Append(false).Build();
        return new StructArray(StructType, 2,
            [new Int32Array.Builder().Append(1).AppendNull().Build(), new StringArray.Builder().Append("x").AppendNull().Build()],
            valid, 1);
    }

    private static IArrowArray BuildMap()
    {
        var entries = new StructArray(
            new StructType([MapOfString.KeyField, MapOfString.ValueField]), 1,
            [new StringArray.Builder().Append("a").Build(), new Int32Array.Builder().Append(1).Build()],
            ArrowBuffer.Empty);
        var offsets = new ArrowBuffer.Builder<int>().Append(0).Append(1).Append(1).Build();
        var valid = new ArrowBuffer.BitmapBuilder().Append(true).Append(false).Build();
        return new MapArray(MapOfString, 2, offsets, entries, valid, 1, 0);
    }

    private static UnionType Union(UnionMode mode)
        => new([new Field("a", Int32Type.Default, true), new Field("b", StringType.Default, true)], [0, 1], mode);

    private static IArrowArray BuildSparseUnion()
    {
        var ids = new ArrowBuffer.Builder<byte>().Append(0).Append(1).Build();
        return new SparseUnionArray(Union(UnionMode.Sparse), 2,
            [new Int32Array.Builder().Append(1).AppendNull().Build(), new StringArray.Builder().AppendNull().Append("x").Build()],
            ids, 0, 0);
    }

    private static IArrowArray BuildDenseUnion()
    {
        var ids = new ArrowBuffer.Builder<byte>().Append(0).Append(1).Build();
        var offsets = new ArrowBuffer.Builder<int>().Append(0).Append(0).Build();
        return new DenseUnionArray(Union(UnionMode.Dense), 2,
            [new Int32Array.Builder().Append(1).Build(), new StringArray.Builder().Append("x").Build()],
            ids, offsets, 0, 0);
    }

    private static IArrowArray BuildDictionary()
        => new DictionaryArray(new DictionaryType(Int32Type.Default, StringType.Default, false),
            new Int32Array.Builder().Append(0).AppendNull().Build(),
            new StringArray.Builder().Append("a").Build());

    private static IArrowArray BuildRunEnd()
        => new RunEndEncodedArray(new Int32Array.Builder().Append(1).Append(2).Build(), new Int32Array.Builder().Append(7).AppendNull().Build());
}
