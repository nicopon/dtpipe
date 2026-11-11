using System;
using System.Collections.Generic;
using Apache.Arrow.Types;
using Apache.Arrow;
using Apache.Arrow.Arrays;
using System.Globalization;
using System.Linq;

namespace Apache.Arrow.Serialization.Mapping;

public readonly struct ArrowTypeResult
{
    public IArrowType ArrowType { get; }
    public IReadOnlyDictionary<string, string>? Metadata { get; }

    public ArrowTypeResult(IArrowType arrowType, IReadOnlyDictionary<string, string>? metadata = null)
    {
        ArrowType = arrowType;
        Metadata = metadata;
    }
}

/// <summary>
/// Centralized mapper for CLR to Arrow types and vice-versa.
/// Provides unified logic for schema generation and UUID handling.
/// </summary>
public static class ArrowTypeMap
{
    // ── UUID byte-order helpers ──────────────────────────────────────────────

    /// <summary>
    /// Converts a .NET Guid to the RFC 4122 big-endian bytes of canonical Arrow UUID storage.
    /// Arrow-facing spelling of <see cref="Rfc4122Guid.ToBigEndianBytes"/>.
    /// </summary>
    public static byte[] ToArrowUuidBytes(Guid guid) => Rfc4122Guid.ToBigEndianBytes(guid);

    /// <summary>
    /// Converts RFC 4122 big-endian UUID bytes (from an Arrow binary column) back to a .NET Guid.
    /// Arrow-facing spelling of <see cref="Rfc4122Guid.FromBigEndianBytes"/>.
    /// </summary>
    public static Guid FromArrowUuidBytes(ReadOnlySpan<byte> b) => Rfc4122Guid.FromBigEndianBytes(b);

    // ── Type mappings ────────────────────────────────────────────────────────

    /// <summary>
    /// Returns the logical Arrow Type and any requisite metadata mapping for a primitive/scalar CLR type.
    /// Does not handle complex nested types (Lists, Maps, Structs) directly.
    /// </summary>
    public static ArrowTypeResult GetLogicalType(Type type)
    {
        if (type == typeof(string)) return new ArrowTypeResult(StringType.Default);
        if (type == typeof(int) || type == typeof(int?)) return new ArrowTypeResult(Int32Type.Default);
        if (type == typeof(long) || type == typeof(long?)) return new ArrowTypeResult(Int64Type.Default);
        if (type == typeof(double) || type == typeof(double?)) return new ArrowTypeResult(DoubleType.Default);
        if (type == typeof(float) || type == typeof(float?)) return new ArrowTypeResult(FloatType.Default);
        if (type == typeof(bool) || type == typeof(bool?)) return new ArrowTypeResult(BooleanType.Default);
        if (type == typeof(byte) || type == typeof(byte?)) return new ArrowTypeResult(UInt8Type.Default);
        if (type == typeof(sbyte) || type == typeof(sbyte?)) return new ArrowTypeResult(Int8Type.Default);
        if (type == typeof(short) || type == typeof(short?)) return new ArrowTypeResult(Int16Type.Default);
        if (type == typeof(ushort) || type == typeof(ushort?)) return new ArrowTypeResult(UInt16Type.Default);
        if (type == typeof(uint) || type == typeof(uint?)) return new ArrowTypeResult(UInt32Type.Default);
        if (type == typeof(ulong) || type == typeof(ulong?)) return new ArrowTypeResult(UInt64Type.Default);
        if (type == typeof(decimal) || type == typeof(decimal?)) return DefaultDecimal;
        // DateTime → Timestamp(null tz) — round-trips correctly via GetClrType(Timestamp(null)) → DateTime
        if (type == typeof(DateTime) || type == typeof(DateTime?)) return new ArrowTypeResult(new TimestampType(TimeUnit.Microsecond, (string?)null));
        // DateTimeOffset → Timestamp with UTC timezone — round-trips correctly via GetClrType(Timestamp) → DateTimeOffset
        if (type == typeof(DateTimeOffset) || type == typeof(DateTimeOffset?)) return new ArrowTypeResult(new TimestampType(TimeUnit.Microsecond, "UTC"));
        if (type == typeof(TimeSpan) || type == typeof(TimeSpan?)) return new ArrowTypeResult(DurationType.Microsecond);
        if (type == typeof(DateOnly) || type == typeof(DateOnly?)) return new ArrowTypeResult(Date32Type.Default);
#if NET6_0_OR_GREATER
        if (type == typeof(TimeOnly) || type == typeof(TimeOnly?)) return new ArrowTypeResult(new Time64Type(TimeUnit.Microsecond));
#endif
        if (type == typeof(Guid) || type == typeof(Guid?)) return new ArrowTypeResult(new FixedSizeBinaryType(16), new Dictionary<string, string> { { "ARROW:extension:name", "arrow.uuid" } });
        // Arrow has no 128-bit integer; DuckDB's opaque extension is the form a 128-bit column
        // travels in, and the one DuckDB reads back as HUGEINT.
        if (type == typeof(Int128) || type == typeof(Int128?)) return Opaque128(ArrowOpaqueCodec.HugeIntMetadata);
        if (type == typeof(UInt128) || type == typeof(UInt128?)) return Opaque128(ArrowOpaqueCodec.UHugeIntMetadata);
        if (type == typeof(byte[])) return new ArrowTypeResult(BinaryType.Default);
        if (type == typeof(object)) return new ArrowTypeResult(StringType.Default); // Placeholder for dynamic/complex types

        // Enums mapping to Int32
        if (type.IsEnum) return new ArrowTypeResult(Int32Type.Default);
        var underlyingType = Nullable.GetUnderlyingType(type);
        if (underlyingType?.IsEnum == true) return new ArrowTypeResult(Int32Type.Default);

        throw new NotSupportedException($"Type {type.FullName} is not a valid scalar type supported by ArrowTypeMap.");
    }

    private static ArrowTypeResult Opaque128(string metadata) => new(new FixedSizeBinaryType(16), new Dictionary<string, string>
    {
        { ArrowExtensionInfo.NameKey, "arrow.opaque" },
        { ArrowExtensionInfo.MetadataKey, metadata }
    });

    /// <summary>
    /// Attempts to get the logical Arrow type for a CLR type without throwing.
    /// Returns false for complex, unsupported, or unknown types — use <see cref="GetLogicalType"/> variants in
    /// <see cref="Apache.Arrow.Serialization.Reflection.ArrowReflectionEngine"/> for those.
    /// </summary>
    public static bool TryGetLogicalType(Type type, out ArrowTypeResult result)
    {
        try
        {
            result = GetLogicalType(type);
            return true;
        }
        catch (NotSupportedException)
        {
            result = default;
            return false;
        }
    }

    /// <summary>
    /// Widest Decimal128 a .NET <see cref="decimal"/> fits into, used for a column whose source
    /// declares no precision or scale.
    /// </summary>
    public static Decimal128Type DefaultDecimalType { get; } = new(38, 18);

    /// <summary>
    /// Field metadata key marking a decimal whose width is <see cref="DefaultDecimalType"/> because
    /// the source declared none, not because it declared (38,18). An Arrow decimal always carries a
    /// precision and scale, so without the mark the two are indistinguishable downstream.
    /// </summary>
    public const string DecimalWidthKey = "Apache.Arrow.Serialization:decimal-width";

    /// <summary>The value <see cref="DecimalWidthKey"/> takes on a decimal that declared no width.</summary>
    public const string UndeclaredDecimalWidth = "undeclared";

    /// <summary>
    /// <see cref="DefaultDecimalType"/>, marked as undeclared. The one place the default is produced
    /// as a field type, so a reader of the field can tell it from a declared (38,18).
    /// </summary>
    public static ArrowTypeResult DefaultDecimal { get; } = new(
        DefaultDecimalType,
        new Dictionary<string, string> { [DecimalWidthKey] = UndeclaredDecimalWidth });

    /// <summary>True when the field is a decimal that declared no width (see <see cref="DecimalWidthKey"/>).</summary>
    public static bool IsUndeclaredDecimal(Field field)
        => field.DataType is Decimal128Type or Decimal256Type
           && field.Metadata is not null
           && field.Metadata.TryGetValue(DecimalWidthKey, out var mark)
           && mark == UndeclaredDecimalWidth;

    /// <summary>
    /// Same mapping, given what the source declared about a numeric column. Decimal is the one
    /// scalar whose Arrow form a CLR type does not fix on its own: the storage is Decimal128
    /// either way, but precision and scale belong to the column, not to <c>System.Decimal</c>.
    /// Anything Arrow cannot express — no precision, one past 38, a scale outside it — falls back
    /// to <see cref="DefaultDecimalType"/>, which holds any value the narrower form could.
    /// </summary>
    public static ArrowTypeResult GetLogicalType(Type type, int? precision, int? scale)
    {
        var logical = GetLogicalType(type);

        if (logical.ArrowType is not Decimal128Type) return logical;

        // A declared width carries no undeclared mark.
        var width = DecimalWidth.Of(precision, scale);
        return width.Declared ? new ArrowTypeResult(new Decimal128Type(width.Precision, width.Scale)) : logical;
    }

    /// <summary>
    /// Constructs a field directly from a logical type mapping result, injecting appropriate metadata.
    /// </summary>
    /// <summary>A type as a person reads it: a decimal with its width, any other by its Arrow name.</summary>
    public static string Spell(IArrowType type) => type switch
    {
        Decimal128Type d => $"decimal({d.Precision}, {d.Scale})",
        Decimal256Type d => $"decimal({d.Precision}, {d.Scale})",
        _ => type.Name
    };

    public static Field GetField(string name, ArrowTypeResult logicalType, bool isNullable = true)
    {
        return new Field(name, logicalType.ArrowType, isNullable,
            logicalType.Metadata?.Select(kvp => new KeyValuePair<string, string>(kvp.Key, kvp.Value)));
    }

    /// <summary>
    /// Returns the CLR type for a given Arrow type.
    /// Performs only unambiguous, direct mappings — no heuristics.
    /// </summary>
    public static Type GetClrType(IArrowType type)
    {
        // Before FixedSizeBinaryType, which every decimal type derives from.
        if (type is Decimal128Type or Decimal256Type or Decimal64Type or Decimal32Type) return typeof(decimal);
        if (type is FixedSizeBinaryType) return typeof(byte[]);
        // TimestampType: no-timezone → DateTime (local/unspecified); with timezone → DateTimeOffset
        if (type is TimestampType ts)
            return string.IsNullOrEmpty(ts.Timezone) ? typeof(DateTime) : typeof(DateTimeOffset);

        return type.TypeId switch
        {
            ArrowTypeId.Boolean => typeof(bool),
            ArrowTypeId.Int8 => typeof(sbyte),
            ArrowTypeId.UInt8 => typeof(byte),
            ArrowTypeId.Int16 => typeof(short),
            ArrowTypeId.UInt16 => typeof(ushort),
            ArrowTypeId.Int32 => typeof(int),
            ArrowTypeId.UInt32 => typeof(uint),
            ArrowTypeId.Int64 => typeof(long),
            ArrowTypeId.UInt64 => typeof(ulong),
            ArrowTypeId.Float => typeof(float),
            ArrowTypeId.Double => typeof(double),
            ArrowTypeId.String => typeof(string),
            ArrowTypeId.Binary => typeof(byte[]),
            ArrowTypeId.Timestamp => typeof(DateTimeOffset), // fallback, covered by TimestampType check above
            // The inverse of GetLogicalType: a date is a DateOnly and a time of day a TimeOnly, so a
            // writer can tell them from a timestamp and a number.
            ArrowTypeId.Date32 => typeof(DateOnly),
            ArrowTypeId.Time32 or ArrowTypeId.Time64 => typeof(TimeOnly),
            ArrowTypeId.Date64 => typeof(DateTime),
            ArrowTypeId.Decimal128 => typeof(decimal),
            ArrowTypeId.Decimal256 => typeof(decimal),
            ArrowTypeId.Duration => typeof(TimeSpan),
            ArrowTypeId.Struct => typeof(Dictionary<string, object?>),
            // Typed on the element, not List<object?>: a writer builds its column schema from this
            // type, and an untyped list makes it guess — Parquet settles on string and then throws
            // when the first int arrives.
            ArrowTypeId.List or ArrowTypeId.LargeList or ArrowTypeId.FixedSizeList => GetListClrType(type),
            ArrowTypeId.Map => typeof(Dictionary<object, object?>),
            // A dictionary-encoded column is its value type; the encoding is storage, not meaning.
            ArrowTypeId.Dictionary => type is DictionaryType dict ? GetClrType(dict.ValueType) : typeof(string),
            _ => typeof(string)
        };
    }

    /// <summary>
    /// Returns the CLR type for a given Arrow <see cref="Field"/>, checking extension metadata first.
    /// </summary>
    public static Type GetClrTypeFromField(Field field)
    {
        if (ArrowExtensionInfo.TryRead(field, out var ext))
        {
            if (ext.Is("arrow.uuid")) return typeof(Guid);
            if (ext.Is("arrow.bool8")) return typeof(bool);
            if (ext.Is("arrow.opaque") && ArrowOpaqueCodec.DecodedType(ext.VendorName, ext.TypeName) is { } decoded)
                return decoded;
        }
        return GetClrType(field.DataType);
    }

    /// <summary>
    /// Extracts the CLR value from an Arrow array at the specified index, respecting logical type
    /// metadata (e.g. UUID) if a <see cref="Field"/> is provided.
    /// </summary>
    /// <remarks>
    /// <see cref="CompileValueReader"/> reads a whole column the same way: change the two together.
    /// </remarks>
    public static object? GetValue(IArrowArray array, int index, Field? field = null)
    {
        if (array.IsNull(index)) return null;

        // 1. Extension types, which carry a logical meaning their storage type does not.
        // Ordered cheapest-first, and measured: this runs on every cell of every columnar read,
        // where a plain int costs ~10 ns, so a couple of extra type tests is a 10 % regression.
        // A field with no metadata carries no extension, and that test is one field access;
        // the storage shapes come second, and a new extension on another one must widen them or
        // it will never be reached.
        if (field is { HasMetadata: true } &&
            array is FixedSizeBinaryArray or BinaryArray or Int8Array &&
            ArrowExtensionInfo.TryRead(field, out var ext))
        {
            if (ext.Is("arrow.uuid") && array is FixedSizeBinaryArray uuidArray)
                return FromArrowUuidBytes(uuidArray.GetBytes(index));

            // A canonical bool8 is an int8 whose non-zero values mean true.
            if (ext.Is("arrow.bool8") && array is Int8Array boolArray)
                return boolArray.GetValue(index) != 0;

            if (ext.Is("arrow.opaque"))
            {
                var payload = array switch
                {
                    FixedSizeBinaryArray a => a.GetBytes(index),
                    BinaryArray a => a.GetBytes(index),
                    _ => ReadOnlySpan<byte>.Empty
                };
                var decoded = ArrowOpaqueCodec.TryDecode(ext.VendorName, ext.TypeName, payload);
                if (decoded != null) return decoded;
            }
        }

        // 2. Standard Types
        object? val = array switch
        {
            BooleanArray a => (object?)a.GetValue(index),
            Int8Array a => a.GetValue(index),
            Int16Array a => a.GetValue(index),
            Int32Array a => a.GetValue(index),
            Int64Array a => a.GetValue(index),
            UInt8Array a => a.GetValue(index),
            UInt16Array a => a.GetValue(index),
            UInt32Array a => a.GetValue(index),
            UInt64Array a => a.GetValue(index),
            FloatArray a => a.GetValue(index),
            DoubleArray a => a.GetValue(index),
            StringArray a => a.GetString(index),
            BinaryArray a => a.GetBytes(index).ToArray(),
            Decimal128Array a => ReadDecimal(a.GetValue(index), field),
            Decimal256Array a => ReadDecimal(a.GetValue(index), field),
            FixedSizeBinaryArray a => GetFixedWidthValue(a, index),
            Date32Array a => a.GetDateOnly(index),
            Date64Array a => a.GetDateTime(index),
            TimestampArray a => a.GetTimestamp(index),
            DurationArray a => a.GetTimeSpan(index),
            Time32Array a => a.GetTime(index),
            Time64Array a => a.GetTime(index),
            StructArray a => GetStructValue(a, index),
            // Before ListArray: MapArray derives from it, so the general case would win and a map
            // would come back as a list of {key,value} structs, contradicting the Dictionary that
            // GetClrType declares for it.
            MapArray a => GetMapValue(a, index),
            ListArray a => GetListValue(a, index),
            FixedSizeListArray a => GetFixedSizeListValue(a, index),
            DictionaryArray a => GetDictionaryValue(a, index, field),
            _ => throw new NotSupportedException(field is null
                ? $"Unsupported Arrow array type for value extraction: {array.GetType().Name}"
                : $"Column '{field.Name}' is {field.DataType.Name}, an Arrow type with no reading here: cast it to one that has, at the source or in a SQL step.")
        };

        // 3. Post-processing Coercion
        // A timezone-less Timestamp column holds a wall clock, so it yields a zone-less DateTime.
        // TemporalNormalization holds this conversion next to its inverse; change them together.
        if (field != null && val is DateTimeOffset dto && field.DataType is TimestampType ts && string.IsNullOrEmpty(ts.Timezone))
        {
            return TemporalNormalization.ToWallClock(dto);
        }

        return val;
    }

    /// <summary>
    /// Whether <see cref="GetValue"/> reads a value of this type, its nested types included. A writer
    /// that reads values answers from this list, so a type it does not name is refused before a target
    /// exists rather than failing at its first value. Change it with that switch.
    /// </summary>
    public static bool HasValueReading(IArrowType type) => type switch
    {
        StructType s => s.Fields.All(f => HasValueReading(f.DataType)),
        MapType m => HasValueReading(m.KeyField.DataType) && HasValueReading(m.ValueField.DataType),
        ListType l => HasValueReading(l.ValueDataType),
        FixedSizeListType l => HasValueReading(l.ValueDataType),
        DictionaryType d => HasValueReading(d.ValueType),
        _ => type.TypeId is ArrowTypeId.Null or ArrowTypeId.Boolean
            or ArrowTypeId.Int8 or ArrowTypeId.Int16 or ArrowTypeId.Int32 or ArrowTypeId.Int64
            or ArrowTypeId.UInt8 or ArrowTypeId.UInt16 or ArrowTypeId.UInt32 or ArrowTypeId.UInt64
            or ArrowTypeId.Float or ArrowTypeId.Double
            or ArrowTypeId.String or ArrowTypeId.Binary or ArrowTypeId.FixedSizedBinary
            or ArrowTypeId.Decimal32 or ArrowTypeId.Decimal64 or ArrowTypeId.Decimal128 or ArrowTypeId.Decimal256
            or ArrowTypeId.Date32 or ArrowTypeId.Date64 or ArrowTypeId.Time32 or ArrowTypeId.Time64
            or ArrowTypeId.Timestamp or ArrowTypeId.Duration
    };

    /// <summary>
    /// <see cref="GetValue"/> for one column, resolved once. The reader returns what
    /// <c>GetValue(array, i, field)</c> returns for every <c>i</c>: null for a null cell, the same value
    /// otherwise, and the same exception where it throws. A caller that walks a column compiles it once
    /// and pays no type test, extension lookup or field read per cell.
    /// </summary>
    /// <remarks>
    /// Its switch is <see cref="GetValue"/>'s, read once instead of per cell: change the two together.
    /// A nested, dictionary-encoded or unread type reads through <see cref="GetValue"/> itself.
    /// </remarks>
    public static Func<int, object?> CompileValueReader(IArrowArray array, Field field)
    {
        ArgumentNullException.ThrowIfNull(array);
        ArgumentNullException.ThrowIfNull(field);

        if (field.HasMetadata &&
            array is FixedSizeBinaryArray or BinaryArray or Int8Array &&
            ArrowExtensionInfo.TryRead(field, out var ext))
        {
            if (ext.Is("arrow.uuid") && array is FixedSizeBinaryArray uuidArray)
                return i => uuidArray.IsNull(i) ? null : FromArrowUuidBytes(uuidArray.GetBytes(i));

            if (ext.Is("arrow.bool8") && array is Int8Array boolArray)
                return i => boolArray.IsNull(i) ? null : boolArray.GetValue(i) != 0;

            if (ext.Is("arrow.opaque"))
                return CompileOpaqueReader(array, ext.VendorName, ext.TypeName, CompileStorageReader(array, field));
        }

        return CompileStorageReader(array, field);
    }

    /// <summary>A payload the codec does not decode reads as its storage, as it does in <see cref="GetValue"/>.</summary>
    private static Func<int, object?> CompileOpaqueReader(IArrowArray array, string vendor, string type, Func<int, object?> storage) => array switch
    {
        FixedSizeBinaryArray a => i => a.IsNull(i) ? null : ArrowOpaqueCodec.TryDecode(vendor, type, a.GetBytes(i)) ?? storage(i),
        BinaryArray a => i => a.IsNull(i) ? null : ArrowOpaqueCodec.TryDecode(vendor, type, a.GetBytes(i)) ?? storage(i),
        _ => i => array.IsNull(i) ? null : ArrowOpaqueCodec.TryDecode(vendor, type, ReadOnlySpan<byte>.Empty) ?? storage(i)
    };

    private static Func<int, object?> CompileStorageReader(IArrowArray array, Field field)
    {
        var undeclared = IsUndeclaredDecimal(field);
        var wallClock = field.DataType is TimestampType ts && string.IsNullOrEmpty(ts.Timezone);

        return array switch
        {
            BooleanArray a => i => a.GetValue(i) is { } v ? v : null,
            Int8Array a => Primitive(a),
            Int16Array a => Primitive(a),
            Int32Array a => Primitive(a),
            Int64Array a => Primitive(a),
            UInt8Array a => Primitive(a),
            UInt16Array a => Primitive(a),
            UInt32Array a => Primitive(a),
            UInt64Array a => Primitive(a),
            FloatArray a => Primitive(a),
            DoubleArray a => Primitive(a),
            StringArray a => i => a.IsNull(i) ? null : a.GetString(i),
            BinaryArray a => i => a.IsNull(i) ? null : a.GetBytes(i).ToArray(),
            Decimal128Array a => undeclared
                ? i => a.GetValue(i) is { } d ? DecimalWidth.Bare(d) : null
                : i => a.GetValue(i) is { } d ? d : null,
            Decimal256Array a => undeclared
                ? i => a.GetValue(i) is { } d ? DecimalWidth.Bare(d) : null
                : i => a.GetValue(i) is { } d ? d : null,
            // The two narrow decimals derive from FixedSizeBinaryArray, so they come before it.
            Decimal32Array a => i => a.GetValue(i) is { } d ? d : null,
            Decimal64Array a => i => a.GetValue(i) is { } d ? d : null,
            FixedSizeBinaryArray a => i => a.IsNull(i) ? null : a.GetBytes(i).ToArray(),
            Date32Array a => i => a.GetDateOnly(i) is { } d ? d : null,
            Date64Array a => i => a.GetDateTime(i) is { } d ? d : null,
            TimestampArray a => wallClock
                ? i => a.GetTimestamp(i) is { } t ? TemporalNormalization.ToWallClock(t) : null
                : i => a.GetTimestamp(i) is { } t ? t : null,
            DurationArray a => i => a.GetTimeSpan(i) is { } d ? d : null,
            Time32Array a => i => a.GetTime(i) is { } t ? t : null,
            Time64Array a => i => a.GetTime(i) is { } t ? t : null,
            _ => i => GetValue(array, i, field)
        };
    }

    private static Func<int, object?> Primitive<T>(PrimitiveArray<T> array) where T : struct, IEquatable<T>
        => i => array.GetValue(i) is { } v ? v : null;

    /// <summary>
    /// A decimal cell. A column that declared no width reads bare: the 18 decimals it stores at are the
    /// default's, not the source's. Here rather than after the switch, so that only a decimal pays for it.
    /// </summary>
    private static object? ReadDecimal(decimal? value, Field? field)
        => field is { HasMetadata: true } && value is { } stored && IsUndeclaredDecimal(field)
            ? DecimalWidth.Bare(stored)
            : value;

    /// <summary>
    /// A fixed-width cell. The two narrow decimal arrays derive from <see cref="FixedSizeBinaryArray"/>:
    /// without this test their cells would come back as the bytes of a decimal. Here rather than in the
    /// main switch, so that only a fixed-width cell pays for it.
    /// </summary>
    private static object? GetFixedWidthValue(FixedSizeBinaryArray array, int index) => array switch
    {
        Decimal32Array a => a.GetValue(index),
        Decimal64Array a => a.GetValue(index),
        _ => array.GetBytes(index).ToArray()
    };

    private static object? GetStructValue(StructArray array, int index)
    {
        if (array.IsNull(index)) return null;
        var dict = new Dictionary<string, object?>(StringComparer.OrdinalIgnoreCase);
        var type = (StructType)array.Data.DataType;

        for (int i = 0; i < type.Fields.Count; i++)
        {
            var field = type.Fields[i];
            var childArray = array.Fields[i];
            dict[field.Name] = GetValue(childArray, index, field);
        }

        return dict;
    }

    /// <summary>
    /// <c>List&lt;TElement&gt;</c> for a list column, falling back to <c>List&lt;object?&gt;</c>
    /// when the element itself has no distinct CLR type to name.
    /// </summary>
    private static Type GetListClrType(IArrowType type)
    {
        var valueField = type switch
        {
            ListType list => list.ValueField,
            LargeListType largeList => largeList.ValueField,
            FixedSizeListType fixedSize => fixedSize.ValueField,
            _ => null
        };

        if (valueField is null) return typeof(List<object?>);

        var elementType = GetClrTypeFromField(valueField);
        return elementType == typeof(object) ? typeof(List<object?>) : typeof(List<>).MakeGenericType(elementType);
    }

    private static object? GetFixedSizeListValue(FixedSizeListArray array, int index)
    {
        if (array.IsNull(index)) return null;
        var type = (FixedSizeListType)array.Data.DataType;
        var size = type.ListSize;
        var values = array.Values;
        var list = new List<object?>(size);

        for (var i = index * size; i < (index + 1) * size; i++)
            list.Add(GetValue(values, i, type.ValueField));

        return list;
    }

    /// <summary>
    /// Resolves a dictionary-encoded value: the index array points into the shared dictionary,
    /// and the decoded value is what a consumer expects to receive.
    /// </summary>
    private static object? GetDictionaryValue(DictionaryArray array, int index, Field? field)
    {
        if (array.IsNull(index)) return null;

        var key = GetValue(array.Indices, index);
        if (key is null) return null;

        var position = Convert.ToInt32(key, CultureInfo.InvariantCulture);
        if (position < 0 || position >= array.Dictionary.Length) return null;

        // The field describes the logical column, not the dictionary's own storage, so it is not
        // forwarded: an extension on the column has already been handled before this point.
        return GetValue(array.Dictionary, position);
    }

    private static object? GetMapValue(MapArray array, int index)
    {
        if (array.IsNull(index)) return null;

        var dict = new Dictionary<object, object?>();
        var type = (MapType)array.Data.DataType;
        var entries = array.KeyValues;
        var keys = entries.Fields[0];
        var values = entries.Fields[1];

        var start = array.ValueOffsets[index];
        var end = array.ValueOffsets[index + 1];

        for (var i = start; i < end; i++)
        {
            var key = GetValue(keys, i, type.KeyField);
            if (key is not null) dict[key] = GetValue(values, i, type.ValueField);
        }

        return dict;
    }

    private static object? GetListValue(ListArray array, int index)
    {
        if (array.IsNull(index)) return null;
        var list = new List<object?>();
        var valueArray = array.Values;
        // The element's field carries what its storage does not: a UUID's extension, a timestamp's zone.
        var elementField = ((ListType)array.Data.DataType).ValueField;
        int start = array.ValueOffsets[index];
        int end = array.ValueOffsets[index + 1];

        for (int i = start; i < end; i++)
        {
            list.Add(GetValue(valueArray, i, elementField));
        }

        return list;
    }
}
