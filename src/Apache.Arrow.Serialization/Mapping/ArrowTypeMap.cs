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
        if (type == typeof(decimal) || type == typeof(decimal?)) return new ArrowTypeResult(DefaultDecimalType);
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
        if (type == typeof(byte[])) return new ArrowTypeResult(BinaryType.Default);
        if (type == typeof(object)) return new ArrowTypeResult(StringType.Default); // Placeholder for dynamic/complex types

        // Enums mapping to Int32
        if (type.IsEnum) return new ArrowTypeResult(Int32Type.Default);
        var underlyingType = Nullable.GetUnderlyingType(type);
        if (underlyingType?.IsEnum == true) return new ArrowTypeResult(Int32Type.Default);

        throw new NotSupportedException($"Type {type.FullName} is not a valid scalar type supported by ArrowTypeMap.");
    }

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
        if (precision is not int p || p < 1 || p > 38) return logical;
        if (scale is not int s || s < 0 || s > p) return logical;

        return new ArrowTypeResult(new Decimal128Type(p, s), logical.Metadata);
    }

    /// <summary>
    /// Constructs a field directly from a logical type mapping result, injecting appropriate metadata.
    /// </summary>
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
        if (type is Decimal128Type) return typeof(decimal);
        if (type is Decimal256Type) return typeof(decimal);
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
            ArrowTypeId.Date32 => typeof(DateTime),
            ArrowTypeId.Date64 => typeof(DateTime),
            ArrowTypeId.Decimal128 => typeof(decimal),
            ArrowTypeId.Decimal256 => typeof(decimal),
            ArrowTypeId.Duration => typeof(TimeSpan),
            ArrowTypeId.Struct => typeof(Dictionary<string, object?>),
            ArrowTypeId.List or ArrowTypeId.LargeList or ArrowTypeId.FixedSizeList => typeof(List<object?>),
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
            if (ext.Is("arrow.opaque") && ArrowOpaqueCodec.CanDecode(ext.VendorName, ext.TypeName))
                return typeof(string);
        }
        return GetClrType(field.DataType);
    }

    /// <summary>
    /// Extracts the CLR value from an Arrow array at the specified index, respecting logical type
    /// metadata (e.g. UUID) if a <see cref="Field"/> is provided.
    /// </summary>
    public static object? GetValue(IArrowArray array, int index, Field? field = null)
    {
        if (array.IsNull(index)) return null;

        // 1. Extension types, which carry a logical meaning their storage type does not.
        if (ArrowExtensionInfo.TryRead(field, out var ext))
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
            Decimal128Array a => a.GetValue(index),
            Decimal256Array a => a.GetValue(index),
            FixedSizeBinaryArray a => a.GetBytes(index).ToArray(),
            Date32Array a => a.GetDateTime(index),
            Date64Array a => a.GetDateTime(index),
            TimestampArray a => a.GetTimestamp(index),
            DurationArray a => a.GetValue(index),
            Time32Array a => a.GetValue(index),
            Time64Array a => a.GetValue(index),
            StructArray a => GetStructValue(a, index),
            // Before ListArray: MapArray derives from it, so the general case would win and a map
            // would come back as a list of {key,value} structs, contradicting the Dictionary that
            // GetClrType declares for it.
            MapArray a => GetMapValue(a, index),
            ListArray a => GetListValue(a, index),
            FixedSizeListArray a => GetFixedSizeListValue(a, index),
            DictionaryArray a => GetDictionaryValue(a, index, field),
            _ => throw new NotSupportedException($"Unsupported Arrow array type for value extraction: {array.GetType().Name}")
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

    private static object? GetFixedSizeListValue(FixedSizeListArray array, int index)
    {
        if (array.IsNull(index)) return null;
        var size = ((FixedSizeListType)array.Data.DataType).ListSize;
        var values = array.Values;
        var list = new List<object?>(size);

        for (var i = index * size; i < (index + 1) * size; i++)
            list.Add(GetValue(values, i));

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
        var entries = array.KeyValues;
        var keys = entries.Fields[0];
        var values = entries.Fields[1];

        var start = array.ValueOffsets[index];
        var end = array.ValueOffsets[index + 1];

        for (var i = start; i < end; i++)
        {
            var key = GetValue(keys, i);
            if (key is not null) dict[key] = GetValue(values, i);
        }

        return dict;
    }

    private static object? GetListValue(ListArray array, int index)
    {
        if (array.IsNull(index)) return null;
        var list = new List<object?>();
        var valueArray = array.Values;
        int start = array.ValueOffsets[index];
        int end = array.ValueOffsets[index + 1];

        for (int i = start; i < end; i++)
        {
            list.Add(GetValue(valueArray, i));
        }

        return list;
    }
}
