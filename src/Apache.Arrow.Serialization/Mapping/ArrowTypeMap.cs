using System;
using System.Collections.Generic;
using Apache.Arrow.Types;

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
    /// Converts a .NET Guid (little-endian first 3 components) to RFC 4122 big-endian bytes
    /// suitable for canonical Arrow UUID storage.
    /// </summary>
    public static byte[] ToArrowUuidBytes(Guid guid)
    {
        var bytes = guid.ToByteArray();
        System.Array.Reverse(bytes, 0, 4); // component A: little → big
        System.Array.Reverse(bytes, 4, 2); // component B: little → big
        System.Array.Reverse(bytes, 6, 2); // component C: little → big
        // components D-E (bytes 8-15) are already big-endian in .NET
        return bytes;
    }

    /// <summary>
    /// Converts RFC 4122 big-endian UUID bytes (from an Arrow binary column) back to a .NET Guid.
    /// </summary>
    public static Guid FromArrowUuidBytes(ReadOnlySpan<byte> b)
    {
        var copy = b.ToArray();
        System.Array.Reverse(copy, 0, 4);
        System.Array.Reverse(copy, 4, 2);
        System.Array.Reverse(copy, 6, 2);
        return new Guid(copy);
    }

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
        if (type == typeof(decimal) || type == typeof(decimal?)) return new ArrowTypeResult(new Decimal128Type(38, 18));
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
            _ => typeof(string)
        };
    }

    /// <summary>
    /// Returns the CLR type for a given Arrow <see cref="Field"/>, checking extension metadata first.
    /// </summary>
    public static Type GetClrTypeFromField(Field field)
    {
        if (field.HasMetadata &&
            field.Metadata.TryGetValue("ARROW:extension:name", out var ext) &&
            string.Equals(ext, "arrow.uuid", StringComparison.OrdinalIgnoreCase))
            return typeof(Guid);
        return GetClrType(field.DataType);
    }
}
