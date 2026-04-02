using Apache.Arrow;
using Apache.Arrow.Arrays;
using Apache.Arrow.Types;

namespace DtPipe.Core.Infrastructure.Arrow;

/// <summary>
/// Centralized mapper for CLR to Arrow types and vice-versa.
/// Provides unified logic for schema generation, builder creation, and value extraction.
///
/// Extensions (like arrow.uuid) are processed strictly via explicit metadata.
/// There are no heuristics or default Guid-inference logic.
///
/// IMPORTANT – inheritance order in switches:
///   Decimal128Type/Decimal256Type inherit from FixedSizeBinaryType.
///   Decimal128Array/Decimal256Array inherit from FixedSizeBinaryArray.
///   Always match Decimal variants BEFORE FixedSizeBinary in switch arms.
/// </summary>
public static class ArrowTypeMapper
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
    /// Returns the CLR type for a given Arrow type.
    /// Performs only unambiguous, direct mappings — no heuristics.
    /// <see cref="FixedSizeBinaryType"/> always maps to <c>typeof(byte[])</c> regardless of byte width;
    /// use <see cref="GetClrTypeFromField"/> to resolve semantic types via extension metadata (e.g. arrow.uuid → Guid).
    /// </summary>
    public static Type GetClrType(IArrowType type)
    {
        // Decimal types inherit from FixedSizeBinaryType — check them before the FixedSizeBinary guard
        if (type is Decimal128Type) return typeof(decimal);
        if (type is Decimal256Type) return typeof(decimal);

        // FixedSizeBinary of any width = generic byte[] — no heuristic inference of Guid or other types
        if (type is FixedSizeBinaryType) return typeof(byte[]);

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
            ArrowTypeId.Timestamp => typeof(DateTimeOffset),
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
    /// Unlike <see cref="GetClrType(IArrowType)"/>, this method resolves semantic extension types:
    /// a field with storage type <see cref="FixedSizeBinaryType"/>(16) and metadata
    /// <c>ARROW:extension:name = arrow.uuid</c> returns <c>typeof(Guid)</c>.
    /// Falls through to <see cref="GetClrType(IArrowType)"/> for all other types.
    /// </summary>
    public static Type GetClrTypeFromField(Field field)
    {
        if (field.HasMetadata &&
            field.Metadata.TryGetValue("ARROW:extension:name", out var ext) &&
            string.Equals(ext, "arrow.uuid", StringComparison.OrdinalIgnoreCase))
            return typeof(Guid);
        return GetClrType(field.DataType);
    }

    /// <summary>
    /// Extracts the value at <paramref name="index"/> from <paramref name="array"/>,
    /// resolving extension types from <paramref name="field"/> metadata when available.
    /// For a field with <c>arrow.uuid</c> extension and a <see cref="FixedSizeBinaryArray"/>,
    /// returns a <see cref="Guid"/> (converted via <see cref="FromArrowUuidBytes"/>).
    /// Falls through to <see cref="GetValue(IArrowArray,int)"/> for all other cases.
    /// </summary>
    public static object? GetValueForField(IArrowArray array, Field field, int index)
    {
        if (array.IsNull(index)) return null;
        if (array is FixedSizeBinaryArray fsba &&
            field.HasMetadata &&
            field.Metadata.TryGetValue("ARROW:extension:name", out var ext) &&
            string.Equals(ext, "arrow.uuid", StringComparison.OrdinalIgnoreCase))
            return FromArrowUuidBytes(fsba.GetBytes(index));
        return GetValue(array, index);
    }

    public static IArrowType GetArrowType(Type clrType)
    {
        var underlyingType = Nullable.GetUnderlyingType(clrType) ?? clrType;

        if (underlyingType == typeof(int)) return Int32Type.Default;
        if (underlyingType == typeof(long)) return Int64Type.Default;
        if (underlyingType == typeof(sbyte)) return Int8Type.Default;
        if (underlyingType == typeof(short)) return Int16Type.Default;
        if (underlyingType == typeof(byte)) return UInt8Type.Default;
        if (underlyingType == typeof(ushort)) return UInt16Type.Default;
        if (underlyingType == typeof(uint)) return UInt32Type.Default;
        if (underlyingType == typeof(ulong)) return UInt64Type.Default;
        if (underlyingType == typeof(double)) return DoubleType.Default;
        if (underlyingType == typeof(float)) return FloatType.Default;
        if (underlyingType == typeof(bool)) return BooleanType.Default;
        if (underlyingType == typeof(string)) return StringType.Default;
        if (underlyingType == typeof(decimal)) return new Decimal128Type(38, 18);
        if (underlyingType == typeof(DateTime)) return Date64Type.Default;
        if (underlyingType == typeof(DateTimeOffset)) return TimestampType.Default;
        if (underlyingType == typeof(byte[])) return BinaryType.Default;
        // Guid → FixedSizeBinary(16) with arrow.uuid extension metadata (set by ArrowSchemaFactory)
        if (underlyingType == typeof(Guid)) return new FixedSizeBinaryType(16);
        if (underlyingType == typeof(TimeSpan)) return DurationType.Millisecond;

        // Fallback
        return StringType.Default;
    }

    public static IArrowArrayBuilder CreateBuilder(IArrowType type)
    {
        return type switch
        {
            BooleanType => new BooleanArray.Builder(),
            Int8Type => new Int8Array.Builder(),
            Int16Type => new Int16Array.Builder(),
            Int32Type => new Int32Array.Builder(),
            Int64Type => new Int64Array.Builder(),
            UInt8Type => new UInt8Array.Builder(),
            UInt16Type => new UInt16Array.Builder(),
            UInt32Type => new UInt32Array.Builder(),
            UInt64Type => new UInt64Array.Builder(),
            FloatType => new FloatArray.Builder(),
            DoubleType => new DoubleArray.Builder(),
            StringType => new StringArray.Builder(),
            BinaryType => new BinaryArray.Builder(),
            // Decimal types BEFORE FixedSizeBinaryType (inheritance order)
            Decimal128Type t => new Decimal128Array.Builder(t),
            Decimal256Type t => new Decimal256Array.Builder(t),
            // FixedSizeBinary maps to the generic builder
            FixedSizeBinaryType fst => new FixedSizeBinaryArrayBuilder(fst.ByteWidth),
            Date32Type => new Date32Array.Builder(),
            Date64Type => new Date64Array.Builder(),
            TimestampType t => new TimestampArray.Builder(t),
            DurationType t => new DurationArray.Builder(t),
            Time32Type t => new Time32Array.Builder(t),
            Time64Type t => new Time64Array.Builder(t),
            _ => throw new NotSupportedException($"Unsupported Arrow type for builder: {type.Name}")
        };
    }

    public static object? GetValue(IArrowArray array, int index)
    {
        if (array.IsNull(index)) return null;

        return array switch
        {
            BooleanArray a => a.GetValue(index),
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
            // Decimal arrays BEFORE FixedSizeBinaryArray (inheritance order)
            Decimal128Array a => a.GetValue(index),
            Decimal256Array a => a.GetValue(index),
            // FixedSizeBinaryArray: always return byte[] — no heuristic inference of Guid.
            // Use GetValueForField(array, field, index) when the Field context is available
            // to resolve arrow.uuid extension metadata into a Guid.
            FixedSizeBinaryArray a => a.GetBytes(index).ToArray(),
            Date32Array a => a.GetDateTime(index),
            Date64Array a => a.GetDateTime(index),
            TimestampArray a => a.GetTimestamp(index),
            DurationArray a => a.GetValue(index),
            Time32Array a => a.GetValue(index),
            Time64Array a => a.GetValue(index),
            _ => throw new NotSupportedException($"Unsupported Arrow array type for value extraction: {array.GetType().Name}")
        };
    }

    public static void AppendNull(IArrowArrayBuilder builder)
    {
        switch (builder)
        {
            case BooleanArray.Builder b: b.AppendNull(); break;
            case Int8Array.Builder b: b.AppendNull(); break;
            case Int16Array.Builder b: b.AppendNull(); break;
            case Int32Array.Builder b: b.AppendNull(); break;
            case Int64Array.Builder b: b.AppendNull(); break;
            case UInt8Array.Builder b: b.AppendNull(); break;
            case UInt16Array.Builder b: b.AppendNull(); break;
            case UInt32Array.Builder b: b.AppendNull(); break;
            case UInt64Array.Builder b: b.AppendNull(); break;
            case FloatArray.Builder b: b.AppendNull(); break;
            case DoubleArray.Builder b: b.AppendNull(); break;
            case StringArray.Builder b: b.AppendNull(); break;
            case BinaryArray.Builder b: b.AppendNull(); break;
            case FixedSizeBinaryArrayBuilder b: b.AppendNull(); break;
            case Decimal128Array.Builder b: b.AppendNull(); break;
            case Decimal256Array.Builder b: b.AppendNull(); break;
            case Date32Array.Builder b: b.AppendNull(); break;
            case Date64Array.Builder b: b.AppendNull(); break;
            case TimestampArray.Builder b: b.AppendNull(); break;
            case DurationArray.Builder b: b.AppendNull(); break;
            case Time32Array.Builder b: b.AppendNull(); break;
            case Time64Array.Builder b: b.AppendNull(); break;
        }
    }

    public static IArrowArray BuildArray(IArrowArrayBuilder builder)
    {
        return builder switch
        {
            BooleanArray.Builder b => b.Build(),
            Int8Array.Builder b => b.Build(),
            Int16Array.Builder b => b.Build(),
            Int32Array.Builder b => b.Build(),
            Int64Array.Builder b => b.Build(),
            UInt8Array.Builder b => b.Build(),
            UInt16Array.Builder b => b.Build(),
            UInt32Array.Builder b => b.Build(),
            UInt64Array.Builder b => b.Build(),
            FloatArray.Builder b => b.Build(),
            DoubleArray.Builder b => b.Build(),
            StringArray.Builder b => b.Build(),
            BinaryArray.Builder b => b.Build(),
            FixedSizeBinaryArrayBuilder b => b.Build(),
            Decimal128Array.Builder b => b.Build(),
            Decimal256Array.Builder b => b.Build(),
            Date32Array.Builder b => b.Build(),
            Date64Array.Builder b => b.Build(),
            TimestampArray.Builder b => b.Build(),
            DurationArray.Builder b => b.Build(),
            Time32Array.Builder b => b.Build(),
            Time64Array.Builder b => b.Build(),
            _ => throw new NotSupportedException($"Unsupported builder type for BuildArray: {builder.GetType().Name}")
        };
    }

    public static void AppendValue(IArrowArrayBuilder builder, object? value)
    {
        if (value == null || value == DBNull.Value)
        {
            AppendNull(builder);
            return;
        }

        switch (builder)
        {
            case StringArray.Builder b: b.Append(value.ToString()); break;
            case Int8Array.Builder b: b.Append(Convert.ToSByte(value)); break;
            case Int16Array.Builder b: b.Append(Convert.ToInt16(value)); break;
            case Int32Array.Builder b: b.Append(Convert.ToInt32(value)); break;
            case Int64Array.Builder b: b.Append(Convert.ToInt64(value)); break;
            case UInt8Array.Builder b: b.Append(Convert.ToByte(value)); break;
            case UInt16Array.Builder b: b.Append(Convert.ToUInt16(value)); break;
            case UInt32Array.Builder b: b.Append(Convert.ToUInt32(value)); break;
            case UInt64Array.Builder b: b.Append(Convert.ToUInt64(value)); break;
            case DoubleArray.Builder b: b.Append(Convert.ToDouble(value)); break;
            case FloatArray.Builder b: b.Append(Convert.ToSingle(value)); break;
            case BooleanArray.Builder b: b.Append(Convert.ToBoolean(value)); break;
            case Decimal128Array.Builder b: b.Append(Convert.ToDecimal(value)); break;
            case Decimal256Array.Builder b: b.Append(Convert.ToDecimal(value)); break;
            case FixedSizeBinaryArrayBuilder b:
                if (value is Guid guid) b.Append(ToArrowUuidBytes(guid));
                else if (value is byte[] fixedBytes && fixedBytes.Length == b.ByteWidth) b.Append(fixedBytes);
                else b.AppendNull();
                break;
            case BinaryArray.Builder b:
                // Generic binary builder: byte[] passed through as-is
                if (value is byte[] binBytes) b.Append((System.Collections.Generic.IEnumerable<byte>)binBytes);
                else b.AppendNull();
                break;
            case Date32Array.Builder b: b.Append(Convert.ToDateTime(value)); break;
            case Date64Array.Builder b: b.Append(Convert.ToDateTime(value)); break;
            case TimestampArray.Builder b:
                if (value is DateTimeOffset dto) b.Append(dto);
                else if (value is DateTime dt) b.Append(dt);
                else b.Append(Convert.ToDateTime(value));
                break;
            default:
                AppendNull(builder);
                break;
        }
    }

    public static void AppendArrayValue(IArrowArrayBuilder builder, IArrowArray array, int index)
    {
        if (array.IsNull(index))
        {
            AppendNull(builder);
            return;
        }

        switch (array)
        {
            case BooleanArray a: ((BooleanArray.Builder)builder).Append(a.GetValue(index)!.Value); break;
            case Int8Array a: ((Int8Array.Builder)builder).Append(a.GetValue(index)!.Value); break;
            case Int16Array a: ((Int16Array.Builder)builder).Append(a.GetValue(index)!.Value); break;
            case Int32Array a: ((Int32Array.Builder)builder).Append(a.GetValue(index)!.Value); break;
            case Int64Array a: ((Int64Array.Builder)builder).Append(a.GetValue(index)!.Value); break;
            case UInt8Array a: ((UInt8Array.Builder)builder).Append(a.GetValue(index)!.Value); break;
            case UInt16Array a: ((UInt16Array.Builder)builder).Append(a.GetValue(index)!.Value); break;
            case UInt32Array a: ((UInt32Array.Builder)builder).Append(a.GetValue(index)!.Value); break;
            case UInt64Array a: ((UInt64Array.Builder)builder).Append(a.GetValue(index)!.Value); break;
            case FloatArray a: ((FloatArray.Builder)builder).Append(a.GetValue(index)!.Value); break;
            case DoubleArray a: ((DoubleArray.Builder)builder).Append(a.GetValue(index)!.Value); break;
            case StringArray a: ((StringArray.Builder)builder).Append(a.GetString(index)); break;
            case BinaryArray a: ((BinaryArray.Builder)builder).Append(a.GetBytes(index)); break;
            // Decimal arrays BEFORE FixedSizeBinaryArray (inheritance order)
            case Decimal128Array a: ((Decimal128Array.Builder)builder).Append(a.GetValue(index)!.Value); break;
            case Decimal256Array a: ((Decimal256Array.Builder)builder).Append(a.GetValue(index)!.Value); break;
            // FixedSizeBinaryArray: route to FixedSizeBinaryArrayBuilder if available, else BinaryArray.Builder
            case FixedSizeBinaryArray a when builder is FixedSizeBinaryArrayBuilder fb:
                fb.Append(a.GetBytes(index)); break;
            case FixedSizeBinaryArray a:
                ((BinaryArray.Builder)builder).Append(a.GetBytes(index)); break;
            case Date32Array a: ((Date32Array.Builder)builder).Append(a.GetDateTime(index)!.Value); break;
            case Date64Array a: ((Date64Array.Builder)builder).Append(a.GetDateTime(index)!.Value); break;
            case TimestampArray a: ((TimestampArray.Builder)builder).Append(a.GetTimestamp(index)!.Value); break;
            case DurationArray a: ((DurationArray.Builder)builder).Append(a.GetValue(index)!.Value); break;
            case Time32Array a: ((Time32Array.Builder)builder).Append(a.GetValue(index)!.Value); break;
            case Time64Array a: ((Time64Array.Builder)builder).Append(a.GetValue(index)!.Value); break;
            default:
                // Boxed fallback for any missed types
                AppendValue(builder, GetValue(array, index));
                break;
        }
    }
}
