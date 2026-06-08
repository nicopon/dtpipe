using Apache.Arrow;
using Apache.Arrow.Arrays;
using Apache.Arrow.Types;
using Apache.Arrow.Serialization.Reflection;
using Apache.Arrow.Serialization.Mapping;

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
    private static readonly List<IArrowTypeHandler> _handlers = new()
    {
        new Handlers.BooleanHandler(),
        new Handlers.Int8Handler(),
        new Handlers.Int16Handler(),
        new Handlers.Int32Handler(),
        new Handlers.Int64Handler(),
        new Handlers.UInt8Handler(),
        new Handlers.UInt16Handler(),
        new Handlers.UInt32Handler(),
        new Handlers.UInt64Handler(),
        new Handlers.FloatHandler(),
        new Handlers.DoubleHandler(),
        new Handlers.StringHandler(),
        new Handlers.BinaryHandler(),
        new Handlers.Decimal128Handler(),
        new Handlers.Decimal256Handler(),
        new Handlers.FixedSizeBinaryHandler(),
        new Handlers.Date32Handler(),
        new Handlers.Date64Handler(),
        new Handlers.TimestampHandler(),
        new Handlers.DurationHandler(),
        new Handlers.Time32Handler(),
        new Handlers.Time64Handler(),
        new Handlers.StructHandler(),
        new Handlers.ListHandler()
    };
    // ── UUID byte-order helpers ──────────────────────────────────────────────

    /// <summary>
    /// Converts a .NET Guid (little-endian first 3 components) to RFC 4122 big-endian bytes
    /// suitable for canonical Arrow UUID storage.
    /// </summary>
    public static byte[] ToArrowUuidBytes(Guid guid) => ArrowTypeMap.ToArrowUuidBytes(guid);

    /// <summary>
    /// Converts RFC 4122 big-endian UUID bytes (from an Arrow binary column) back to a .NET Guid.
    /// </summary>
    public static Guid FromArrowUuidBytes(ReadOnlySpan<byte> b) => ArrowTypeMap.FromArrowUuidBytes(b);

    // ── Type mappings ────────────────────────────────────────────────────────

    /// <summary>
    /// Returns the CLR type for a given Arrow type.
    /// Performs only unambiguous, direct mappings — no heuristics.
    /// <see cref="FixedSizeBinaryType"/> always maps to <c>typeof(byte[])</c> regardless of byte width;
    /// use <see cref="GetClrTypeFromField"/> to resolve semantic types via extension metadata (e.g. arrow.uuid → Guid).
    /// </summary>
    public static Type GetClrType(IArrowType type) => Apache.Arrow.Serialization.Mapping.ArrowTypeMap.GetClrType(type);

    /// <summary>
    /// Returns the CLR type for a given Arrow <see cref="Field"/>, checking extension metadata first.
    /// Unlike <see cref="GetClrType(IArrowType)"/>, this method resolves semantic extension types:
    /// a field with storage type <see cref="FixedSizeBinaryType"/>(16) and metadata
    /// <c>ARROW:extension:name = arrow.uuid</c> returns <c>typeof(Guid)</c>.
    /// Falls through to <see cref="GetClrType(IArrowType)"/> for all other types.
    /// </summary>
    public static Type GetClrTypeFromField(Field field) => Apache.Arrow.Serialization.Mapping.ArrowTypeMap.GetClrTypeFromField(field);

    /// <summary>
    /// Extracts the value at <paramref name="index"/> from <paramref name="array"/>,
    /// resolving extension types from <paramref name="field"/> metadata when available.
    /// For a field with <c>arrow.uuid</c> extension and a <see cref="FixedSizeBinaryArray"/>,
    /// returns a <see cref="Guid"/> (converted via <see cref="FromArrowUuidBytes"/>).
    /// Falls through to <see cref="GetValue(IArrowArray,int)"/> for all other cases.
    /// </summary>
    public static object? GetValueForField(IArrowArray array, Field field, int index)
        => ArrowTypeMap.GetValue(array, index, field);

    public static Apache.Arrow.Serialization.Mapping.ArrowTypeResult GetLogicalType(Type clrType) => Apache.Arrow.Serialization.Mapping.ArrowTypeMap.GetLogicalType(clrType);

    public static Apache.Arrow.Field GetField(string name, Type clrType, bool isNullable = true) => 
        Apache.Arrow.Serialization.Mapping.ArrowTypeMap.GetField(name, GetLogicalType(clrType), isNullable);


    public static IArrowArrayBuilder CreateBuilder(IArrowType type)
    {
        var handler = _handlers.FirstOrDefault(h => h.CanHandle(type));
        if (handler != null) return handler.CreateBuilder(type);

        throw new NotSupportedException($"Unsupported Arrow type for builder: {type.Name}");
    }

    public static object? GetValue(IArrowArray array, int index)
        => ArrowTypeMap.GetValue(array, index);

    public static void AppendNull(IArrowArrayBuilder builder)
    {
        var handler = _handlers.FirstOrDefault(h => h.CanHandle(builder));
        if (handler != null)
        {
            handler.AppendNull(builder);
            return;
        }

        throw new NotSupportedException($"Unsupported builder type for AppendNull: {builder.GetType().Name}");
    }

    public static IArrowArray BuildArray(IArrowArrayBuilder builder)
    {
        var handler = _handlers.FirstOrDefault(h => h.CanHandle(builder));
        if (handler != null) return handler.Build(builder);

        throw new NotSupportedException($"Unsupported builder type for BuildArray: {builder.GetType().Name}");
    }

    public static void AppendValue(IArrowArrayBuilder builder, object? value)
    {
        var handler = _handlers.FirstOrDefault(h => h.CanHandle(builder));
        if (handler != null)
        {
            handler.AppendValue(builder, value);
            return;
        }

        throw new NotSupportedException($"Unsupported builder type for AppendValue: {builder.GetType().Name}");
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
