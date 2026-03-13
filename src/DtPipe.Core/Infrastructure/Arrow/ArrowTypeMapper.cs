using Apache.Arrow;
using Apache.Arrow.Types;

namespace DtPipe.Core.Infrastructure.Arrow;

/// <summary>
/// Centralized mapper for CLR to Arrow types and vice-versa.
/// Provides unified logic for schema generation, builder creation, and value extraction.
/// </summary>
public static class ArrowTypeMapper
{
    public static Type GetClrType(IArrowType type)
    {
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

    public static IArrowType GetArrowType(Type clrType)
    {
        var underlyingType = Nullable.GetUnderlyingType(clrType) ?? clrType;

        if (underlyingType == typeof(int)) return Int32Type.Default;
        if (underlyingType == typeof(long)) return Int64Type.Default;
        if (underlyingType == typeof(double)) return DoubleType.Default;
        if (underlyingType == typeof(float)) return FloatType.Default;
        if (underlyingType == typeof(bool)) return BooleanType.Default;
        if (underlyingType == typeof(string)) return StringType.Default;
        if (underlyingType == typeof(decimal)) return new Decimal128Type(38, 18);
        if (underlyingType == typeof(DateTime)) return Date64Type.Default;
        if (underlyingType == typeof(DateTimeOffset)) return TimestampType.Default;
        if (underlyingType == typeof(byte[])) return BinaryType.Default;
        if (underlyingType == typeof(Guid)) return BinaryType.Default;
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
            Decimal128Type t => new Decimal128Array.Builder(t),
            Decimal256Type t => new Decimal256Array.Builder(t),
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
            Decimal128Array a => a.GetValue(index),
            Decimal256Array a => a.GetValue(index),
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
            case Int32Array.Builder b: b.Append(Convert.ToInt32(value)); break;
            case Int64Array.Builder b: b.Append(Convert.ToInt64(value)); break;
            case DoubleArray.Builder b: b.Append(Convert.ToDouble(value)); break;
            case FloatArray.Builder b: b.Append(Convert.ToSingle(value)); break;
            case BooleanArray.Builder b: b.Append(Convert.ToBoolean(value)); break;
            case Decimal128Array.Builder b: b.Append(Convert.ToDecimal(value)); break;
            case BinaryArray.Builder b:
                if (value is byte[] bytes) b.Append((System.Collections.Generic.IEnumerable<byte>)bytes);
                else if (value is Guid guid) b.Append((System.Collections.Generic.IEnumerable<byte>)guid.ToByteArray());
                else b.AppendNull();
                break;
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
            case Decimal128Array a: ((Decimal128Array.Builder)builder).Append(a.GetValue(index)!.Value); break;
            case Decimal256Array a: ((Decimal256Array.Builder)builder).Append(a.GetValue(index)!.Value); break;
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
