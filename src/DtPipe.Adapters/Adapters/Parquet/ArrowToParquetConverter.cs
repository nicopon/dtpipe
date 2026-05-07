using Apache.Arrow;
using Apache.Arrow.Arrays;
using DtPipe.Core.Infrastructure.Arrow;
using Parquet;
using Parquet.Data;
using Parquet.Schema;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace DtPipe.Adapters.Parquet;

public static class ArrowToParquetConverter
{
    public static Task WriteColumnAsync(ParquetRowGroupWriter writer, IArrowArray arrowArray, DataField dataField, CancellationToken ct = default)
    {
        bool isGuidField = dataField.ClrNullableIfHasNullsType == typeof(Guid)
                        || dataField.ClrNullableIfHasNullsType == typeof(Guid?);
        return arrowArray switch
        {
            Int32Array a => writer.WriteAsync<int>(dataField, ExtractPrimitiveValues<int, Int32Array>(a).AsMemory(), cancellationToken: ct),
            Int64Array a => writer.WriteAsync<long>(dataField, ExtractPrimitiveValues<long, Int64Array>(a).AsMemory(), cancellationToken: ct),
            DoubleArray a => writer.WriteAsync<double>(dataField, ExtractPrimitiveValues<double, DoubleArray>(a).AsMemory(), cancellationToken: ct),
            FloatArray a => writer.WriteAsync<float>(dataField, ExtractPrimitiveValues<float, FloatArray>(a).AsMemory(), cancellationToken: ct),
            BooleanArray a => writer.WriteAsync<bool>(dataField, ExtractBooleanValues(a).AsMemory(), cancellationToken: ct),
            StringArray a => writer.WriteAsync(dataField, (IReadOnlyCollection<string?>)ExtractStringValues(a)),
            Decimal128Array a => writer.WriteAsync<decimal>(dataField, ExtractDecimalValues<Decimal128Array>(a).AsMemory(), cancellationToken: ct),
            Decimal256Array a => writer.WriteAsync<decimal>(dataField, ExtractDecimalValues<Decimal256Array>(a).AsMemory(), cancellationToken: ct),
            Date64Array a => writer.WriteAsync<DateTime>(dataField, ExtractDate64Values(a).AsMemory(), cancellationToken: ct),
            // Timestamp: no-timezone fields are typed as DateTime — coerce DateTimeOffset to DateTime
            TimestampArray a when (Nullable.GetUnderlyingType(dataField.ClrNullableIfHasNullsType) ?? dataField.ClrNullableIfHasNullsType) == typeof(DateTime) =>
                writer.WriteAsync<DateTime>(dataField, ExtractTimestampAsDateTimeValues(a).AsMemory(), cancellationToken: ct),
            TimestampArray a => writer.WriteAsync<DateTimeOffset>(dataField, ExtractTimestampValues(a).AsMemory(), cancellationToken: ct),
            // FixedSizeBinaryArray(16) with arrow.uuid → DtPipe internal UUID format
            FixedSizeBinaryArray a when isGuidField => writer.WriteAsync<Guid>(dataField, ExtractGuidValuesFromFixed(a).AsMemory(), cancellationToken: ct),
            // BinaryArray legacy: kept for sources that still emit BinaryType for UUID
            BinaryArray a when isGuidField => writer.WriteAsync<Guid>(dataField, ExtractGuidValues(a).AsMemory(), cancellationToken: ct),
            BinaryArray a => writer.WriteAsync(dataField, (IReadOnlyCollection<byte[]?>)ExtractBinaryValues(a)),
            // FixedSizeBinaryArray without Guid field → generic binary bytes
            FixedSizeBinaryArray a => writer.WriteAsync(dataField, (IReadOnlyCollection<byte[]?>)ExtractFixedBinaryValues(a)),
            _ => throw new NotSupportedException($"Arrow array type {arrowArray.GetType().Name} is not supported for Parquet conversion yet.")
        };
    }


    private static Guid?[] ExtractGuidValuesFromFixed(FixedSizeBinaryArray array)
    {
        var result = new Guid?[array.Length];
        for (int i = 0; i < array.Length; i++)
        {
            if (array.IsNull(i)) { result[i] = null; continue; }
            result[i] = ArrowTypeMapper.FromArrowUuidBytes(array.GetBytes(i));
        }
        return result;
    }

    private static Guid?[] ExtractGuidValues(BinaryArray array)
    {
        var result = new Guid?[array.Length];
        for (int i = 0; i < array.Length; i++)
        {
            if (array.IsNull(i)) { result[i] = null; continue; }
            var bytes = array.GetBytes(i).ToArray();
            result[i] = bytes.Length == 16 ? new Guid(bytes) : null;
        }
        return result;
    }

    private static T?[] ExtractPrimitiveValues<T, TArray>(TArray array)
        where T : struct, IEquatable<T>
        where TArray : PrimitiveArray<T>
    {
        var result = new T?[array.Length];
        for (int i = 0; i < array.Length; i++)
        {
            result[i] = array.IsNull(i) ? null : array.GetValue(i);
        }
        return result;
    }

    private static decimal?[] ExtractDecimalValues<TArray>(TArray array)
        where TArray : IArrowArray
    {
        var result = new decimal?[array.Length];
        for (int i = 0; i < array.Length; i++)
        {
            result[i] = (decimal?)ArrowTypeMapper.GetValue(array, i);
        }
        return result;
    }

    private static bool?[] ExtractBooleanValues(BooleanArray array)
    {
        var result = new bool?[array.Length];
        for (int i = 0; i < array.Length; i++)
        {
            result[i] = array.IsNull(i) ? null : array.GetValue(i);
        }
        return result;
    }

    private static string?[] ExtractStringValues(StringArray array)
    {
        var result = new string?[array.Length];
        for (int i = 0; i < array.Length; i++)
        {
            result[i] = array.IsNull(i) ? null : array.GetString(i);
        }
        return result;
    }

    private static DateTime?[] ExtractDate64Values(Date64Array array)
    {
        var result = new DateTime?[array.Length];
        for (int i = 0; i < array.Length; i++)
        {
            result[i] = array.IsNull(i) ? null : array.GetDateTime(i);
        }
        return result;
    }

    private static DateTime?[] ExtractTimestampAsDateTimeValues(TimestampArray array)
    {
        var result = new DateTime?[array.Length];
        for (int i = 0; i < array.Length; i++)
        {
            if (array.IsNull(i)) { result[i] = null; continue; }
            var dto = array.GetTimestamp(i);
            result[i] = dto?.DateTime;
        }
        return result;
    }

    private static DateTimeOffset?[] ExtractTimestampValues(TimestampArray array)
    {
        var result = new DateTimeOffset?[array.Length];
        for (int i = 0; i < array.Length; i++)
        {
            result[i] = array.IsNull(i) ? null : array.GetTimestamp(i);
        }
        return result;
    }

    private static byte[][] ExtractBinaryValues(BinaryArray array)
    {
        var result = new byte[array.Length][];
        for (int i = 0; i < array.Length; i++)
        {
            result[i] = array.IsNull(i) ? [] : array.GetBytes(i).ToArray();
        }
        return result;
    }

    private static byte[][] ExtractFixedBinaryValues(FixedSizeBinaryArray array)
    {
        var result = new byte[array.Length][];
        for (int i = 0; i < array.Length; i++)
        {
            result[i] = array.IsNull(i) ? [] : array.GetBytes(i).ToArray();
        }
        return result;
    }
}
