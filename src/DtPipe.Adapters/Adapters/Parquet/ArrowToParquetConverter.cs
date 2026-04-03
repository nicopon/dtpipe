using Apache.Arrow;
using Apache.Arrow.Arrays;
using DtPipe.Core.Infrastructure.Arrow;
using Parquet.Data;
using Parquet.Schema;

namespace DtPipe.Adapters.Parquet;

public static class ArrowToParquetConverter
{
    public static DataColumn Convert(IArrowArray arrowArray, DataField dataField)
    {
        bool isGuidField = dataField.ClrNullableIfHasNullsType == typeof(Guid)
                        || dataField.ClrNullableIfHasNullsType == typeof(Guid?);
        return arrowArray switch
        {
            Int32Array a => new DataColumn(dataField, ExtractPrimitiveValues<int, Int32Array>(a)),
            Int64Array a => new DataColumn(dataField, ExtractPrimitiveValues<long, Int64Array>(a)),
            DoubleArray a => new DataColumn(dataField, ExtractPrimitiveValues<double, DoubleArray>(a)),
            FloatArray a => new DataColumn(dataField, ExtractPrimitiveValues<float, FloatArray>(a)),
            BooleanArray a => new DataColumn(dataField, ExtractBooleanValues(a)),
            StringArray a => new DataColumn(dataField, ExtractStringValues(a)),
            Decimal128Array a => new DataColumn(dataField, ExtractDecimalValues<Decimal128Array>(a)),
            Decimal256Array a => new DataColumn(dataField, ExtractDecimalValues<Decimal256Array>(a)),
            Date64Array a => new DataColumn(dataField, ExtractDate64Values(a)),
            // Timestamp: no-timezone fields are typed as DateTime — coerce DateTimeOffset to DateTime
            TimestampArray a when (Nullable.GetUnderlyingType(dataField.ClrNullableIfHasNullsType) ?? dataField.ClrNullableIfHasNullsType) == typeof(DateTime) =>
                new DataColumn(dataField, ExtractTimestampAsDateTimeValues(a)),
            TimestampArray a => new DataColumn(dataField, ExtractTimestampValues(a)),
            // FixedSizeBinaryArray(16) with arrow.uuid → DtPipe internal UUID format
            FixedSizeBinaryArray a when isGuidField => new DataColumn(dataField, ExtractGuidValuesFromFixed(a)),
            // BinaryArray legacy: kept for sources that still emit BinaryType for UUID
            BinaryArray a when isGuidField => new DataColumn(dataField, ExtractGuidValues(a)),
            BinaryArray a => new DataColumn(dataField, ExtractBinaryValues(a)),
            // FixedSizeBinaryArray without Guid field → generic binary bytes
            FixedSizeBinaryArray a => new DataColumn(dataField, ExtractFixedBinaryValues(a)),
            _ => throw new NotSupportedException($"Arrow array type {arrowArray.GetType().Name} is not supported for Parquet conversion yet.")
        };
    }

    private static byte[]?[] ExtractFixedBinaryValues(FixedSizeBinaryArray array)
    {
        var result = new byte[]?[array.Length];
        for (int i = 0; i < array.Length; i++)
            result[i] = array.IsNull(i) ? null : array.GetBytes(i).ToArray();
        return result;
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
}
