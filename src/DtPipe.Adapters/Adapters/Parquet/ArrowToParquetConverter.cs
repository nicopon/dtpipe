using Apache.Arrow;
using Apache.Arrow.Arrays;
using DtPipe.Core.Infrastructure.Arrow;
using Parquet;
using Parquet.Data;
using Parquet.Schema;
using ParquetField = Parquet.Schema.Field;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace DtPipe.Adapters.Parquet;

public static class ArrowToParquetConverter
{
    public static Task WriteColumnAsync(ParquetRowGroupWriter writer, IArrowArray arrowArray, Apache.Arrow.Field arrowField, ParquetField field, CancellationToken ct = default)
    {
        if (field is ListField listField)
        {
            if (arrowArray is not ListArray listArray)
                throw new NotSupportedException(
                    $"Column '{field.Name}' is a Parquet list but the pipeline produced " +
                    $"{arrowArray.GetType().Name}. The schema and the data must agree.");

            return WriteListColumnAsync(writer, listArray, listField, ct);
        }

        var dataField = (DataField)field;

        // An extension type (bool8, a vendor's opaque type) is read as the CLR value its field names:
        // the declared column, fixed from that field, is what the storage type is written under.
        if (arrowField.HasMetadata && arrowField.Metadata.ContainsKey("ARROW:extension:name") && !IsUuid(arrowField))
        {
            var clr = ArrowTypeMapper.GetClrTypeFromField(arrowField);
            if (clr == typeof(bool))
                return writer.WriteAsync<bool>(dataField, ReadBools(arrowArray, arrowField).AsMemory(), cancellationToken: ct);
            if (clr == typeof(string))
                return writer.WriteAsync(dataField, (IReadOnlyCollection<string?>)ReadStrings(arrowArray, arrowField));
        }

        bool isGuidField = dataField.ClrNullableIfHasNullsType == typeof(Guid)
                        || dataField.ClrNullableIfHasNullsType == typeof(Guid?);
        return arrowArray switch
        {
            Int8Array a => writer.WriteAsync<sbyte>(dataField, ExtractPrimitiveValues<sbyte, Int8Array>(a).AsMemory(), cancellationToken: ct),
            Int16Array a => writer.WriteAsync<short>(dataField, ExtractPrimitiveValues<short, Int16Array>(a).AsMemory(), cancellationToken: ct),
            UInt8Array a => writer.WriteAsync<byte>(dataField, ExtractPrimitiveValues<byte, UInt8Array>(a).AsMemory(), cancellationToken: ct),
            UInt16Array a => writer.WriteAsync<ushort>(dataField, ExtractPrimitiveValues<ushort, UInt16Array>(a).AsMemory(), cancellationToken: ct),
            UInt32Array a => writer.WriteAsync<uint>(dataField, ExtractPrimitiveValues<uint, UInt32Array>(a).AsMemory(), cancellationToken: ct),
            UInt64Array a => writer.WriteAsync<ulong>(dataField, ExtractPrimitiveValues<ulong, UInt64Array>(a).AsMemory(), cancellationToken: ct),
            Date32Array a => writer.WriteAsync<DateOnly>(dataField, ExtractDateOnlyValues(a).AsMemory(), cancellationToken: ct),
            Time32Array a => writer.WriteAsync<long>(dataField, ExtractTimeMicros(a.Length, i => a.GetTime(i)).AsMemory(), cancellationToken: ct),
            Time64Array a => writer.WriteAsync<long>(dataField, ExtractTimeMicros(a.Length, i => a.GetTime(i)).AsMemory(), cancellationToken: ct),
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
            // An instant is written as its UTC time under an isAdjustedToUTC timestamp.
            TimestampArray a => writer.WriteAsync<DateTime>(dataField, ExtractTimestampAsUtcValues(a).AsMemory(), cancellationToken: ct),
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


    /// <summary>
    /// Writes an Arrow list column as a Parquet LIST, in the three-level encoding the format
    /// defines. Levels are emitted per row, values only for present elements:
    /// <list type="bullet">
    /// <item>definition 0 — the list itself is NULL</item>
    /// <item>definition 1 — the list is present but empty</item>
    /// <item>definition <c>MaxDefinitionLevel - 1</c> — the element is NULL</item>
    /// <item>definition <c>MaxDefinitionLevel</c> — the element holds a value</item>
    /// <item>repetition 0 opens a row, 1 continues the current list</item>
    /// </list>
    /// The encoding was read back from a file DuckDB wrote, not inferred: a wrong level silently
    /// reshapes the data instead of failing.
    /// </summary>
    private static Task WriteListColumnAsync(ParquetRowGroupWriter writer, ListArray listArray, ListField listField, CancellationToken ct)
    {
        var itemField = (DataField)listField.Item;
        int maxDef = itemField.MaxDefinitionLevel;

        var definitions = new List<int>(listArray.Length);
        var repetitions = new List<int>(listArray.Length);
        var valueIndices = new List<int>(listArray.Values.Length);

        for (int row = 0; row < listArray.Length; row++)
        {
            if (listArray.IsNull(row))
            {
                definitions.Add(0);
                repetitions.Add(0);
                continue;
            }

            int start = listArray.ValueOffsets[row];
            int end = listArray.ValueOffsets[row + 1];

            if (start == end)
            {
                definitions.Add(1);
                repetitions.Add(0);
                continue;
            }

            for (int i = start; i < end; i++)
            {
                definitions.Add(listArray.Values.IsNull(i) ? maxDef - 1 : maxDef);
                repetitions.Add(i == start ? 0 : 1);
                if (!listArray.Values.IsNull(i)) valueIndices.Add(i);
            }
        }

        return WriteListValuesAsync(writer, itemField, listArray.Values, valueIndices,
            definitions.ToArray(), repetitions.ToArray(), ct);
    }

    private static Task WriteListValuesAsync(
        ParquetRowGroupWriter writer, DataField itemField, IArrowArray values,
        List<int> indices, int[] definitions, int[] repetitions, CancellationToken ct)
    {
        var defs = (ReadOnlyMemory<int>?)definitions.AsMemory();
        var reps = (ReadOnlyMemory<int>?)repetitions.AsMemory();

        return values switch
        {
            Int32Array a => writer.WriteAllPartsAsync<int>(itemField, Gather<int>(indices, a.GetValue).AsMemory(), defs, reps, ct),
            Int64Array a => writer.WriteAllPartsAsync<long>(itemField, Gather<long>(indices, a.GetValue).AsMemory(), defs, reps, ct),
            Int16Array a => writer.WriteAllPartsAsync<short>(itemField, Gather<short>(indices, a.GetValue).AsMemory(), defs, reps, ct),
            DoubleArray a => writer.WriteAllPartsAsync<double>(itemField, Gather<double>(indices, a.GetValue).AsMemory(), defs, reps, ct),
            FloatArray a => writer.WriteAllPartsAsync<float>(itemField, Gather<float>(indices, a.GetValue).AsMemory(), defs, reps, ct),
            BooleanArray a => writer.WriteAllPartsAsync<bool>(itemField, Gather<bool>(indices, a.GetValue).AsMemory(), defs, reps, ct),
            Decimal128Array a => writer.WriteAllPartsAsync<decimal>(itemField, Gather<decimal>(indices, a.GetValue).AsMemory(), defs, reps, ct),
            // Parquet.Net exposes definition levels only through WriteAllPartsAsync, whose T is
            // constrained to value types. The string overload carries repetition levels alone,
            // which cannot tell a NULL list from an empty one — so a text list is refused rather
            // than written with a null/empty distinction we could not honour.
            StringArray => throw new NotSupportedException(
                $"Parquet column '{itemField.Path}': lists of text are not supported yet. " +
                "Project the column in your query, for example with array_to_json(col)::text."),

            _ => throw new NotSupportedException(
                $"Parquet list of {values.GetType().Name} is not supported. " +
                "Cast the column in your query, for example to a list of a numeric type.")
        };
    }

    /// <summary>Present values only, in list order — nulls are carried by the definition levels.</summary>
    private static T[] Gather<T>(List<int> indices, Func<int, T?> get) where T : struct
    {
        var result = new T[indices.Count];
        for (int i = 0; i < indices.Count; i++) result[i] = get(indices[i])!.Value;
        return result;
    }

    private static bool IsUuid(Apache.Arrow.Field field)
        => field.Metadata.TryGetValue("ARROW:extension:name", out var name) && name == "arrow.uuid";

    private static bool?[] ReadBools(IArrowArray array, Apache.Arrow.Field field)
    {
        var result = new bool?[array.Length];
        for (int i = 0; i < array.Length; i++)
            result[i] = (bool?)ArrowTypeMapper.GetValueForField(array, field, i);
        return result;
    }

    private static string?[] ReadStrings(IArrowArray array, Apache.Arrow.Field field)
    {
        var result = new string?[array.Length];
        for (int i = 0; i < array.Length; i++)
            result[i] = (string?)ArrowTypeMapper.GetValueForField(array, field, i);
        return result;
    }

    private static DateOnly?[] ExtractDateOnlyValues(Date32Array array)
    {
        var result = new DateOnly?[array.Length];
        for (int i = 0; i < array.Length; i++)
            result[i] = array.GetDateOnly(i);
        return result;
    }

    /// <summary>Microseconds since midnight, the unit of the TIME(MICROS) field the schema declares.</summary>
    private static long?[] ExtractTimeMicros(int length, Func<int, TimeOnly?> get)
    {
        var result = new long?[length];
        for (int i = 0; i < length; i++)
            result[i] = get(i) is TimeOnly t ? t.Ticks / TimeSpan.TicksPerMicrosecond : null;
        return result;
    }

    private static DateTime?[] ExtractTimestampAsUtcValues(TimestampArray array)
    {
        var result = new DateTime?[array.Length];
        for (int i = 0; i < array.Length; i++)
            result[i] = array.GetTimestamp(i)?.UtcDateTime;
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
