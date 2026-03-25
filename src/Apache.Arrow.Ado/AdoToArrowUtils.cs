using System;
using System.Data.Common;
using System.Collections.Generic;
using Apache.Arrow.Types;

namespace Apache.Arrow.Ado;

/// <summary>
/// Utility methods for converting ADO.NET objects to Arrow objects.
/// </summary>
public static class AdoToArrowUtils
{
    /// <summary>
    /// Infers the Arrow type from the ADO.NET <see cref="DbColumn"/> metadata.
    /// This is the default type resolver used when no custom resolver is configured.
    /// Falls back to <see cref="StringType"/> for unknown or unsupported types.
    /// </summary>
    public static IArrowType GetArrowTypeFromDbColumn(DbColumn column)
    {
        var dataType = column.DataType ?? typeof(string);
        var underlyingType = Nullable.GetUnderlyingType(dataType) ?? dataType;

        if (underlyingType == typeof(int)) return Int32Type.Default;
        if (underlyingType == typeof(long)) return Int64Type.Default;
        if (underlyingType == typeof(short)) return Int16Type.Default;
        if (underlyingType == typeof(sbyte)) return Int8Type.Default;
        if (underlyingType == typeof(byte)) return UInt8Type.Default;
        if (underlyingType == typeof(ushort)) return UInt16Type.Default;
        if (underlyingType == typeof(uint)) return UInt32Type.Default;
        if (underlyingType == typeof(ulong)) return UInt64Type.Default;
        if (underlyingType == typeof(double)) return DoubleType.Default;
        if (underlyingType == typeof(float)) return FloatType.Default;
        if (underlyingType == typeof(bool)) return BooleanType.Default;
        if (underlyingType == typeof(string)) return StringType.Default;
        if (underlyingType == typeof(byte[])) return BinaryType.Default;

        // Guid: map to string for maximum interoperability (matches Java arrow-jdbc behavior)
        if (underlyingType == typeof(Guid)) return StringType.Default;

        // Dates and Times — use DataTypeName for DATE vs DATETIME distinction
        if (underlyingType == typeof(DateTime))
        {
            return string.Equals(column.DataTypeName, "DATE", StringComparison.OrdinalIgnoreCase)
                ? (IArrowType)Date32Type.Default
                : Date64Type.Default;
        }

        if (underlyingType == typeof(DateTimeOffset))
            return TimestampType.Default;

        if (underlyingType == typeof(TimeSpan))
        {
            return string.Equals(column.DataTypeName, "TIME", StringComparison.OrdinalIgnoreCase)
                ? (IArrowType)new Time32Type(TimeUnit.Millisecond)
                : DurationType.Millisecond;
        }

        // Decimals — use actual precision/scale from schema
        if (underlyingType == typeof(decimal))
        {
            int precision = Math.Max(1, column.NumericPrecision ?? 38);
            int scale = Math.Max(0, column.NumericScale ?? 18);
            return precision <= 38
                ? (IArrowType)new Decimal128Type(precision, scale)
                : new Decimal256Type(Math.Min(76, precision), scale);
        }

        return StringType.Default;
    }

    /// <summary>
    /// Creates an Arrow <see cref="Schema"/> from a <see cref="System.Data.Common.DbDataReader"/>'s column schema,
    /// using the type resolver from <paramref name="config"/>.
    /// </summary>
    public static Schema CreateSchema(System.Data.Common.DbDataReader reader, AdoToArrowConfig config)
    {
        var columns = reader.GetColumnSchema();
        var numColumns = columns.Count;
        var fields = new Field[numColumns];

        for (int i = 0; i < numColumns; i++)
        {
            var col = columns[i];
            var arrowType = config.TypeResolver(col);
            var isNullable = col.AllowDBNull ?? true;
            var name = string.IsNullOrEmpty(col.ColumnName) ? $"Column{i}" : col.ColumnName;

            IReadOnlyDictionary<string, string>? metadata = null;
            if (config.IncludeMetadata)
            {
                var metaDict = new Dictionary<string, string>();
                if (!string.IsNullOrEmpty(col.DataTypeName)) metaDict["ado:DataTypeName"] = col.DataTypeName;
                if (col.ColumnSize.HasValue) metaDict["ado:ColumnSize"] = col.ColumnSize.Value.ToString();
                if (col.NumericPrecision.HasValue) metaDict["ado:NumericPrecision"] = col.NumericPrecision.Value.ToString();
                if (col.NumericScale.HasValue) metaDict["ado:NumericScale"] = col.NumericScale.Value.ToString();
                if (metaDict.Count > 0) metadata = metaDict;
            }

            fields[i] = new Field(name, arrowType, isNullable,
                metadata?.Select(kvp => new KeyValuePair<string, string>(kvp.Key, kvp.Value)));
        }

        return new Schema(fields, null);
    }
}
