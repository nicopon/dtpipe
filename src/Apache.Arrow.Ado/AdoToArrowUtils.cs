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
    public static Apache.Arrow.Serialization.Mapping.ArrowTypeResult GetLogicalTypeFromDbColumn(DbColumn column)
    {
        var dataType = column.DataType ?? typeof(string);
        var underlyingType = Nullable.GetUnderlyingType(dataType) ?? dataType;

        // Dates and Times — use DataTypeName for DATE vs DATETIME distinction
        if (underlyingType == typeof(DateTime))
        {
            if (string.Equals(column.DataTypeName, "DATE", StringComparison.OrdinalIgnoreCase))
            {
                return new Apache.Arrow.Serialization.Mapping.ArrowTypeResult(Date32Type.Default);
            }
            return new Apache.Arrow.Serialization.Mapping.ArrowTypeResult(Date64Type.Default);
        }

        if (underlyingType == typeof(TimeSpan) && string.Equals(column.DataTypeName, "TIME", StringComparison.OrdinalIgnoreCase))
        {
            return new Apache.Arrow.Serialization.Mapping.ArrowTypeResult(new Time32Type(TimeUnit.Millisecond));
        }

        // Decimals — use actual precision/scale from schema
        if (underlyingType == typeof(decimal))
        {
            int precision = Math.Max(1, column.NumericPrecision ?? 38);
            int scale = Math.Max(0, column.NumericScale ?? 18);
            return new Apache.Arrow.Serialization.Mapping.ArrowTypeResult(
                precision <= 38
                    ? (IArrowType)new Decimal128Type(precision, scale)
                    : new Decimal256Type(Math.Min(76, precision), scale)
            );
        }

        // Fallback to central mapping for primitives (int, GUID with its UUID metadata, etc.)
        try
        {
            return Apache.Arrow.Serialization.Mapping.ArrowTypeMap.GetLogicalType(underlyingType);
        }
        catch (NotSupportedException)
        {
            return new Apache.Arrow.Serialization.Mapping.ArrowTypeResult(StringType.Default);
        }
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
            var logicalType = config.TypeResolver(col);
            var isNullable = col.AllowDBNull ?? true;
            var name = string.IsNullOrEmpty(col.ColumnName) ? $"Column{i}" : col.ColumnName;

            IReadOnlyDictionary<string, string>? baseMetadata = logicalType.Metadata;

            IReadOnlyDictionary<string, string>? finalMetadata = baseMetadata;
            if (config.IncludeMetadata)
            {
                var metaDict = baseMetadata != null 
                    ? new Dictionary<string, string>(baseMetadata) 
                    : new Dictionary<string, string>();
                if (!string.IsNullOrEmpty(col.DataTypeName)) metaDict["ado:DataTypeName"] = col.DataTypeName;
                if (col.ColumnSize.HasValue) metaDict["ado:ColumnSize"] = col.ColumnSize.Value.ToString();
                if (col.NumericPrecision.HasValue) metaDict["ado:NumericPrecision"] = col.NumericPrecision.Value.ToString();
                if (col.NumericScale.HasValue) metaDict["ado:NumericScale"] = col.NumericScale.Value.ToString();
                if (metaDict.Count > 0) finalMetadata = metaDict;
            }

            fields[i] = new Field(name, logicalType.ArrowType, isNullable,
                finalMetadata?.Select(kvp => new KeyValuePair<string, string>(kvp.Key, kvp.Value)));
        }

        return new Schema(fields, null);
    }
}
