using Apache.Arrow;
using Apache.Arrow.Types;
using DtPipe.Core.Models;
using System.Linq;

namespace DtPipe.Core.Infrastructure.Arrow;

public static class ArrowSchemaFactory
{
    public static Schema Create(IReadOnlyList<PipeColumnInfo> columns)
    {
        var builder = new Schema.Builder();
        foreach (var col in columns)
        {
            // Use the centralized mapping logic that ensures all metadata (e.g. arrow.uuid) is consistently applied
            builder.Field(ArrowTypeMapper.GetField(col.Name, col.ClrType, col.IsNullable));
        }
        return builder.Build();
    }

    /// <summary>
    /// Builds the Arrow schema from <paramref name="columns"/> (PipeColumnInfo — the authoritative
    /// source for column types after the InitializeAsync chain), then enriches individual fields from
    /// <paramref name="richSchema"/> when the base Arrow type is compatible, e.g. to preserve
    /// Timestamp timezone, Decimal precision/scale, or arrow.uuid metadata. If a field's base
    /// Arrow type in <paramref name="richSchema"/> differs from the PipeColumnInfo-derived type
    /// (schema mutation by a transformer), the PipeColumnInfo-derived field is kept.
    /// </summary>
    public static Schema CreateEnriched(IReadOnlyList<PipeColumnInfo> columns, Schema richSchema)
    {
        var baseSchema = Create(columns);
        var fields = new List<Field>(baseSchema.FieldsList.Count);
        foreach (var baseField in baseSchema.FieldsList)
        {
            var richIdx = richSchema.GetFieldIndex(baseField.Name);
            if (richIdx >= 0)
            {
                var richField = richSchema.GetFieldByIndex(richIdx);
                // Accept the richer field only when the Arrow base type is the same C# class:
                // - TimestampType vs TimestampType (timezone enrichment): OK.
                // - StringType vs Int32Type (transformer mutation): NOT OK — keep PipeColumnInfo-derived field.
                fields.Add(richField.DataType.GetType() == baseField.DataType.GetType()
                    ? richField
                    : baseField);
            }
            else
            {
                fields.Add(baseField);
            }
        }
        return new Schema(fields, baseSchema.Metadata);
    }

    /// <summary>
    /// Returns true if the schema contains rich types (Struct, List, or Map).
    /// </summary>
    public static bool IsRichSchema(Schema schema)
        => schema.FieldsList.Any(f => f.DataType is StructType or ListType or MapType);

    public static IReadOnlyList<PipeColumnInfo> ToPipeColumns(Schema schema)
    {
        return schema.FieldsList.Select(f => new PipeColumnInfo(
            f.Name,
            ArrowTypeMapper.GetClrTypeFromField(f),
            f.IsNullable
        )).ToList();
    }
}
