using Apache.Arrow;
using Apache.Arrow.Serialization.Mapping;
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
            builder.Field(ArrowTypeMapper.GetField(col.Name, col.ClrType, col.IsNullable, col.Precision, col.Scale));
        }
        return builder.Build();
    }

    /// <summary>
    /// Builds the Arrow schema from <paramref name="columns"/> (PipeColumnInfo — the authoritative
    /// source for column types after the InitializeAsync chain), then enriches individual fields from
    /// <paramref name="richSchema"/> when the base Arrow type is compatible, e.g. to preserve
    /// Timestamp timezone, Decimal precision/scale, or arrow.uuid metadata. If a field's base
    /// Arrow type in <paramref name="richSchema"/> differs from the PipeColumnInfo-derived type
    /// (schema mutation by a transformer), the PipeColumnInfo-derived field is kept. A column the CLR type
    /// cannot describe (a struct has no Arrow form of its own, a type with no reading reads as text) keeps
    /// the field it arrived in, so a writer's gate answers for the type the column really has.
    /// </summary>
    public static Schema CreateEnriched(IReadOnlyList<PipeColumnInfo> columns, Schema richSchema)
    {
        var fields = new List<Field>(columns.Count);
        foreach (var column in columns)
        {
            var richIdx = richSchema.GetFieldIndex(column.Name);
            var richField = richIdx >= 0 ? richSchema.GetFieldByIndex(richIdx) : null;

            if (richField != null
                && ArrowTypeMapper.GetClrTypeFromField(richField) == column.ClrType
                && (!ArrowTypeMapper.TryGetLogicalType(column.ClrType, out _) || !ArrowTypeMap.HasValueReading(richField.DataType)))
            {
                fields.Add(richField);
                continue;
            }

            var baseField = ArrowTypeMapper.GetField(column.Name, column.ClrType, column.IsNullable, column.Precision, column.Scale);
            // Accept the richer field only when the Arrow base type is the same C# class:
            // - TimestampType vs TimestampType (timezone enrichment): OK.
            // - StringType vs Int32Type (transformer mutation): NOT OK — keep PipeColumnInfo-derived field.
            fields.Add(richField != null && richField.DataType.GetType() == baseField.DataType.GetType()
                ? richField
                : baseField);
        }
        return new Schema(fields, null);
    }

    /// <summary>The fields of <paramref name="schema"/> whose CLR type has no Arrow form of its own, a struct's or a map's.</summary>
    public static Schema FieldsWithNoClrForm(Schema schema)
        => new(schema.FieldsList.Where(f => !ArrowTypeMapper.TryGetLogicalType(ArrowTypeMapper.GetClrTypeFromField(f), out _)), null);

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
            f.IsNullable,
            Precision: DeclaredPrecision(f),
            Scale: DeclaredScale(f)
        )).ToList();
    }

    /// <summary>
    /// Precision an Arrow decimal declares, so a column that goes Schema → PipeColumnInfo →
    /// Schema comes back at the width it left with rather than at the default. A decimal marked
    /// undeclared reports none: the default width it carries is not something the source said.
    /// </summary>
    private static int? DeclaredPrecision(Field f) => ArrowTypeMap.IsUndeclaredDecimal(f) ? null : f.DataType switch
    {
        Decimal128Type d => d.Precision,
        Decimal256Type d => d.Precision,
        _ => null
    };

    /// <inheritdoc cref="DeclaredPrecision"/>
    private static int? DeclaredScale(Field f) => ArrowTypeMap.IsUndeclaredDecimal(f) ? null : f.DataType switch
    {
        Decimal128Type d => d.Scale,
        Decimal256Type d => d.Scale,
        _ => null
    };
}
