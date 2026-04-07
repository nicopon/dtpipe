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
