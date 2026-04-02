using Apache.Arrow;
using Apache.Arrow.Types;
using DtPipe.Core.Models;

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
}
