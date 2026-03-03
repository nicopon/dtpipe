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
            builder.Field(new Field(col.Name, ArrowTypeMapper.GetArrowType(col.ClrType), col.IsNullable));
        }
        return builder.Build();
    }
}
