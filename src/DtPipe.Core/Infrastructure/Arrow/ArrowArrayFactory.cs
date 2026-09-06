using Apache.Arrow;
using Apache.Arrow.Types;

namespace DtPipe.Core.Infrastructure.Arrow;

public static class ArrowArrayFactory
{
    public static IArrowArray Create(System.Array data, Type clrType, bool isNullable)
    {
        var logicalResult = ArrowTypeMapper.GetLogicalType(clrType);
        var builder = ArrowTypeMapper.CreateBuilder(logicalResult.ArrowType);
        var append = ArrowTypeMapper.ResolveAppender(builder);

        foreach (var val in data)
        {
            append(val);
        }

        return ArrowTypeMapper.BuildArray(builder);
    }
}
