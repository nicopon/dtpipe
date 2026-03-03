using Apache.Arrow;
using Apache.Arrow.Types;

namespace DtPipe.Core.Infrastructure.Arrow;

public static class ArrowArrayFactory
{
    public static IArrowArray Create(System.Array data, Type clrType, bool isNullable)
    {
        var arrowType = ArrowTypeMapper.GetArrowType(clrType);
        var builder = ArrowTypeMapper.CreateBuilder(arrowType);

        foreach (var val in data)
        {
            ArrowTypeMapper.AppendValue(builder, val);
        }

        return ArrowTypeMapper.BuildArray(builder);
    }
}
