using Apache.Arrow;
using Apache.Arrow.Types;

namespace DtPipe.Core.Infrastructure.Arrow;

public static class ArrowArrayFactory
{
    /// <summary>
    /// Builds one Arrow array from a decoded CLR column. <c>precision</c> and <c>scale</c> are
    /// what the source declares about a numeric column, null when it declares nothing.
    /// </summary>
    /// <remarks>
    /// A caller that passes a precision and a scale here must give the same pair to
    /// <see cref="ArrowSchemaFactory.Create"/>: the array and the field that describes it have to
    /// carry one Decimal128 type, or the RecordBatch built from them disagrees with its schema.
    /// </remarks>
    public static IArrowArray Create(System.Array data, Type clrType, bool isNullable, int? precision = null, int? scale = null)
    {
        var logicalResult = ArrowTypeMapper.GetLogicalType(clrType, precision, scale);
        var builder = ArrowTypeMapper.CreateBuilder(logicalResult.ArrowType);
        var append = ArrowTypeMapper.ResolveAppender(builder);

        foreach (var val in data)
        {
            append(val);
        }

        return ArrowTypeMapper.BuildArray(builder);
    }
}
