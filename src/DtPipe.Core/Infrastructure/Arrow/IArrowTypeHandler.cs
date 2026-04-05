using Apache.Arrow;
using Apache.Arrow.Types;

namespace DtPipe.Core.Infrastructure.Arrow;

internal interface IArrowTypeHandler
{
    bool CanHandle(IArrowType type);
    bool CanHandle(IArrowArrayBuilder builder);
    IArrowArrayBuilder CreateBuilder(IArrowType type);
    void AppendNull(IArrowArrayBuilder builder);
    IArrowArray Build(IArrowArrayBuilder builder);
    void AppendValue(IArrowArrayBuilder builder, object? value);
}
