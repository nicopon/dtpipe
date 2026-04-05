using Apache.Arrow;
using Apache.Arrow.Types;
using System;

namespace DtPipe.Core.Infrastructure.Arrow.Handlers;

internal class StructHandler : IArrowTypeHandler
{
    public bool CanHandle(IArrowType type) => type is StructType;
    public bool CanHandle(IArrowArrayBuilder builder) => builder is StructArrayManualBuilder;

    public IArrowArrayBuilder CreateBuilder(IArrowType type) => new StructArrayManualBuilder((StructType)type);

    public void AppendNull(IArrowArrayBuilder builder)
    {
        if (builder is StructArrayManualBuilder b) b.AppendNull();
    }

    public IArrowArray Build(IArrowArrayBuilder builder)
    {
        return builder is StructArrayManualBuilder b ? b.Build() : throw new InvalidOperationException();
    }

    public void AppendValue(IArrowArrayBuilder builder, object? value)
    {
        if (builder is StructArrayManualBuilder b) b.AppendValue(value);
    }
}

internal class ListHandler : IArrowTypeHandler
{
    public bool CanHandle(IArrowType type) => type is ListType;
    public bool CanHandle(IArrowArrayBuilder builder) => builder is ListArrayManualBuilder;

    public IArrowArrayBuilder CreateBuilder(IArrowType type) => new ListArrayManualBuilder((ListType)type);

    public void AppendNull(IArrowArrayBuilder builder)
    {
        if (builder is ListArrayManualBuilder b) b.AppendNull();
    }

    public IArrowArray Build(IArrowArrayBuilder builder)
    {
        return builder is ListArrayManualBuilder b ? b.Build() : throw new InvalidOperationException();
    }

    public void AppendValue(IArrowArrayBuilder builder, object? value)
    {
        if (builder is ListArrayManualBuilder b) b.AppendValue(value);
    }
}
