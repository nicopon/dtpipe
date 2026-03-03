using DtPipe.Core.Abstractions;
using DtPipe.Core.Infrastructure.Arrow;

namespace DtPipe.Adapters.Infrastructure.Arrow;

/// <summary>
/// Factory for ColumnarToRowBridge.
/// </summary>
public sealed class ArrowColumnarToRowBridgeFactory : IColumnarToRowBridgeFactory
{
    public IColumnarToRowBridge CreateBridge()
    {
        return new ArrowColumnarToRowBridge();
    }
}
