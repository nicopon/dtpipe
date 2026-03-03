using DtPipe.Core.Abstractions;
using DtPipe.Core.Infrastructure.Arrow;
using Microsoft.Extensions.Logging;

namespace DtPipe.Adapters.Infrastructure.Arrow;

/// <summary>
/// Factory for ArrowRowToColumnarBridge.
/// </summary>
public sealed class ArrowRowToColumnarBridgeFactory(ILogger<ArrowRowToColumnarBridge> logger) : IRowToColumnarBridgeFactory
{
    public IRowToColumnarBridge CreateBridge()
    {
        return new ArrowRowToColumnarBridge(logger);
    }
}
