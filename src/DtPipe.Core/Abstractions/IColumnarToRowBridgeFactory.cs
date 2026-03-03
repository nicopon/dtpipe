using Apache.Arrow;

namespace DtPipe.Core.Abstractions;

/// <summary>
/// Factory to create IColumnarToRowBridge instances.
/// </summary>
public interface IColumnarToRowBridgeFactory
{
    IColumnarToRowBridge CreateBridge();
}
