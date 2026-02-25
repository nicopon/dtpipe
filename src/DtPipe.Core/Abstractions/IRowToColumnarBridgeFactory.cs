namespace DtPipe.Core.Abstractions;

/// <summary>
/// Factory to create IRowToColumnarBridge instances.
/// </summary>
public interface IRowToColumnarBridgeFactory
{
    IRowToColumnarBridge CreateBridge();
}
