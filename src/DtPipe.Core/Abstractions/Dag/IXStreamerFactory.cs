using DtPipe.Core.Abstractions;
using DtPipe.Core.Pipelines.Dag;

namespace DtPipe.Core.Abstractions.Dag;

/// <summary>
/// A factory for creating XStreamers. An XStreamer is a component capable
/// of mixing multiple input data streams into a single output stream.
/// </summary>
public interface IXStreamerFactory : IDataFactory
{
    /// <summary>
    /// Creates a new instance of a stream reader that acts as the XStreamer.
    /// It configures itself by reading upstream channels from the registry.
    /// </summary>
    IStreamReader CreateXStreamer(IMemoryChannelRegistry registry, BranchDefinition config);
}
